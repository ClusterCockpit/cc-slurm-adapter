package cc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/config"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/slurm/common"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/trace"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/types"

	"github.com/nats-io/nats.go"

	"github.com/ClusterCockpit/cc-lib/ccMessage"
	"github.com/ClusterCockpit/cc-lib/schema"
)

type CacheJobState struct {
	CacheEvictAge int
	Stale         bool
	Running       bool
}

const CACHE_EVICT_COUNT int = 5

type CCApi struct {
	hostname   string
	natsConn   *nats.Conn
	httpClient http.Client

	// map['clusterName'] -> map[slurmId] -> CC Job State, which are currently running (or ran recently).
	// When a jobs is started, they are inserted into the map. When they are stopped
	// they are set as not running. They are eventually removed again (to avoid leaking memory),
	// but only after CachEvictCountdown as gone down to zero. We do not remove the immediately
	// to avoid the situation where e.g. a poll event stops a job, and the corresponding PrEp stop event
	// won't find the job in the cache anymore. In that case, a stop_job would be sent, even the stop would already have stopped.
	JobCache      map[string]map[int64]*CacheJobState
	JobCacheValid bool
	JobCacheDate  time.Time

	slurmApi slurm_common.SlurmApi
}

func NewCCApi(slurmApi slurm_common.SlurmApi) (*CCApi, error) {
	ccApi := &CCApi{
		slurmApi: slurmApi,
	}

	// Init HTTP client
	tr := &http.Transport{
		MaxIdleConns:    10,
		IdleConnTimeout: 2 * time.Duration(config.Config.SlurmPollInterval) * time.Second,
	}
	ccApi.httpClient = http.Client{Transport: tr}

	var err error
	ccApi.hostname, err = os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Unable to obtain hostname: %w", err)
	}

	// Init NATS client
	options := make([]nats.Option, 0)
	if len(config.Config.NatsUser) > 0 {
		options = append(options, nats.UserInfo(config.Config.NatsUser, config.Config.NatsPassword))
	}
	if len(config.Config.NatsCredsFile) > 0 {
		options = append(options, nats.UserCredentials(config.Config.NatsCredsFile))
	}
	if len(config.Config.NatsNKeySeedFile) > 0 {
		r, err := nats.NkeyOptionFromSeed(config.Config.NatsNKeySeedFile)
		if err != nil {
			return nil, fmt.Errorf("Unable to open NKeySeedFile: %w", err)
		}
		options = append(options, r)
	}
	if len(config.Config.NatsServer) > 0 {
		natsAddr := fmt.Sprintf("nats://%s:%d", config.Config.NatsServer, config.Config.NatsPort)
		trace.Info("Connecting to NATS: %s", natsAddr)
		ccApi.natsConn, err = nats.Connect(natsAddr, options...)
		if err != nil {
			return nil, fmt.Errorf("Unable to connect to NATS (server: %s): %w", natsAddr, err)
		}
	}

	return ccApi, nil
}

func (api *CCApi) Close() {
	trace.Debug("Closing HTTP connections")
	api.httpClient.CloseIdleConnections()
	trace.Debug("Closing NATS")
	if api.natsConn != nil {
		api.natsConn.Close()
		api.natsConn = nil
	}
}

func (api *CCApi) CacheUpdate() error {
	// We maintain a local cache of which jobs are marked as running in cc-backend.
	// Only refresh it if the cache was invalidated or if it wasn't refreshed for some time.
	if api.JobCacheValid && api.JobCacheDate.Add(time.Duration(config.Config.CcPollInterval)*time.Second).After(time.Now()) {
		return nil
	}

	respJobs, err := api.ccGet("/jobs/?state=running&items-per-page=9999999&page=1&with-metadata=false")
	if err != nil {
		api.JobCacheValid = false
		return fmt.Errorf("Unable to GET running jobs from cc-backend: %w", err)
	}

	defer respJobs.Body.Close()
	body, err := io.ReadAll(respJobs.Body)
	if err != err {
		return fmt.Errorf("Unable to GET running jobs from cc-backend: %w", err)
	}

	if respJobs.StatusCode != 200 {
		return fmt.Errorf("Calling /jobs/ failed with HTTP %d: %s", respJobs.StatusCode, string(body))
	}

	getJobsApiResponse := struct {
		Jobs  []*schema.Job `json:"jobs"`
		Items int           `json:"items"`
		Page  int           `json:"page"`
	}{}

	err = json.Unmarshal(body, &getJobsApiResponse)
	if err != nil {
		return fmt.Errorf("Error in JSON returned from cc-backend: %w, JSON: %s", err, body)
	}

	// init cache if not done so yet
	initial := false
	if api.JobCache == nil {
		api.JobCache = make(map[string]map[int64]*CacheJobState)
		for _, cluster := range api.slurmApi.GetClusterNames() {
			api.JobCache[cluster] = make(map[int64]*CacheJobState)
		}
		initial = true
		trace.Info("Running initial cc-backend <---> cc-slurm-adapter synchronization. This may take a while.")
	}

	// Create mapping from cluster -> jobid -> job from cc-backend data.
	// Compare this mapping to our current cache to detect, if there have been differences.
	ccJobState := make(map[string]map[int64]*schema.Job)
	for _, job := range getJobsApiResponse.Jobs {
		if ccJobState[job.Cluster] == nil {
			ccJobState[job.Cluster] = make(map[int64]*schema.Job)
		}
		ccJobState[job.Cluster][job.JobID] = job
	}

	// Now compare new cc-backend state --> our old cc-backend state
	ccJobCount := 0
	for _, ccJobClusterState := range ccJobState {
		for _, ccJob := range ccJobClusterState {
			if string(ccJob.State) != "running" {
				trace.Warn("cc-backend REST API returned job, which isn't running, even though we only asked for running jobs. Ignoring cc-job (i.e. not Slurm job) %d", ccJob.ID)
				continue
			}

			if api.JobCache[ccJob.Cluster] == nil {
				// Skip jobs from cc-backend, which do not belong to the clusters that we manage.
				// It may be nicer to not request them in the first place, but it's easier for now...
				continue
			}

			ccJobCount += 1

			cacheJob, ok := api.JobCache[ccJob.Cluster][ccJob.JobID]
			if ok && !cacheJob.Running {
				// All jobs fetched are assumed to be running, so do not allow that cache state.
				ok = false
			}

			if ok {
				continue
			}

			if !initial {
				trace.Warn("Cache desync detected! Fetching running job (%s, %d) from cc-backend to cache.", ccJob.Cluster, ccJob.JobID)
			}

			slurmJob, err := api.slurmApi.QueryJob(ccJob.Cluster, uint32(ccJob.JobID))
			if err != nil {
				trace.Error("Unable to correct desync. Slurm failed to query job (%s, %d): %v", ccJob.Cluster, ccJob.JobID, err)
				continue
			}

			api.JobCache[ccJob.Cluster][ccJob.JobID] = &CacheJobState{
				Running: true,
			}

			err = api.SyncJob(slurmJob, true)
			if err != nil {
				trace.Error("Unable to correct desync (state may be inconsistent now!). Sync to cc-backend failed: %v", err)
			}
		}
	}

	// ... and now new cc-backend state <-- out old cc-backend state
	for cluster, ccJobClusterCache := range api.JobCache {
		for jobId, cacheJob := range ccJobClusterCache {
			_, ok := ccJobState[cluster][jobId]
			if !ok && !cacheJob.Running {
				// If our local job state is not set to 'running' anymore, it's okay if cc-backend doesn't know this job.
				ok = true
			}

			if ok {
				continue
			}

			if !initial {
				trace.Warn("Cache desync detected! Resetting missing/stopped job (%s, %d) from cc-backend in cache.", cluster, jobId)
			}

			slurmJob, err := api.slurmApi.QueryJob(cluster, uint32(jobId))
			if err != nil {
				trace.Error("Unable to correct desync. Slurm failed to query job (%s, %d): %v", cluster, jobId, err)
				continue
			}

			cacheJob.CacheEvictAge = 0
			cacheJob.Running = false

			err = api.SyncJob(slurmJob, true)
			if err != nil {
				trace.Error("Unable to correct desync (state may be inconsistent now!). Sync to cc-backend failed: %v", err)
			}
		}
	}

	api.JobCacheDate = time.Now()
	api.JobCacheValid = true

	trace.Info("CC Job Cache updated. Number of running jobs: %d", ccJobCount)
	return nil
}

func (api *CCApi) CacheGC() {
	for _, cachedJobStates := range api.JobCache {
		for jobId, cachedJobState := range cachedJobStates {
			if cachedJobState.Running {
				cachedJobState.CacheEvictAge = 0
				continue
			}

			cachedJobState.CacheEvictAge += 1
			if cachedJobState.CacheEvictAge > CACHE_EVICT_COUNT {
				delete(cachedJobStates, jobId)
			}
		}
	}
}

func (api *CCApi) SyncJob(job slurm_common.SacctJob, force bool) error {
	startJobData, err := api.slurmApi.JobToCCStartJob(job)
	if err != nil {
		return err
	}

	if !force && checkIgnoreJob(job, startJobData) {
		return nil
	}

	err = api.StartJob(job, startJobData)
	if err != nil {
		return err
	}

	// TODO If the job already exists in cc-backend, make sure to update values if interested, which may have changed.
	// In the future, this could be used to implement updating of job time limits, etc.
	// At the moment, we can't do this yet, because cc-backend doesn't have an API endpoint to alter an existing job.

	// Only submit stop job, if it has actually finished
	if !job.IsFinished() {
		return nil
	}

	err = api.StopJob(job)
	if err != nil {
		return err
	}

	api.JobCache[job.GetCluster()][job.GetJobId()].Stale = false
	return nil
}

func (api *CCApi) StartJob(job slurm_common.SacctJob, startJobData *types.CCStartJobRequest) error {
	cluster := job.GetCluster()
	jobId := job.GetJobId()

	_, jobKnown := api.JobCache[cluster][jobId]
	if jobKnown {
		return nil
	}

	startJobDataJSON, err := json.Marshal(startJobData)
	if err != nil {
		return fmt.Errorf("Unable to convert StartJobRequest to JSON: %w", err)
	}

	var respStart *http.Response
	if config.Config.CcRestSubmitJobs {
		respStart, err = api.ccPost("/jobs/start_job/", startJobDataJSON)
		if err != nil {
			return err
		}

		defer respStart.Body.Close()

		body, err := io.ReadAll(respStart.Body)
		if err != nil {
			return err
		}

		if respStart.StatusCode != 201 && respStart.StatusCode != 422 {
			// If the POST is not successful raise an error.
			return fmt.Errorf("Calling /jobs/start_job/ (%s, %d) failed with HTTP %d: Body %s", cluster, jobId, respStart.StatusCode, string(body))
		}
	}

	// Status Code 201 -> the job was newly created
	// Status Code 422 -> the job already existed
	// If job submission via REST is disabled, unconditionally send NATS message
	if (!config.Config.CcRestSubmitJobs || respStart.StatusCode == 201) && api.natsConn != nil {
		trace.Info("Sent start_job successfully (%s, %d)", cluster, jobId)
		tags := map[string]string{
			"hostname": api.hostname,
			"type":     "node",
			"type-id":  "0",
			"function": "start_job",
		}
		msg, err := ccmessage.NewEvent("job", tags, nil, string(startJobDataJSON), time.Unix(startJobData.StartTime, 0))
		if err != nil {
			trace.Warn("ccmessage.NewEvent() failed for job (%s, %d) failed: %s", cluster, jobId, err)
		} else {
			err = api.natsConn.Publish(config.Config.NatsSubject, []byte(msg.ToLineProtocol(nil)))
			if err != nil {
				trace.Warn("Unable to publish message on NATS for job (%s, %d): %s", cluster, jobId, err)
			}
		}

		if !jobHasResources(schema.Job(*startJobData)) {
			// This should only happen if we resynchronize a job, after is has already stopped for some time.
			trace.Warn("Unable to obtain Job (%s, %d) with hwthread/accelerator information. Some metrics may be missing in ClusterCockpit.", job.GetCluster(), job.GetJobId())
		}
	}

	api.JobCache[job.GetCluster()][job.GetJobId()] = &CacheJobState{
		Running: true,
	}
	return nil
}

func (api *CCApi) StopJob(job slurm_common.SacctJob) error {
	cluster := job.GetCluster()
	jobId := job.GetJobId()

	cachedJobState, jobKnown := api.JobCache[cluster][jobId]
	if jobKnown && !cachedJobState.Running {
		return nil
	}

	stopJobData, err := api.slurmApi.JobToCCStopJob(job)
	if err != nil {
		return err
	}

	stopJobDataJSON, err := json.Marshal(stopJobData)
	if err != nil {
		return fmt.Errorf("Unable to convert StopJobRequest to JSON: %w", err)
	}

	var respStop *http.Response
	if config.Config.CcRestSubmitJobs {
		respStop, err = api.ccPost("/jobs/stop_job/", stopJobDataJSON)
		if err != nil {
			return err
		}

		defer respStop.Body.Close()
		body, err := io.ReadAll(respStop.Body)
		if err != nil {
			return err
		}

		if respStop.StatusCode != 200 && respStop.StatusCode != 422 {
			return fmt.Errorf("Calling /jobs/stop_job/ (cluster=%s jobid=%d known=%v running=%v) failed with HTTP %d: Body %s", cluster, jobId, jobKnown, cachedJobState.Running, respStop.StatusCode, string(body))
		}

		if respStop.StatusCode == 422 {
			// While it should usually not occur a 422 (i.e. job was already stopped),
			// this may still occur if something in the state was glitched.
			trace.Warn("Calling /jobs/stop_job/ (cluster=%s jobid=%d) failed with HTTP 422 (non-fatal): Body %s", cluster, jobId, string(body))
		}
	}

	if (!config.Config.CcRestSubmitJobs || respStop.StatusCode == 200) && api.natsConn != nil {
		trace.Info("Sent stop_job successfully (%s, %d)", cluster, jobId)
		tags := map[string]string{
			"hostname": api.hostname,
			"type":     "node",
			"type-id":  "0",
			"function": "stop_job",
		}
		msg, err := ccmessage.NewEvent("job", tags, nil, string(stopJobDataJSON), time.Unix(stopJobData.StopTime, 0))
		if err != nil {
			trace.Warn("ccmessage.NewEvent() failed for job (%s, %d) failed: %s", cluster, jobId, err)
		} else {
			err = api.natsConn.Publish(config.Config.NatsSubject, []byte(msg.ToLineProtocol(nil)))
			if err != nil {
				trace.Warn("Unable to publish message on NATS for job (%s, %d): %s", cluster, jobId, err)
			}
		}
	}

	if jobKnown {
		cachedJobState.Running = false
	} else {
		// I can't imagine that this case occurs in practice, but who knows...
		trace.Warn("Stopping a job (%s, %d), which is known by cc-backend but not in our cache. Did we miss a job start event?", cluster, jobId)
		api.JobCache[cluster][jobId] = &CacheJobState{
			Running: false,
		}
	}
	return nil
}

func (api *CCApi) SyncStats() error {
	//slurmStateToCCSate := make(map[string]string)

	for _, cluster := range api.slurmApi.GetClusterNames() {
		// Obtain various cluster stats like used CPUs, GPUs, etc.
		ccNodeStats, err := api.slurmApi.QueryNodeStats(cluster)
		if err != nil {
			return err
		}

		if ccNodeStats == nil {
			continue
		}

		request := types.CCNodeStatRequest{
			Cluster: cluster,
			Nodes:   ccNodeStats,
		}

		nodeStateDataJSON, err := json.Marshal(request)
		if err != nil {
			return fmt.Errorf("Unable to convert NodeState to JSON: %w", err)
		}

		respNodeState, err := api.ccPost("/nodestate/", nodeStateDataJSON)
		if err != nil {
			return err
		}

		defer respNodeState.Body.Close()
		body, err := io.ReadAll(respNodeState.Body)
		if err != nil {
			return err
		}

		if respNodeState.StatusCode != 200 {
			return fmt.Errorf("Calling /nodes/update/ (%s) failed with HTTP: %d: Body: %s", cluster, respNodeState.StatusCode, string(body))
		}
	}

	trace.Info("Updated CC node state on clusters %v", api.slurmApi.GetClusterNames())
	return nil
}

func (api *CCApi) ccPost(relApiUrl string, bodyJson []byte) (*http.Response, error) {
	trace.Debug("POST to function %s: %s", relApiUrl, string(bodyJson))

	url := fmt.Sprintf("%s/api%s", config.Config.CcRestUrl, relApiUrl)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyJson))
	if err != nil {
		return nil, err
	}

	req.Header.Set("accept", "application/ld+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-AUTH-TOKEN", config.Config.CcRestJwt)

	return api.httpClient.Do(req)
}

func (api *CCApi) ccGet(relApiUrl string) (*http.Response, error) {
	url := fmt.Sprintf("%s/api%s", config.Config.CcRestUrl, relApiUrl)
	trace.Debug("GET to function %s", relApiUrl)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("accept", "application/ld+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-AUTH-TOKEN", config.Config.CcRestJwt)

	return api.httpClient.Do(req)
}

func checkIgnoreJob(job slurm_common.SacctJob, startJobData *types.CCStartJobRequest) bool {
	// We may want to filter out certain jobs, that shall not be submitted to cc-backend.
	// Put more rules here if necessary.
	trace.Debug("Checking whether job %d should be ignored", job.GetJobId())

	if len(startJobData.Resources) == 0 {
		// This should only happen for jobs, which are immediately cancelled or jobs, which are still pending.
		trace.Debug("Ignoring job %d, which has no resources associated. This job was probably never scheduled (state=%s).", job.GetJobId(), job.GetState())
		return true
	}

	if startJobData.StartTime == 0 {
		trace.Debug("Ignoring job %d, which has no start time set. This job probably hasn't startet yet.", job.GetJobId())
		return true
	}

	// If all hosts used in this job don't match the ignore pattern, discard the job.
	// Accordingly, if at least one host of the job does not match the pattern, the job
	// is not discarded.
	if len(config.Config.IgnoreHosts) > 0 {
		trace.Debug("Checking job %d against ignore hosts list.", job.GetJobId())
		atLeastOneHostAllowed := false
		for _, r := range startJobData.Resources {
			// The validity of the regexp is checked on startup, so no need to check it here.
			match, _ := regexp.MatchString(config.Config.IgnoreHosts, r.Hostname)
			if !match {
				atLeastOneHostAllowed = true
				break
			}
		}

		if !atLeastOneHostAllowed {
			trace.Debug("Ignoring job %d, which matches the hostname ignore pattern.", job.GetJobId())
			return true
		}
	}

	return false
}

func jobHasResources(job schema.Job) bool {
	hasResource := false
	for _, resource := range job.Resources {
		if resource.HWThreads != nil {
			hasResource = true
			break
		}

		if resource.Accelerators != nil {
			hasResource = true
			break
		}
	}

	return hasResource
}
