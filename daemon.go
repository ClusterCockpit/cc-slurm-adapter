package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClusterCockpit/cc-backend/pkg/schema"
	"github.com/ClusterCockpit/cc-lib/ccMessage"
	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
	"github.com/nats-io/nats.go"
)

type StartJob struct {
	schema.BaseJob
	StartTime int64 `json:"startTime"`
}

type StopJob struct {
	JobId    uint32          `json:"jobId"     db:"job_id"`
	Cluster  string          `json:"cluster"   db:"cluster"`
	State    schema.JobState `json:"jobState"  db:"state"`
	StopTime int64           `json:"stopTime"  db:"stop_time"`
}

type CacheJobState struct {
	CacheEvictAge int
	Stale         bool
	Running       bool
}

const CACHE_EVICT_COUNT int = 5

var (
	ipcSocket  net.Listener
	httpClient http.Client
	natsConn   *nats.Conn
	jobEvents  []PrologEpilogSlurmctldEnv
	hostname   string
	// map['clusterName'] -> map[slurmId] -> CC Job State, which are currently running (or ran recently).
	// When a jobs is started, they are inserted into the map. When they are stopped
	// they are set as not running. They are eventually removed again (to avoid leaking memory),
	// but only after CachEvictCountdown as gone down to zero. We do not remove the immediately
	// to avoid the situation where e.g. a poll event stops a job, and the corresponding PrEp stop event
	// won't find the job in the cache anymore. In that case, a stop_job would be sent, even the stop would already have stopped.
	ccJobCache      map[string]map[int64]*CacheJobState
	ccJobCacheValid bool
	ccJobCacheDate  time.Time
	slurmClusters   []string
)

func DaemonMain() error {
	trace.Info("Starting Daemon")

	err := daemonInit()
	if err != nil {
		return fmt.Errorf("Unable to initialize Daemon: %w", err)
	}
	defer daemonQuit()

	/* Init Signal Handling */
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	signalCtx, signalCancel := context.WithCancel(context.Background())

	// start background routine that handles the Unix Socket
	ipcSocketChan := make(chan []byte, 1024)
	go ipcSocketListenRoutine(signalCtx, ipcSocketChan)

	queryDelay := time.Duration(Config.SlurmQueryDelay) * time.Second

	pollEventInterval := time.Duration(Config.SlurmPollInterval) * time.Second
	pollEventTicker := time.NewTicker(queryDelay)
	pollEventFirst := true

	jobEventTimer := time.NewTimer(time.Duration(0))
	<-jobEventTimer.C
	jobEventPending := false

	/* Signal Handler definition */
	go func() {
		<-signalChan
		trace.Debug("Received signal, shutting down...")

		pollEventTicker.Stop()
		jobEventTimer.Stop()

		// cause Accept() on Unix socket to fail and unblock its routine.
		signalCancel()
		ipcSocket.Close()
	}()

	for {
		/* Wait for the following cases:
		 * - quit signal
		 *   -> cancel loop
		 * - IPC message received (binary invoked with -prolog or -epilog)
		 *   -> enqueue job to be queried via 'sacct' shortly after
		 * - event timer elapsed
		 *   -> query jobs via 'sacct' */
		select {
		case <-signalCtx.Done():
			trace.Debug("Daemon terminating")
			return nil
		case msg := <-ipcSocketChan:
			trace.Debug("Process IPC message (%d pending messages)", len(ipcSocketChan))
			err = jobEventEnqueue(msg)
			if err != nil {
				trace.Error("Unable to parse IPC message: %s", err)
			}

			/* We want to quickly poll Slurm after a job notification came in.
			 * Though, we have to make sure we don't infinitely reset the timer if the IPC channel gets spammed.
			 * This would otherwise cause the timer to continoulsy get reset and never actually fire. */
			if !jobEventPending {
				jobEventTimer.Reset(queryDelay)
				jobEventPending = true
			}
		case <-jobEventTimer.C:
			trace.Info("Job Event timer triggered")
			SlurmSacctCacheClear()
			jobEventsProcess()
			if len(jobEvents) > 0 {
				jobEventTimer.Reset(queryDelay)
			} else {
				jobEventPending = false
			}
		case <-pollEventTicker.C:
			trace.Info("Poll Event Timer triggered")
			if pollEventFirst {
				// The first time, the ticker is run with very small interval, to avoid startup delay.
				// Increase the interval to the normal interval after the first trigger.
				pollEventFirst = false
				pollEventTicker.Reset(pollEventInterval)
			}

			SlurmSacctCacheClear()
			err = ccCacheUpdate()
			if err != nil {
				trace.Error("Unable to update cc-backend cache. Trying later...")
				break
			}
			processSlurmSacctPoll()
			processSlurmSqueuePoll()
			ccCacheGC()
		}

		trace.Debug("Main loop iteration complete, waiting for next event...")
	}
}

func ipcSocketListenRoutine(ctx context.Context, chn chan<- []byte) {
	for {
		conn, err := ipcSocket.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				trace.Debug("Cancelling Unix Socket Accept Loop")
				return
			default:
				trace.Error("Error while accepting connection over Unix Socket: %s", err)
				continue
			}
		}

		go func() {
			/* Run the connection handling asynchronously. This allows
			 * the socket to accept a new connection almost immediatley. */
			defer conn.Close()
			trace.Debug("Receiving IPC message")
			msg, err := io.ReadAll(conn)
			if err != nil {
				trace.Error("Failed to receive IPC message over Unix Socket: %s", err)
				return
			}
			chn <- msg
		}()
	}
}

func daemonInit() error {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		return fmt.Errorf("Unable to obtain hostname: %w", err)
	}

	/* Assert last_run is writable. That way crash immediately instead after a long delay. */
	lastRunSet(lastRunGet())

	/* Verify Slurm Permissions */
	SlurmCheckPerms()

	/* Init Unix Socket */
	trace.Debug("Opening Socket")

	/* First check, if another daemon instance is already running.
	 * If a pid file is found, check if that process is still running.
	 * If it is still running, raise an error. If it is not running,
	 * the pid file is orphaned, and can be deleted. If no pid file exists
	 * we can safely start the daemon immediately. */
	pidFileContent, err := os.ReadFile(Config.PidFilePath)
	if err == nil {
		trimmedPidFileContent := strings.TrimSpace(string(pidFileContent))
		_, err := os.Stat(fmt.Sprintf("/proc/%s", trimmedPidFileContent))
		if err == nil {
			return fmt.Errorf("Unable to start daemon. Found an already running daemon with PID: %s", trimmedPidFileContent)
		}
	}

	err = os.WriteFile(Config.PidFilePath, []byte(fmt.Sprintf("%d", os.Getpid())), 0644)
	if err != nil {
		return fmt.Errorf("Unable to create pid file: %w", err)
	}

	os.Remove(Config.IpcSockPath)
	ipcSocket, err = net.Listen("unix", Config.IpcSockPath)
	if err != nil {
		os.Remove(Config.PidFilePath)
		return fmt.Errorf("Unable to create socket (is there an existing socket with bad permissions?): %w", err)
	}

	err = os.Chmod(Config.IpcSockPath, 0666)
	if err != nil {
		ipcSocket.Close()
		os.Remove(Config.IpcSockPath)
		os.Remove(Config.PidFilePath)
		return fmt.Errorf("Failed to set permissions via chmod on IPC Socket: %w", err)
	}

	/* Init HTTP Client */
	tr := &http.Transport{
		MaxIdleConns:    10,
		IdleConnTimeout: 2 * time.Duration(Config.SlurmPollInterval) * time.Second,
	}
	httpClient = http.Client{Transport: tr}

	/* Init NATS Client */
	options := make([]nats.Option, 0)
	if len(Config.NatsUser) > 0 {
		options = append(options, nats.UserInfo(Config.NatsUser, Config.NatsPassword))
	}
	if len(Config.NatsCredsFile) > 0 {
		options = append(options, nats.UserCredentials(Config.NatsCredsFile))
	}
	if len(Config.NatsNKeySeedFile) > 0 {
		r, err := nats.NkeyOptionFromSeed(Config.NatsNKeySeedFile)
		if err != nil {
			return fmt.Errorf("Unable to open NKeySeedFile: %w", err)
		}
		options = append(options, r)
	}
	if len(Config.NatsServer) > 0 {
		natsAddr := fmt.Sprintf("nats://%s:%d", Config.NatsServer, Config.NatsPort)
		trace.Info("Connecting to NATS: %s", natsAddr)
		natsConn, err = nats.Connect(natsAddr, options...)
		if err != nil {
			ipcSocket.Close()
			os.Remove(Config.IpcSockPath)
			os.Remove(Config.PidFilePath)
			return fmt.Errorf("Unable to connect to NATS (server: %s): %w", natsAddr, err)
		}
	}

	/* job events queue initialization */
	jobEvents = make([]PrologEpilogSlurmctldEnv, 0)

	/* Get the clusters managed by Slurm */
	slurmClusters, err = SlurmGetClusterNames()
	if err != nil {
		return fmt.Errorf("Unable to determine cluster hostnames: %w", err)
	}
	trace.Debug("Detected Slurm clusters: %v", slurmClusters)

	printWelcome()

	/* Init cc job state cache */
	trace.Debug("Fetching initial job state from cc-backend")
	err = ccCacheUpdate()
	if err != nil {
		return fmt.Errorf("Failed to update cc-backend job cache: %w", err)
	}
	return nil
}

func printWelcome() {
	trace.Info("Initialization complete")
	rev := ""
	modified := false
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				rev = setting.Value
			} else if setting.Key == "vcs.modified" {
				if strings.ToLower(setting.Value) == "true" {
					modified = true
				}
			}
		}
	}
	if rev == "" {
		rev = "(unknown revision)"
	}
	if modified && rev != "" {
		rev += "-dirty"
	}
	trace.Info("Running cc-slurm-adapter %s", rev)
}

func ccCacheUpdate() error {
	/* We maintain a local cache of which jobs are marked as running in cc-backend.
	 * Only refresh it if the cache was invalidated or if it wasn't refreshed for some time. */
	if ccJobCacheValid && ccJobCacheDate.Add(time.Duration(Config.CcPollInterval)*time.Second).After(time.Now()) {
		return nil
	}

	respJobs, err := ccGet("/jobs/?state=running&items-per-page=9999999&page=1&with-metadata=false")
	if err != nil {
		ccJobCacheValid = false
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
		Jobs  []*schema.JobMeta `json:"jobs"`
		Items int               `json:"items"`
		Page  int               `json:"page"`
	}{}

	err = json.Unmarshal(body, &getJobsApiResponse)
	if err != nil {
		return fmt.Errorf("Error in JSON returned from cc-backend: %w, JSON: %s", err, body)
	}

	/* init cache if not done so yet */
	initial := false
	if ccJobCache == nil {
		ccJobCache = make(map[string]map[int64]*CacheJobState)
		for _, cluster := range slurmClusters {
			ccJobCache[cluster] = make(map[int64]*CacheJobState)
		}
		initial = true
		trace.Info("Running initial cc-backend <---> cc-slurm-adapter synchronization. This may take a while.")
	}

	// Create mapping from cluster -> jobid -> job from cc-backend data.
	// Compare this mapping to our current cache to detect, if there have been differences.
	ccJobState := make(map[string]map[int64]*schema.JobMeta)
	for _, job := range getJobsApiResponse.Jobs {
		if ccJobState[job.Cluster] == nil {
			ccJobState[job.Cluster] = make(map[int64]*schema.JobMeta)
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

			if ccJobCache[ccJob.Cluster] == nil {
				// Skip jobs from cc-backend, which do not belong to the clusters that we manage.
				// It may be nicer to not request them in the first place, but it's easier for now...
				continue
			}

			ccJobCount += 1

			cacheJob, ok := ccJobCache[ccJob.Cluster][ccJob.JobID]
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

			slurmJob, err := SlurmQueryJob(ccJob.Cluster, uint32(ccJob.JobID))
			if err != nil {
				trace.Error("Unable to correct desync. Slurm failed to query job (%s, %d): %v", ccJob.Cluster, ccJob.JobID, err)
				continue
			}

			ccJobCache[ccJob.Cluster][ccJob.JobID] = &CacheJobState{
				Running: true,
			}

			err = ccSyncJob(*slurmJob)
			if err != nil {
				trace.Error("Unable to correct desync (state may be inconsistent now!). Sync to cc-backend failed: %v", err)
			}
		}
	}

	// ... and now new cc-backend state <-- out old cc-backend state
	for cluster, ccJobClusterCache := range ccJobCache {
		for jobId, cacheJob := range ccJobClusterCache {
			ccJob, ok := ccJobState[cluster][jobId]
			if !ok && !cacheJob.Running {
				// If our local job state is not set to 'running' anymore, it's okay if cc-backend doesn't know this job.
				ok = true
			}

			if ok {
				continue
			}

			if !initial {
				trace.Warn("Cache desync detected! Resetting missing/stopped job (%s, %d) from cc-backend in cache.", ccJob.Cluster, ccJob.JobID)
			}

			slurmJob, err := SlurmQueryJob(ccJob.Cluster, uint32(ccJob.JobID))
			if err != nil {
				trace.Error("Unable to correct desync. Slurm failed to query job (%s, %d): %v", ccJob.Cluster, ccJob.JobID, err)
				continue
			}

			cacheJob.CacheEvictAge = 0
			cacheJob.Running = false

			err = ccSyncJob(*slurmJob)
			if err != nil {
				trace.Error("Unable to correct desync (state may be inconsistent now!). Sync to cc-backend failed: %v", err)
			}
		}
	}

	ccJobCacheDate = time.Now()
	ccJobCacheValid = true

	trace.Info("CC Job Cache updated. Number of running jobs: %d", ccJobCount)
	return nil
}

func ccCacheGC() {
	for _, cachedJobStates := range ccJobCache {
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

func jobEventEnqueue(ipcMsg []byte) error {
	/* The message received contains a JSON, which contains all relevant
	 * environment variables from here:
	 * https://slurm.schedmd.com/prolog_epilog.html.
	 * Please keep in mind that some of the environment variables are only
	 * available in TaskProlog/TaskEpilog. However, we only run in slurmctld
	 * context, so only their appropriate values are available. */
	var env PrologEpilogSlurmctldEnv
	err := json.Unmarshal(ipcMsg, &env)
	if err != nil {
		return fmt.Errorf("Unable to parse IPC message as JSON (%w). Either a 3rd party is writing to our Unix socket or there is a bug in our IPC protocol: '%s'", err, string(ipcMsg))
	}

	env.SacctAttempts = 0

	jobEvents = append(jobEvents, env)
	return nil
}

func jobEventsProcess() {
	trace.Debug("jobEventsProcess()")
	newJobEvents := make([]PrologEpilogSlurmctldEnv, 0)
	for index, jobEvent := range jobEvents {
		jobEventId, err := strconv.ParseUint(jobEvent.SLURM_JOB_ID, 10, 32)
		if err != nil {
			trace.Warn("SLURM_JOB_ID contains non-integer value: %v", err)
			continue
		}

		jobEventCluster := jobEvent.SLURM_CLUSTER_NAME
		if !slices.Contains(slurmClusters, jobEventCluster) {
			trace.Warn("SLURM_CLUSTER_NAME=%s is not managed by us. This should usually not happen or it means that the PrEp hook notified us about a job's cluster, which wasn't reported by 'sinfo'", jobEventCluster)
			continue
		}

		job, err := SlurmQueryJob(jobEventCluster, uint32(jobEventId))
		if err != nil {
			// We want to avoid job events getting delivered out of order.
			// Accordingly, cancel the execution of the loop if there is an error.
			// All leftover job events will get carried over to the next iteration
			trace.Debug("Job (%s) not ready: %v", jobEvent.SLURM_JOB_ID, err)
			jobEvents[index].SacctAttempts += 1
			if jobEvents[index].SacctAttempts < 5 {
				newJobEvents = append(newJobEvents, jobEvents[index:]...)
				break
			} else {
				trace.Warn("Job (%d) exceeded max query retries of %d. Giving up on job.", jobEventId, Config.SlurmMaxRetries)
				continue
			}
		}

		err = ccSyncJob(*job)
		if err != nil {
			trace.Warn("Syncing job (%s, %d) via PrEp hook failed. Trying later during regular poll...", jobEventCluster, jobEventId)
		}
	}

	jobEvents = newJobEvents
}

func processSlurmSacctPoll() {
	trace.Debug("processSlurmSacctPoll()")
	lastRun := lastRunGet().Add(time.Duration(-1 * time.Second)) // -1 second for good measure to avoid overlap error
	thisRun := time.Now()

	if lastRun.Add(time.Duration(Config.SlurmQueryMaxSpan) * time.Second).Before(thisRun) {
		trace.Warn("sacct was polled %s ago, which is higher than maximum %d. Limiting to maximum. Either we didn't run for a while or haven't run at all. If jobs from the past are missing, increase the maxmimum duration in the configuration. This warning will go away after the next job.", thisRun.Sub(lastRun).Truncate(time.Second).String(), Config.SlurmQueryMaxSpan)
		lastRun = thisRun.Add(time.Duration(-Config.SlurmQueryMaxSpan) * time.Second)
	}

	/* Detect time change (e.g. summer/winter time). If ... */
	_, beginOffset := lastRun.Zone()
	_, endOffset := thisRun.Zone()
	if endOffset < beginOffset {
		/* If the time has gone backwards, move the begin time stamp backwards accordingly.
		 * This way we make sure we pass a correct local time to Slurm, where 'begin'
		 * is actually always before 'end'.
		 * I am not entirely sure that this works reliably or if Go will correctly
		 * handle those changes. */
		trace.Warn("Time change detected: Moving last run %d seconds backwards", endOffset-beginOffset)
		lastRun = lastRun.Add(-time.Duration(endOffset-beginOffset) * time.Second)
	}

	for _, cluster := range slurmClusters {
		jobs, err := SlurmQueryJobsTimeRange(cluster, lastRun, thisRun)
		if err != nil {
			trace.Error("Unable to query Slurm for jobs (is Slurm available?): %s", err)
			return
		}

		for _, job := range jobs {
			err = ccSyncJob(job)
			if err != nil {
				trace.Error("Syncing job to ClusterCockpit failed (%s). Trying later...", err)
				return
			}
		}

		if len(jobs) > 0 {
			// Avoid unecessary time stamp file writes if no jobs were actually submitted since the last check
			lastRunSet(thisRun)
		}
	}
}

func processSlurmSqueuePoll() {
	trace.Debug("processSlurmSqueuePoll()")
	for _, cluster := range slurmClusters {
		jobs, err := SlurmQueryJobsActive(cluster)
		if err != nil {
			trace.Error("Unable to query Slurm via squeue (is Slurm available?): %v", err)
			return
		}

		slurmIsJobRunning := make(map[int64]bool)
		for _, scJob := range jobs {
			if strings.ToLower(string(*scJob.JobState)) != "running" {
				continue
			}

			slurmIsJobRunning[int64(*scJob.JobId)] = true
		}

		// Check if there are any stale jobs in cc-backend, which are no longer known to Slurm.
		// This should usually not happen, but in the past Slurm would occasionally lie to use and we would miss
		// job stops.
		for jobId, cachedJobState := range ccJobCache[cluster] {
			if !cachedJobState.Running {
				continue
			}

			if slurmIsJobRunning[jobId] {
				continue
			}

			if !cachedJobState.Stale {
				// Do not immediately report a job as stale. Give it chance for one more iteration to be cleaned up via poll.
				// Otherwise a job, which has stopped since the last poll, will immediately be reported as stale.
				cachedJobState.Stale = true
				continue
			}

			trace.Warn("Detected stale job in cc-backend (%s, %d). Trying to synchronize...", cluster, jobId)
			job, err := SlurmQueryJob(cluster, uint32(jobId))
			if err != nil {
				trace.Error("Failed to query cc-backend's stale job from Slurm: %v", err)
				continue
			}

			trace.Warn("Stale job state is: %s", string(*job.State.Current))

			err = ccSyncJob(*job)
			if err != nil {
				trace.Error("Failed to sync cc-backend's stale job from Slurm: %v", err)
			}
		}
	}
}

func daemonQuit() {
	trace.Debug("Closing NATS")
	natsConn.Close()

	/* While we can handle orphaned pid files and sockets,
	 * we should clean them up after we're done.
	 * The PID check is also not 100% reliable, since we just
	 * check against any process with that PID and not if it
	 * actually is the daemon... */
	trace.Debug("Closing Socket")
	ipcSocket.Close()
	os.Remove(Config.IpcSockPath)
	os.Remove(Config.PidFilePath)
}

func lastRunGet() time.Time {
	statInfo, err := os.Stat(Config.LastRunPath)
	if errors.Is(err, os.ErrNotExist) {
		return time.Unix(0, 0)
	}
	if err != nil {
		trace.Fatal("Unable to determine time of last run: %s", err)
	}

	return statInfo.ModTime()
}

func lastRunSet(timeStamp time.Time) {
	trace.Debug("lastRunSet")
	f, err := os.OpenFile(Config.LastRunPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		trace.Fatal("Unable to set time of last run: %s", err)
	}

	f.Close()

	err = os.Chtimes(Config.LastRunPath, timeStamp, timeStamp)
	if err != nil {
		trace.Fatal("Unable to set time of last run: %s", err)
	}
}

func ccSyncJob(job SacctJob) error {
	/* Assert the job exists in cc-backend. Ignore if the job already exists. */
	if Config.CcRestUrl == "" {
		trace.Info("Skipping submission to ClusterCockpit REST. Missing URL. This feature is optional, so we will continue running")
		return nil
	}

	startJobData, err := slurmJobToCcStartJob(job)
	if err != nil {
		return err
	}

	if checkIgnoreJob(job, startJobData) {
		return nil
	}

	err = ccStartJob(job, startJobData)
	if err != nil {
		return err
	}

	// TODO If the job already exists in cc-backend, make sure to update values if interested, which may have changed.
	// In the future, this could be used to implement updating of job time limits, etc.
	// At the moment, we can't do this yet, because cc-backend doesn't have an API endpoint to alter an existing job.

	/* Only submit stop job, if it has actually finished */
	if job.Time.End.Number <= 0 {
		/* A job which hasn't finished, has no end time set. This is easier than
		 * comparing against all possible job states. */
		return nil
	}

	err = ccStopJob(job)
	if err != nil {
		return err
	}

	ccJobCache[*job.Cluster][int64(*job.JobId)].Stale = false
	return nil
}

func ccStartJob(job SacctJob, startJobData *StartJob) error {
	cluster := *job.Cluster
	jobId := int64(*job.JobId)

	_, jobKnown := ccJobCache[cluster][jobId]
	if jobKnown {
		return nil
	}

	startJobDataJSON, err := json.Marshal(startJobData)
	if err != nil {
		return fmt.Errorf("Unable to convert StartJob to JSON: %w", err)
	}

	respStart, err := ccPost("/jobs/start_job/", startJobDataJSON)
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

	// Status Code 201 -> the job was newly created
	// Status Code 422 -> the job already existed
	if respStart.StatusCode == 201 {
		trace.Info("Sent start_job successfully (%s, %d)", cluster, jobId)
		tags := map[string]string{
			"hostname": hostname,
			"type":     "node",
			"type-id":  "0",
			"function": "start_job",
		}
		msg, err := ccmessage.NewEvent("job", tags, nil, string(startJobDataJSON), time.Unix(startJobData.StartTime, 0))
		if err != nil {
			trace.Warn("ccmessage.NewEvent() failed for job (%s, %d) failed: %s", cluster, jobId, err)
		} else {
			err = natsConn.Publish(Config.NatsSubject, []byte(msg.ToLineProtocol(nil)))
			if err != nil {
				trace.Warn("Unable to publish message on NATS for job (%s, %d): %s", cluster, jobId, err)
			}
		}
	}

	ccJobCache[*job.Cluster][int64(*job.JobId)] = &CacheJobState{
		Running: true,
	}
	return nil
}

func ccStopJob(job SacctJob) error {
	cluster := *job.Cluster
	jobId := int64(*job.JobId)

	cachedJobState, jobKnown := ccJobCache[cluster][jobId]
	if jobKnown && !cachedJobState.Running {
		return nil
	}

	stopJobData := slurmJobToCcStopJob(job)
	stopJobDataJSON, err := json.Marshal(stopJobData)
	if err != nil {
		return fmt.Errorf("Unable to convert StopJob to JSON: %w", err)
	}

	respStop, err := ccPost("/jobs/stop_job/", stopJobDataJSON)
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
		/* While it should usually not occur a 422 (i.e. job was already stopped),
		 * this may still occur if something in the state was glitched. */
		trace.Warn("Calling /jobs/stop_job/ (cluster=%s jobid=%d) failed with HTTP 422 (non-fatal): Body %s", cluster, jobId, string(body))
	}

	if respStop.StatusCode == 200 {
		trace.Info("Sent stop_job successfully (%s, %d)", cluster, jobId)
		tags := map[string]string{
			"hostname": hostname,
			"type":     "node",
			"type-id":  "0",
			"function": "stop_job",
		}
		msg, err := ccmessage.NewEvent("job", tags, nil, string(stopJobDataJSON), time.Unix(stopJobData.StopTime, 0))
		if err != nil {
			trace.Warn("ccmessage.NewEvent() failed for job (%s, %d) failed: %s", cluster, jobId, err)
		} else {
			err = natsConn.Publish(Config.NatsSubject, []byte(msg.ToLineProtocol(nil)))
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
		ccJobCache[cluster][jobId] = &CacheJobState{
			Running: false,
		}
	}
	return nil
}

func ccPost(relApiUrl string, bodyJson []byte) (*http.Response, error) {
	trace.Debug("POST to function %s: %s", relApiUrl, string(bodyJson))

	url := fmt.Sprintf("%s/api%s", Config.CcRestUrl, relApiUrl)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyJson))
	if err != nil {
		return nil, err
	}

	req.Header.Set("accept", "application/ld+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-AUTH-TOKEN", Config.CcRestJwt)

	return httpClient.Do(req)
}

func ccGet(relApiUrl string) (*http.Response, error) {
	url := fmt.Sprintf("%s/api%s", Config.CcRestUrl, relApiUrl)
	trace.Debug("GET to function %s", relApiUrl)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("accept", "application/ld+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-AUTH-TOKEN", Config.CcRestJwt)

	return httpClient.Do(req)
}

func slurmJobToCcStartJob(job SacctJob) (*StartJob, error) {
	/* TODO Maybe we should move this into slurm.go. We shouldn't really use slurm
	 * datastructures outside of slurm.go. */
	scJob, err := SlurmGetScontrolJob(job)
	if err != nil {
		return nil, err
	}

	resources, err := SlurmGetResources(job, scJob)
	if err != nil {
		/* This error should only occur for criticial errors.
		 * Non critical errors won't enter this case. */
		return nil, err
	}

	metaData := make(map[string]string)
	metaData["jobScript"] = SlurmGetJobScript(job)
	metaData["jobName"] = *job.Name
	metaData["slurmInfo"] = SlurmGetJobInfoText(job)

	var exclusive int32
	if scJob != nil {
		if scJob.Exclusive != nil && string(*scJob.Exclusive) == "true" {
			exclusive = 1
		} else if scJob.Shared != nil {
			if string(*scJob.Shared) == "user" {
				exclusive = 0
			} else if string(*scJob.Shared) == "none" {
				exclusive = 1
			} else if string(*scJob.Shared) == "" {
				exclusive = 0
			}
		} else {
			trace.Debug("No information available about exclusive/shared for job %d.", *job.JobId)
		}
	}

	ccStartJob := StartJob{
		BaseJob: schema.BaseJob{
			Cluster:      *job.Cluster,
			Partition:    *job.Partition,
			Project:      *job.Account,
			ArrayJobId:   int64(*job.Array.JobId),
			NumNodes:     int32(job.AllocationNodes.Number),
			NumHWThreads: int32(job.Required.CPUs.Number),
			Exclusive:    exclusive,
			Walltime:     job.Time.Limit.Number * 60, // slurm reports the limit in MINUTES, not seconds
			Resources:    resources,
			MetaData:     metaData,
			JobID:        int64(*job.JobId),
			User:         *job.User,
		},
		StartTime: job.Time.Start.Number,
	}

	/* Determine number of CPUs and accelerators. Use requested values
	 * as base, and use allocated values, if available. */
	setResources := func(tresList []SacctJobTres, ccStartJob *StartJob) {
		for _, tres := range tresList {
			if *tres.Type == "cpu" {
				ccStartJob.BaseJob.NumHWThreads = *tres.Count
			}

			if *tres.Type == "gres" && *tres.Name == "gpu" {
				ccStartJob.BaseJob.NumAcc = *tres.Count
			}
		}
	}

	setResources(job.Tres.Requested, &ccStartJob)
	setResources(job.Tres.Allocated, &ccStartJob)

	return &ccStartJob, nil
}

func slurmJobToCcStopJob(job SacctJob) StopJob {
	ccStopJob := StopJob{
		JobId:    *job.JobId,
		Cluster:  *job.Cluster,
		State:    schema.JobState(strings.ToLower(string(*job.State.Current))),
		StopTime: job.Time.End.Number,
	}

	/* WORKAROUNDS due to cc-backend's lack of support for them.
	 * Ideally this should be removed in the future. */
	if ccStopJob.State == "node_fail" {
		trace.Warn("Alterting status 'node_fail' to 'failure' for job %d. If this is finally supported in cc-backend, the code generating this message can be removed", *job.JobId)
		ccStopJob.State = "failure"
	}

	if ccStopJob.State == "failure" {
		trace.Debug("Altering status 'failure' to 'failed' for job %d", *job.JobId)
		ccStopJob.State = "failed"
	}
	return ccStopJob
}

func checkIgnoreJob(job SacctJob, startJobData *StartJob) bool {
	/* We may want to filter out certain jobs, that shall not be submitted to cc-backend.
	 * Put more rules here if necessary. */
	trace.Debug("Checking whether job %d should be ignored", *job.JobId)

	if len(startJobData.Resources) == 0 {
		// This should only happen for jobs, which are immediately cancelled or jobs, which are still pending.
		trace.Debug("Ignoring job %d, which has no resources associated. This job was probably never scheduled (state=%s).", *job.JobId, string(*job.State.Current))
		return true
	}

	if startJobData.StartTime == 0 {
		trace.Debug("Ignoring job %d, which has no start time set. This job probably hasn't startet yet.", *job.JobId)
		return true
	}

	/* If all hosts used in this job don't match the ignore pattern, discard the job.
	 * Accordingly, if at least one host of the job does not match the pattern, the job
	 * is not discarded. */
	if len(Config.IgnoreHosts) > 0 {
		trace.Debug("Checking job %d against ignore hosts list.", *job.JobId)
		atLeastOneHostAllowed := false
		for _, r := range startJobData.Resources {
			/* The validity of the regexp is checked on startup, so no need to check it here. */
			match, _ := regexp.MatchString(Config.IgnoreHosts, r.Hostname)
			if !match {
				atLeastOneHostAllowed = true
				break
			}
		}

		if !atLeastOneHostAllowed {
			trace.Debug("Ignoring job %d, which matches the hostname ignore pattern.", *job.JobId)
			return true
		}
	}

	return false
}
