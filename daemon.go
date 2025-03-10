package main

import (
	"fmt"
	"net"
	"os"
	"io"
	"strings"
	"time"
	"context"
	"os/signal"
	"syscall"
	"encoding/json"
	"net/http"
	"strconv"
	"bytes"
	"errors"
	"regexp"

	"github.com/ClusterCockpit/cc-lib/ccMessage"
	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
	"github.com/ClusterCockpit/cc-backend/pkg/schema"
	"github.com/nats-io/nats.go"
)

type StartJob struct {
	schema.BaseJob
	StartTime int64 `json:"startTime"`
}

type StopJob struct {
	JobId 	  uint32           `json:"jobId"     db:"job_id"`
	Cluster   string           `json:"cluster"   db:"cluster"`
	State     schema.JobState  `json:"jobState"  db:"state"`
	StopTime  int64            `json:"stopTime"  db:"stop_time"`
}

var (
	ipcSocket   net.Listener
	httpClient  http.Client
	natsConn    *nats.Conn
	jobEvents   []PrologEpilogSlurmctldEnv
	hostname    string
)

func DaemonMain() error {
	trace.Info("Starting Daemon")

	err := daemonInit()
	if err != nil {
		return fmt.Errorf("Unable to initialize Daemon: %w", err)
	}
	defer daemonQuit()


	/* Init Signal Handling */
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	signalCtx, signalCancel := context.WithCancel(context.Background())

	// start background routine that handles the Unix Socket
	ipcSocketChan := make(chan []byte)
	go ipcSocketListenRoutine(signalCtx, ipcSocketChan)

	queryDelay := time.Duration(Config.SlurmQueryDelay) * time.Second
	pollEventInterval := time.Duration(Config.SlurmPollInterval) * time.Second
	pollEventChan := make(chan struct{})
	pollEventTimer := time.AfterFunc(queryDelay, func() { pollEventChan <- struct{}{}})
	pollEventNext := time.Now().Add(queryDelay)

	/* Signal Handler definition */
	go func() {
		<-signalChan
		trace.Debug("Received signal, shutting down...")

		pollEventTimer.Stop()

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
			trace.Debug("Process IPC message")
			err = processJobNotify(msg)
			if err != nil {
				trace.Error("Unable to parse IPC message: %s", err)
			}

			/* We want to quickly poll Slurm after a job notification came in.
			 * Though, we have to make sure we don't infinitely reset the timer if the IPC channel gets spammed.
			 * This would otherwise cause the timer to continoulsy get reset and never actually fire. */
			if pollEventNext.After(time.Now().Add(queryDelay)) {
				pollEventNext = time.Now().Add(queryDelay)
				pollEventTimer.Reset(queryDelay)
			}
		case <-pollEventChan:
			trace.Debug("Timer triggered Slurm polling")
			processJobEvents()
			processSlurmSacctPoll()
			// TODO enable the following line, once we actually implemented
			// Updating of jobs, after they've started
			//processSlurmSqueuePoll()

			if len(jobEvents) > 0 {
				/* If there are still jobs in the event queue,
				 * reschedule in the next few seconds. */
				pollEventNext = time.Now().Add(queryDelay)
				pollEventTimer.Reset(queryDelay)
			} else {
				/* If there are no jobs in the event queue,
				 * wait longer until the next poll. */
				pollEventNext = time.Now().Add(pollEventInterval)
				pollEventTimer.Reset(pollEventInterval)
			}
		}
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
		MaxIdleConns:	10,
		IdleConnTimeout: 2 * time.Duration(Config.SlurmPollInterval) * time.Second,
	}
	httpClient = http.Client{Transport: tr}

	/* Init NATS Client */
	options := make([]nats.Option, 0)
	natsEnabled := false
	if len(Config.NatsUser) > 0 {
		options = append(options, nats.UserInfo(Config.NatsUser, Config.NatsPassword))
		natsEnabled = true
	}
	if len(Config.NatsCredsFile) > 0 {
		options = append(options, nats.UserCredentials(Config.NatsCredsFile))
		natsEnabled = true
	}
	if len(Config.NatsNKeySeedFile) > 0 {
		r, err := nats.NkeyOptionFromSeed(Config.NatsNKeySeedFile)
		if err != nil {
			return fmt.Errorf("Unable to open NKeySeedFile: %w" ,err)
		}
		options = append(options, r)
		natsEnabled = true
	}
	if natsEnabled {
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

	trace.Debug("Initialization complete")
	return nil
}

func processJobNotify(ipcMsg []byte) error {
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

func processJobEvents() {
	trace.Debug("processJobEvents()")
	newJobEvents := make([]PrologEpilogSlurmctldEnv, 0)
	for index, jobEvent := range jobEvents {
		jobEventId, err := strconv.ParseUint(jobEvent.SLURM_JOB_ID, 10, 32)
		if err != nil {
			trace.Warn("SLURM_JOB_ID contains non-integer value: %w", err)
			continue
		}

		_, err = SlurmQueryJob(uint32(jobEventId))
		if err != nil {
			// We want to avoid job events getting delivered out of order.
			// Accordingly, cancel the execution of the loop if there is an error.
			// All leftover job events will get carried over to the next iteration
			trace.Debug("Job (%s) not ready: %s", jobEvent.SLURM_JOB_ID, err)
			jobEvent.SacctAttempts += 1
			if jobEvent.SacctAttempts < 5 {
				newJobEvents = append(newJobEvents, jobEvents[index:]...)
				break
			} else {
				trace.Warn("Job (%s) exceeded max query retries of %d. Giving up on job.", jobEvent.SLURM_JOB_ID, Config.SlurmMaxRetries)
				continue
			}
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
		trace.Warn("Time change detected: Moving last run %d seconds backwards")
		lastRun = lastRun.Add(-time.Duration(endOffset - beginOffset) * time.Second)
	}

	jobs, err := SlurmQueryJobsTimeRange(lastRun, thisRun)
	if err != nil {
		trace.Error("Unable to query Slurm for jobs (is Slurm available?): %s", err)
		return
	}

	for _, job := range jobs {
		err = ccSyncJob(job, lastRun)
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

func processSlurmSqueuePoll() {
	trace.Debug("processSlurmSqueuePoll()")
	jobs, err := SlurmQueryJobsActive()
	if err != nil {
		trace.Error("Unable to query Slurm via squeue (is Slurm available?): %s", err)
		return
	}

	lastRun := lastRunGet().Add(time.Duration(-1 * time.Second)) // -1 second for good measure to avoid overlap error

	for _, job := range jobs {
		// TODO This case still doesn't function correctly.
		// This will need ccSyncJob to have some kind of feedback mechanism with cc-backend
		// to query non-completed jobs and retrieve their state.
		// UPDATE: We now have a cache of cc-backend's job state. However, at the moment this is only
		// the job ID. We could extend this to the full job, which would allow us to detect, if anything has changed.
		err = ccSyncJob(job, lastRun)
		if err != nil {
			trace.Error("Syncing job to ClusterCockpit failed (%s). Trying later...", err)
			return
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
	f, err := os.OpenFile(Config.LastRunPath, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		trace.Fatal("Unable to set time of last run: %s", err)
	}

	f.Close()

	err = os.Chtimes(Config.LastRunPath, timeStamp, timeStamp)
	if err != nil {
		trace.Fatal("Unable to set time of last run: %s", err)
	}
}

func ccSyncJob(job SacctJob, lastRun time.Time) error {
	/* Assert the job exists in cc-backend. Ignore if the job already exists. */
	if Config.CcRestUrl == "" {
		trace.Info("Skipping submission to ClusterCockpit REST. Missing URL. This feature is optional, so we will continue running")
		return nil
	}

	startJobData, err := slurmJobToCcStartJob(job)
	if err != nil {
		return err
	}

	if checkIngoreJob(job, startJobData, lastRun) {
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
		/* If the POST is not successful or if the entry already exists (which is ok),
		 * raise an error. */

		return fmt.Errorf("Calling /jobs/start_job/ (cluster=%s jobid=%d) failed with HTTP %d: Body %s", *job.Cluster, *job.JobId, respStart.StatusCode, string(body))
	}

	if respStart.StatusCode == 201 {
		trace.Debug("Sending start_job to NATS for job %d", job.JobId)
		tags := map[string]string{
			"hostname": hostname,
			"type": "node",
			"type-id": "0",
			"function": "start_job",
		}
		msg, err := ccmessage.NewEvent("job", tags, nil, string(startJobDataJSON), time.Unix(startJobData.StartTime, 0))
		if err != nil {
			trace.Warn("ccmessage.NewEvent() failed for job %d failed: %s", *job.JobId, err)
		} else {
			err = natsConn.Publish(Config.NatsSubject, []byte(msg.ToLineProtocol(nil)))
			if err != nil {
				trace.Warn("Unable to publish message on NATS for job %d: %s", *job.JobId, err)
			}
		}
	}

	// TODO If the job already exists in cc-backend, make sure to update values if interested, which may have changed.
	// In the future, this could be used to implement updating of job time limits, etc.

	/* Only submit stop job, if it has actually finished */
	if job.Time.End.Number <= 0 {
		/* A job which hasn't finished, has no end time set. This is easier than
		 * comparing against all possible job states. */
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
	body, err = io.ReadAll(respStop.Body)
	if err != nil {
		return err
	}

	if respStop.StatusCode != 200 && respStop.StatusCode != 422 {
		return fmt.Errorf("Calling /jobs/stop_job/ (cluster=%s jobid=%d) failed with HTTP %d: Body %s", *job.Cluster, *job.JobId, respStop.StatusCode, string(body))
	}

	if respStop.StatusCode == 422 {
		/* While it should usually not occur a 422 (i.e. job was already stopped),
		 * this may still occur if something in the state was glitched. */
		trace.Warn("Calling /jobs/stop_job/ (cluster=%s jobid=%d) failed with HTTP 422 (non-fatal): Body %s", *job.Cluster, *job.JobId, string(body))
	}

	if respStop.StatusCode == 200 {
		trace.Debug("Sending stop_job to NATS for job %d", job.JobId)
		tags := map[string]string{
			"hostname": hostname,
			"type": "node",
			"type-id": "0",
			"function": "stop_job",
		}
		msg, err := ccmessage.NewEvent("job", tags, nil, string(stopJobDataJSON), time.Unix(stopJobData.StopTime, 0))
		if err != nil {
			trace.Warn("ccmessage.NewEvent() failed for job %d failed: %s", *job.JobId, err)
		} else {
			err = natsConn.Publish(Config.NatsSubject, []byte(msg.ToLineProtocol(nil)))
			if err != nil {
				trace.Warn("Unable to publish message on NATS for job %d: %s", *job.JobId, err)
			}
		}
	}

	return nil
}

func ccPost(relApiUrl string, bodyJson []byte) (*http.Response, error) {
	trace.Debug("POST to function %s: %s", relApiUrl, string(bodyJson))

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api%s", Config.CcRestUrl, relApiUrl), bytes.NewBuffer(bodyJson))
	if err != nil {
		return nil, err
	}

	req.Header.Set("accept", "application/ld+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-AUTH-TOKEN", Config.CcRestJwt)

	return httpClient.Do(req)
}

func slurmJobToCcStartJob(job SacctJob) (*StartJob, error) {
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
	metaData["jobScript"] = *job.Script
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
			Cluster: *job.Cluster,
			Partition: *job.Partition,
			Project: *job.Account,
			ArrayJobId: int64(*job.Array.JobId),
			NumNodes: int32(job.AllocationNodes.Number),
			NumHWThreads: int32(job.Required.CPUs.Number),
			Exclusive: exclusive,
			Walltime: job.Time.Limit.Number,
			Resources: resources,
			MetaData: metaData,
			JobID: int64(*job.JobId),
			User: *job.User,
		},
		StartTime: job.Time.Start.Number,
	}

	/* Determine number of CPUs and accelerators. Use requested values
	 * as base, and use allocated values, if available. */
	setResources := func (tresList []SacctJobTres, ccStartJob *StartJob) {
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
		JobId: *job.JobId,
		Cluster: *job.Cluster,
		State: schema.JobState(strings.ToLower(string(*job.State.Current))),
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

func checkIngoreJob(job SacctJob, startJobData *StartJob, lastRun time.Time) bool {
	/* We may want to filter out certain jobs, that shall not be submitted to cc-backend.
	 * Put more rules here if necessary. */
	trace.Debug("Checking whether job %d should be ignored", *job.JobId)

	startTime := time.Unix(job.Time.Start.Number, 0)
	if startTime.Before(lastRun) && job.Time.End.Number <= 0 {
		trace.Debug("Not submitting job %d, with startTime (%s) before lastRun (%s), that hasn't ended yet. This job has likely already been submitted.", *job.JobId, startTime, lastRun)
		return true
	}

	if len(startJobData.Resources) == 0 {
		trace.Info("Ignoring job %d, which has no resources associated. This job was probably never scheduled.", *job.JobId)
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
