package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/cc"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/config"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/prep"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/slurm"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

var (
	jobEvents  []prep.SlurmctldEnv
	slurmClusters   []string
)

func DaemonMain() error {
	trace.Info("Starting Daemon")

	// Init Signal Handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	signalCtx, signalCancel := context.WithCancel(context.Background())

	// start background routine that handles the Unix Socket
	prepEventChan := make(chan []byte, 1024)

	err := daemonInit(signalCtx, prepEventChan)
	defer daemonQuit()
	if err != nil {
		signalCancel()
		return fmt.Errorf("Unable to initialize Daemon: %w", err)
	}

	queryDelay := time.Duration(config.Config.SlurmQueryDelay) * time.Second

	pollEventInterval := time.Duration(config.Config.SlurmPollInterval) * time.Second
	pollEventTicker := time.NewTicker(queryDelay)
	pollEventFirst := true

	jobEventTimer := time.NewTimer(time.Duration(0))
	<-jobEventTimer.C
	jobEventPending := false

	// Signal Handler definition
	go func() {
		<-signalChan
		trace.Debug("Received signal, shutting down...")

		pollEventTicker.Stop()
		jobEventTimer.Stop()

		// cause Accept() on Unix socket to fail and unblock its routine.
		signalCancel()
	}()

	for {
		// Wait for the following cases:
		// - quit signal
		//   -> cancel loop
		// - PrEp message received (binary invoked with -prolog or -epilog)
		//   -> enqueue job to be queried via 'sacct' shortly after
		// - event timer elapsed
		//   -> query jobs via 'sacct'
		select {
		case <-signalCtx.Done():
			trace.Debug("Daemon terminating")
			return nil
		case msg := <-prepEventChan:
			trace.Debug("Process PrEp message (%d pending messages)", len(prepEventChan))
			err = jobEventEnqueue(msg)
			if err != nil {
				trace.Error("Unable to parse PrEp message: %s", err)
			}

			// We want to quickly poll Slurm after a job notification came in.
			// Though, we have to make sure we don't infinitely reset the timer if the PrEp channel gets spammed.
			// This would otherwise cause the timer to continoulsy get reset and never actually fire.
			if !jobEventPending {
				jobEventTimer.Reset(queryDelay)
				jobEventPending = true
			}
		case <-jobEventTimer.C:
			trace.Info("Job Event timer triggered")
			slurm.SacctCacheClear()
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

			slurm.SacctCacheClear()
			err = cc.CacheUpdate()
			if err != nil {
				trace.Error("Unable to update cc-backend cache. Trying later...")
				break
			}
			err = cc.SyncStats()
			if err != nil {
				trace.Error("Unable to sync stats to cc-backend. Trying later...")
				break
			}
			processSlurmSacctPoll()
			processSlurmSqueuePoll()
			cc.CacheGC()
		}

		trace.Debug("Main loop iteration complete, waiting for next event...")
	}
}

func daemonInit(ctx context.Context, prepEventChan chan []byte) error {
	var err error

	// Assert last_run is writable. That way crash immediately instead after a long delay.
	lastRunSet(lastRunGet())

	// Verify Slurm Permissions
	slurm.CheckPerms()

	// Init Unix Socket
	trace.Debug("Opening Socket")

	// First check, if another daemon instance is already running.
	// If a pid file is found, check if that process is still running.
	// If it is still running, raise an error. If it is not running,
	// the pid file is orphaned, and can be deleted. If no pid file exists
	// we can safely start the daemon immediately.
	pidFileContent, err := os.ReadFile(config.Config.PidFilePath)
	if err == nil {
		trimmedPidFileContent := strings.TrimSpace(string(pidFileContent))
		_, err := os.Stat(fmt.Sprintf("/proc/%s", trimmedPidFileContent))
		if err == nil {
			return fmt.Errorf("Unable to start daemon. Found an already running daemon with PID: %s", trimmedPidFileContent)
		}
	}

	err = os.WriteFile(config.Config.PidFilePath, []byte(fmt.Sprintf("%d", os.Getpid())), 0644)
	if err != nil {
		return fmt.Errorf("Unable to create pid file: %w", err)
	}

	// Get the clusters managed by Slurm
	slurmClusters, err = slurm.GetClusterNames()
	if err != nil {
		return fmt.Errorf("Unable to determine cluster hostnames: %w", err)
	}
	trace.Debug("Detected Slurm clusters: %v", slurmClusters)

	// Init PrEp server
	err = prep.ServerInit(ctx, prepEventChan)
	if err != nil {
		return err
	}

	// Init ClusterCockpit interface
	err = cc.Init(slurmClusters)
	if err != nil {
		return err
	}

	// job events queue initialization
	jobEvents = make([]prep.SlurmctldEnv, 0)

	printWelcome()

	// Init cc job state cache
	trace.Debug("Fetching initial job state from cc-backend")
	err = cc.CacheUpdate()
	if err != nil {
		return fmt.Errorf("Failed to update cc-backend job cache: %w", err)
	}
	return nil
}

func daemonQuit() {
	// While we can handle orphaned pid files and sockets,
	// we should clean them up after we're done.
	// The PID check is also not 100% reliable, since we just
	// check against any process with that PID and not if it
	// actually is the daemon...
	trace.Debug("Closing Socket")
	prep.ServerQuit()
	sockType, sockAddr := config.GetProtoAddr(config.Config.PrepSockListenPath)
	if sockType == "unix" {
		os.Remove(sockAddr)
	}
	os.Remove(config.Config.PidFilePath)
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

func jobEventEnqueue(prepMsg []byte) error {
	// The message received contains a JSON, which contains all relevant
	// environment variables from here:
	// https://slurm.schedmd.com/prolog_epilog.html.
	// Please keep in mind that some of the environment variables are only
	// available in TaskProlog/TaskEpilog. However, we only run in slurmctld
	// context, so only their appropriate values are available.
	var env prep.SlurmctldEnv
	err := json.Unmarshal(prepMsg, &env)
	if err != nil {
		return fmt.Errorf("Unable to parse PrEp message as JSON (%w). Either a 3rd party is writing to our Unix socket or there is a bug in our IPC protocol: '%s'", err, string(prepMsg))
	}

	env.SacctAttempts = 0

	jobEvents = append(jobEvents, env)
	return nil
}

func jobEventsProcess() {
	trace.Debug("jobEventsProcess()")
	newJobEvents := make([]prep.SlurmctldEnv, 0)
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

		job, err := slurm.QueryJob(jobEventCluster, uint32(jobEventId))
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
				trace.Warn("Job (%d) exceeded max query retries of %d. Giving up on job.", jobEventId, config.Config.SlurmMaxRetries)
				continue
			}
		}

		err = cc.SyncJob(*job, false)
		if err != nil {
			trace.Warn("Syncing job (%s, %d) via PrEp hook failed (we will try again later during regular poll): %v", jobEventCluster, jobEventId, err)
		}
	}

	jobEvents = newJobEvents
}

func processSlurmSacctPoll() {
	trace.Debug("processSlurmSacctPoll()")
	lastRun := lastRunGet().Add(time.Duration(-1 * time.Second)) // -1 second for good measure to avoid overlap error
	thisRun := time.Now()

	if lastRun.Add(time.Duration(config.Config.SlurmQueryMaxSpan) * time.Second).Before(thisRun) {
		trace.Warn("sacct was polled %s ago, which is higher than maximum %d. Limiting to maximum. Either we didn't run for a while or haven't run at all. If jobs from the past are missing, increase the maxmimum duration in the configuration. This warning will go away after the next job.", thisRun.Sub(lastRun).Truncate(time.Second).String(), config.Config.SlurmQueryMaxSpan)
		lastRun = thisRun.Add(time.Duration(-config.Config.SlurmQueryMaxSpan) * time.Second)
	}

	// Detect time change (e.g. summer/winter time). If ...
	_, beginOffset := lastRun.Zone()
	_, endOffset := thisRun.Zone()
	if endOffset < beginOffset {
		// If the time has gone backwards, move the begin time stamp backwards accordingly.
		// This way we make sure we pass a correct local time to Slurm, where 'begin'
		// is actually always before 'end'.
		// I am not entirely sure that this works reliably or if Go will correctly
		// handle those changes.
		trace.Warn("Time change detected: Moving last run %d seconds backwards", endOffset-beginOffset)
		lastRun = lastRun.Add(-time.Duration(endOffset-beginOffset) * time.Second)
	}

	for _, cluster := range slurmClusters {
		jobs, err := slurm.QueryJobsTimeRange(cluster, lastRun, thisRun)
		if err != nil {
			trace.Error("Unable to query Slurm for jobs (is Slurm available?): %s", err)
			return
		}

		for _, job := range jobs {
			err = cc.SyncJob(job, false)
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
		jobs, err := slurm.QueryJobsActive(cluster)
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
		for jobId, cachedJobState := range cc.JobCache[cluster] {
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
			job, err := slurm.QueryJob(cluster, uint32(jobId))
			if err != nil {
				trace.Error("Failed to query cc-backend's stale job from Slurm: %v", err)
				continue
			}

			trace.Warn("Stale job state is: %s", string(*job.State.Current))

			err = cc.SyncJob(*job, false)
			if err != nil {
				trace.Error("Failed to sync cc-backend's stale job from Slurm: %v", err)
			}
		}
	}
}

func lastRunGet() time.Time {
	statInfo, err := os.Stat(config.Config.LastRunPath)
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
	f, err := os.OpenFile(config.Config.LastRunPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		trace.Fatal("Unable to set time of last run: %s", err)
	}

	f.Close()

	err = os.Chtimes(config.Config.LastRunPath, timeStamp, timeStamp)
	if err != nil {
		trace.Fatal("Unable to set time of last run: %s", err)
	}
}
