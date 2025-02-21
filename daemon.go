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

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
	"github.com/ClusterCockpit/cc-backend/pkg/schema"

	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

type StartJob struct {
	schema.BaseJob
	StartTime time.Time
}

type StopJob struct {
	JobId 	  int64	           `json:"jobId"     db:"job_id"`
	Cluster   string           `json:"cluster"   db:"cluster"`
	StartTime int64            `json:"startTime" db:"start_time"`
	State     schema.JobState  `json:"jobState"  db:"state"`
	StopTime  int64            `json:"stopTime"  db:"stop_time"`
}

var (
	ipcSocket          net.Listener
	db                  *sql.DB

	incompleteStartJobs []uint32
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

	pollEventChan := make(chan struct{})
	pollEventTimer := time.AfterFunc(time.Duration(Config.SlurmPollSeconds) * time.Second, func() { pollEventChan <- struct{}{}})

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
		select {
		case <-signalCtx.Done():
			trace.Info("Daemon terminating")
			return nil
		case msg := <-ipcSocketChan:
			err = jobPrologEpilogNotify(msg)
			if err != nil {
				trace.Error("Unable to parse IPC message: %s", err)
			}
		case <-pollEventChan:
			trace.Debug("Timer triggered Slurm polling")
			pollEventTimer.Reset(time.Duration(Config.SlurmPollSeconds) * time.Second)
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
			/* Run the connection handling asynchronously. */
			defer conn.Close()
			trace.Debug("Receiving IPC message")
			msg, err := connectionReadAll(conn)
			if err != nil {
				trace.Error("Failed to receive IPC message over Unix Socket: %s", err)
				return
			}
			chn <- msg
		}()
	}
}

func daemonInit() error {
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

	/* Init Database connection */
	trace.Debug("Opening database: %s", Config.DbPath)
	db, err = sql.Open("sqlite3", Config.DbPath)
	if err != nil {
		ipcSocket.Close()
		os.Remove(Config.IpcSockPath)
		os.Remove(Config.PidFilePath)
		return fmt.Errorf("Unable to open database: %w", err)
	}

	/* Assert required tables exist in database. */
	trace.Debug("Assert required tables exist")
	err = createDbTables()
	if err != nil {
		db.Close()
		ipcSocket.Close()
		os.Remove(Config.IpcSockPath)
		os.Remove(Config.PidFilePath)
		return fmt.Errorf("Unable to create tables in database: %w", err)
	}

	trace.Debug("Initialization complete")
	return nil
}

func jobPrologEpilogNotify(ipcMsg []byte) error {
	/* The message received contains a JSON, which contains all relevant
	 * environment variables from here:
	 * https://slurm.schedmd.com/prolog_epilog.html.
	 * Please keep in mind that some of the environment variables are only
	 * available in TaskProlog/TaskEpilog. However, we only run in slurmctld
	 * context, so only their appropriate values are available. */
	var env PrologEpilogSlurmctldEnv
	err := json.Unmarshal(ipcMsg, &env)
	if err != nil {
		return fmt.Errorf("Unable to parse IPC message as JSON (%w). Either a 3rd party is writing to our Unix socket or there is a bug in the IPC procotocl.", err)
	}

	if env.SLURM_SCRIPT_CONTEXT == "prolog_slurmctld" {
		return jobPrologNotify(env)
	} else if env.SLURM_SCRIPT_CONTEXT == "epilog_slurmctld" {
		return jobEpilogNotify(env)
	} else {
		return fmt.Errorf("Invalid/unsupported SLURM_SCRIPT_CONTEXT: %s. Only prolog_slurmctld and epilog_slurmctld is supported")
	}
}

func jobPrologNotify(env PrologEpilogSlurmctldEnv) error {
	trace.Debug("Handle Notify Prolog")
//	nnodes, err := strconv.Atoi(env.SLURM_JOB_NUM_NODES)
//	if err != nil {
//		return fmt.Errorf("Unable to convert SLURM_JOB_NUM_NODES to integer: %w", err)
//	}
//	ncpus_per_node, err := strconv.Atoi(env.SLURM_JOB_CPUS_PER_NODE)
//	if err != nil {
//		return fmt.Errorf("Unable to convert SLURM_JOB_CPUS_PER_NODE to integer: %w", err)
//	}
//
	//newJob := BaseJob{
	//	Cluster: env.SLURM_CLUSTER_NAME,
	//	Partition: env.SLURM_JOB_PARTITION,
	//	Project: env.SLURM_JOB_ACCOUNT,
	//	User: env.SLURM_JOB_USER,
	//	// State is not available in PrEp
	//	// Resources is not available in PrEp
	//	ArrayJobId: env.SLURM_ARRAY_JOB_ID,
	//	// Walltime is not available in PrEp
	//	JobID: env.SLURM_JOB_ID,
	//	// Exclusive is not available in PrEp
	//	StartTime: env.SLURM_JOB_START_TIME,
	//	NumNodes: nnodes,
	//	NumHWThreads: ncpus_per_node * nnodes,
	//}

	//return jobAddToIncomplete(newJob)
	return nil
}

func jobEpilogNotify(env PrologEpilogSlurmctldEnv) error {
	trace.Debug("Handle Notify Epilog")
	return nil
}

func createDbTables() error {
	/* Two main tables exist currently:
	 * - pending_start_jobs (jobs which haven't been submitted to cc-backend REST yet)
	 * - pending_stop_jobs (jobs which haven't been submitted to cc-backend REST yet) */
	pending_start_jobs_schema := `
	CREATE TABLE IF NOT EXISTS pending_start_jobs (
	  cluster VARCHAR(255) NOT NULL,
	  sub_cluster VARCHAR(255) NOT NULL,
	  partition VARCHAR(255) NOT NULL,
	  project VARCHAR(255) NOT NULL,
	  user VARCHAR(255) NOT NULL,
	  state VARCHAR(255) NOT NULL,
	  array_job_id INTEGER NOT NULL,
	  job_id INTEGER PRIMARY_KEY NOT NULL,
	  num_nodes INTEGER NOT NULL,
	  num_hwthreads INTEGER NOT NULL,
	  resources TEXT NOT NULL,
	  exclusive INTEGER NOT NULL,
	  start_time INTEGER NOT NULL,
	  walltime INTEGER NOT NULL,
	  job_script VARCHAR(255) NOT NULL,
	  job_name VARCHAR(255) NOT NULL,
	  slurm_info TEXT NOT NULL
	);`

	_, err := db.Exec(pending_start_jobs_schema)
	if err != nil {
		return fmt.Errorf("Unable to create table: %w", err)
	}

	pending_stop_jobs_schema := `
	CREATE TABLE IF NOT EXISTS pending_stop_jobs (
	  job_id INTEGER PRIMARY_KEY NOT NULL,
	  cluster VARCHAR(255) NOT NULL,
	  stop_time INTEGER NOT NULL,
	  state VARCHAR(255) NOT NULL
	);`

	_, err = db.Exec(pending_stop_jobs_schema)
	if err != nil {
		return fmt.Errorf("Unable to create table: %w", err)
	}

	return nil
}

func daemonQuit() {
	/* Deinit Database connection */
	trace.Debug("Closing Database")
	db.Close()

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

func connectionReadAll(con net.Conn) ([]byte, error) {
	message := make([]byte, 0)
	block := make([]byte, 512)

	for {
		bytes, err := con.Read(block)
		if err != nil && err != io.EOF {
			return message, fmt.Errorf("Failed to read bytes on Unix Socket: %w", err)
		}
		message = append(message, block[:bytes]...)
		if err == io.EOF {
			break
		}
	}

	return message, nil
}
