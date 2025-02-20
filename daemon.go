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
	ipcSocket           net.Listener
	db                  *sql.DB

	incompleteStartJobs []StartJob
	pendingStartJobs    []StartJob
	pendingStopJobs     []StopJob
)

func DaemonMain() error {
	trace.Info("Starting Daemon")

	err := daemonInit()
	defer daemonQuit()
	if err != nil {
		return fmt.Errorf("Unable to initialize Daemon: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		trace.Debug("Received signal, shutting down...")
		cancel()
		ipcSocket.Close()
	}()

	running := true
	for running {
		unixListener, _ := ipcSocket.(*net.UnixListener)
		unixListener.SetDeadline(time.Now().Add(time.Duration(Config.SlurmPollSeconds) * time.Second))

		trace.Debug("socket.Accept()")
		con, err := ipcSocket.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				trace.Debug("TIMEOUT")
				continue
			}

			select {
			case <-ctx.Done():
				trace.Debug("Cancelling Accept() via Signal")
				running = false
			default:
				trace.Debugf("ERROR: %s", err)
			}
		} else {
			trace.Debug("Accept successful")
			msg, err := connectionReadAll(con)
			if err != nil {
				return fmt.Errorf("Failed to process message: %w", err)
			}
			trace.Debugf("%s\n", msg)
			con.Close()
		}
	}

	return nil
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
		return fmt.Errorf("Unable to create socket (is there an existing socket with bad permissions?): %w", err)
	}

	/* Init Database connection */
	trace.Debugf("Opening database: %s", Config.DbPath)
	db, err = sql.Open("sqlite3", Config.DbPath)
	if err != nil {
		return fmt.Errorf("Unable to open database: %w", err)
	}

	/* Assert required tables exist in database. */
	trace.Debugf("Assert required tables exist")
	err = createDbTables()
	if err != nil {
		return fmt.Errorf("Unable to create tables in database: %w", err)
	}

	trace.Debugf("Initialization complete")
	return nil
}

func createDbTables() error {
	/* Three main tables exist currently:
	 * - incomplete_start_jobs (jobs requiring more info from Slurm)
	 * - pending_start_jobs (jobs which haven't been submitted to cc-backend REST yet)
	 * - pending_stop_jobs (jobs which haven't been submitted to cc-backend REST yet) */

	incomplete_start_jobs_schema := `
	CREATE TABLE IF NOT EXISTS incomplete_start_jobs (
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

	_, err := db.Exec(incomplete_start_jobs_schema)
	if err != nil {
		return fmt.Errorf("Unable to create table: %w", err)
	}
	
	pending_start_jobs_schema := `
	CREATE TABLE IF NOT EXISTS incomplete_jobs (
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

	_, err = db.Exec(pending_start_jobs_schema)
	if err != nil {
		return fmt.Errorf("Unable to create table: %w", err)
	}

	pending_stop_jobs_schema := `
	CREATE TABLE IF NOT EXISTS pending_stop_jobs (
	  job_id INTEGER PRIMARY_KEY NOT NULL,
	  cluster VARCHAR(255) NOT NULL,
	  stop_time INTEGER NOT NULL,
	  state VARCHAR(255) NOT NULL,
	);`

	_, err = db.Exec(pending_stop_jobs_schema)
	if err != nil {
		return fmt.Errorf("Unable to create table: %w", err)
	}

	return nil
}

func daemonQuit() {
	trace.Debug("Closing Socket")

	/* Deinit Database connection */
	if db != nil {
		db.Close()
	}

	/* While we can handle orphaned pid files and sockets,
	 * we should clean them up after we're done. */
	if ipcSocket != nil {
		ipcSocket.Close()
	}
	os.Remove(Config.PidFilePath)
	os.Remove(Config.IpcSockPath)
}

func connectionReadAll(con net.Conn) (string, error) {
	message := ""
	block := make([]byte, 512)

	for {
		bytes, err := con.Read(block)
		if err != nil && err != io.EOF {
			return message, fmt.Errorf("Failed to read bytes on Unix Socket: %w", err)
		}
		message += string(block[:bytes])
		if err == io.EOF {
			break
		}
	}

	return message, nil
}
