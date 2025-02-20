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

type StopJob struct {
	JobId 	  int64	           `json:"jobId" db:"job_id"`
	Cluster   string           `json:"cluster" db:"cluster"`
	StartTime int64            `json:"startTime" db:"start_time"`
	State     schema.JobState  `json:"jobState" db:"state"`
	StopTime  int64            `json:"stopTime" db:"stop_time"`
}

var (
	ipcSocket        net.Listener
	db               *sql.DB

	incompleteJobs   []schema.BaseJob
	pendingStartJobs []schema.BaseJob
	pendingStopJobs  []StopJob
)

func DaemonMain() error {
	trace.Info("Starting Daemon")

	err := DaemonInit()
	defer DaemonQuit()
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
			msg, err := ConnectionReadAll(con)
			if err != nil {
				return fmt.Errorf("Failed to process message: %w", err)
			}
			trace.Debugf("%s\n", msg)
			con.Close()
		}
	}

	return nil
}

func DaemonInit() error {
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

	return nil
}

func DaemonQuit() {
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

func ConnectionReadAll(con net.Conn) (string, error) {
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
