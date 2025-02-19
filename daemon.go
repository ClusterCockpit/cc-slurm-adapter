package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

const (
	PID_FILE_PATH string = "/run/cc-slurm-adapter/daemon.pid"
	IPC_SOCK_PATH        = "/run/cc-slurm-adapter/daemon.sock"
)

var (
	ipcSocket net.Listener
)

func DaemonMain() error {
	trace.Info("Starting Daemon")

	err := SocketOpen()
	defer SocketClose()
	if err != nil {
		return err
	}

	time.Sleep(10 * time.Second)

	return nil
}

func SocketOpen() error {
	trace.Debug("Opening Socket")

	/* First check, if another daemon instance is already running.
	 * If a pid file is found, check if that process is still running.
	 * If it is still running, raise an error. If it is not running,
	 * the pid file is orphaned, and can be deleted. If no pid file exists
	 * we can safely start the daemon immediately. */
	pidFileContent, err := os.ReadFile(PID_FILE_PATH)
	if err == nil {
		trimmedPidFileContent := strings.TrimSpace(string(pidFileContent))
		_, err := os.Stat(fmt.Sprintf("/proc/%s", trimmedPidFileContent))
		if err == nil {
			return fmt.Errorf("Unable to start daemon. Found an already running daemon with PID: %s", trimmedPidFileContent)
		}
	}

	err = os.WriteFile(PID_FILE_PATH, []byte(fmt.Sprintf("%d", os.Getpid())), 0644)
	if err != nil {
		return fmt.Errorf("Unable to create pid file: %w", err)
	}

	_ = os.Remove(IPC_SOCK_PATH)
	ipcSocket, err := net.Listen("unix", IPC_SOCK_PATH)
	if err != nil {
		return fmt.Errorf("Unable to create socket (is there an existing socket with bad permissions?): %w", err)
	}

	_ = ipcSocket

	return nil
}

func SocketClose() {
	trace.Debug("Closing Socket")

	/* While we can handle orphaned pid files and sockets,
	 * we should clean them up after we're done. */
	_ = os.Remove(PID_FILE_PATH)
	_ = os.Remove(IPC_SOCK_PATH)
}
