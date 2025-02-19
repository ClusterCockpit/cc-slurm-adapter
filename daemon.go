package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
	"context"
	"os/signal"
	"syscall"

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
		unixListener.SetDeadline(time.Now().Add(1 * time.Second))

		trace.Debug("socket.Accept()")
		con, err := ipcSocket.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				trace.Debug("Cancelling Accept() via Signal")
				running = false
			default:
				trace.Debugf("ERROR: %s", err)
			}
		} else {
			trace.Debugf("Accept successful")
			con.Close()
		}
	}

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

	os.Remove(IPC_SOCK_PATH)
	ipcSocket, err = net.Listen("unix", IPC_SOCK_PATH)
	if err != nil {
		return fmt.Errorf("Unable to create socket (is there an existing socket with bad permissions?): %w", err)
	}

	return nil
}

func SocketClose() {
	trace.Debug("Closing Socket")

	/* While we can handle orphaned pid files and sockets,
	 * we should clean them up after we're done. */
	ipcSocket.Close()
	os.Remove(PID_FILE_PATH)
	os.Remove(IPC_SOCK_PATH)
}
