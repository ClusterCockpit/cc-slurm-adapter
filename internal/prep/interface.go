package prep

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/config"
	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

var (
	prepSocket net.Listener
)

func ServerInit(ctx context.Context, prepEventChan chan []byte) error {
	var err error

	sockType, sockAddr := config.GetProtoAddr(config.Config.PrepSockListenPath)
	if sockType == "unix" {
		os.Remove(sockAddr)
	}

	prepSocket, err = net.Listen(sockType, sockAddr)
	if err != nil {
		return fmt.Errorf("Unable to create socket (is there an existing socket with bad permissions?): %w", err)
	}

	trace.Debug("Listening for PrEp events on '%s:%s'", sockType, sockAddr)

	if sockType == "unix" {
		err = os.Chmod(sockAddr, 0666)
		if err != nil {
			return fmt.Errorf("Failed to set permissions via chmod on PrEp Socket: %w", err)
		}
	}

	go prepSocketListenRoutine(ctx, prepEventChan)
	return nil
}

func ServerQuit() {
	os.Remove(config.Config.PidFilePath)
	sockType, sockAddr := config.GetProtoAddr(config.Config.PrepSockListenPath)
	if sockType == "unix" {
		os.Remove(sockAddr)
	}
	prepSocket.Close()
}

func prepSocketListenRoutine(ctx context.Context, chn chan<- []byte) {
	for {
		conn, err := prepSocket.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				trace.Debug("Cancelling PrEp accept loop")
				return
			default:
				trace.Error("Error while accepting PrEp event: %s", err)
				continue
			}
		}

		go func() {
			// Run the connection handling asynchronously. This allows
			// the socket to accept a new connection almost immediatley.
			defer conn.Close()
			trace.Debug("Receiving PrEp message")
			msg, err := io.ReadAll(conn)
			if err != nil {
				trace.Error("Failed to receive PrEp event: %s", err)
				return
			}
			chn <- msg
		}()
	}
}
