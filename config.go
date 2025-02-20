package main

import (
	"os"
	"fmt"
	"encoding/json"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

const (
	DEFAULT_CONFIG_PATH string     = "/etc/cc-slurm-adapter.json"

	DEFAULT_PID_FILE_PATH          = "/run/cc-slurm-adapter/daemon.pid"
	DEFAULT_IPC_SOCK_PATH          = "/run/cc-slurm-adapter/daemon.sock"
	DEFAULT_SLURM_POLL_SECONDS int = 60
)

var (
	Config ProgramConfig
)

type ProgramConfig struct {
	PidFilePath string   `json:"pidFilePath"`
	IpcSockPath string   `json:"ipcSocketPath"`
	SlurmPollSeconds int `json:"slurmPollSeconds"`
}

func LoadConfig(configPath string) error {
	if configPath == "" {
		configPath = DEFAULT_CONFIG_PATH
	}

	// default values
	newConf := ProgramConfig{
		PidFilePath: DEFAULT_PID_FILE_PATH,
		IpcSockPath: DEFAULT_IPC_SOCK_PATH,
		SlurmPollSeconds: DEFAULT_SLURM_POLL_SECONDS,
	}

	fileContents, err := os.ReadFile(configPath)
	if err != nil {
		trace.Warnf("Unable to read config file, using default values: %s", err)
	}

	err = json.Unmarshal(fileContents, newConf)
	if err != nil {
		return fmt.Errorf("Unable to parse Config JSON: %w", err)
	}

	Config = newConf
	return nil
}
