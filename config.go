package main

import (
	"os"
	"encoding/json"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

const (
	DEFAULT_CONFIG_PATH string     = "/etc/cc-slurm-adapter.json"

	DEFAULT_PID_FILE_PATH          = "/run/cc-slurm-adapter/daemon.pid"
	DEFAULT_IPC_SOCK_PATH          = "/run/cc-slurm-adapter/daemon.sock"
	DEFAULT_DB_PATH                = "/var/lib/cc-slurm-adapter/jobs.db"
	DEFAULT_SLURM_POLL_SECONDS int = 60
)

var (
	Config ProgramConfig
)

type ProgramConfig struct {
	PidFilePath string   `json:"pidFilePath"`
	IpcSockPath string   `json:"ipcSocketPath"`
	DbPath string        `json:"dbPath"`
	SlurmPollSeconds int `json:"slurmPollSeconds"`
}

func LoadConfig(configPath string) {
	if configPath == "" {
		configPath = DEFAULT_CONFIG_PATH
	}

	// default values
	newConf := ProgramConfig{
		PidFilePath: DEFAULT_PID_FILE_PATH,
		IpcSockPath: DEFAULT_IPC_SOCK_PATH,
		DbPath: DEFAULT_DB_PATH,
		SlurmPollSeconds: DEFAULT_SLURM_POLL_SECONDS,
	}

	fileContents, err := os.ReadFile(configPath)
	if err != nil {
		trace.Warnf("Unable to read config file, using default values: %s", err)
	} else {
		err = json.Unmarshal(fileContents, newConf)
		if err != nil {
			trace.Fatalf("Unable to parse Config JSON: %s", err)
		}
	}

	Config = newConf
}
