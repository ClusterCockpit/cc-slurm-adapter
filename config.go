package main

import (
	"os"
	"encoding/json"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

const (
	DEFAULT_CONFIG_PATH string      = "/etc/cc-slurm-adapter/config.json"

	DEFAULT_PID_FILE_PATH           = "/run/cc-slurm-adapter/daemon.pid"
	DEFAULT_IPC_SOCK_PATH           = "/run/cc-slurm-adapter/daemon.sock"
	DEFAULT_LAST_RUN_PATH           = "/var/lib/cc-slurm-adapter/lastrun"
	DEFAULT_SLURM_POLL_INTERVAL int = 60
	DEFAULT_SLURM_QUERY_DELAY       = 1
	DEFAULT_SLURM_QUERY_MAX_SPAN    = 7 * 24 * 60 * 60
	DEFAULT_SLURM_MAX_RETRIES       = 10

	DEFAULT_NATS_SUBJECT string     = "jobs"
	DEFAULT_NATS_PORT uint16        = 4222

	DEFAULT_CC_POLL_INTERVAL int    = 300
)

var (
	Config ProgramConfig
)

type ProgramConfig struct {
	PidFilePath string      `json:"pidFilePath"`
	IpcSockPath string      `json:"ipcSocketPath"`
	LastRunPath string      `json:"lastRunPath"`
	SlurmPollInterval int   `json:"slurmPollInterval"`
	SlurmQueryDelay int     `json:"slurmQueryDelay"`
	SlurmQueryMaxSpan int   `json:"slurmQueryMaxSpan"`
	SlurmMaxRetries int     `json:"slurmMaxRetries"`
	CcRestUrl string        `json:"ccRestUrl"`
	CcRestJwt string        `json:"ccRestJwt"`
	CcPollInterval int      `json:"ccPollInterval"`
	NvidiaPciAddrs []string `json:"nvidiaPciAddrs"`
	NatsServer string       `json:"natsServer"`
	NatsPort uint16         `json:"natsPort"`
	NatsSubject string      `json:"natsSubject"`
	NatsUser string         `json:"natsUser"`
	NatsPassword string     `json:"natsPassword"`
	NatsCredsFile string    `json:"natsCredsFile"`
	NatsNKeySeedFile string `json:"natsNKeySeedFile"`
}

func LoadConfig(configPath string) {
	if configPath == "" {
		configPath = DEFAULT_CONFIG_PATH
	}

	// default values
	newConf := ProgramConfig{
		PidFilePath: DEFAULT_PID_FILE_PATH,
		IpcSockPath: DEFAULT_IPC_SOCK_PATH,
		LastRunPath: DEFAULT_LAST_RUN_PATH,
		CcPollInterval: DEFAULT_CC_POLL_INTERVAL,
		SlurmPollInterval: DEFAULT_SLURM_POLL_INTERVAL,
		SlurmQueryDelay: DEFAULT_SLURM_QUERY_DELAY,
		SlurmQueryMaxSpan: DEFAULT_SLURM_QUERY_MAX_SPAN,
		SlurmMaxRetries: DEFAULT_SLURM_MAX_RETRIES,
		NvidiaPciAddrs: make([]string, 0),
		NatsPort: DEFAULT_NATS_PORT,
		NatsSubject: DEFAULT_NATS_SUBJECT,
	}

	fileContents, err := os.ReadFile(configPath)
	if err != nil {
		trace.Info("Unable to read config file, using default values: %s", err)
	} else {
		err = json.Unmarshal(fileContents, &newConf)
		if err != nil {
			trace.Fatal("Unable to parse Config JSON: %s", err)
		}
	}

	if newConf.SlurmPollInterval < 1 {
		// using 0 would yield active waiting, so avoid that
		trace.Warn("config: slurmPollInterval %d < 1: Setting to 1", newConf.SlurmPollInterval)
		newConf.SlurmPollInterval = 1
	}

	if newConf.SlurmQueryDelay < 1 {
		// using 0 would yield active waiting, so avoid that
		trace.Warn("config: slurmQueryDelay %d < 1: Setting to 1", newConf.SlurmQueryDelay)
		newConf.SlurmQueryDelay = 1
	}

	Config = newConf
}
