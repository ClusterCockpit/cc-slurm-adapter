package main

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

const (
	DEFAULT_CONFIG_PATH string = "/etc/cc-slurm-adapter/config.json"

	DEFAULT_PID_FILE_PATH            = "/run/cc-slurm-adapter/daemon.pid"
	DEFAULT_IPC_SOCK_PATH            = "/run/cc-slurm-adapter/daemon.sock"
	DEFAULT_LAST_RUN_PATH            = "/var/lib/cc-slurm-adapter/lastrun"
	DEFAULT_SLURM_POLL_INTERVAL  int = 60
	DEFAULT_SLURM_QUERY_DELAY        = 1
	DEFAULT_SLURM_QUERY_MAX_SPAN     = 7 * 24 * 60 * 60
	DEFAULT_SLURM_MAX_RETRIES        = 10

	DEFAULT_NATS_SUBJECT string = "jobs"
	DEFAULT_NATS_PORT    uint16 = 4222

	DEFAULT_CC_POLL_INTERVAL int = 6 * 60 * 60
)

var (
	Config ProgramConfig
)

type ProgramConfig struct {
	PidFilePath       string              `json:"pidFilePath"`
	IpcSockListenPath string              `json:"ipcSockListenPath"`
	IpcSockConnectPath string             `json:"ipcSockConnectPath"`
	LastRunPath       string              `json:"lastRunPath"`
	SlurmPollInterval int                 `json:"slurmPollInterval"`
	SlurmQueryDelay   int                 `json:"slurmQueryDelay"`   // TODO give this a better name
	SlurmQueryMaxSpan int                 `json:"slurmQueryMaxSpan"` // TODO change the name of this
	SlurmMaxRetries   int                 `json:"slurmMaxRetries"`
	CcRestUrl         string              `json:"ccRestUrl"`
	CcRestJwt         string              `json:"ccRestJwt"`
	CcPollInterval    int                 `json:"ccPollInterval"`
	GpuPciAddrs       map[string][]string `json:"gpuPciAddrs"`
	IgnoreHosts       string              `json:"ignoreHosts"`
	NatsServer        string              `json:"natsServer"`
	NatsPort          uint16              `json:"natsPort"`
	NatsSubject       string              `json:"natsSubject"`
	NatsUser          string              `json:"natsUser"`
	NatsPassword      string              `json:"natsPassword"`
	NatsCredsFile     string              `json:"natsCredsFile"`
	NatsNKeySeedFile  string              `json:"natsNKeySeedFile"`
}

func LoadConfig(configPath string) {
	orgConfigPath := configPath
	if configPath == "" {
		configPath = DEFAULT_CONFIG_PATH
	}

	// default values
	newConf := ProgramConfig{
		PidFilePath:       DEFAULT_PID_FILE_PATH,
		IpcSockListenPath: DEFAULT_IPC_SOCK_PATH,
		IpcSockConnectPath: DEFAULT_IPC_SOCK_PATH,
		LastRunPath:       DEFAULT_LAST_RUN_PATH,
		CcPollInterval:    DEFAULT_CC_POLL_INTERVAL,
		SlurmPollInterval: DEFAULT_SLURM_POLL_INTERVAL,
		SlurmQueryDelay:   DEFAULT_SLURM_QUERY_DELAY,
		SlurmQueryMaxSpan: DEFAULT_SLURM_QUERY_MAX_SPAN,
		SlurmMaxRetries:   DEFAULT_SLURM_MAX_RETRIES,
		GpuPciAddrs:       make(map[string][]string),
		NatsPort:          DEFAULT_NATS_PORT,
		NatsSubject:       DEFAULT_NATS_SUBJECT,
	}

	fileContents, err := os.ReadFile(configPath)
	if err != nil {
		if orgConfigPath == "" {
			trace.Info("Unable to read config file, using default values: %v", err)
		} else {
			trace.Fatal("Unable to read config file: %v", err)
		}
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

	for hostnameRegexp, _ := range newConf.GpuPciAddrs {
		_, err = regexp.Compile(hostnameRegexp)
		if err != nil {
			trace.Fatal("Error in config file: Invalid regex '%s': %v", hostnameRegexp, err)
		}
	}

	if len(Config.IgnoreHosts) > 0 {
		_, err = regexp.Compile(Config.IgnoreHosts)
		if err != nil {
			trace.Fatal("Error in config file: Invalid regex '%s': %v", Config.IgnoreHosts, err)
		}
	}

	Config = newConf
}

func GetProtoAddr(s string) (string, string) {
	// Config.IpcSock{Listen,Connect}Path allowed formats:
	// /var/lib/path_to_unix_socket
	// unix:/var/lib/path_to_unix_socket
	// tcp:127.0.0.1:12345
	// tcp:0.0.0.0:12345
	// tcp:[::1]:12345
	// tcp:[::]:12345
	// tcp::12345

	addrElements := strings.SplitN(s, ":", 2)
	if len(addrElements) == 0 {
		return "", ""
	} else if len(addrElements) == 1 {
		return "unix", s
	} else {
		return strings.ToLower(addrElements[0]), addrElements[1]
	}
}
