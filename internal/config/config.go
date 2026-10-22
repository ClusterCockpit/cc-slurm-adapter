package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/trace"

	"github.com/ClusterCockpit/cc-lib/v2/fleet"
)

const (
	DEFAULT_CONFIG_PATH string = "/etc/cc-slurm-adapter/config.json"

	DEFAULT_PID_FILE_PATH            = "/run/cc-slurm-adapter/daemon.pid"
	DEFAULT_PREP_SOCK_PATH           = "/run/cc-slurm-adapter/daemon.sock"
	DEFAULT_LAST_RUN_PATH            = "/var/lib/cc-slurm-adapter/lastrun"
	DEFAULT_SLURM_POLL_INTERVAL  int = 60
	DEFAULT_SLURM_QUERY_DELAY        = 1
	DEFAULT_SLURM_QUERY_MAX_SPAN     = 7 * 24 * 60 * 60
	DEFAULT_SLURM_MAX_RETRIES        = 10

	DEFAULT_NATS_SUBJECT string = "jobs"
	DEFAULT_NATS_PORT    uint16 = 4222

	DEFAULT_CC_POLL_INTERVAL   int  = 6 * 60 * 60
	DEFAULT_CC_REST_SUBMIT_JOB bool = true

	DEFAULT_FLEET_HEARTBEAT_INTERVAL   int = 30
	DEFAULT_FLEET_CONFIG_POLL_INTERVAL int = 60
	DEFAULT_FLEET_CONFIG_CACHE_PATH        = "/var/lib/cc-slurm-adapter/fleet-config.json"
)

var (
	Config ProgramConfig

	// Holds the local config file, so that the effective configuration can be
	// rebuilt whenever a new fleet configuration arrives.
	overlay fleet.Overlay[ProgramConfig]
)

type ProgramConfig struct {
	PidFilePath             string              `json:"pidFilePath"`
	PrepSockListenPath      string              `json:"prepSockListenPath"`
	PrepSockConnectPath     string              `json:"prepSockConnectPath"`
	LastRunPath             string              `json:"lastRunPath"`
	SlurmPollInterval       int                 `json:"slurmPollInterval"`
	SlurmQueryDelay         int                 `json:"slurmQueryDelay"`   // TODO give this a better name
	SlurmQueryMaxSpan       int                 `json:"slurmQueryMaxSpan"` // TODO change the name of this
	SlurmMaxRetries         int                 `json:"slurmMaxRetries"`
	CcRestUrl               string              `json:"ccRestUrl"`
	CcRestJwt               string              `json:"ccRestJwt"`
	CcRestSubmitJobs        bool                `json:"ccRestSubmitJobs"`
	CcPollInterval          int                 `json:"ccPollInterval"`
	GpuPciAddrs             map[string][]string `json:"gpuPciAddrs"`
	IgnoreHosts             string              `json:"ignoreHosts"`
	NatsServer              string              `json:"natsServer"`
	NatsPort                uint16              `json:"natsPort"`
	NatsSubject             string              `json:"natsSubject"`
	NatsUser                string              `json:"natsUser"`
	NatsPassword            string              `json:"natsPassword"`
	NatsCredsFile           string              `json:"natsCredsFile"`
	NatsNKeySeedFile        string              `json:"natsNKeySeedFile"`
	FleetEnabled            bool                `json:"fleetEnabled"`
	FleetCluster            string              `json:"fleetCluster"`
	FleetHeartbeatInterval  int                 `json:"fleetHeartbeatInterval"`
	FleetHeartbeatSubject   string              `json:"fleetHeartbeatSubject"`
	FleetConfigPollInterval int                 `json:"fleetConfigPollInterval"`
	FleetConfigCachePath    string              `json:"fleetConfigCachePath"`
}

// Keys which are only ever taken from the local config file. A fleet
// configuration cannot override them, either because they are needed to reach
// cc-backend in the first place or because they are host local paths.
var localOnlyKeys = []string{
	"pidFilePath",
	"prepSockListenPath",
	"prepSockConnectPath",
	"lastRunPath",
	"ccRestUrl",
	"ccRestJwt",
	"fleetEnabled",
	"fleetCluster",
	"fleetHeartbeatInterval",
	"fleetHeartbeatSubject",
	"fleetConfigPollInterval",
	"fleetConfigCachePath",
}

// Keys which may be set by a fleet configuration, but only take effect at
// daemon startup (they are used to establish the NATS connection).
var startupOnlyKeys = []string{
	"natsServer",
	"natsPort",
	"natsUser",
	"natsPassword",
	"natsCredsFile",
	"natsNKeySeedFile",
}

func defaultConfig() ProgramConfig {
	return ProgramConfig{
		PidFilePath:             DEFAULT_PID_FILE_PATH,
		PrepSockListenPath:      DEFAULT_PREP_SOCK_PATH,
		PrepSockConnectPath:     DEFAULT_PREP_SOCK_PATH,
		LastRunPath:             DEFAULT_LAST_RUN_PATH,
		CcPollInterval:          DEFAULT_CC_POLL_INTERVAL,
		CcRestSubmitJobs:        DEFAULT_CC_REST_SUBMIT_JOB,
		SlurmPollInterval:       DEFAULT_SLURM_POLL_INTERVAL,
		SlurmQueryDelay:         DEFAULT_SLURM_QUERY_DELAY,
		SlurmQueryMaxSpan:       DEFAULT_SLURM_QUERY_MAX_SPAN,
		SlurmMaxRetries:         DEFAULT_SLURM_MAX_RETRIES,
		GpuPciAddrs:             make(map[string][]string),
		NatsPort:                DEFAULT_NATS_PORT,
		NatsSubject:             DEFAULT_NATS_SUBJECT,
		FleetHeartbeatInterval:  DEFAULT_FLEET_HEARTBEAT_INTERVAL,
		FleetConfigPollInterval: DEFAULT_FLEET_CONFIG_POLL_INTERVAL,
		FleetConfigCachePath:    DEFAULT_FLEET_CONFIG_CACHE_PATH,
	}
}

func Load(configPath string) {
	orgConfigPath := configPath
	if configPath == "" {
		configPath = DEFAULT_CONFIG_PATH
	}

	fileContents, err := os.ReadFile(configPath)
	if err != nil {
		if orgConfigPath == "" {
			trace.Info("Unable to read config file, using default values: %v", err)
		} else {
			trace.Fatal("Unable to read config file: %v", err)
		}
		fileContents = nil
	}

	localOverlay := newOverlay(fileContents)
	newConf, err := localOverlay.Build(nil)
	if err != nil {
		trace.Fatal("Error in config file: %v", err)
	}

	overlay = localOverlay
	Config = newConf
}

// newOverlay creates the overlay, which builds the effective configuration:
// defaults, overlayed by the local config file, overlayed by the fleet
// configuration (if any). Local-only keys are always taken from the local
// config file.
func newOverlay(local []byte) fleet.Overlay[ProgramConfig] {
	return fleet.Overlay[ProgramConfig]{
		Defaults:    defaultConfig,
		Local:       local,
		LocalOnly:   localOnlyKeys,
		StartupOnly: startupOnlyKeys,
		Validate:    validate,
	}
}

func validate(conf *ProgramConfig) error {
	if conf.SlurmPollInterval < 1 {
		// using 0 would yield active waiting, so avoid that
		trace.Warn("config: slurmPollInterval %d < 1: Setting to 1", conf.SlurmPollInterval)
		conf.SlurmPollInterval = 1
	}

	if conf.SlurmQueryDelay < 1 {
		// using 0 would yield active waiting, so avoid that
		trace.Warn("config: slurmQueryDelay %d < 1: Setting to 1", conf.SlurmQueryDelay)
		conf.SlurmQueryDelay = 1
	}

	if conf.FleetHeartbeatInterval < 1 {
		trace.Warn("config: fleetHeartbeatInterval %d < 1: Setting to 1", conf.FleetHeartbeatInterval)
		conf.FleetHeartbeatInterval = 1
	}

	if conf.FleetConfigPollInterval < 1 {
		trace.Warn("config: fleetConfigPollInterval %d < 1: Setting to 1", conf.FleetConfigPollInterval)
		conf.FleetConfigPollInterval = 1
	}

	for hostnameRegexp := range conf.GpuPciAddrs {
		_, err := regexp.Compile(hostnameRegexp)
		if err != nil {
			return fmt.Errorf("Invalid regex '%s': %w", hostnameRegexp, err)
		}
	}

	if len(conf.IgnoreHosts) > 0 {
		_, err := regexp.Compile(conf.IgnoreHosts)
		if err != nil {
			return fmt.Errorf("Invalid regex '%s': %w", conf.IgnoreHosts, err)
		}
	}

	return nil
}

// ApplyFleet rebuilds the effective configuration from the local config file
// and the given fleet configuration blob. An empty blob means that no fleet
// configuration applies, i.e. the local config file is used as is.
//
// If startup is false, startup-only keys keep their running values and a
// warning names those, which would have changed.
//
// On error the current configuration is kept.
func ApplyFleet(blob []byte, startup bool) error {
	res, err := overlay.Apply(Config, blob, startup)
	if err != nil {
		return err
	}

	if !res.Config.CcRestSubmitJobs && len(res.Config.NatsServer) == 0 {
		return fmt.Errorf("Either NATS or REST job submission must be enabled.")
	}

	Config = res.Config
	res.Log()
	return nil
}

func GetProtoAddr(s string) (string, string) {
	// Config.PrepSock{Listen,Connect}Path allowed formats:
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
