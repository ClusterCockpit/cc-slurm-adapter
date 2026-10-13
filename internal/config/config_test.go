package config

import (
	"reflect"
	"slices"
	"testing"
)

const testLocalConfig = `{
	"ccRestUrl": "http://local:8080",
	"ccRestJwt": "local-jwt",
	"ignoreHosts": "^local",
	"slurmPollInterval": 30,
	"natsServer": "nats-local",
	"gpuPciAddrs": {"^a": ["00:01"], "^b": ["00:02"]},
	"fleetEnabled": true
}`

func setupLocal(t *testing.T) {
	t.Helper()
	conf, err := build([]byte(testLocalConfig), nil)
	if err != nil {
		t.Fatalf("build local config: %v", err)
	}
	localConfig = []byte(testLocalConfig)
	Config = conf
}

func TestApplyFleetOverlay(t *testing.T) {
	setupLocal(t)

	changed, restart, err := ApplyFleet([]byte(`{
		"ignoreHosts": "^fleet",
		"gpuPciAddrs": {"^b": ["00:03", "00:04"], "^c": ["00:05"]},
		"log-level": "debug"
	}`), true)
	if err != nil {
		t.Fatalf("ApplyFleet: %v", err)
	}
	if len(restart) != 0 {
		t.Errorf("restartRequired = %v, want none at startup", restart)
	}

	if Config.IgnoreHosts != "^fleet" {
		t.Errorf("ignoreHosts = %q, want fleet value", Config.IgnoreHosts)
	}
	if Config.SlurmPollInterval != 30 {
		t.Errorf("slurmPollInterval = %d, want local value 30", Config.SlurmPollInterval)
	}

	// Maps merge key by key, arrays are replaced wholesale.
	wantGpu := map[string][]string{"^a": {"00:01"}, "^b": {"00:03", "00:04"}, "^c": {"00:05"}}
	if !reflect.DeepEqual(Config.GpuPciAddrs, wantGpu) {
		t.Errorf("gpuPciAddrs = %v, want %v", Config.GpuPciAddrs, wantGpu)
	}

	slices.Sort(changed)
	if want := []string{"gpuPciAddrs", "ignoreHosts"}; !slices.Equal(changed, want) {
		t.Errorf("changed = %v, want %v", changed, want)
	}
}

func TestApplyFleetLocalOnlyKeys(t *testing.T) {
	setupLocal(t)

	_, _, err := ApplyFleet([]byte(`{
		"ccRestUrl": "http://evil:8080",
		"ccRestJwt": "fleet-jwt",
		"pidFilePath": "/tmp/x.pid",
		"fleetEnabled": false
	}`), true)
	if err != nil {
		t.Fatalf("ApplyFleet: %v", err)
	}

	if Config.CcRestUrl != "http://local:8080" || Config.CcRestJwt != "local-jwt" {
		t.Errorf("ccRest* overridden by fleet: %q %q", Config.CcRestUrl, Config.CcRestJwt)
	}
	if Config.PidFilePath != DEFAULT_PID_FILE_PATH {
		t.Errorf("pidFilePath overridden by fleet: %q", Config.PidFilePath)
	}
	if !Config.FleetEnabled {
		t.Errorf("fleetEnabled overridden by fleet")
	}
}

func TestApplyFleetStartupOnlyKeys(t *testing.T) {
	setupLocal(t)

	changed, restart, err := ApplyFleet([]byte(`{"natsServer": "nats-fleet", "natsSubject": "fleetjobs"}`), false)
	if err != nil {
		t.Fatalf("ApplyFleet: %v", err)
	}

	if Config.NatsServer != "nats-local" {
		t.Errorf("natsServer = %q, want running value at runtime", Config.NatsServer)
	}
	if !slices.Equal(restart, []string{"natsServer"}) {
		t.Errorf("restartRequired = %v, want [natsServer]", restart)
	}
	if Config.NatsSubject != "fleetjobs" {
		t.Errorf("natsSubject = %q, want hot-reloaded fleet value", Config.NatsSubject)
	}
	if !slices.Equal(changed, []string{"natsSubject"}) {
		t.Errorf("changed = %v, want [natsSubject]", changed)
	}

	// At startup, startup-only keys do apply.
	setupLocal(t)
	_, _, err = ApplyFleet([]byte(`{"natsServer": "nats-fleet"}`), true)
	if err != nil {
		t.Fatalf("ApplyFleet: %v", err)
	}
	if Config.NatsServer != "nats-fleet" {
		t.Errorf("natsServer = %q, want fleet value at startup", Config.NatsServer)
	}
}

func TestApplyFleetInvalidKeepsConfig(t *testing.T) {
	setupLocal(t)

	for _, blob := range []string{
		`{"ignoreHosts": "("}`,
		`{"gpuPciAddrs": {"(": []}}`,
		`{"slurmPollInterval": "soon"}`,
		`[1, 2]`,
		`{"ccRestSubmitJobs": false, "natsServer": ""}`,
	} {
		_, _, err := ApplyFleet([]byte(blob), true)
		if err == nil {
			t.Errorf("ApplyFleet(%s): expected error", blob)
		}
		if Config.IgnoreHosts != "^local" || Config.NatsServer != "nats-local" || !Config.CcRestSubmitJobs {
			t.Errorf("ApplyFleet(%s): config changed despite error", blob)
		}
	}
}

func TestApplyFleetEmptyRevertsToLocal(t *testing.T) {
	setupLocal(t)

	if _, _, err := ApplyFleet([]byte(`{"ignoreHosts": "^fleet"}`), false); err != nil {
		t.Fatalf("ApplyFleet: %v", err)
	}

	changed, _, err := ApplyFleet(nil, false)
	if err != nil {
		t.Fatalf("ApplyFleet(nil): %v", err)
	}
	if Config.IgnoreHosts != "^local" {
		t.Errorf("ignoreHosts = %q, want local value after fleet config was removed", Config.IgnoreHosts)
	}
	if !slices.Equal(changed, []string{"ignoreHosts"}) {
		t.Errorf("changed = %v, want [ignoreHosts]", changed)
	}
}

func TestBuildValidatesLocalIgnoreHosts(t *testing.T) {
	// Used to be checked against the old global config instead of the new one.
	if _, err := build([]byte(`{"ignoreHosts": "("}`), nil); err == nil {
		t.Errorf("expected error for invalid local ignoreHosts regex")
	}
}
