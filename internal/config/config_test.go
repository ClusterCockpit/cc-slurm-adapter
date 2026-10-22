package config

import (
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
	overlay = newOverlay([]byte(testLocalConfig))
	conf, err := overlay.Build(nil)
	if err != nil {
		t.Fatalf("build local config: %v", err)
	}
	Config = conf
}

func TestApplyFleetLocalOnlyKeys(t *testing.T) {
	setupLocal(t)

	err := ApplyFleet([]byte(`{
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

	err := ApplyFleet([]byte(`{"natsServer": "nats-fleet", "natsSubject": "fleetjobs"}`), false)
	if err != nil {
		t.Fatalf("ApplyFleet: %v", err)
	}

	if Config.NatsServer != "nats-local" {
		t.Errorf("natsServer = %q, want running value at runtime", Config.NatsServer)
	}
	if Config.NatsSubject != "fleetjobs" {
		t.Errorf("natsSubject = %q, want hot-reloaded fleet value", Config.NatsSubject)
	}

	// At startup, startup-only keys do apply.
	setupLocal(t)
	err = ApplyFleet([]byte(`{"natsServer": "nats-fleet"}`), true)
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
		err := ApplyFleet([]byte(blob), true)
		if err == nil {
			t.Errorf("ApplyFleet(%s): expected error", blob)
		}
		if Config.IgnoreHosts != "^local" || Config.NatsServer != "nats-local" || !Config.CcRestSubmitJobs {
			t.Errorf("ApplyFleet(%s): config changed despite error", blob)
		}
	}
}

func TestBuildValidatesLocalIgnoreHosts(t *testing.T) {
	// Used to be checked against the old global config instead of the new one.
	o := newOverlay([]byte(`{"ignoreHosts": "("}`))
	if _, err := o.Build(nil); err == nil {
		t.Errorf("expected error for invalid local ignoreHosts regex")
	}
}
