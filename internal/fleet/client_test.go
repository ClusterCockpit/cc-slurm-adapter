package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/ClusterCockpit/cc-lib/v2/ccMessage"
)

const testToken = "test-jwt"

// fakeBackend implements the subset of cc-backend's fleet REST API used by the client.
type fakeBackend struct {
	t *testing.T

	mu            sync.Mutex
	registrations []map[string]any
	registerPaths []string
	instanceID    string
	config        string // empty: answer 204
	etag          string
	heartbeats    int
	deregistered  bool
	ifNoneMatch   []string
}

func (b *fakeBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if r.Header.Get("X-Auth-Token") != testToken {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/fleet/register/"):
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.registrations = append(b.registrations, body)
		b.registerPaths = append(b.registerPaths, r.URL.Path)
		b.instanceID = strings.Repeat("ab", 15) + string(rune('a'+len(b.registrations)%26)) + "0"
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"instanceId": b.instanceID, "configRevision": 0})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/fleet/config/"):
		if strings.TrimPrefix(r.URL.Path, "/api/fleet/config/") != b.instanceID {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b.ifNoneMatch = append(b.ifNoneMatch, r.Header.Get("If-None-Match"))
		if b.config == "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Header.Get("If-None-Match") == b.etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", b.etag)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(b.config))
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/fleet/heartbeat/"):
		if strings.TrimPrefix(r.URL.Path, "/api/fleet/heartbeat/") != b.instanceID {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b.heartbeats++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/fleet/deregister/"):
		b.deregistered = true
		b.instanceID = ""
		w.WriteHeader(http.StatusNoContent)
	default:
		b.t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (b *fakeBackend) setConfig(config, etag string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.config, b.etag = config, etag
}

func newTestClient(t *testing.T, cluster string) (*Client, *fakeBackend) {
	t.Helper()
	backend := &fakeBackend{t: t}
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)

	c := NewClient(Options{
		BaseURL:            server.URL,
		Token:              testToken,
		Hostname:           "slurmctl01",
		Cluster:            cluster,
		Meta:               map[string]string{"version": "test"},
		HeartbeatInterval:  20 * time.Millisecond,
		ConfigPollInterval: 20 * time.Millisecond,
		CachePath:          filepath.Join(t.TempDir(), "fleet-config.json"),
	})
	return c, backend
}

func TestRegisterScope(t *testing.T) {
	ctx := context.Background()

	c, backend := newTestClient(t, "fritz")
	if err := c.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if backend.registerPaths[0] != "/api/fleet/register/cluster/" {
		t.Errorf("path = %s, want cluster scope", backend.registerPaths[0])
	}
	if backend.registrations[0]["cluster"] != "fritz" || backend.registrations[0]["serviceType"] != "ccsa" {
		t.Errorf("unexpected registration body %v", backend.registrations[0])
	}

	c, backend = newTestClient(t, "")
	if err := c.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if backend.registerPaths[0] != "/api/fleet/register/infra/" {
		t.Errorf("path = %s, want infra scope", backend.registerPaths[0])
	}
	if _, ok := backend.registrations[0]["cluster"]; ok {
		t.Errorf("infra registration must not contain cluster: %v", backend.registrations[0])
	}
}

func TestPullConfig(t *testing.T) {
	ctx := context.Background()
	c, backend := newTestClient(t, "fritz")

	if _, _, err := c.PullConfig(ctx); !errors.Is(err, ErrUnknownInstance) {
		t.Errorf("PullConfig before Register: err = %v, want ErrUnknownInstance", err)
	}

	if err := c.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// 204: nothing deployed.
	blob, changed, err := c.PullConfig(ctx)
	if err != nil || blob != nil || changed {
		t.Errorf("204: blob=%s changed=%v err=%v", blob, changed, err)
	}

	// 200: new config.
	backend.setConfig(`{"ignoreHosts":"^x"}`, `"1"`)
	blob, changed, err = c.PullConfig(ctx)
	if err != nil || string(blob) != `{"ignoreHosts":"^x"}` || !changed {
		t.Errorf("200: blob=%s changed=%v err=%v", blob, changed, err)
	}

	// 304: unchanged, ETag was sent back.
	blob, changed, err = c.PullConfig(ctx)
	if err != nil || string(blob) != `{"ignoreHosts":"^x"}` || changed {
		t.Errorf("304: blob=%s changed=%v err=%v", blob, changed, err)
	}
	if got := backend.ifNoneMatch[len(backend.ifNoneMatch)-1]; got != `"1"` {
		t.Errorf("If-None-Match = %q, want \"1\"", got)
	}

	// 204 after a config existed: changed back to nothing, cache is removed.
	backend.setConfig("", "")
	blob, changed, err = c.PullConfig(ctx)
	if err != nil || blob != nil || !changed {
		t.Errorf("204 after 200: blob=%s changed=%v err=%v", blob, changed, err)
	}
	if _, err := os.Stat(c.opts.CachePath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("cache file still exists after 204: %v", err)
	}

	// 404: instance gone.
	backend.mu.Lock()
	backend.instanceID = "gone"
	backend.mu.Unlock()
	if _, _, err := c.PullConfig(ctx); !errors.Is(err, ErrUnknownInstance) {
		t.Errorf("404: err = %v, want ErrUnknownInstance", err)
	}
	if c.Registered() {
		t.Errorf("client still registered after 404")
	}
}

func TestBootstrapUsesCache(t *testing.T) {
	ctx := context.Background()
	c, backend := newTestClient(t, "fritz")
	backend.setConfig(`{"ignoreHosts":"^x"}`, `"1"`)

	if blob := c.Bootstrap(ctx); string(blob) != `{"ignoreHosts":"^x"}` {
		t.Fatalf("Bootstrap = %s", blob)
	}
	info, err := os.Stat(c.opts.CachePath)
	if err != nil {
		t.Fatalf("cache not written: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("cache mode = %v, want 0600", info.Mode().Perm())
	}

	// Restart with backend reachable: ETag from cache yields 304, cached config is used.
	c2 := NewClient(c.opts)
	if blob := c2.Bootstrap(ctx); string(blob) != `{"ignoreHosts":"^x"}` {
		t.Errorf("Bootstrap with cache = %s", blob)
	}
	if got := backend.ifNoneMatch[len(backend.ifNoneMatch)-1]; got != `"1"` {
		t.Errorf("If-None-Match = %q, want cached ETag", got)
	}

	// Restart with backend unreachable: cached config is used.
	opts := c.opts
	opts.BaseURL = "http://127.0.0.1:1"
	c3 := NewClient(opts)
	if blob := c3.Bootstrap(ctx); string(blob) != `{"ignoreHosts":"^x"}` {
		t.Errorf("Bootstrap offline = %s", blob)
	}
	if c3.Registered() {
		t.Errorf("client registered although backend is unreachable")
	}
}

func TestRunReregistersAndDeliversConfig(t *testing.T) {
	c, backend := newTestClient(t, "fritz")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}

	out := make(chan []byte, 1)
	done := make(chan struct{})
	go func() {
		c.Run(ctx, func() *nats.Conn { return nil }, out)
		close(done)
	}()

	// Simulate cc-backend forgetting the instance, then deploy a config.
	backend.mu.Lock()
	backend.instanceID = "gone"
	backend.mu.Unlock()
	backend.setConfig(`{"slurmMaxRetries":3}`, `"2"`)

	select {
	case blob := <-out:
		if string(blob) != `{"slurmMaxRetries":3}` {
			t.Errorf("Run delivered %s", blob)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not deliver config")
	}

	cancel()
	<-done

	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.registrations) < 2 {
		t.Errorf("registrations = %d, want re-registration", len(backend.registrations))
	}
	if backend.heartbeats == 0 {
		t.Errorf("no REST heartbeats received")
	}
}

func TestDeregister(t *testing.T) {
	ctx := context.Background()
	c, backend := newTestClient(t, "fritz")

	// Not registered: no-op.
	if err := c.Deregister(ctx); err != nil {
		t.Errorf("Deregister unregistered: %v", err)
	}

	if err := c.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Deregister(ctx); err != nil {
		t.Errorf("Deregister: %v", err)
	}
	if !backend.deregistered || c.Registered() {
		t.Errorf("deregistered=%v registered=%v", backend.deregistered, c.Registered())
	}
}

func TestHeartbeatMessage(t *testing.T) {
	id := strings.Repeat("0123456789abcdef", 2)
	data, err := heartbeatMessage(id, time.Unix(1734000000, 0))
	if err != nil {
		t.Fatalf("heartbeatMessage: %v", err)
	}

	msgs, err := ccmessage.FromBytes(data)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("FromBytes(%s): %v (%d messages)", data, err, len(msgs))
	}
	msg := msgs[0]
	if msg.Name() != "fleet" {
		t.Errorf("measurement = %s, want fleet", msg.Name())
	}
	if f, _ := msg.GetTag("function"); f != "heartbeat" {
		t.Errorf("function tag = %s, want heartbeat", f)
	}
	event, ok := msg.GetEventValue()
	if !ok {
		t.Fatalf("no event field in %s", data)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(event), &payload); err != nil || len(payload) != 1 || payload["instanceId"] != id {
		t.Errorf("event payload = %s (%v)", event, err)
	}
}
