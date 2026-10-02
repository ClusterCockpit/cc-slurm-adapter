// Package fleet implements the client side of cc-backend's fleet service:
// registration, heartbeats, central configuration deployment and
// deregistration.
//
// The instance id returned on registration is a bearer credential and must
// never be logged.
package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/trace"

	"github.com/nats-io/nats.go"

	"github.com/ClusterCockpit/cc-lib/v2/ccMessage"
)

const SERVICE_TYPE string = "ccsa"

const BOOTSTRAP_TIMEOUT = 10 * time.Second

var ErrUnknownInstance = errors.New("fleet: unknown or deregistered instance")

type Options struct {
	BaseURL  string
	Token    string
	Hostname string
	// Cluster selects the registration scope: cluster scope if set, infra scope otherwise.
	Cluster            string
	Meta               map[string]string
	HeartbeatInterval  time.Duration
	HeartbeatSubject   string
	ConfigPollInterval time.Duration
	CachePath          string
}

type Client struct {
	opts       Options
	httpClient http.Client

	mu         sync.Mutex
	instanceID string
	etag       string
	blob       []byte // last known fleet configuration, nil if none applies
}

type cacheFile struct {
	ETag   string          `json:"etag"`
	Config json.RawMessage `json:"config"`
}

func NewClient(opts Options) *Client {
	return &Client{
		opts:       opts,
		httpClient: http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) scope() string {
	if c.opts.Cluster != "" {
		return "cluster"
	}
	return "infra"
}

// Bootstrap loads the cached fleet configuration and makes one bounded attempt
// to register and pull the current configuration. It never fails: if
// cc-backend is unreachable, the cached configuration (if any) is returned
// and Run will keep trying to register in the background.
func (c *Client) Bootstrap(ctx context.Context) []byte {
	c.loadCache()

	ctx, cancel := context.WithTimeout(ctx, BOOTSTRAP_TIMEOUT)
	defer cancel()

	err := c.Register(ctx)
	if err != nil {
		trace.Warn("fleet: Registration failed, retrying in background: %v", err)
		if c.blob != nil {
			trace.Warn("fleet: Using cached fleet configuration from '%s'", c.opts.CachePath)
		}
		return c.blob
	}

	blob, _, err := c.PullConfig(ctx)
	if err != nil {
		trace.Warn("fleet: Pulling configuration failed, retrying in background: %v", err)
		return c.blob
	}
	return blob
}

// Register issues a fresh instance id.
func (c *Client) Register(ctx context.Context) error {
	payload := map[string]any{
		"hostname":    c.opts.Hostname,
		"serviceType": SERVICE_TYPE,
		"metaData":    c.opts.Meta,
	}
	if c.opts.Cluster != "" {
		payload["cluster"] = c.opts.Cluster
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := c.do(ctx, http.MethodPost, fmt.Sprintf("/api/fleet/register/%s/", c.scope()), body, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return unexpectedStatus("register", resp)
	}

	var reg struct {
		InstanceID string `json:"instanceId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&reg); err != nil {
		return fmt.Errorf("fleet register: Unable to parse response: %w", err)
	}
	if reg.InstanceID == "" {
		return fmt.Errorf("fleet register: Response contains no instance id")
	}

	c.mu.Lock()
	c.instanceID = reg.InstanceID
	c.mu.Unlock()

	if c.opts.Cluster != "" {
		trace.Info("fleet: Registered as %s on host '%s' for cluster '%s'", SERVICE_TYPE, c.opts.Hostname, c.opts.Cluster)
	} else {
		trace.Info("fleet: Registered as %s on host '%s' (infra scope)", SERVICE_TYPE, c.opts.Hostname)
	}
	return nil
}

func (c *Client) Registered() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.instanceID != ""
}

// PullConfig fetches the merged fleet configuration. It returns the currently
// applicable configuration blob (nil if no fleet configuration applies) and
// whether it changed since the last successful pull.
func (c *Client) PullConfig(ctx context.Context) ([]byte, bool, error) {
	c.mu.Lock()
	instanceID, etag := c.instanceID, c.etag
	c.mu.Unlock()

	if instanceID == "" {
		return nil, false, ErrUnknownInstance
	}

	resp, err := c.do(ctx, http.MethodGet, "/api/fleet/config/"+instanceID, nil, etag)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	c.mu.Lock()
	defer c.mu.Unlock()

	switch resp.StatusCode {
	case http.StatusOK:
		blob, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, false, fmt.Errorf("fleet config: Unable to read response: %w", err)
		}
		c.etag = resp.Header.Get("ETag")
		c.blob = blob
		c.writeCache()
		trace.Debug("fleet: Received new configuration (revision %s)", c.etag)
		return blob, true, nil
	case http.StatusNotModified:
		return c.blob, false, nil
	case http.StatusNoContent:
		changed := c.blob != nil
		c.etag = ""
		c.blob = nil
		c.removeCache()
		return nil, changed, nil
	case http.StatusNotFound:
		c.instanceID = ""
		return nil, false, ErrUnknownInstance
	default:
		return nil, false, unexpectedStatus("config", resp)
	}
}

// Heartbeat sends a heartbeat via NATS if a connection and subject are
// available and via REST otherwise. Only the REST variant is able to detect an
// unknown instance id.
func (c *Client) Heartbeat(ctx context.Context, nc *nats.Conn) error {
	c.mu.Lock()
	instanceID := c.instanceID
	c.mu.Unlock()

	if instanceID == "" {
		return ErrUnknownInstance
	}

	if nc != nil && c.opts.HeartbeatSubject != "" {
		msg, err := heartbeatMessage(instanceID, time.Now())
		if err != nil {
			return err
		}
		return nc.Publish(c.opts.HeartbeatSubject, msg)
	}

	resp, err := c.do(ctx, http.MethodPost, "/api/fleet/heartbeat/"+instanceID, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK:
		return nil
	case http.StatusNotFound:
		c.mu.Lock()
		c.instanceID = ""
		c.mu.Unlock()
		return ErrUnknownInstance
	default:
		return unexpectedStatus("heartbeat", resp)
	}
}

func heartbeatMessage(instanceID string, tm time.Time) ([]byte, error) {
	payload, err := json.Marshal(map[string]string{"instanceId": instanceID})
	if err != nil {
		return nil, err
	}
	msg, err := ccmessage.NewEvent("fleet", map[string]string{"function": "heartbeat"}, nil, string(payload), tm)
	if err != nil {
		return nil, err
	}
	return []byte(msg.ToLineProtocol(nil)), nil
}

// Deregister drops the identity, which removes the service from discovery
// rosters immediately. Not being able to deregister is not fatal, the service
// will simply become stale in cc-backend.
func (c *Client) Deregister(ctx context.Context) error {
	c.mu.Lock()
	instanceID := c.instanceID
	c.instanceID = ""
	c.mu.Unlock()

	if instanceID == "" {
		return nil
	}

	resp, err := c.do(ctx, http.MethodDelete, "/api/fleet/deregister/"+instanceID, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return unexpectedStatus("deregister", resp)
	}

	trace.Info("fleet: Deregistered")
	return nil
}

// Run sends heartbeats and polls the configuration until ctx is cancelled.
// It (re-)registers whenever the instance is unknown. Every configuration
// change is sent on out, which should be buffered. If the receiver is busy,
// a pending older configuration is replaced by the newer one, so that Run
// never blocks and heartbeats are sent in time.
func (c *Client) Run(ctx context.Context, natsConn func() *nats.Conn, out chan []byte) {
	heartbeatTicker := time.NewTicker(c.opts.HeartbeatInterval)
	defer heartbeatTicker.Stop()
	configTicker := time.NewTicker(c.opts.ConfigPollInterval)
	defer configTicker.Stop()

	failing := false
	reportErr := func(what string, err error) {
		if failing {
			trace.Debug("fleet: %s failed: %v", what, err)
		} else {
			trace.Warn("fleet: %s failed (further failures are only logged at debug level): %v", what, err)
			failing = true
		}
	}
	reportOk := func() {
		if failing {
			trace.Info("fleet: Connection to cc-backend fleet service restored")
			failing = false
		}
	}

	// Registers if necessary. After a (re-)registration, immediately send a heartbeat
	// so the service becomes active, and pull the configuration.
	ensureRegistered := func() bool {
		if c.Registered() {
			return true
		}
		if err := c.Register(ctx); err != nil {
			reportErr("Registration", err)
			return false
		}
		if err := c.Heartbeat(ctx, natsConn()); err != nil {
			reportErr("Heartbeat", err)
		}
		c.pullAndSend(ctx, out, reportErr, reportOk)
		return true
	}

	// The registration may not have happened yet during Bootstrap,
	// in which case we want to send the initial heartbeat right away.
	if !c.Registered() {
		ensureRegistered()
	} else if err := c.Heartbeat(ctx, natsConn()); err != nil {
		reportErr("Heartbeat", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			if !ensureRegistered() {
				continue
			}
			err := c.Heartbeat(ctx, natsConn())
			if errors.Is(err, ErrUnknownInstance) {
				trace.Warn("fleet: Instance unknown to cc-backend, registering again")
				ensureRegistered()
			} else if err != nil {
				reportErr("Heartbeat", err)
			} else {
				reportOk()
			}
		case <-configTicker.C:
			if !ensureRegistered() {
				continue
			}
			c.pullAndSend(ctx, out, reportErr, reportOk)
		}
	}
}

func (c *Client) pullAndSend(ctx context.Context, out chan []byte, reportErr func(string, error), reportOk func()) {
	blob, changed, err := c.PullConfig(ctx)
	if errors.Is(err, ErrUnknownInstance) {
		// Registration happens on the next tick.
		trace.Warn("fleet: Instance unknown to cc-backend, registering again")
		return
	}
	if err != nil {
		reportErr("Pulling configuration", err)
		return
	}
	reportOk()

	if !changed {
		return
	}

	for {
		select {
		case out <- blob:
			return
		case <-ctx.Done():
			return
		default:
			// Receiver is busy and an older configuration is still pending. Replace it.
			select {
			case <-out:
			default:
			}
		}
	}
}

func (c *Client) do(ctx context.Context, method string, path string, body []byte, ifNoneMatch string) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.opts.BaseURL+path, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Auth-Token", c.opts.Token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}

	return c.httpClient.Do(req)
}

func unexpectedStatus(what string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Errorf("fleet %s: Unexpected HTTP status %s: %s", what, resp.Status, bytes.TrimSpace(body))
}

func (c *Client) loadCache() {
	if c.opts.CachePath == "" {
		return
	}

	data, err := os.ReadFile(c.opts.CachePath)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		trace.Warn("fleet: Unable to read config cache: %v", err)
		return
	}

	var cache cacheFile
	if err := json.Unmarshal(data, &cache); err != nil || len(cache.Config) == 0 {
		trace.Warn("fleet: Ignoring invalid config cache '%s': %v", c.opts.CachePath, err)
		return
	}

	c.mu.Lock()
	c.etag = cache.ETag
	c.blob = cache.Config
	c.mu.Unlock()
}

// writeCache must be called with c.mu held.
func (c *Client) writeCache() {
	if c.opts.CachePath == "" {
		return
	}

	data, err := json.Marshal(cacheFile{ETag: c.etag, Config: c.blob})
	if err != nil {
		trace.Warn("fleet: Unable to write config cache: %v", err)
		return
	}

	// Write to a temporary file and rename it, so that the cache is never torn.
	// The file may contain credentials (e.g. natsPassword), so keep it private (CreateTemp uses 0600).
	tmp, err := os.CreateTemp(filepath.Dir(c.opts.CachePath), ".fleet-config-*.tmp")
	if err != nil {
		trace.Warn("fleet: Unable to write config cache: %v", err)
		return
	}
	defer os.Remove(tmp.Name())

	_, err = tmp.Write(data)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), c.opts.CachePath)
	}
	if err != nil {
		trace.Warn("fleet: Unable to write config cache: %v", err)
	}
}

// removeCache must be called with c.mu held.
func (c *Client) removeCache() {
	if c.opts.CachePath == "" {
		return
	}
	if err := os.Remove(c.opts.CachePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		trace.Warn("fleet: Unable to remove config cache: %v", err)
	}
}
