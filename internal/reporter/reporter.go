// Package reporter is the HTTP client that talks to the SentinelHQ server.
// It owns nothing else: it only knows how to authenticate, retry, and shape
// the payloads. Watchers and the registrar feed it; it does the network.
package reporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sentinelhq-sh/outpost/internal/config"
	"github.com/sentinelhq-sh/outpost/internal/version"
)

// Client is the high-level API consumed by the rest of the agent.
// It serializes calls onto an underlying http.Client, retries idempotent
// requests with exponential backoff, and centralizes auth + UA headers.
type Client struct {
	baseURL     string
	apiKey      string
	fingerprint string
	name        string
	runtime     string
	http        *http.Client
}

// New builds a Client from the validated runtime configuration. The runtime
// reported to the backend is the execution ENVIRONMENT (MonitorRuntime:
// manual|kubernetes|…), not the cluster-attachment strategy (config.Runtime).
// Defaults to manual; main calls SetRuntime once the environment is resolved.
func New(cfg *config.Config) *Client {
	return &Client{
		baseURL:     cfg.APIBaseURL,
		apiKey:      cfg.APIKey,
		fingerprint: cfg.Fingerprint,
		name:        cfg.AgentName,
		runtime:     "manual",
		http: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
	}
}

// SetRuntime sets the execution environment reported on register (a valid
// MonitorRuntime value: manual|kubernetes|…). Called by main once resolved.
func (c *Client) SetRuntime(runtime string) {
	c.runtime = runtime
}

// DiscoveredJob is one CronJob (or equivalent) the agent has found. The server
// reconciles the full set per agent — anything not in the list is soft-deleted.
type DiscoveredJob struct {
	SourceID    string         `json:"source_id"`
	Name        string         `json:"name"`
	Schedule    string         `json:"schedule"`
	Timezone    string         `json:"timezone,omitempty"`
	GracePeriod int            `json:"grace_period,omitempty"`
	SourceMeta  map[string]any `json:"source_meta,omitempty"`
}

// JobsPayload is the body of POST /api/v1/outpost/jobs (CronJob discovery).
type JobsPayload struct {
	Runtime    string          `json:"runtime"`
	SourceMeta map[string]any  `json:"source_meta,omitempty"`
	Discovered []DiscoveredJob `json:"discovered"`
}

// JobsResponse is the server's reconciliation report.
type JobsResponse struct {
	Created []string `json:"created"`
	Removed []string `json:"removed"`
}

// ReportJobs sends the full discovered-job set; the server reconciles it for
// this agent. Requires the agent to be approved (else 403).
func (c *Client) ReportJobs(ctx context.Context, payload JobsPayload) (*JobsResponse, error) {
	var out JobsResponse
	if err := c.post(ctx, "/api/v1/outpost/jobs", payload, &out); err != nil {
		return nil, fmt.Errorf("report jobs: %w", err)
	}
	return &out, nil
}

// RegisterPayload is the body of POST /api/v1/outpost/register. The agent
// announces itself; the backend keys it on (project, fingerprint).
type RegisterPayload struct {
	Fingerprint string         `json:"fingerprint"`
	Name        string         `json:"name"`
	Runtime     string         `json:"runtime"`
	SourceMeta  map[string]any `json:"source_meta,omitempty"`
}

// RegisterResponse carries the agent's current status: pending|approved|revoked.
type RegisterResponse struct {
	Agent struct {
		Status string `json:"status"`
	} `json:"agent"`
}

// Register announces this agent to the backend and returns its current status.
// Idempotent on (project, fingerprint).
func (c *Client) Register(ctx context.Context) (*RegisterResponse, error) {
	payload := RegisterPayload{
		Fingerprint: c.fingerprint,
		Name:        c.name,
		Runtime:     c.runtime,
	}
	var out RegisterResponse
	if err := c.post(ctx, "/api/v1/outpost/register", payload, &out); err != nil {
		return nil, fmt.Errorf("register: %w", err)
	}
	return &out, nil
}

// EventPayload is the body of POST /api/v1/outpost/events. The agent emits one
// per CronJob/Job/Pod state change it observes.
type EventPayload struct {
	SourceID       string `json:"source_id"`
	State          string `json:"state"` // run | checkpoint | complete | fail
	Duration       int    `json:"duration,omitempty"`
	ExitCode       int    `json:"exit_code,omitempty"`
	Message        string `json:"message,omitempty"`
	IdempotencyKey string `json:"idempotency_key"`
}

// Event reports a single state change. Idempotency: the server dedupes by
// idempotency_key so retries on transient errors are safe.
func (c *Client) Event(ctx context.Context, payload EventPayload) error {
	return c.post(ctx, "/api/v1/outpost/events", payload, nil)
}

// HeartbeatResponse is the body of POST /api/v1/outpost/heartbeat. Epochs maps
// monitor_id -> a version that bumps whenever that pool's active/standby layout
// changes; the agent re-pulls /config when an epoch it tracks moves.
type HeartbeatResponse struct {
	Alive  bool           `json:"alive"`
	Epochs map[string]int `json:"epochs"`
}

// Heartbeat pings liveness (every ~2s). Single attempt, no backoff: a missed
// beat simply retries on the next tick, and the server's staleness window
// tolerates a few misses before failing the agent over.
func (c *Client) Heartbeat(ctx context.Context) (*HeartbeatResponse, error) {
	var out HeartbeatResponse
	if err := c.doPost(ctx, "/api/v1/outpost/heartbeat", struct{}{}, &out); err != nil {
		return nil, fmt.Errorf("heartbeat: %w", err)
	}
	return &out, nil
}

// ProbeSpec is one active probe the agent must execute on its interval. The
// server is the source of truth; the agent only runs it and POSTs results
// back through Event(). Result.Up=true -> "complete", false -> "fail".
type ProbeSpec struct {
	ID              int    `json:"id"`
	SourceID        string `json:"source_id"` // echoed back in EventPayload.SourceID
	Name            string `json:"name"`
	Kind            string `json:"kind"` // http | tcp | dns | icmp
	Target          string `json:"target"`
	Interval        int    `json:"interval"` // seconds
	TimeoutMs       int    `json:"timeout_ms"`
	ExpectedStatus  int    `json:"expected_status,omitempty"` // http
	Keyword         string `json:"keyword,omitempty"`         // http
	ExpectedRecord  string `json:"expected_record,omitempty"` // dns
	FollowRedirects bool   `json:"follow_redirects"`          // http
	VerifyTLS       bool   `json:"verify_tls"`                // http
}

// ConfigResponse is the body of GET /api/v1/outpost/config. Monitors are the
// observed-job side (watched via the K8s API); Probes are the active side
// the agent must execute itself.
type ConfigResponse struct {
	ProjectID int         `json:"project_id"`
	Probes    []ProbeSpec `json:"probes"`
}

// Config pulls the agent's configuration — currently the list of probes to
// execute. Called periodically so server-side changes propagate.
func (c *Client) Config(ctx context.Context) (*ConfigResponse, error) {
	var out ConfigResponse
	if err := c.get(ctx, "/api/v1/outpost/config", &out); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &out, nil
}

// ── HTTP plumbing ────────────────────────────────────────────────────────

// post sends a JSON body, decodes the JSON response into `out` (or discards
// it when nil), and retries transient failures with exponential backoff.
func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	var lastErr error

	// 4 attempts, 0.5s → 1s → 2s → 4s. Total worst-case: ~7.5s.
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(1<<(attempt-1)) * 500 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := c.doPost(ctx, path, body, out)
		if err == nil {
			return nil
		}

		// Don't retry client-side errors that won't change on retry
		// (4xx except 408, 425, 429). doPost flags them via ErrPermanent.
		if errors.Is(err, ErrPermanent) {
			return err
		}
		lastErr = err
	}

	return fmt.Errorf("after retries: %w", lastErr)
}

// get fetches a JSON resource and decodes it into `out`. Single attempt:
// config pulls are periodic, so a transient failure simply retries on the
// next tick rather than blocking the loop with backoff.
func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("X-Agent-Fingerprint", c.fingerprint)
	req.Header.Set("User-Agent", fmt.Sprintf("sentinelhq-outpost/%s (%s)", version.Version, version.Commit))

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

// ErrPermanent flags errors that retrying won't fix (e.g. 401, 403, 404,
// 422). The caller — and the retry loop — sees this and stops immediately.
var ErrPermanent = errors.New("permanent error")

func (c *Client) doPost(ctx context.Context, path string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("X-Agent-Fingerprint", c.fingerprint)
	req.Header.Set("User-Agent", fmt.Sprintf("sentinelhq-outpost/%s (%s)", version.Version, version.Commit))

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if out == nil {
			_, _ = io.Copy(io.Discard, resp.Body) // drain for keep-alive
			return nil
		}
		return json.NewDecoder(resp.Body).Decode(out)

	case resp.StatusCode == 408, resp.StatusCode == 425, resp.StatusCode == 429,
		resp.StatusCode >= 500:
		// Transient — let the retry loop have another go.
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("transient %d: %s", resp.StatusCode, truncate(string(body), 200))

	default:
		// 4xx — won't get better on retry.
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %d %s", ErrPermanent, resp.StatusCode, truncate(string(body), 200))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
