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
	baseURL string
	apiKey  string
	http    *http.Client
}

// New builds a Client from the validated runtime configuration.
func New(cfg *config.Config) *Client {
	return &Client{
		baseURL: cfg.APIBaseURL,
		apiKey:  cfg.APIKey,
		http: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
	}
}

// DiscoveredJob is one CronJob (or equivalent) the agent has found and
// wishes to register on the server. The server reconciles the full set —
// anything not in the list is soft-deleted.
type DiscoveredJob struct {
	SourceID    string         `json:"source_id"`
	Name        string         `json:"name"`
	Schedule    string         `json:"schedule"`
	Timezone    string         `json:"timezone,omitempty"`
	GracePeriod int            `json:"grace_period,omitempty"`
	SourceMeta  map[string]any `json:"source_meta,omitempty"`
}

// RegisterPayload is the body of POST /v1/outpost/register.
type RegisterPayload struct {
	Runtime    string          `json:"runtime"`
	SourceMeta map[string]any  `json:"source_meta,omitempty"`
	Discovered []DiscoveredJob `json:"discovered"`
}

// RegisterResponse is the server's reconciliation report.
type RegisterResponse struct {
	Created []string `json:"created"`
	Updated []string `json:"updated"`
	Removed []string `json:"removed"`
}

// Register sends the full list of discovered jobs to the server. Idempotent:
// the server treats it as the source of truth for this (project, runtime).
func (c *Client) Register(ctx context.Context, payload RegisterPayload) (*RegisterResponse, error) {
	var out RegisterResponse
	if err := c.post(ctx, "/v1/outpost/register", payload, &out); err != nil {
		return nil, fmt.Errorf("register: %w", err)
	}
	return &out, nil
}

// EventPayload is the body of POST /v1/outpost/events. The agent emits one
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
	return c.post(ctx, "/v1/outpost/events", payload, nil)
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
	req.Header.Set("User-Agent", fmt.Sprintf("sentinelhq-outpost/%s (%s)", version.Version, version.Commit))

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if out == nil {
			io.Copy(io.Discard, resp.Body) // drain for keep-alive
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
