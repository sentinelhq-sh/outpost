// Package config gathers runtime configuration from environment variables.
// The agent is meant to live in a Kubernetes Deployment so 12-factor env vars
// are the natural transport — no files to mount, no secrets in flags.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// AgentMode controls whether the agent pushes to the SentinelHQ backend.
// /metrics is always exposed regardless of mode.
type AgentMode string

const (
	// ModeSentinel pushes events to the SentinelHQ backend (+ always exposes /metrics).
	ModeSentinel AgentMode = "sentinel"
	// ModePrometheus exposes /metrics only, no push to the SentinelHQ backend.
	ModePrometheus AgentMode = "prometheus"
)

// Config holds the validated runtime configuration for the outpost agent.
// All fields are populated from environment variables by Load().
type Config struct {
	// Mode selects the active output(s). See AgentMode constants.
	Mode AgentMode

	// APIBaseURL is the SentinelHQ server root (e.g. https://api.sentinelhq.cloud).
	// No trailing slash. Required in sentinel and both modes.
	APIBaseURL string

	// APIKey is the project-scoped Bearer token used to authenticate every
	// request. Mounted from a Kubernetes Secret in production.
	// Required in sentinel and both modes.
	APIKey string

	// ClusterName labels every event so multi-cluster orgs can tell sources
	// apart. Falls back to "default" when unset.
	ClusterName string

	// Addr is the listen address for /healthz, /readyz, and /metrics.
	Addr string

	// PollInterval controls how often the registrar re-syncs the discovered
	// CronJobs with the server. Watch loops handle events in real time so
	// this is just a safety net for missed deletes / clock skew.
	PollInterval time.Duration

	// HTTPTimeout caps every API call. Kept short so a slow server can't
	// pin a worker goroutine forever.
	HTTPTimeout time.Duration
}

// PushEnabled reports whether the agent should forward events to the SentinelHQ backend.
func (c *Config) PushEnabled() bool {
	return c.Mode == ModeSentinel
}

// Load reads, validates, and returns a Config. It returns a structured error
// describing every missing-or-invalid field so the operator can fix them all
// in one pass instead of restarting the agent multiple times.
func Load() (*Config, error) {
	cfg := &Config{
		Mode:         AgentMode(getEnv("SENTINELHQ_AGENT_MODE", "")),
		APIBaseURL:   getEnv("SENTINELHQ_API_URL", ""),
		APIKey:       getEnv("SENTINELHQ_API_KEY", ""),
		ClusterName:  getEnv("SENTINELHQ_CLUSTER_NAME", "default"),
		Addr:         getEnv("SENTINELHQ_ADDR", ":8080"),
		PollInterval: getEnvDuration("SENTINELHQ_POLL_INTERVAL", 5*time.Minute),
		HTTPTimeout:  getEnvDuration("SENTINELHQ_HTTP_TIMEOUT", 10*time.Second),
	}

	var errs []error

	switch cfg.Mode {
	case ModeSentinel, ModePrometheus:
		// valid
	case "":
		errs = append(errs, errors.New(
			"SENTINELHQ_AGENT_MODE is required: sentinel or prometheus",
		))
	default:
		errs = append(errs, fmt.Errorf(
			"SENTINELHQ_AGENT_MODE %q is invalid: must be sentinel or prometheus",
			cfg.Mode,
		))
	}

	if cfg.PushEnabled() {
		if cfg.APIBaseURL == "" {
			errs = append(errs, errors.New("SENTINELHQ_API_URL is required in sentinel mode"))
		}
		if cfg.APIKey == "" {
			errs = append(errs, errors.New("SENTINELHQ_API_KEY is required in sentinel mode"))
		}
	}

	if cfg.PollInterval < 30*time.Second {
		errs = append(errs, fmt.Errorf("SENTINELHQ_POLL_INTERVAL must be at least 30s, got %s", cfg.PollInterval))
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	// Accept either Go duration syntax ("5m", "30s") or raw integer seconds.
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	return fallback
}
