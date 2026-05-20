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

// Config holds the validated runtime configuration for the outpost agent.
// All fields are populated from environment variables by Load().
type Config struct {
	// APIBaseURL is the SentinelHQ server root (e.g. https://api.sentinelhq.cloud).
	// No trailing slash. Optional in standalone Prometheus mode.
	APIBaseURL string

	// APIKey is the project-scoped Bearer token used to authenticate every
	// request. Mounted from a Kubernetes Secret in production. Optional in
	// standalone Prometheus mode.
	APIKey string

	// ClusterName labels every event so multi-cluster orgs can tell sources
	// apart. Falls back to "default" when unset.
	ClusterName string

	// HealthzAddr is the local listen address for the liveness/readiness
	// probes (e.g. ":9090").
	HealthzAddr string

	// PrometheusAddr is the listen address for the /metrics endpoint
	// (e.g. ":9091"). Empty disables Prometheus export entirely.
	//
	// When set with an empty APIBaseURL/APIKey, the agent runs in
	// standalone mode: it watches the cluster, exposes metrics for any
	// Prometheus-compatible scraper, and never reaches out to the
	// SentinelHQ backend.
	PrometheusAddr string

	// PollInterval controls how often the registrar re-syncs the discovered
	// CronJobs with the server. Watch loops handle events in real time so
	// this is just a safety net for missed deletes / clock skew.
	PollInterval time.Duration

	// HTTPTimeout caps every API call. Kept short so a slow server can't
	// pin a worker goroutine forever.
	HTTPTimeout time.Duration
}

// PushEnabled reports whether the agent should forward events to the
// SentinelHQ backend. False when running in standalone Prometheus mode.
func (c *Config) PushEnabled() bool {
	return c.APIBaseURL != "" && c.APIKey != ""
}

// PrometheusEnabled reports whether the /metrics endpoint should be served.
func (c *Config) PrometheusEnabled() bool {
	return c.PrometheusAddr != ""
}

// Load reads, validates, and returns a Config. It returns a structured error
// describing every missing-or-invalid field so the operator can fix them all
// in one pass instead of restarting the agent multiple times.
func Load() (*Config, error) {
	cfg := &Config{
		APIBaseURL:     getEnv("SENTINELHQ_API_URL", ""),
		APIKey:         getEnv("SENTINELHQ_API_KEY", ""),
		ClusterName:    getEnv("SENTINELHQ_CLUSTER_NAME", "default"),
		HealthzAddr:    getEnv("SENTINELHQ_HEALTHZ_ADDR", ":9090"),
		PrometheusAddr: getEnv("SENTINELHQ_PROMETHEUS_ADDR", ""),
		PollInterval:   getEnvDuration("SENTINELHQ_POLL_INTERVAL", 5*time.Minute),
		HTTPTimeout:    getEnvDuration("SENTINELHQ_HTTP_TIMEOUT", 10*time.Second),
	}

	var errs []error

	// The agent must do at least one of (push, prometheus). Doing neither
	// would mean we watch the cluster and throw the data away.
	if !cfg.PushEnabled() && !cfg.PrometheusEnabled() {
		errs = append(errs, errors.New(
			"agent has no output configured — set SENTINELHQ_API_URL + SENTINELHQ_API_KEY (push mode), "+
				"or SENTINELHQ_PROMETHEUS_ADDR (standalone Prometheus mode), or both",
		))
	}

	// Push mode: both URL and key required together. Half-config is a bug.
	if cfg.APIBaseURL != "" && cfg.APIKey == "" {
		errs = append(errs, errors.New("SENTINELHQ_API_KEY is required when SENTINELHQ_API_URL is set"))
	}
	if cfg.APIKey != "" && cfg.APIBaseURL == "" {
		errs = append(errs, errors.New("SENTINELHQ_API_URL is required when SENTINELHQ_API_KEY is set"))
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
