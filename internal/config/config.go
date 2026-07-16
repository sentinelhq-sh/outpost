// Package config gathers runtime configuration from environment variables.
// The agent is meant to live in a Kubernetes Deployment so 12-factor env vars
// are the natural transport — no files to mount, no secrets in flags.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
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

// Runtime selects whether the agent attaches to a Kubernetes cluster (watch
// CronJobs, leader-elect via a Lease) or runs standalone (probes only, no K8s).
type Runtime string

const (
	// RuntimeAuto tries Kubernetes; falls back to standalone if unavailable.
	RuntimeAuto Runtime = "auto"
	// RuntimeKubernetes requires a cluster — the agent exits if K8s is unreachable.
	RuntimeKubernetes Runtime = "kubernetes"
	// RuntimeStandalone never touches K8s: no watcher, no leader election. The
	// agent is always leader and only runs active probes pulled from the backend.
	RuntimeStandalone Runtime = "standalone"
)

// Config holds the validated runtime configuration for the outpost agent.
// All fields are populated from environment variables by Load().
type Config struct {
	// Mode selects the active output(s). See AgentMode constants.
	Mode AgentMode

	// Runtime selects the cluster attachment strategy. See Runtime constants.
	// Defaults to auto.
	Runtime Runtime

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

	// AgentName is the human-readable name shown in the UI agents list.
	// Falls back to the hostname.
	AgentName string

	// Fingerprint is the agent's stable identity within a project. The backend
	// keys the registered agent on it. Falls back to the hostname.
	Fingerprint string

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
	host, _ := os.Hostname()
	if host == "" {
		host = "outpost"
	}

	cfg := &Config{
		Mode:         AgentMode(getEnv("SENTINELHQ_AGENT_MODE", "")),
		Runtime:      Runtime(getEnv("SENTINELHQ_RUNTIME", string(RuntimeAuto))),
		APIBaseURL:   getEnv("SENTINELHQ_API_URL", ""),
		APIKey:       getEnv("SENTINELHQ_API_KEY", ""),
		ClusterName:  getEnv("SENTINELHQ_CLUSTER_NAME", "default"),
		AgentName:    getEnv("SENTINELHQ_AGENT_NAME", host),
		Fingerprint:  getEnv("SENTINELHQ_FINGERPRINT", resolveFingerprint(host)),
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

	switch cfg.Runtime {
	case RuntimeAuto, RuntimeKubernetes, RuntimeStandalone:
		// valid
	default:
		errs = append(errs, fmt.Errorf(
			"SENTINELHQ_RUNTIME %q is invalid: must be auto, kubernetes, or standalone",
			cfg.Runtime,
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

	// Standalone only does active probes, which require the backend.
	if cfg.Runtime == RuntimeStandalone && !cfg.PushEnabled() {
		errs = append(errs, errors.New("standalone runtime requires sentinel mode (probes push to the backend)"))
	}

	if cfg.PollInterval < 30*time.Second {
		errs = append(errs, fmt.Errorf("SENTINELHQ_POLL_INTERVAL must be at least 30s, got %s", cfg.PollInterval))
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return cfg, nil
}

// resolveFingerprint returns a stable per-host identity: the Linux machine-id
// if available, else the hostname. Operators can override via SENTINELHQ_FINGERPRINT
// (e.g. inject the K8s pod/replicaset uid).
func resolveFingerprint(host string) string {
	for _, path := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
		if b, err := os.ReadFile(path); err == nil {
			if id := strings.TrimSpace(string(b)); id != "" {
				return id
			}
		}
	}
	return host
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
