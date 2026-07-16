package config

import (
	"testing"
)

// withEnv sets env vars for the duration of a test and clears them after.
func withEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestRuntimeDefaultsToAuto(t *testing.T) {
	withEnv(t, map[string]string{
		"SENTINELHQ_AGENT_MODE": "sentinel",
		"SENTINELHQ_API_URL":    "https://api.example.com",
		"SENTINELHQ_API_KEY":    "key",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Runtime != RuntimeAuto {
		t.Fatalf("expected default runtime %q, got %q", RuntimeAuto, cfg.Runtime)
	}
}

func TestRuntimeStandaloneRequiresSentinel(t *testing.T) {
	withEnv(t, map[string]string{
		"SENTINELHQ_AGENT_MODE": "prometheus",
		"SENTINELHQ_RUNTIME":    "standalone",
	})

	if _, err := Load(); err == nil {
		t.Fatal("expected error: standalone requires sentinel mode")
	}
}

func TestRuntimeStandaloneWithSentinelIsValid(t *testing.T) {
	withEnv(t, map[string]string{
		"SENTINELHQ_AGENT_MODE": "sentinel",
		"SENTINELHQ_RUNTIME":    "standalone",
		"SENTINELHQ_API_URL":    "https://api.example.com",
		"SENTINELHQ_API_KEY":    "key",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Runtime != RuntimeStandalone {
		t.Fatalf("expected standalone, got %q", cfg.Runtime)
	}
}

func TestRuntimeInvalidRejected(t *testing.T) {
	withEnv(t, map[string]string{
		"SENTINELHQ_AGENT_MODE": "sentinel",
		"SENTINELHQ_RUNTIME":    "bogus",
		"SENTINELHQ_API_URL":    "https://api.example.com",
		"SENTINELHQ_API_KEY":    "key",
	})

	if _, err := Load(); err == nil {
		t.Fatal("expected error for invalid runtime")
	}
}
