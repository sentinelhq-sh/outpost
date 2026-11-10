package config

import (
	"strings"
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

func TestResolveFingerprint(t *testing.T) {
	cases := []struct {
		name, pinned, instance, clusterID, want string
	}{
		{"pinned wins", "fixed", "ns/outpost", "c0ffee", "fixed"},
		{"cluster scoped", "", "ns/outpost", "c0ffee", "k8s/c0ffee/ns/outpost"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Fingerprint: tc.pinned, Instance: tc.instance}
			cfg.ResolveFingerprint(tc.clusterID)

			if cfg.Fingerprint != tc.want {
				t.Errorf("Fingerprint = %q, want %q", cfg.Fingerprint, tc.want)
			}
		})
	}
}

func TestResolveFingerprintOutsideKubernetesIsPerHost(t *testing.T) {
	cfg := &Config{}
	cfg.ResolveFingerprint("ignored")

	if cfg.Fingerprint == "" || strings.HasPrefix(cfg.Fingerprint, "k8s/") {
		t.Errorf("Fingerprint = %q, want a per-host identity", cfg.Fingerprint)
	}
}
