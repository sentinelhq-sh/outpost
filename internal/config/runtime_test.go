package config

import "testing"

func TestDetectMonitorRuntimeOverride(t *testing.T) {
	t.Setenv("SENTINELHQ_AGENT_RUNTIME", "docker")
	if got := DetectMonitorRuntime(); got != MonitorRuntimeDocker {
		t.Fatalf("override ignored: got %q, want %q", got, MonitorRuntimeDocker)
	}
}

func TestDetectMonitorRuntimeInvalidOverrideFallsThrough(t *testing.T) {
	t.Setenv("SENTINELHQ_AGENT_RUNTIME", "bogus")
	// Invalid override must be ignored; detection then runs. We can't assert the
	// exact host environment, but it must be a valid backend value.
	got := DetectMonitorRuntime()
	if _, ok := validMonitorRuntimes[got]; !ok {
		t.Fatalf("detected runtime %q is not a valid MonitorRuntime", got)
	}
}

func TestDetectMonitorRuntimeKubernetesEnv(t *testing.T) {
	t.Setenv("SENTINELHQ_AGENT_RUNTIME", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	if got := DetectMonitorRuntime(); got != MonitorRuntimeKubernetes {
		t.Fatalf("KUBERNETES_SERVICE_HOST not detected: got %q", got)
	}
}
