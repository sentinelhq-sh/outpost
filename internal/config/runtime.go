package config

import (
	"os"
	"strings"
)

// MonitorRuntime is the execution environment the agent reports to the backend
// on register. Distinct from Runtime (the cluster-attachment strategy): an agent
// may attach to K8s yet, in edge cases, run elsewhere. These values mirror the
// backend's MonitorRuntime enum and must stay in sync with it.
const (
	MonitorRuntimeKubernetes = "kubernetes"
	MonitorRuntimeDocker     = "docker"
	MonitorRuntimeSystemd    = "systemd"
	MonitorRuntimeProxmox    = "proxmox"
	MonitorRuntimeManual     = "manual"
)

// validMonitorRuntimes is the set accepted by the backend, used to validate an
// operator-supplied override.
var validMonitorRuntimes = map[string]struct{}{
	MonitorRuntimeKubernetes: {},
	MonitorRuntimeDocker:     {},
	MonitorRuntimeSystemd:    {},
	MonitorRuntimeProxmox:    {},
	MonitorRuntimeManual:     {},
}

// DetectMonitorRuntime inspects the host to determine the execution environment
// the agent runs in. Probed by reliable signals, most specific first:
//   - kubernetes: the service-account token is mounted (true in-cluster signal,
//     unlike "a kube client could be built" which a laptop kubeconfig satisfies).
//   - proxmox: a PVE host (pvedaemon present).
//   - docker: /.dockerenv exists or the init cgroup mentions docker.
//   - systemd: launched by systemd (INVOCATION_ID set) or systemd is PID 1.
//   - manual: none of the above (bare process, script, cron).
//
// SENTINELHQ_AGENT_RUNTIME overrides detection when set to a valid value; an
// invalid override is ignored and detection runs instead.
func DetectMonitorRuntime() string {
	if v := strings.TrimSpace(os.Getenv("SENTINELHQ_AGENT_RUNTIME")); v != "" {
		if _, ok := validMonitorRuntimes[v]; ok {
			return v
		}
	}

	if inKubernetes() {
		return MonitorRuntimeKubernetes
	}
	if isProxmox() {
		return MonitorRuntimeProxmox
	}
	if inDocker() {
		return MonitorRuntimeDocker
	}
	if underSystemd() {
		return MonitorRuntimeSystemd
	}
	return MonitorRuntimeManual
}

// inKubernetes reports whether the agent runs in a pod: the projected
// service-account token is the canonical in-cluster signal.
func inKubernetes() bool {
	if fileExists("/var/run/secrets/kubernetes.io/serviceaccount/token") {
		return true
	}
	// Env injected by the kubelet for every container.
	return os.Getenv("KUBERNETES_SERVICE_HOST") != ""
}

// isProxmox reports whether the host is a Proxmox VE node.
func isProxmox() bool {
	return fileExists("/etc/pve") || fileExists("/usr/bin/pvedaemon")
}

// inDocker reports whether the agent runs inside a Docker container.
func inDocker() bool {
	if fileExists("/.dockerenv") {
		return true
	}
	if b, err := os.ReadFile("/proc/1/cgroup"); err == nil {
		c := string(b)
		return strings.Contains(c, "docker") || strings.Contains(c, "containerd")
	}
	return false
}

// underSystemd reports whether the agent was started by systemd.
func underSystemd() bool {
	if os.Getenv("INVOCATION_ID") != "" {
		return true
	}
	return fileExists("/run/systemd/system")
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
