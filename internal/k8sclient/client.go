// Package k8sclient builds a kubernetes.Interface usable from either inside
// the cluster (in-cluster config, the production path) or from a developer's
// laptop (kubeconfig fallback). The agent doesn't care which one — it gets a
// ready-to-use client.
package k8sclient

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
)

// New returns a kubernetes.Interface and the resolved namespace the agent
// is running in. The namespace is needed by the leader-election lease,
// which must live in the same namespace as the pods.
//
// Resolution order:
//  1. In-cluster service account (production)
//  2. KUBECONFIG env var (CI / Docker dev)
//  3. ~/.kube/config (laptop dev)
func New() (kubernetes.Interface, string, error) {
	cfg, ns, err := buildConfig()
	if err != nil {
		return nil, "", err
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, "", fmt.Errorf("build kubernetes client: %w", err)
	}

	return client, ns, nil
}

func buildConfig() (*rest.Config, string, error) {
	// In-cluster path: works when the binary runs inside a pod with a
	// service account token mounted. This is the production path.
	if cfg, err := rest.InClusterConfig(); err == nil {
		ns, _ := readNamespaceFromServiceAccountFile()
		if ns == "" {
			ns = "default"
		}
		return cfg, ns, nil
	}

	// Local-dev path: fall back to a kubeconfig file.
	path := os.Getenv("KUBECONFIG")
	if path == "" {
		path = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, "", fmt.Errorf("load kubeconfig from %s: %w", path, err)
	}

	// For laptop dev, default to the "default" namespace unless the
	// developer has set POD_NAMESPACE explicitly. The Helm chart in
	// production injects this via the downward API.
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		ns = "default"
	}
	return cfg, ns, nil
}

func readNamespaceFromServiceAccountFile() (string, error) {
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", err
	}
	return string(data), nil
}
