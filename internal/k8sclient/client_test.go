package k8sclient

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestClusterIDIsTheKubeSystemUID(t *testing.T) {
	client := fake.NewClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "c0ffee"},
	})

	id, err := ClusterID(context.Background(), client)
	if err != nil {
		t.Fatalf("ClusterID: %v", err)
	}
	if id != "c0ffee" {
		t.Errorf("ClusterID = %q, want c0ffee", id)
	}
}

func TestClusterIDFailsWithoutKubeSystem(t *testing.T) {
	if _, err := ClusterID(context.Background(), fake.NewClientset()); err == nil {
		t.Error("ClusterID succeeded without a kube-system namespace")
	}
}
