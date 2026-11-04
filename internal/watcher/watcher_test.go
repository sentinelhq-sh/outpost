package watcher

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func cronJob() *batchv1.CronJob {
	return &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{
		Name:        "backup",
		Namespace:   "prod",
		Annotations: map[string]string{AnnotationKey: "true"},
	}}
}

func job(uid string, status batchv1.JobStatus) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "backup-" + uid,
			Namespace:       "prod",
			UID:             types.UID(uid),
			OwnerReferences: []metav1.OwnerReference{{Kind: "CronJob", APIVersion: "batch/v1", Name: "backup"}},
		},
		Status: status,
	}
}

func succeeded(start, end time.Time) batchv1.JobStatus {
	return batchv1.JobStatus{
		StartTime:      &metav1.Time{Time: start},
		CompletionTime: &metav1.Time{Time: end},
		Conditions:     []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: end}}},
	}
}

func next(t *testing.T, w *Watcher) LifecycleEvent {
	t.Helper()
	select {
	case ev := <-w.Lifecycles():
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no lifecycle event within 5s")
		return LifecycleEvent{}
	}
}

// start runs a watcher on a fake cluster. A short resync recovers Jobs listed
// before their CronJob, which is also what happens in production.
func start(t *testing.T, client *fake.Clientset) *Watcher {
	t.Helper()
	w := New(client, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = w.Run(ctx) }()
	return w
}

func TestJobFinishedBeforeStartIsReplayedWithItsRealTime(t *testing.T) {
	end := time.Now().Add(-time.Hour).Truncate(time.Second)
	began := end.Add(-90 * time.Second)
	w := start(t, fake.NewClientset(cronJob(), job("old", succeeded(began, end))))

	ev := next(t, w)

	if !ev.Replayed {
		t.Error("expected Replayed for a Job finished before the agent started")
	}
	if !ev.OccurredAt.Equal(end) {
		t.Errorf("OccurredAt = %v, want the Job's completion %v", ev.OccurredAt, end)
	}
	if !ev.DurationKnown || ev.Duration != 90 {
		t.Errorf("duration = %d known=%v, want 90 known", ev.Duration, ev.DurationKnown)
	}
}

func TestFailedJobGetsItsDurationFromTheCondition(t *testing.T) {
	end := time.Now().Add(-time.Hour).Truncate(time.Second)
	began := end.Add(-30 * time.Second)
	// Kubernetes leaves CompletionTime empty on failed Jobs.
	w := start(t, fake.NewClientset(cronJob(), job("failed", batchv1.JobStatus{
		StartTime:  &metav1.Time{Time: began},
		Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: end}, Reason: "BackoffLimitExceeded"}},
	})))

	ev := next(t, w)

	if ev.Kind != EventFail || !ev.OccurredAt.Equal(end) {
		t.Errorf("got %s at %v, want fail at %v", ev.Kind, ev.OccurredAt, end)
	}
	if !ev.DurationKnown || ev.Duration != 30 {
		t.Errorf("duration = %d known=%v, want 30 known", ev.Duration, ev.DurationKnown)
	}
}

func TestJobFinishedAfterStartIsLive(t *testing.T) {
	client := fake.NewClientset(cronJob())
	w := start(t, client)
	time.Sleep(100 * time.Millisecond) // let the informers start

	end := time.Now().Add(time.Second)
	_, err := client.BatchV1().Jobs("prod").Create(context.Background(), job("new", succeeded(end.Add(-time.Second), end)), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	if ev := next(t, w); ev.Replayed {
		t.Error("a Job finishing after the agent started must count as live")
	}
}
