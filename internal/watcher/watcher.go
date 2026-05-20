// Package watcher runs the Kubernetes informers that turn raw API server
// events into business signals the rest of the agent can act on:
//
//   - DiscoveredJob events  — emitted when an annotated CronJob appears,
//     changes, or disappears. The registrar consumes these to sync the
//     monitor list on the SentinelHQ server.
//   - LifecycleEvent events — emitted for every state change of a Job/Pod
//     belonging to an annotated CronJob (started, succeeded, failed).
//     The emitter consumes these to ping the server.
//
// All informers run on every pod (leader and standby) so the cache is
// warm everywhere. Whether the resulting events are *forwarded* to the
// server is decided downstream, by the emitter, based on leader state.
package watcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	batchv1listers "k8s.io/client-go/listers/batch/v1"
	"k8s.io/client-go/tools/cache"
)

// AnnotationKey is the marker an operator puts on their CronJob to opt it
// into SentinelHQ monitoring. The value must equal "true" (case-sensitive).
const AnnotationKey = "sentinelhq.sh/monitor"

// DiscoveredJob describes one annotated CronJob in the cluster.
type DiscoveredJob struct {
	// SourceID is stable + cluster-unique: "{namespace}/{cronjob-name}".
	SourceID string

	Name        string
	Namespace   string
	Schedule    string
	Timezone    string
	GracePeriod int
	// Annotations holds the full annotation map so the registrar can read
	// optional SentinelHQ-specific tunables (severity, grace, etc.).
	Annotations map[string]string
}

// EventKind enumerates the lifecycle transitions the watcher reports.
type EventKind string

const (
	EventRun      EventKind = "run"
	EventComplete EventKind = "complete"
	EventFail     EventKind = "fail"
)

// LifecycleEvent is a single observed transition for a CronJob-owned Job.
type LifecycleEvent struct {
	SourceID string // matches the parent CronJob's SourceID
	Kind     EventKind

	// IdempotencyKey lets the SentinelHQ server dedupe retries. We use the
	// Job's UID — guaranteed unique across the cluster lifetime.
	IdempotencyKey string

	Duration int    // seconds, 0 when unknown
	ExitCode int    // 0 = success
	Message  string // short failure reason, empty on success
}

// Watcher owns the informers and the channels the rest of the agent reads
// from. Start it once; cancel the ctx to stop.
type Watcher struct {
	client kubernetes.Interface
	log    *slog.Logger

	factory    informers.SharedInformerFactory
	cronLister batchv1listers.CronJobLister // set in Run(); used by onJobChange

	discoveries chan DiscoveredJob  // ← used by the registrar
	removals    chan string         // ← source IDs that disappeared
	lifecycles  chan LifecycleEvent // ← used by the emitter

	// known tracks source IDs the watcher has ever marked as monitored.
	// We only emit a Removal when we previously emitted a Discovery for it
	// — otherwise every non-annotated CronJob in the cluster would generate
	// noise on the removals channel at startup.
	//
	// jobStates tracks, per Job UID, the highest lifecycle event already
	// emitted. Kubernetes Jobs trigger UpdateFunc many times for the same
	// transition (StartTime added, Active count, Conditions appended); we
	// only emit each kind once per Job. Cleared on Job deletion.
	mu        sync.Mutex
	known     map[string]struct{}
	jobStates map[string]EventKind
}

// New constructs an idle Watcher. Call Run to start the informers.
//
// resyncPeriod is the safety net for missed events — informers re-list
// the API every N. 5 minutes is the k8s default for most controllers.
func New(client kubernetes.Interface, resync time.Duration, log *slog.Logger) *Watcher {
	return &Watcher{
		client:      client,
		log:         log,
		factory:     informers.NewSharedInformerFactory(client, resync),
		discoveries: make(chan DiscoveredJob, 64),
		removals:    make(chan string, 64),
		lifecycles:  make(chan LifecycleEvent, 256),
		known:       make(map[string]struct{}),
		jobStates:   make(map[string]EventKind),
	}
}

// Discoveries returns the channel emitting every annotated CronJob seen.
// Closed when the underlying context cancels.
func (w *Watcher) Discoveries() <-chan DiscoveredJob { return w.discoveries }

// Removals returns source IDs of CronJobs that vanished (deletion or
// removal of the annotation).
func (w *Watcher) Removals() <-chan string { return w.removals }

// Lifecycles returns the channel emitting Job-level transitions for the
// monitored CronJobs.
func (w *Watcher) Lifecycles() <-chan LifecycleEvent { return w.lifecycles }

// Run blocks until ctx is cancelled. It wires up handlers on the CronJob
// and Job informers and lets the shared factory drive everything.
func (w *Watcher) Run(ctx context.Context) error {
	cronJobs := w.factory.Batch().V1().CronJobs()
	w.cronLister = cronJobs.Lister()

	cronInformer := cronJobs.Informer()
	jobInformer := w.factory.Batch().V1().Jobs().Informer()

	if _, err := cronInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.onCronJobChange(obj, false) },
		UpdateFunc: func(_, obj any) { w.onCronJobChange(obj, false) },
		DeleteFunc: func(obj any) { w.onCronJobChange(obj, true) },
	}); err != nil {
		return fmt.Errorf("cronjob handler: %w", err)
	}

	if _, err := jobInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.onJobChange(obj) },
		UpdateFunc: func(_, obj any) { w.onJobChange(obj) },
		DeleteFunc: func(obj any) { w.onJobDelete(obj) },
	}); err != nil {
		return fmt.Errorf("job handler: %w", err)
	}

	w.log.Info("starting informers")
	w.factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), cronInformer.HasSynced, jobInformer.HasSynced) {
		return fmt.Errorf("cache sync failed before context cancel")
	}
	w.log.Info("informers synced")

	<-ctx.Done()
	close(w.discoveries)
	close(w.removals)
	close(w.lifecycles)
	return nil
}

// ── Internal handlers ────────────────────────────────────────────────────

func (w *Watcher) onCronJobChange(obj any, deleted bool) {
	cj, ok := obj.(*batchv1.CronJob)
	if !ok {
		// On deletion the informer may give us a DeletedFinalStateUnknown
		// wrapper instead of the typed object — unwrap once.
		if tomb, isTomb := obj.(cache.DeletedFinalStateUnknown); isTomb {
			if cj, ok = tomb.Obj.(*batchv1.CronJob); !ok {
				return
			}
		} else {
			return
		}
	}

	sourceID := sourceIDFor(cj.Namespace, cj.Name)
	monitored := isMonitored(cj.Annotations)

	w.mu.Lock()
	_, wasKnown := w.known[sourceID]
	w.mu.Unlock()

	// Three transitions matter:
	//   1. Now monitored AND not deleted  → emit Discovery
	//   2. Was known, but no longer monitored OR deleted → emit Removal
	//   3. Anything else (never seen, still not monitored) → ignore silently
	switch {
	case monitored && !deleted:
		w.mu.Lock()
		w.known[sourceID] = struct{}{}
		w.mu.Unlock()
		w.log.Info("cronjob discovered", "source_id", sourceID, "schedule", cj.Spec.Schedule)
		w.discoveries <- DiscoveredJob{
			SourceID:    sourceID,
			Name:        cj.Name,
			Namespace:   cj.Namespace,
			Schedule:    cj.Spec.Schedule,
			Timezone:    timezoneOf(cj),
			GracePeriod: gracePeriodOf(cj),
			Annotations: cj.Annotations,
		}

	case wasKnown:
		w.mu.Lock()
		delete(w.known, sourceID)
		w.mu.Unlock()
		w.log.Info("cronjob removed", "source_id", sourceID)
		w.removals <- sourceID
	}
}

func (w *Watcher) onJobChange(obj any) {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return
	}

	// Only Jobs whose owner is a CronJob interest us. Standalone Jobs and
	// Argo workflow Jobs are out of scope for Phase 2.
	owner := cronJobOwnerOf(job)
	if owner == "" {
		return
	}

	// Filter out Jobs whose parent CronJob is not annotated. Without this
	// every CronJob in the cluster — annotated or not — would emit metrics.
	parent, err := w.cronLister.CronJobs(job.Namespace).Get(owner)
	if err != nil || !isMonitored(parent.Annotations) {
		return
	}

	sourceID := sourceIDFor(job.Namespace, owner)
	uid := string(job.UID)

	// Determine the current lifecycle state. k8s Job UpdateFunc fires many
	// times for the same Job (StartTime added, Active count changes,
	// Conditions appended, CompletionTime set). We only want one event per
	// transition: track the highest kind already emitted and short-circuit.
	var kind EventKind
	switch {
	case isJobFailed(job):
		kind = EventFail
	case isJobSucceeded(job):
		kind = EventComplete
	case isJobStarted(job):
		kind = EventRun
	default:
		return
	}

	w.mu.Lock()
	prev, seen := w.jobStates[uid]
	// Skip if we already emitted this kind, or a stronger one. The ordering
	// run → complete|fail is monotonic — once a Job is terminal we never
	// want to walk back to "run".
	if seen && (prev == kind || prev == EventComplete || prev == EventFail) {
		w.mu.Unlock()
		return
	}
	w.jobStates[uid] = kind
	w.mu.Unlock()

	ev := LifecycleEvent{
		SourceID:       sourceID,
		Kind:           kind,
		IdempotencyKey: uid + ":" + string(kind),
	}
	switch kind {
	case EventFail:
		ev.Duration = jobDuration(job)
		ev.Message = failMessage(job)
	case EventComplete:
		ev.Duration = jobDuration(job)
	}

	w.lifecycles <- ev
}

// onJobDelete cleans up the per-Job dedup map when k8s garbage-collects an
// old Job (via successfulJobsHistoryLimit / failedJobsHistoryLimit).
// Without this, jobStates would grow unboundedly across the agent's lifetime.
func (w *Watcher) onJobDelete(obj any) {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		if tomb, isTomb := obj.(cache.DeletedFinalStateUnknown); isTomb {
			if job, ok = tomb.Obj.(*batchv1.Job); !ok {
				return
			}
		} else {
			return
		}
	}
	w.mu.Lock()
	delete(w.jobStates, string(job.UID))
	w.mu.Unlock()
}

// ── Helpers ──────────────────────────────────────────────────────────────

func isMonitored(annotations map[string]string) bool {
	return annotations[AnnotationKey] == "true"
}

func sourceIDFor(namespace, name string) string {
	return namespace + "/" + name
}

// timezoneOf reads the standard `spec.timeZone` field (k8s 1.27+). Falls
// back to the cluster scheduler default (UTC) when absent.
func timezoneOf(cj *batchv1.CronJob) string {
	if cj.Spec.TimeZone != nil && *cj.Spec.TimeZone != "" {
		return *cj.Spec.TimeZone
	}
	return "UTC"
}

// gracePeriodOf reads an opt-in `sentinelhq.sh/grace-seconds: "120"`
// annotation. Returns 0 (server default applies) when missing/invalid.
func gracePeriodOf(cj *batchv1.CronJob) int {
	s := cj.Annotations["sentinelhq.sh/grace-seconds"]
	if s == "" {
		return 0
	}
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// cronJobOwnerOf returns the CronJob name from the Job's ownerReferences,
// or "" if the Job is standalone.
func cronJobOwnerOf(job *batchv1.Job) string {
	for _, ref := range job.OwnerReferences {
		if ref.Kind == "CronJob" && ref.APIVersion == "batch/v1" {
			return ref.Name
		}
	}
	return ""
}

func isJobStarted(job *batchv1.Job) bool {
	return job.Status.StartTime != nil && !isJobSucceeded(job) && !isJobFailed(job)
}

func isJobSucceeded(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isJobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobDuration(job *batchv1.Job) int {
	if job.Status.StartTime == nil || job.Status.CompletionTime == nil {
		return 0
	}
	return int(job.Status.CompletionTime.Sub(job.Status.StartTime.Time).Seconds())
}

func failMessage(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			if c.Reason != "" {
				return c.Reason
			}
			return c.Message
		}
	}
	return "job failed"
}
