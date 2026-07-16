// Package registrar keeps the SentinelHQ server's view of the cluster's
// annotated CronJobs in sync with reality. It does NOT push lifecycle
// events (see emitter for that) — it only owns the list of monitors.
//
// Sync strategy: maintain an in-memory snapshot of all currently-known
// DiscoveredJobs. On any change (or every PollInterval), POST the full
// list to /api/v1/outpost/register, which is idempotent on the server side.
//
// Only the leader is allowed to call the registrar's Sync — the channel
// it reads from is the same on every pod, but the IsLeader gate avoids
// duplicate writes when multiple pods are running.
package registrar

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sentinelhq-sh/outpost/internal/reporter"
	"github.com/sentinelhq-sh/outpost/internal/watcher"
)

// LeaderChecker is the small interface registrar needs from the elector.
// Decoupling here lets us swap it in tests for a constant true/false.
type LeaderChecker interface {
	IsLeader() bool
}

// Registrar consumes Discoveries/Removals from a Watcher, keeps an
// in-memory map of the cluster's monitored CronJobs, and periodically
// reconciles that map with the SentinelHQ server.
type Registrar struct {
	client       *reporter.Client
	leader       LeaderChecker
	log          *slog.Logger
	pollInterval time.Duration
	clusterName  string

	mu      sync.Mutex
	current map[string]watcher.DiscoveredJob // keyed by SourceID
	dirty   bool                             // a change happened since last sync
}

// New builds an idle Registrar. Call Run to start consuming events.
func New(
	client *reporter.Client,
	leader LeaderChecker,
	clusterName string,
	pollInterval time.Duration,
	log *slog.Logger,
) *Registrar {
	return &Registrar{
		client:       client,
		leader:       leader,
		clusterName:  clusterName,
		pollInterval: pollInterval,
		log:          log,
		current:      make(map[string]watcher.DiscoveredJob),
	}
}

// Run blocks until ctx is cancelled. It listens to the watcher's channels
// for changes and runs a periodic safety-net sync.
func (r *Registrar) Run(
	ctx context.Context,
	discoveries <-chan watcher.DiscoveredJob,
	removals <-chan string,
) {
	tick := time.NewTicker(r.pollInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case d, ok := <-discoveries:
			if !ok {
				return
			}
			r.upsert(d)

		case sourceID, ok := <-removals:
			if !ok {
				return
			}
			r.remove(sourceID)

		case <-tick.C:
			r.maybeSync(ctx)
		}
	}
}

func (r *Registrar) upsert(d watcher.DiscoveredJob) {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, had := r.current[d.SourceID]
	if had && discoveriesEqual(existing, d) {
		return
	}
	r.current[d.SourceID] = d
	r.dirty = true
}

func (r *Registrar) remove(sourceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.current[sourceID]; !ok {
		return
	}
	delete(r.current, sourceID)
	r.dirty = true
}

// maybeSync pushes the current map to the server, but only when we are
// the leader AND something has actually changed (or the periodic tick is
// covering for a missed event).
func (r *Registrar) maybeSync(ctx context.Context) {
	if !r.leader.IsLeader() {
		return
	}

	r.mu.Lock()
	if !r.dirty && len(r.current) > 0 {
		// Periodic re-sync even when not dirty — catches the server-side
		// state drift (admin manually deleted a monitor, etc.).
		r.dirty = true
	}
	if !r.dirty {
		r.mu.Unlock()
		return
	}

	snapshot := make([]watcher.DiscoveredJob, 0, len(r.current))
	for _, d := range r.current {
		snapshot = append(snapshot, d)
	}
	r.dirty = false
	r.mu.Unlock()

	r.pushSnapshot(ctx, snapshot)
}

func (r *Registrar) pushSnapshot(ctx context.Context, jobs []watcher.DiscoveredJob) {
	payload := reporter.JobsPayload{
		Runtime: "kubernetes",
		SourceMeta: map[string]any{
			"cluster": r.clusterName,
		},
		Discovered: make([]reporter.DiscoveredJob, len(jobs)),
	}
	for i, j := range jobs {
		payload.Discovered[i] = reporter.DiscoveredJob{
			SourceID:    j.SourceID,
			Name:        j.Name,
			Schedule:    j.Schedule,
			Timezone:    j.Timezone,
			GracePeriod: j.GracePeriod,
			SourceMeta: map[string]any{
				"namespace": j.Namespace,
			},
		}
	}

	resp, err := r.client.ReportJobs(ctx, payload)
	if err != nil {
		r.log.Error("registrar sync failed", "error", err.Error())
		r.mu.Lock()
		r.dirty = true // re-try next tick
		r.mu.Unlock()
		return
	}
	r.log.Info("registrar synced",
		"created", len(resp.Created),
		"removed", len(resp.Removed),
	)
}

func discoveriesEqual(a, b watcher.DiscoveredJob) bool {
	return a.Name == b.Name &&
		a.Schedule == b.Schedule &&
		a.Timezone == b.Timezone &&
		a.GracePeriod == b.GracePeriod
}
