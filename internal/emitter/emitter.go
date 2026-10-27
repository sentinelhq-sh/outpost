// Package emitter is the fan-out point for lifecycle events. Every event
// the watcher produces lands here and is dispatched to two outputs:
//
//   - Prometheus metrics: counter + histogram + last-run gauge.
//     Updated on EVERY pod (leader and standby) — scrapers don't care who
//     wrote the sample, they just need any pod to expose it.
//   - SentinelHQ backend push: only on the leader, and only when push mode
//     is enabled. Standby pods skip the network call.
//
// Backend pushes go through an in-memory FIFO: a send that fails because the
// server is unreachable or the key/approval is being fixed stays queued and
// is retried with backoff instead of being dropped (a lost "complete" turns
// into a false missed-run alert). The server dedupes by idempotency_key, so
// replays are safe, and retrying the head of the queue keeps run order.
// Events are only dropped when the server rejects them (404/422), when the
// queue is full (oldest first), or on shutdown.
//
// Either output can be wired or skipped independently — pass nil to disable.
package emitter

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/sentinelhq-sh/outpost/internal/metrics"
	"github.com/sentinelhq-sh/outpost/internal/reporter"
	"github.com/sentinelhq-sh/outpost/internal/watcher"
)

// LeaderChecker is the small interface the elector exposes.
type LeaderChecker interface {
	IsLeader() bool
}

const (
	defaultMaxPending = 10_000
	defaultMinBackoff = 5 * time.Second
	defaultMaxBackoff = 5 * time.Minute
)

// Emitter dispatches events to one or both outputs. Either field may be
// nil — the agent supports push-only, prometheus-only, or dual mode.
type Emitter struct {
	client  *reporter.Client    // optional: nil disables backend push
	metrics *metrics.Collectors // optional: nil disables Prometheus
	leader  LeaderChecker
	log     *slog.Logger

	mu         sync.Mutex
	pending    []reporter.EventPayload // FIFO of events not yet accepted
	wake       chan struct{}
	maxPending int
	minBackoff time.Duration
	maxBackoff time.Duration
}

// New builds an Emitter. At least one of (client, metrics) must be set —
// validation lives in the caller, the emitter itself just routes.
func New(client *reporter.Client, m *metrics.Collectors, leader LeaderChecker, log *slog.Logger) *Emitter {
	return &Emitter{
		client:     client,
		metrics:    m,
		leader:     leader,
		log:        log,
		wake:       make(chan struct{}, 1),
		maxPending: defaultMaxPending,
		minBackoff: defaultMinBackoff,
		maxBackoff: defaultMaxBackoff,
	}
}

// Run blocks until ctx is cancelled, reading events from the channel.
func (e *Emitter) Run(ctx context.Context, events <-chan watcher.LifecycleEvent) {
	if e.client != nil {
		go e.sendLoop(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			e.dispatch(ctx, ev)
		}
	}
}

func (e *Emitter) dispatch(ctx context.Context, ev watcher.LifecycleEvent) {
	// Prometheus first: cheap, in-memory, runs on every pod so the metric
	// stays consistent regardless of which one a scraper hits.
	if e.metrics != nil {
		namespace, cronjob := splitSourceID(ev.SourceID)
		e.metrics.RecordRun(
			namespace, cronjob, string(ev.Kind),
			ev.Duration,
			time.Now().Unix(),
		)
	}

	// Backend push: only the leader, and only when configured.
	if e.client == nil {
		return
	}
	if !e.leader.IsLeader() {
		e.log.Debug("skipping backend push — not the leader",
			"source_id", ev.SourceID,
			"kind", string(ev.Kind),
		)
		return
	}

	e.enqueue(reporter.EventPayload{
		SourceID:       ev.SourceID,
		State:          string(ev.Kind),
		Duration:       ev.Duration,
		ExitCode:       ev.ExitCode,
		Message:        ev.Message,
		IdempotencyKey: ev.IdempotencyKey,
	})
}

func (e *Emitter) enqueue(ev reporter.EventPayload) {
	e.mu.Lock()
	if len(e.pending) >= e.maxPending {
		dropped := e.pending[0]
		e.pending = e.pending[1:]
		e.log.Error("event queue full, dropping oldest event",
			"source_id", dropped.SourceID,
			"kind", dropped.State,
		)
	}
	e.pending = append(e.pending, ev)
	e.mu.Unlock()

	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// sendLoop drains the queue in order. A retryable failure keeps the event at
// the head and waits, doubling the delay up to maxBackoff.
func (e *Emitter) sendLoop(ctx context.Context) {
	backoff := e.minBackoff

	for {
		ev, ok := e.head()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-e.wake:
				continue
			}
		}

		err := e.client.Event(ctx, ev)
		switch {
		case err == nil:
			e.remove(ev)
			backoff = e.minBackoff
			e.log.Info("event sent", "source_id", ev.SourceID, "kind", ev.State)

		case errors.Is(err, reporter.ErrPermanent):
			e.remove(ev)
			e.log.Error("event rejected by the server, dropping",
				"source_id", ev.SourceID,
				"kind", ev.State,
				"error", err.Error(),
			)

		default:
			if ctx.Err() != nil {
				e.log.Warn("shutting down with unsent events", "pending", e.size())
				return
			}
			e.log.Warn("event send failed, will retry",
				"source_id", ev.SourceID,
				"kind", ev.State,
				"pending", e.size(),
				"retry_in", backoff.String(),
				"error", err.Error(),
			)
			select {
			case <-ctx.Done():
				e.log.Warn("shutting down with unsent events", "pending", e.size())
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, e.maxBackoff)
		}
	}
}

func (e *Emitter) head() (reporter.EventPayload, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.pending) == 0 {
		return reporter.EventPayload{}, false
	}
	return e.pending[0], true
}

// remove drops ev if it is still at the head: a full queue may already have
// evicted it while it was being sent.
func (e *Emitter) remove(ev reporter.EventPayload) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.pending) > 0 && e.pending[0] == ev {
		e.pending = e.pending[1:]
	}
}

func (e *Emitter) size() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

// splitSourceID splits "namespace/name" back into its components. The
// watcher always uses that exact format; defensive fallback for safety.
func splitSourceID(sourceID string) (namespace, cronjob string) {
	idx := strings.IndexByte(sourceID, '/')
	if idx < 0 {
		return "default", sourceID
	}
	return sourceID[:idx], sourceID[idx+1:]
}
