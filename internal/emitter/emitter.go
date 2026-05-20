// Package emitter is the fan-out point for lifecycle events. Every event
// the watcher produces lands here and is dispatched to two outputs:
//
//   - Prometheus metrics: counter + histogram + last-run gauge.
//     Updated on EVERY pod (leader and standby) — scrapers don't care who
//     wrote the sample, they just need any pod to expose it.
//   - SentinelHQ backend push: only on the leader, and only when push mode
//     is enabled. Standby pods skip the network call.
//
// Either output can be wired or skipped independently — pass nil to disable.
package emitter

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/sentinelhq-sh/outpost/internal/metrics"
	"github.com/sentinelhq-sh/outpost/internal/reporter"
	"github.com/sentinelhq-sh/outpost/internal/watcher"
)

// LeaderChecker is the small interface the elector exposes.
type LeaderChecker interface {
	IsLeader() bool
}

// Emitter dispatches events to one or both outputs. Either field may be
// nil — the agent supports push-only, prometheus-only, or dual mode.
type Emitter struct {
	client  *reporter.Client    // optional: nil disables backend push
	metrics *metrics.Collectors // optional: nil disables Prometheus
	leader  LeaderChecker
	log     *slog.Logger
}

// New builds an Emitter. At least one of (client, metrics) must be set —
// validation lives in the caller, the emitter itself just routes.
func New(client *reporter.Client, m *metrics.Collectors, leader LeaderChecker, log *slog.Logger) *Emitter {
	return &Emitter{client: client, metrics: m, leader: leader, log: log}
}

// Run blocks until ctx is cancelled, reading events from the channel.
func (e *Emitter) Run(ctx context.Context, events <-chan watcher.LifecycleEvent) {
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

	err := e.client.Event(ctx, reporter.EventPayload{
		SourceID:       ev.SourceID,
		State:          string(ev.Kind),
		Duration:       ev.Duration,
		ExitCode:       ev.ExitCode,
		Message:        ev.Message,
		IdempotencyKey: ev.IdempotencyKey,
	})
	if err != nil {
		e.log.Error("emit failed",
			"source_id", ev.SourceID,
			"kind", string(ev.Kind),
			"error", err.Error(),
		)
		return
	}
	e.log.Info("event sent",
		"source_id", ev.SourceID,
		"kind", string(ev.Kind),
	)
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
