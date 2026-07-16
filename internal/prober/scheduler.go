package prober

import (
	"context"
	"log/slog"
	"time"

	"github.com/sentinelhq-sh/outpost/internal/reporter"
)

// LeaderChecker gates execution: only the leader runs probes so N standby
// pods don't hammer the same target. Same interface the emitter uses.
type LeaderChecker interface {
	IsLeader() bool
}

// Scheduler pulls the probe list from the backend periodically and runs each
// probe on its own interval. Results are POSTed back through the reporter.
// Only the leader executes; followers pull nothing and stay idle.
// runningProbe tracks a probe loop and the spec it was started with, so a
// later reconcile can tell whether the spec changed and restart the loop.
type runningProbe struct {
	cancel context.CancelFunc
	spec   reporter.ProbeSpec
}

type Scheduler struct {
	client            *reporter.Client
	leader            LeaderChecker
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	log               *slog.Logger

	running    map[string]runningProbe // source_id -> running loop + its spec
	lastEpochs map[string]int          // monitor_id -> last epoch seen via heartbeat
}

func New(client *reporter.Client, leader LeaderChecker, pollInterval time.Duration, log *slog.Logger) *Scheduler {
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}
	return &Scheduler{
		client:            client,
		leader:            leader,
		pollInterval:      pollInterval,
		heartbeatInterval: 2 * time.Second,
		log:               log,
		running:           make(map[string]runningProbe),
		lastEpochs:        make(map[string]int),
	}
}

// Run blocks until ctx is cancelled. Two loops drive it:
//   - a fast heartbeat (2s) that keeps this agent live in the backend's
//     failover arbiter and reconciles immediately when an epoch changes
//     (the active/standby layout shifted — e.g. we just got promoted);
//   - a slow config tick (pollInterval) as a safety net for spec changes
//     and missed epoch signals.
func (s *Scheduler) Run(ctx context.Context) {
	cfgTick := time.NewTicker(s.pollInterval)
	defer cfgTick.Stop()
	hbTick := time.NewTicker(s.heartbeatInterval)
	defer hbTick.Stop()

	s.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			s.stopAll()
			return
		case <-cfgTick.C:
			s.reconcile(ctx)
		case <-hbTick.C:
			if s.heartbeat(ctx) {
				s.reconcile(ctx) // epoch moved — pull the new active set now
			}
		}
	}
}

// heartbeat pings liveness and reports whether any tracked epoch changed since
// the last beat (meaning we should re-pull config). Followers skip it: only the
// leader executes, so only it needs to be live in the pool.
func (s *Scheduler) heartbeat(ctx context.Context) bool {
	if !s.leader.IsLeader() {
		return false
	}

	resp, err := s.client.Heartbeat(ctx)
	if err != nil {
		s.log.Warn("heartbeat failed", "error", err.Error())
		return false
	}

	changed := false
	for monitorID, epoch := range resp.Epochs {
		if prev, ok := s.lastEpochs[monitorID]; !ok || prev != epoch {
			changed = true
		}
		s.lastEpochs[monitorID] = epoch
	}

	return changed
}

// reconcile pulls the current probe set and starts/stops per-probe loops to
// match it. On follower pods it stops everything — only the leader probes.
func (s *Scheduler) reconcile(ctx context.Context) {
	if !s.leader.IsLeader() {
		s.stopAll()
		return
	}

	cfg, err := s.client.Config(ctx)
	if err != nil {
		s.log.Warn("probe config pull failed", "error", err.Error())
		return
	}

	desired := make(map[string]reporter.ProbeSpec, len(cfg.Probes))
	for _, spec := range cfg.Probes {
		desired[spec.SourceID] = spec
	}

	var started, stopped, updated int

	// Stop loops no longer desired.
	for id, rp := range s.running {
		if _, ok := desired[id]; !ok {
			rp.cancel()
			delete(s.running, id)
			stopped++
		}
	}

	// Start new probes; restart those whose spec changed (target, interval…).
	for id, spec := range desired {
		if rp, ok := s.running[id]; ok {
			if rp.spec == spec {
				continue // unchanged — leave its loop running
			}
			rp.cancel() // spec changed — restart with the new one
			updated++
		} else {
			started++
		}

		loopCtx, cancel := context.WithCancel(ctx)
		s.running[id] = runningProbe{cancel: cancel, spec: spec}
		go s.runProbe(loopCtx, spec)
	}

	if started+stopped+updated > 0 {
		s.log.Info("probes reconciled",
			"started", started, "stopped", stopped, "updated", updated,
			"active", len(s.running),
		)
	}
}

// runProbe executes one probe on its interval until its context is cancelled.
func (s *Scheduler) runProbe(ctx context.Context, spec reporter.ProbeSpec) {
	interval := time.Duration(spec.Interval) * time.Second
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.execute(ctx, spec) // probe immediately, then on each tick

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.execute(ctx, spec)
		}
	}
}

// execute runs the probe once and reports the result as a backend event.
func (s *Scheduler) execute(ctx context.Context, spec reporter.ProbeSpec) {
	res := Probe(ctx, spec)

	state := "complete"
	if !res.Up {
		state = "fail"
	}

	payload := reporter.EventPayload{
		SourceID: spec.SourceID,
		State:    state,
		Duration: res.LatencyMs,
		ExitCode: res.StatusCode,
		Message:  res.Message,
	}

	if err := s.client.Event(ctx, payload); err != nil {
		s.log.Warn("probe result push failed",
			"source_id", spec.SourceID,
			"state", state,
			"error", err.Error(),
		)
	}
}

func (s *Scheduler) stopAll() {
	for id, rp := range s.running {
		rp.cancel()
		delete(s.running, id)
	}
}
