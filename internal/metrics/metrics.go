// Package metrics owns the Prometheus collectors the agent exposes so a
// running cluster can be observed from any Prometheus-compatible system
// (Prometheus, VictoriaMetrics, Grafana Agent, OpenTelemetry Collector).
//
// The collectors are populated as a side-effect of the watcher emitting
// LifecycleEvents and the elector flipping its leader flag. Whether the
// agent ALSO pushes to the SentinelHQ backend is orthogonal — both modes
// coexist without sharing state.
package metrics

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/sentinelhq-sh/outpost/internal/version"
)

const (
	statusRun      = "run"
	statusComplete = "complete"
)

// Collectors bundles every Prometheus metric the agent owns. Holding them
// on a struct (rather than as package-globals) keeps tests hermetic — each
// test can spin up its own Registry without leaking state.
type Collectors struct {
	registry *prometheus.Registry

	// Finished runs per CronJob. Labels: namespace, cronjob, status
	// (complete|fail). Every pod exposes them: aggregate with max by, not sum.
	RunsTotal *prometheus.CounterVec

	// Per-CronJob duration distribution in seconds. Observed at the end of
	// each Job (complete or fail). Labels: namespace, cronjob.
	DurationSeconds *prometheus.HistogramVec

	// Job-reported time (Unix seconds) of the latest transition of any kind,
	// and of the latest success. Labels: namespace, cronjob.
	LastRunTimestamp     *prometheus.GaugeVec
	LastSuccessTimestamp *prometheus.GaugeVec

	// Agent self-health.
	Leader         prometheus.Gauge
	MonitorsActive prometheus.Gauge
	Info           *prometheus.GaugeVec

	mu          sync.Mutex
	tracked     map[string]struct{} // source IDs of the monitored CronJobs
	lastRun     map[string]float64  // highest value per source ID: replayed
	lastSuccess map[string]float64  // Jobs arrive in no particular order
}

// Run is one lifecycle transition of a CronJob-owned Job.
type Run struct {
	SourceID      string // namespace/cronjob
	Status        string // run | complete | fail
	DurationSec   int
	DurationKnown bool
	OccurredAt    time.Time
	// Replayed transitions predate this process: they refresh the timestamps
	// but are not counted again.
	Replayed bool
}

// New builds a fresh Registry and registers all collectors. Returns the
// bundle so callers can both expose the registry (HTTP) and mutate the
// metrics (watcher, elector).
func New(clusterName string) *Collectors {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	c := &Collectors{
		registry:    reg,
		tracked:     make(map[string]struct{}),
		lastRun:     make(map[string]float64),
		lastSuccess: make(map[string]float64),

		RunsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sentinelhq_cronjob_runs_total",
				Help: "Finished CronJob runs observed by the agent, by outcome (complete|fail).",
			},
			[]string{"namespace", "cronjob", "status"},
		),

		DurationSeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "sentinelhq_cronjob_duration_seconds",
				Help: "Distribution of CronJob run durations in seconds.",
				// Buckets tuned for cron jobs: 1s → 1h. Bigger jobs will
				// land in the +Inf bucket which is enough for alerting.
				Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600},
			},
			[]string{"namespace", "cronjob"},
		),

		LastRunTimestamp: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "sentinelhq_cronjob_last_run_timestamp_seconds",
				Help: "Unix timestamp of the last run start or end, regardless of outcome.",
			},
			[]string{"namespace", "cronjob"},
		),

		LastSuccessTimestamp: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "sentinelhq_cronjob_last_success_timestamp_seconds",
				Help: "Unix timestamp of the last successful run.",
			},
			[]string{"namespace", "cronjob"},
		),

		Leader: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "sentinelhq_outpost_leader",
				Help: "1 if this pod currently holds the leader lease, 0 otherwise.",
			},
		),

		MonitorsActive: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "sentinelhq_outpost_monitors_active",
				Help: "Number of annotated CronJobs the agent is currently tracking.",
			},
		),

		Info: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "sentinelhq_outpost_info",
				Help: "Build info — always set to 1, label dimensions carry version / commit / cluster.",
			},
			[]string{"version", "commit", "cluster"},
		),
	}

	// One-shot info metric, written once at startup.
	c.Info.WithLabelValues(version.Version, version.Commit, clusterName).Set(1)

	return c
}

// Registry exposes the *prometheus.Registry so the HTTP server can wire
// promhttp.HandlerFor against it.
func (c *Collectors) Registry() *prometheus.Registry { return c.registry }

// ── Mutation helpers ─────────────────────────────────────────────────────
//
// All mutation goes through typed methods rather than direct counter
// access, so the call sites read like business code, not Prometheus glue.

// RecordRun updates the timestamps, then counts finished runs seen live.
func (c *Collectors) RecordRun(r Run) {
	namespace, cronjob := SplitSourceID(r.SourceID)
	occurredAt := float64(r.OccurredAt.Unix())

	c.mu.Lock()
	if occurredAt > c.lastRun[r.SourceID] {
		c.lastRun[r.SourceID] = occurredAt
		c.LastRunTimestamp.WithLabelValues(namespace, cronjob).Set(occurredAt)
	}
	if r.Status == statusComplete && occurredAt > c.lastSuccess[r.SourceID] {
		c.lastSuccess[r.SourceID] = occurredAt
		c.LastSuccessTimestamp.WithLabelValues(namespace, cronjob).Set(occurredAt)
	}
	c.mu.Unlock()

	if r.Replayed || r.Status == statusRun {
		return
	}

	c.RunsTotal.WithLabelValues(namespace, cronjob, r.Status).Inc()
	if r.DurationKnown {
		c.DurationSeconds.WithLabelValues(namespace, cronjob).Observe(float64(r.DurationSec))
	}
}

// SetLeader flips the leader gauge.
func (c *Collectors) SetLeader(isLeader bool) {
	if isLeader {
		c.Leader.Set(1)
	} else {
		c.Leader.Set(0)
	}
}

// TrackCronJob records a monitored CronJob. Safe to call repeatedly.
func (c *Collectors) TrackCronJob(sourceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tracked[sourceID] = struct{}{}
	c.MonitorsActive.Set(float64(len(c.tracked)))
}

// UntrackCronJob forgets a CronJob that no longer exists or is no longer
// monitored, and drops its series so it stops appearing in queries.
func (c *Collectors) UntrackCronJob(sourceID string) {
	namespace, cronjob := SplitSourceID(sourceID)
	labels := prometheus.Labels{"namespace": namespace, "cronjob": cronjob}

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.tracked, sourceID)
	delete(c.lastRun, sourceID)
	delete(c.lastSuccess, sourceID)
	c.MonitorsActive.Set(float64(len(c.tracked)))
	c.RunsTotal.DeletePartialMatch(labels)
	c.DurationSeconds.DeletePartialMatch(labels)
	c.LastRunTimestamp.DeletePartialMatch(labels)
	c.LastSuccessTimestamp.DeletePartialMatch(labels)
}

// SplitSourceID splits "namespace/name" back into its components. The
// watcher always uses that exact format; defensive fallback for safety.
func SplitSourceID(sourceID string) (namespace, cronjob string) {
	namespace, cronjob, found := strings.Cut(sourceID, "/")
	if !found {
		return "default", sourceID
	}
	return namespace, cronjob
}
