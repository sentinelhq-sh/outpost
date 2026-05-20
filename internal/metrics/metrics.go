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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/sentinelhq-sh/outpost/internal/version"
)

// Collectors bundles every Prometheus metric the agent owns. Holding them
// on a struct (rather than as package-globals) keeps tests hermetic — each
// test can spin up its own Registry without leaking state.
type Collectors struct {
	registry *prometheus.Registry

	// Per-CronJob counters — incremented as the watcher observes lifecycle
	// events. Labels: namespace, cronjob, status (complete|fail).
	RunsTotal *prometheus.CounterVec

	// Per-CronJob duration distribution in seconds. Observed at the end of
	// each Job (complete or fail). Labels: namespace, cronjob.
	DurationSeconds *prometheus.HistogramVec

	// Timestamp (Unix seconds) of the most recent run for each CronJob.
	// Labels: namespace, cronjob.
	LastRunTimestamp *prometheus.GaugeVec

	// Agent self-health.
	Leader         prometheus.Gauge
	MonitorsActive prometheus.Gauge
	Info           *prometheus.GaugeVec
}

// New builds a fresh Registry and registers all collectors. Returns the
// bundle so callers can both expose the registry (HTTP) and mutate the
// metrics (watcher, elector).
func New(clusterName string) *Collectors {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)

	c := &Collectors{
		registry: reg,

		RunsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sentinelhq_cronjob_runs_total",
				Help: "Total CronJob executions observed by the agent.",
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
				Help: "Unix timestamp of the last observed run, regardless of outcome.",
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

// RecordRun bumps the runs counter and the last-run timestamp.
func (c *Collectors) RecordRun(namespace, cronjob, status string, durationSec int, observedAt int64) {
	c.RunsTotal.WithLabelValues(namespace, cronjob, status).Inc()
	c.LastRunTimestamp.WithLabelValues(namespace, cronjob).Set(float64(observedAt))

	if durationSec > 0 {
		c.DurationSeconds.WithLabelValues(namespace, cronjob).Observe(float64(durationSec))
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

// SetMonitorsActive updates the count of tracked CronJobs.
func (c *Collectors) SetMonitorsActive(n int) {
	c.MonitorsActive.Set(float64(n))
}

// ForgetCronJob drops the metrics for a CronJob that no longer exists,
// so a deleted job stops appearing in Prometheus after a restart.
func (c *Collectors) ForgetCronJob(namespace, cronjob string) {
	c.RunsTotal.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "cronjob": cronjob})
	c.DurationSeconds.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "cronjob": cronjob})
	c.LastRunTimestamp.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "cronjob": cronjob})
}
