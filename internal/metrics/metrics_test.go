package metrics

import (
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

// value sums a metric's counter or gauge samples matching status ("" = any).
func value(t *testing.T, c *Collectors, name, status string) float64 {
	t.Helper()
	families, err := c.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			matches := status == ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "status" && l.GetValue() == status {
					matches = true
				}
			}
			if matches {
				total += m.GetCounter().GetValue() + m.GetGauge().GetValue()
			}
		}
	}
	return total
}

func histogramCount(t *testing.T, c *Collectors) uint64 {
	t.Helper()
	families, _ := c.Registry().Gather()
	var total uint64
	for _, family := range families {
		if family.GetName() == "sentinelhq_cronjob_duration_seconds" {
			for _, m := range family.GetMetric() {
				total += m.GetHistogram().GetSampleCount()
			}
		}
	}
	return total
}

func TestCountsOnlyFinishedRuns(t *testing.T) {
	c := New("test")

	c.RecordRun(Run{SourceID: "prod/backup", Status: "run", OccurredAt: t0})
	c.RecordRun(Run{SourceID: "prod/backup", Status: "complete", DurationKnown: true, DurationSec: 0, OccurredAt: t0})
	c.RecordRun(Run{SourceID: "prod/backup", Status: "fail", OccurredAt: t0})

	if got := value(t, c, "sentinelhq_cronjob_runs_total", ""); got != 2 {
		t.Errorf("runs_total = %v, want 2 (run not counted)", got)
	}
	if got := histogramCount(t, c); got != 1 {
		t.Errorf("histogram count = %d, want 1 (sub-second counted, unknown skipped)", got)
	}
}

func TestReplayedRunsRefreshTimestampsWithoutCounting(t *testing.T) {
	c := New("test")

	c.RecordRun(Run{SourceID: "prod/backup", Status: "complete", DurationKnown: true, DurationSec: 5, OccurredAt: t0, Replayed: true})

	if got := value(t, c, "sentinelhq_cronjob_runs_total", ""); got != 0 {
		t.Errorf("runs_total = %v, want 0", got)
	}
	if got := histogramCount(t, c); got != 0 {
		t.Errorf("histogram count = %d, want 0", got)
	}
	if got := value(t, c, "sentinelhq_cronjob_last_success_timestamp_seconds", ""); got != float64(t0.Unix()) {
		t.Errorf("last_success = %v, want the Job's time %d", got, t0.Unix())
	}
}

func TestTimestampsNeverGoBackwardsAndFailureIsNotSuccess(t *testing.T) {
	c := New("test")
	newer := t0.Add(time.Hour)

	c.RecordRun(Run{SourceID: "prod/backup", Status: "complete", OccurredAt: newer, Replayed: true})
	c.RecordRun(Run{SourceID: "prod/backup", Status: "complete", OccurredAt: t0, Replayed: true})
	later := newer.Add(time.Hour)
	c.RecordRun(Run{SourceID: "prod/backup", Status: "fail", OccurredAt: later})

	if got := value(t, c, "sentinelhq_cronjob_last_success_timestamp_seconds", ""); got != float64(newer.Unix()) {
		t.Errorf("last_success = %v, want %d", got, newer.Unix())
	}
	if got := value(t, c, "sentinelhq_cronjob_last_run_timestamp_seconds", ""); got != float64(later.Unix()) {
		t.Errorf("last_run = %v, want %d", got, later.Unix())
	}
}

func TestUntrackDropsTheSeriesOfThatCronJobOnly(t *testing.T) {
	c := New("test")
	c.TrackCronJob("prod/backup")
	c.TrackCronJob("prod/backup") // idempotent
	c.TrackCronJob("prod/report")
	c.RecordRun(Run{SourceID: "prod/backup", Status: "complete", DurationKnown: true, OccurredAt: t0})
	c.RecordRun(Run{SourceID: "prod/report", Status: "complete", DurationKnown: true, OccurredAt: t0})

	if got := value(t, c, "sentinelhq_outpost_monitors_active", ""); got != 2 {
		t.Fatalf("monitors_active = %v, want 2", got)
	}

	c.UntrackCronJob("prod/backup")

	if got := value(t, c, "sentinelhq_outpost_monitors_active", ""); got != 1 {
		t.Errorf("monitors_active = %v, want 1", got)
	}
	if got := value(t, c, "sentinelhq_cronjob_runs_total", ""); got != 1 {
		t.Errorf("runs_total = %v, want 1 (only prod/report left)", got)
	}
}

func TestExposesRuntimeCollectors(t *testing.T) {
	families, err := New("test").Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := make(map[string]bool, len(families))
	for _, family := range families {
		names[family.GetName()] = true
	}
	for _, want := range []string{"go_goroutines", "process_start_time_seconds"} {
		if !names[want] {
			t.Errorf("missing %s", want)
		}
	}
}
