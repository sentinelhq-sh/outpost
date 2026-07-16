package prober

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sentinelhq-sh/outpost/internal/config"
	"github.com/sentinelhq-sh/outpost/internal/reporter"
)

type fakeLeader struct{ leader atomic.Bool }

func (f *fakeLeader) IsLeader() bool { return f.leader.Load() }

// backend captures probe-result events and serves a static config.
type backend struct {
	mu     sync.Mutex
	events []reporter.EventPayload
}

func (b *backend) handler(target string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/outpost/config", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(reporter.ConfigResponse{
			ProjectID: 1,
			Probes: []reporter.ProbeSpec{
				{ID: 1, SourceID: "probe-1", Kind: "http", Target: target, ExpectedStatus: 200, Interval: 30, TimeoutMs: 1000},
			},
		})
	})
	mux.HandleFunc("/api/v1/outpost/events", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var ev reporter.EventPayload
		_ = json.Unmarshal(body, &ev)
		b.mu.Lock()
		b.events = append(b.events, ev)
		b.mu.Unlock()
		w.WriteHeader(200)
	})
	return mux
}

func (b *backend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}

func newClient(baseURL string) *reporter.Client {
	return reporter.New(&config.Config{APIBaseURL: baseURL, APIKey: "test", HTTPTimeout: 2 * time.Second})
}

func TestSchedulerLeaderProbesAndReports(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	b := &backend{}
	srv := httptest.NewServer(b.handler(target.URL))
	defer srv.Close()

	leader := &fakeLeader{}
	leader.leader.Store(true)

	s := New(newClient(srv.URL), leader, 50*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)

	// The first reconcile runs immediately and the probe fires once on start.
	deadline := time.Now().Add(2 * time.Second)
	for b.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	if b.count() == 0 {
		t.Fatal("expected at least one probe result reported")
	}
	b.mu.Lock()
	ev := b.events[0]
	b.mu.Unlock()
	if ev.SourceID != "probe-1" || ev.State != "complete" {
		t.Fatalf("unexpected event: %+v", ev)
	}
}

func TestSchedulerFollowerDoesNotProbe(t *testing.T) {
	b := &backend{}
	srv := httptest.NewServer(b.handler("http://unused"))
	defer srv.Close()

	leader := &fakeLeader{} // stays false

	s := New(newClient(srv.URL), leader, 50*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	s.Run(ctx)

	if b.count() != 0 {
		t.Fatalf("follower must not probe, got %d events", b.count())
	}
}

// swapBackend serves a config whose probe target can be swapped at runtime,
// and records which target each probe hit (via the X-Probed-Target header echo
// is not available, so targets are distinct servers that tag their events).
type swapBackend struct {
	target atomic.Value // string
	mu     sync.Mutex
	events []reporter.EventPayload
}

func (b *swapBackend) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/outpost/config", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(reporter.ConfigResponse{
			ProjectID: 1,
			Probes: []reporter.ProbeSpec{
				{ID: 1, SourceID: "probe-1", Kind: "http", Target: b.target.Load().(string), ExpectedStatus: 200, Interval: 30, TimeoutMs: 1000},
			},
		})
	})
	mux.HandleFunc("/api/v1/outpost/events", func(w http.ResponseWriter, r *http.Request) {
		var ev reporter.EventPayload
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &ev)
		b.mu.Lock()
		b.events = append(b.events, ev)
		b.mu.Unlock()
		w.WriteHeader(200)
	})
	return mux
}

// TestSchedulerRestartsOnSpecChange proves a changed target (same source_id)
// is picked up on the next reconcile — the regression we fixed.
func TestSchedulerRestartsOnSpecChange(t *testing.T) {
	var hitA, hitB atomic.Int32
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hitA.Add(1); w.WriteHeader(200) }))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hitB.Add(1); w.WriteHeader(200) }))
	defer srvB.Close()

	b := &swapBackend{}
	b.target.Store(srvA.URL)
	srv := httptest.NewServer(b.handler())
	defer srv.Close()

	leader := &fakeLeader{}
	leader.leader.Store(true)

	s := New(newClient(srv.URL), leader, 50*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer cancel()

	// Wait until target A is probed.
	deadline := time.Now().Add(2 * time.Second)
	for hitA.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hitA.Load() == 0 {
		t.Fatal("expected target A to be probed")
	}

	// Swap the config target to B; the next reconcile must restart the loop.
	b.target.Store(srvB.URL)
	deadline = time.Now().Add(2 * time.Second)
	for hitB.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hitB.Load() == 0 {
		t.Fatal("expected target B to be probed after spec change (loop did not restart)")
	}
}

// failoverBackend models a standby agent: /config returns no probes until the
// agent is "promoted", and /heartbeat bumps the epoch on promotion to signal a
// re-pull. Proves the fast heartbeat path picks up probes without waiting for
// the slow config tick.
type failoverBackend struct {
	promoted atomic.Bool
	epoch    atomic.Int32
	mu       sync.Mutex
	events   []reporter.EventPayload
}

func (b *failoverBackend) handler(target string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/outpost/heartbeat", func(w http.ResponseWriter, _ *http.Request) {
		// Promotion bumps the epoch once, the signal the agent re-pulls on.
		if b.promoted.Load() && b.epoch.Load() == 0 {
			b.epoch.Store(1)
		}
		_ = json.NewEncoder(w).Encode(reporter.HeartbeatResponse{
			Alive:  true,
			Epochs: map[string]int{"42": int(b.epoch.Load())},
		})
	})
	mux.HandleFunc("/api/v1/outpost/config", func(w http.ResponseWriter, _ *http.Request) {
		probes := []reporter.ProbeSpec{}
		if b.promoted.Load() {
			probes = append(probes, reporter.ProbeSpec{
				ID: 1, SourceID: "probe-1", Kind: "http", Target: target, ExpectedStatus: 200, Interval: 30, TimeoutMs: 1000,
			})
		}
		_ = json.NewEncoder(w).Encode(reporter.ConfigResponse{ProjectID: 1, Probes: probes})
	})
	mux.HandleFunc("/api/v1/outpost/events", func(w http.ResponseWriter, r *http.Request) {
		var ev reporter.EventPayload
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &ev)
		b.mu.Lock()
		b.events = append(b.events, ev)
		b.mu.Unlock()
		w.WriteHeader(200)
	})
	return mux
}

func (b *failoverBackend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}

// TestSchedulerPromotionViaEpoch proves a standby agent starts probing within a
// heartbeat of being promoted, driven by the epoch bump (not the slow config
// tick — set long here so only the heartbeat path can trigger it).
func TestSchedulerPromotionViaEpoch(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer target.Close()

	b := &failoverBackend{}
	srv := httptest.NewServer(b.handler(target.URL))
	defer srv.Close()

	leader := &fakeLeader{}
	leader.leader.Store(true)

	// Long config tick: if probing starts, it must be the heartbeat that did it.
	s := New(newClient(srv.URL), leader, 1*time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.heartbeatInterval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer cancel()

	// Standby: no probes yet.
	time.Sleep(150 * time.Millisecond)
	if b.count() != 0 {
		t.Fatalf("standby agent must not probe, got %d events", b.count())
	}

	// Promote: the next heartbeat bumps the epoch -> reconcile -> probing.
	b.promoted.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for b.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if b.count() == 0 {
		t.Fatal("expected probing to start within a heartbeat of promotion")
	}
}
