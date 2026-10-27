package emitter

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
	"github.com/sentinelhq-sh/outpost/internal/watcher"
)

type alwaysLeader struct{}

func (alwaysLeader) IsLeader() bool { return true }

// backend records accepted events and answers with status(attempt, payload).
type backend struct {
	mu       sync.Mutex
	accepted []string // idempotency keys, in arrival order
	attempts atomic.Int32
	status   func(attempt int32, ev reporter.EventPayload) int
}

func (b *backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var ev reporter.EventPayload
	_ = json.NewDecoder(r.Body).Decode(&ev)
	code := b.status(b.attempts.Add(1), ev)
	if code == http.StatusOK {
		b.mu.Lock()
		b.accepted = append(b.accepted, ev.IdempotencyKey)
		b.mu.Unlock()
	}
	w.WriteHeader(code)
}

func (b *backend) keys() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.accepted...)
}

// start runs an emitter against b with millisecond backoffs. 401/403/404 are
// used for failures: the reporter does not fast-retry them, keeping tests quick.
func start(t *testing.T, b *backend) (*Emitter, chan<- watcher.LifecycleEvent) {
	t.Helper()
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)

	client := reporter.New(&config.Config{
		APIBaseURL:  srv.URL,
		APIKey:      "test-key",
		Fingerprint: "fp-test",
		HTTPTimeout: 2 * time.Second,
	})
	e := New(client, nil, alwaysLeader{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.minBackoff = 5 * time.Millisecond
	e.maxBackoff = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	events := make(chan watcher.LifecycleEvent, 10)
	go e.Run(ctx, events)

	return e, events
}

func event(key string) watcher.LifecycleEvent {
	return watcher.LifecycleEvent{SourceID: "default/backup", Kind: watcher.EventComplete, IdempotencyKey: key}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}

func TestRetriesUntilTheServerAccepts(t *testing.T) {
	b := &backend{status: func(attempt int32, _ reporter.EventPayload) int {
		if attempt <= 2 {
			return http.StatusUnauthorized // e.g. key being rotated
		}
		return http.StatusOK
	}}
	e, events := start(t, b)

	events <- event("run-1")

	eventually(t, func() bool { return len(b.keys()) == 1 && e.size() == 0 })
}

func TestKeepsRunOrderAcrossRetries(t *testing.T) {
	b := &backend{status: func(attempt int32, _ reporter.EventPayload) int {
		if attempt == 1 {
			return http.StatusForbidden
		}
		return http.StatusOK
	}}
	_, events := start(t, b)

	events <- event("run-1")
	events <- event("run-2")

	eventually(t, func() bool { return len(b.keys()) == 2 })
	if got := b.keys(); got[0] != "run-1" || got[1] != "run-2" {
		t.Fatalf("order = %v, want [run-1 run-2]", got)
	}
}

func TestDropsRejectedEventsWithoutBlockingTheQueue(t *testing.T) {
	b := &backend{status: func(_ int32, ev reporter.EventPayload) int {
		if ev.IdempotencyKey == "unknown-monitor" {
			return http.StatusNotFound
		}
		return http.StatusOK
	}}
	e, events := start(t, b)

	events <- event("unknown-monitor")
	events <- event("run-1")

	eventually(t, func() bool { return len(b.keys()) == 1 && e.size() == 0 })
	if b.attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2 (rejected event not retried)", b.attempts.Load())
	}
}

func TestDropsOldestEventWhenTheQueueIsFull(t *testing.T) {
	b := &backend{status: func(int32, reporter.EventPayload) int { return http.StatusUnauthorized }}
	e, _ := start(t, b)
	e.mu.Lock()
	e.maxPending = 2
	e.mu.Unlock()

	for _, key := range []string{"run-1", "run-2", "run-3"} {
		e.enqueue(reporter.EventPayload{IdempotencyKey: key})
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.pending) != 2 || e.pending[0].IdempotencyKey != "run-2" || e.pending[1].IdempotencyKey != "run-3" {
		t.Fatalf("pending = %+v, want [run-2 run-3]", e.pending)
	}
}
