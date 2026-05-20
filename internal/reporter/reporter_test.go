package reporter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sentinelhq-sh/outpost/internal/config"
)

// newTestClient wires the reporter to a test HTTP server with a stable cfg.
// The HTTPTimeout is wide enough not to flake on slow CI, narrow enough not
// to slow down the test run.
func newTestClient(srv *httptest.Server) *Client {
	return New(&config.Config{
		APIBaseURL:  srv.URL,
		APIKey:      "test-key",
		HTTPTimeout: 2 * time.Second,
	})
}

func TestRegisterSuccess(t *testing.T) {
	var gotAuth, gotPath, gotBody string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"created":["a"],"updated":["b"],"removed":[]}`))
	}))
	defer srv.Close()

	c := newTestClient(srv)
	resp, err := c.Register(context.Background(), RegisterPayload{
		Runtime:    "kubernetes",
		Discovered: []DiscoveredJob{{SourceID: "a", Name: "Hourly backup", Schedule: "0 * * * *"}},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("Authorization header = %q, want Bearer test-key", gotAuth)
	}
	if gotPath != "/v1/outpost/register" {
		t.Errorf("path = %q, want /v1/outpost/register", gotPath)
	}
	if !strings.Contains(gotBody, `"runtime":"kubernetes"`) {
		t.Errorf("body missing runtime: %s", gotBody)
	}
	if len(resp.Created) != 1 || resp.Created[0] != "a" {
		t.Errorf("created = %v, want [a]", resp.Created)
	}
}

func TestEventIdempotencyKeyForwarded(t *testing.T) {
	var got EventPayload

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(srv)
	err := c.Event(context.Background(), EventPayload{
		SourceID:       "default/backup",
		State:          "complete",
		IdempotencyKey: "k8s-job-uid-123",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.IdempotencyKey != "k8s-job-uid-123" {
		t.Errorf("idempotency_key not forwarded; got %q", got.IdempotencyKey)
	}
}

func TestRetriesTransientErrors(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"created":[],"updated":[],"removed":[]}`))
	}))
	defer srv.Close()

	c := newTestClient(srv)
	_, err := c.Register(context.Background(), RegisterPayload{Runtime: "kubernetes"})

	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestDoesNotRetryPermanentErrors(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		// 401 = bad API key. Retrying won't fix it.
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"invalid token"}`))
	}))
	defer srv.Close()

	c := newTestClient(srv)
	_, err := c.Register(context.Background(), RegisterPayload{Runtime: "kubernetes"})

	if !errors.Is(err, ErrPermanent) {
		t.Errorf("expected ErrPermanent, got %v", err)
	}
	if attempts.Load() != 1 {
		t.Errorf("expected 1 attempt (no retry on permanent error), got %d", attempts.Load())
	}
}

func TestRespectsContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slow handler — we'll cancel before it responds.
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(srv)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.Register(ctx, RegisterPayload{Runtime: "kubernetes"})
	if err == nil {
		t.Fatal("expected error after context cancel, got nil")
	}
}
