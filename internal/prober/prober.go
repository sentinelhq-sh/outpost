// Package prober executes active probes (HTTP, TCP, DNS, ICMP) against a
// target and reports the outcome. SentinelHQ owns the probe config; the
// prober is a dumb relay: it runs the check and returns a Result. The
// scheduler turns Results into backend events.
package prober

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sentinelhq-sh/outpost/internal/reporter"
	"github.com/sentinelhq-sh/outpost/internal/version"
)

// probeUserAgent identifies probe traffic to targets so operators (and their
// WAFs) can recognize it. Go's default "Go-http-client/1.1" is often blocked.
func probeUserAgent() string {
	return fmt.Sprintf("SentinelHQ-Outpost/%s (+https://sentinelhq.cloud/bot)", version.Version)
}

// Result is the outcome of a single probe execution.
type Result struct {
	Up         bool
	LatencyMs  int
	StatusCode int    // HTTP status; 0 for non-HTTP probes
	Message    string // failure reason, empty when Up
}

// Probe runs the spec against its target and returns the result. It never
// returns an error: a failed probe is a Result with Up=false and a Message.
func Probe(ctx context.Context, spec reporter.ProbeSpec) Result {
	timeout := time.Duration(spec.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()

	switch spec.Kind {
	case "http":
		return finish(start, probeHTTP(ctx, spec))
	case "tcp":
		return finish(start, probeTCP(ctx, spec))
	case "dns":
		return finish(start, probeDNS(ctx, spec))
	case "icmp":
		return finish(start, probeICMP(ctx, spec))
	default:
		return Result{Up: false, Message: fmt.Sprintf("unknown probe kind %q", spec.Kind)}
	}
}

// finish stamps latency and normalizes the partial result.
func finish(start time.Time, r Result) Result {
	r.LatencyMs = int(time.Since(start).Milliseconds())
	return r
}

func probeHTTP(ctx context.Context, spec reporter.ProbeSpec) Result {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !spec.VerifyTLS},
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			if spec.FollowRedirects {
				return nil
			}
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, spec.Target, nil)
	if err != nil {
		return Result{Up: false, Message: "invalid target: " + err.Error()}
	}
	req.Header.Set("User-Agent", probeUserAgent())

	resp, err := client.Do(req)
	if err != nil {
		return Result{Up: false, Message: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()

	if spec.ExpectedStatus != 0 && resp.StatusCode != spec.ExpectedStatus {
		return Result{
			Up:         false,
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("expected status %d, got %d", spec.ExpectedStatus, resp.StatusCode),
		}
	}

	if spec.Keyword != "" {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // cap 1 MiB
		if !strings.Contains(string(body), spec.Keyword) {
			return Result{
				Up:         false,
				StatusCode: resp.StatusCode,
				Message:    fmt.Sprintf("keyword %q not found in body", spec.Keyword),
			}
		}
	}

	return Result{Up: true, StatusCode: resp.StatusCode}
}

func probeTCP(ctx context.Context, spec reporter.ProbeSpec) Result {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", spec.Target)
	if err != nil {
		return Result{Up: false, Message: err.Error()}
	}
	_ = conn.Close()
	return Result{Up: true}
}

func probeDNS(ctx context.Context, spec reporter.ProbeSpec) Result {
	var r net.Resolver
	addrs, err := r.LookupHost(ctx, spec.Target)
	if err != nil {
		return Result{Up: false, Message: err.Error()}
	}
	if len(addrs) == 0 {
		return Result{Up: false, Message: "no records resolved"}
	}

	if spec.ExpectedRecord != "" {
		for _, a := range addrs {
			if a == spec.ExpectedRecord {
				return Result{Up: true}
			}
		}
		return Result{Up: false, Message: fmt.Sprintf("expected record %q not in %v", spec.ExpectedRecord, addrs)}
	}

	return Result{Up: true}
}

// probeICMP is not yet implemented: raw ICMP needs CAP_NET_RAW which is not
// guaranteed in a restricted pod. Reported as down with a clear message so
// the gap is visible rather than silently passing.
func probeICMP(_ context.Context, _ reporter.ProbeSpec) Result {
	return Result{Up: false, Message: "icmp probe not implemented (requires CAP_NET_RAW)"}
}
