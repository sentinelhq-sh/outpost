// Command outpost is the SentinelHQ Kubernetes agent. Runs as a Deployment
// with replicas >= 2 + leader election. Two output modes, freely combinable:
//
//   - Push to the SentinelHQ backend  (when API_URL + API_KEY are set).
//   - Expose Prometheus /metrics      (when PROMETHEUS_ADDR is set).
//
// At least one must be configured. Standalone Prometheus mode requires no
// SentinelHQ account — the agent can be used as a pure cluster observer.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sentinelhq-sh/outpost/internal/config"
	"github.com/sentinelhq-sh/outpost/internal/emitter"
	"github.com/sentinelhq-sh/outpost/internal/k8sclient"
	"github.com/sentinelhq-sh/outpost/internal/leaderelector"
	"github.com/sentinelhq-sh/outpost/internal/metrics"
	"github.com/sentinelhq-sh/outpost/internal/registrar"
	"github.com/sentinelhq-sh/outpost/internal/reporter"
	"github.com/sentinelhq-sh/outpost/internal/version"
	"github.com/sentinelhq-sh/outpost/internal/watcher"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	logger.Info("outpost starting",
		"version", version.Version,
		"commit", version.Commit,
	)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err.Error())
		os.Exit(1)
	}

	logger.Info("configuration loaded",
		"push_enabled", cfg.PushEnabled(),
		"prometheus_enabled", cfg.PrometheusEnabled(),
		"cluster", cfg.ClusterName,
		"healthz_addr", cfg.HealthzAddr,
		"prometheus_addr", cfg.PrometheusAddr,
		"poll_interval", cfg.PollInterval.String(),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Outputs — conditional on config.
	var pushClient *reporter.Client
	if cfg.PushEnabled() {
		pushClient = reporter.New(cfg)
	}

	var prom *metrics.Collectors
	if cfg.PrometheusEnabled() {
		prom = metrics.New(cfg.ClusterName)
	}

	// k8s client — required regardless of mode (we always watch the cluster).
	kclient, namespace, err := k8sclient.New()
	if err != nil {
		logger.Error("kubernetes client init failed", "error", err.Error())
		os.Exit(1)
	}
	logger.Info("kubernetes client ready", "namespace", namespace)

	identity := os.Getenv("POD_NAME")
	if identity == "" {
		identity, _ = os.Hostname()
	}

	watch := watcher.New(kclient, 5*time.Minute, logger)
	em := emitter.New(pushClient, prom, leaderProxy{}, logger)

	// The registrar is only useful in push mode — without a backend to push
	// to, there's nothing to register. In Prometheus-only mode it's nil.
	var reg *registrar.Registrar
	if pushClient != nil {
		reg = registrar.New(pushClient, leaderProxy{}, cfg.ClusterName, cfg.PollInterval, logger)
	}

	// Leader election — still needed in Prometheus-only mode? No, because
	// nothing in that mode is leader-gated. But running it is harmless and
	// keeps a single code path. The metric is informative regardless.
	elector, err := leaderelector.New(kclient, leaderelector.Options{
		LeaseName: "sentinelhq-outpost",
		Namespace: namespace,
		Identity:  identity,
	}, logger)
	if err != nil {
		logger.Error("leader elector init failed", "error", err.Error())
		os.Exit(1)
	}

	leaderFlag.Store(false)
	elector.OnAcquire(func(_ context.Context) {
		leaderFlag.Store(true)
		readyz.Store(true)
		if prom != nil {
			prom.SetLeader(true)
		}
	})
	elector.OnLose(func() {
		leaderFlag.Store(false)
		if prom != nil {
			prom.SetLeader(false)
		}
	})

	// HTTP servers. healthz is always on. /metrics is on a separate port so
	// the kubelet probe traffic and the Prometheus scrape traffic don't
	// share a socket — easier NetworkPolicies, separate auth later if needed.
	startHealthz(cfg.HealthzAddr, logger)
	if cfg.PrometheusEnabled() {
		startMetrics(cfg.PrometheusAddr, prom, logger)
	}

	// In Prometheus-only mode there's no smoke-test to do — flip readyz
	// immediately so the pod can accept scrapes right away.
	if !cfg.PushEnabled() {
		readyz.Store(true)
	}

	// Long-running goroutines. They all watch ctx and exit cleanly on cancel.
	errCh := make(chan error, 4)

	go func() { errCh <- elector.Run(ctx) }()
	go func() { errCh <- watch.Run(ctx) }()
	go func() {
		em.Run(ctx, watch.Lifecycles())
		errCh <- nil
	}()
	if reg != nil {
		go func() {
			reg.Run(ctx, watch.Discoveries(), watch.Removals())
			errCh <- nil
		}()
	} else {
		// No registrar — drain the channels so the watcher never blocks.
		go drain(ctx, watch.Discoveries(), watch.Removals(), prom)
	}

	select {
	case <-ctx.Done():
		logger.Info("outpost shutting down (signal)")
	case err := <-errCh:
		if err != nil {
			logger.Error("component crashed", "error", err.Error())
		}
		cancel()
	}

	time.Sleep(200 * time.Millisecond)
}

// drain consumes the discovery channels in Prometheus-only mode so the
// watcher's send-side never blocks. Also keeps the monitors_active gauge
// up to date as a side effect.
func drain(
	ctx context.Context,
	discoveries <-chan watcher.DiscoveredJob,
	removals <-chan string,
	prom *metrics.Collectors,
) {
	tracked := make(map[string]struct{})

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-discoveries:
			if !ok {
				return
			}
			tracked[d.SourceID] = struct{}{}
			if prom != nil {
				prom.SetMonitorsActive(len(tracked))
			}
		case id, ok := <-removals:
			if !ok {
				return
			}
			delete(tracked, id)
			if prom != nil {
				prom.SetMonitorsActive(len(tracked))
			}
		}
	}
}

// ── Healthz / readyz ─────────────────────────────────────────────────────

var readyz atomic.Bool

func startHealthz(addr string, logger *slog.Logger) {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "ok",
			"version": version.Version,
		})
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if readyz.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ready"}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not-ready"}`))
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("healthz listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("healthz crashed", "error", err.Error())
		}
	}()
}

// startMetrics spins up the Prometheus /metrics endpoint on its own port.
// Kept separate from healthz so cluster operators can apply different
// network policies (scrape access vs probe access).
func startMetrics(addr string, prom *metrics.Collectors, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prom.Registry(), promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("prometheus /metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server crashed", "error", err.Error())
		}
	}()
}

// ── Leader flag ──────────────────────────────────────────────────────────

var leaderFlag atomic.Bool

type leaderProxy struct{}

func (leaderProxy) IsLeader() bool { return leaderFlag.Load() }
