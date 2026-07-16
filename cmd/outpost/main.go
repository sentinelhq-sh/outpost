// Command outpost is the SentinelHQ Kubernetes agent. Runs as a Deployment
// with replicas >= 2 + leader election.
//
// Two modes controlled by SENTINELHQ_AGENT_MODE:
//   - sentinel:    pushes CronJob lifecycle events to the SentinelHQ backend.
//   - prometheus:  exposes /metrics only, no SentinelHQ account required.
//
// /healthz, /readyz, and /metrics are always served on SENTINELHQ_ADDR (:8080).
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
	"k8s.io/client-go/kubernetes"

	"github.com/sentinelhq-sh/outpost/internal/config"
	"github.com/sentinelhq-sh/outpost/internal/emitter"
	"github.com/sentinelhq-sh/outpost/internal/k8sclient"
	"github.com/sentinelhq-sh/outpost/internal/leaderelector"
	"github.com/sentinelhq-sh/outpost/internal/metrics"
	"github.com/sentinelhq-sh/outpost/internal/prober"
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
		"mode", cfg.Mode,
		"push_enabled", cfg.PushEnabled(),
		"cluster", cfg.ClusterName,
		"addr", cfg.Addr,
		"poll_interval", cfg.PollInterval.String(),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Outputs — conditional on config.
	var pushClient *reporter.Client
	if cfg.PushEnabled() {
		pushClient = reporter.New(cfg)
	}

	prom := metrics.New(cfg.ClusterName)

	// Resolve the runtime: try Kubernetes unless explicitly standalone. In auto
	// mode a K8s failure downgrades to standalone; in kubernetes mode it's fatal.
	var kclient kubernetes.Interface
	var namespace string
	if cfg.Runtime != config.RuntimeStandalone {
		kclient, namespace, err = k8sclient.New()
		if err != nil {
			if cfg.Runtime == config.RuntimeKubernetes {
				logger.Error("kubernetes client init failed", "error", err.Error())
				os.Exit(1)
			}
			logger.Info("kubernetes unavailable — running standalone (probes only)", "error", err.Error())
			kclient = nil
		} else {
			logger.Info("kubernetes client ready", "namespace", namespace)
		}
	} else {
		logger.Info("standalone runtime — Kubernetes disabled (probes only)")
	}

	standalone := kclient == nil

	// Report the effective execution environment (MonitorRuntime). If we attached
	// to a cluster, kubernetes is authoritative; otherwise detect from the host
	// (docker/systemd/proxmox/manual). An operator override is honored inside
	// DetectMonitorRuntime.
	if pushClient != nil {
		runtime := config.MonitorRuntimeKubernetes
		if standalone {
			runtime = config.DetectMonitorRuntime()
		}
		pushClient.SetRuntime(runtime)
		logger.Info("reporting runtime", "runtime", runtime)
	}

	// Register with the backend and wait for approval before doing any work.
	// The agent announces itself (cle projet + fingerprint); an operator must
	// approve it in the UI. Until then we poll and stay idle.
	if pushClient != nil {
		startServer(cfg.Addr, prom, logger) // serve /healthz early so probes pass
		if !waitForApproval(ctx, pushClient, cfg.PollInterval, logger) {
			logger.Info("outpost shutting down before approval")
			return
		}
	}

	errCh := make(chan error, 5)
	em := emitter.New(pushClient, prom, leaderProxy{}, logger)

	if standalone {
		// No cluster: the agent is the sole instance, so it's leader by
		// definition. No watcher, no registrar, no Lease.
		leaderFlag.Store(true)
		prom.SetLeader(true)
		readyz.Store(true)
	} else {
		identity := os.Getenv("POD_NAME")
		if identity == "" {
			identity, _ = os.Hostname()
		}

		watch := watcher.New(kclient, 5*time.Minute, logger)

		var reg *registrar.Registrar
		if pushClient != nil {
			reg = registrar.New(pushClient, leaderProxy{}, cfg.ClusterName, cfg.PollInterval, logger)
		}

		elector, eerr := leaderelector.New(kclient, leaderelector.Options{
			LeaseName: "sentinelhq-outpost",
			Namespace: namespace,
			Identity:  identity,
		}, logger)
		if eerr != nil {
			logger.Error("leader elector init failed", "error", eerr.Error())
			os.Exit(1)
		}

		leaderFlag.Store(false)
		elector.OnAcquire(func(_ context.Context) {
			leaderFlag.Store(true)
			readyz.Store(true)
			prom.SetLeader(true)
		})
		elector.OnLose(func() {
			leaderFlag.Store(false)
			prom.SetLeader(false)
		})

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
			go drain(ctx, watch.Discoveries(), watch.Removals(), prom)
		}
	}

	// Prometheus-only mode hasn't started the server yet (push mode did,
	// before the approval wait). Start it now and flip readyz for scrapes.
	if !cfg.PushEnabled() {
		startServer(cfg.Addr, prom, logger)
		readyz.Store(true)
	}

	// Probe scheduler — pulls active probes from the backend and executes
	// them (leader-only). Push mode only. Works with or without Kubernetes.
	if pushClient != nil {
		probeScheduler := prober.New(pushClient, leaderProxy{}, cfg.PollInterval, logger)
		go func() {
			probeScheduler.Run(ctx)
			errCh <- nil
		}()
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

// waitForApproval registers the agent and blocks until the backend reports it
// approved. Returns false if ctx is cancelled first. Polls every pollInterval
// while pending/revoked. Register errors are retried (transient backend issues).
func waitForApproval(ctx context.Context, client *reporter.Client, pollInterval time.Duration, logger *slog.Logger) bool {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		resp, err := client.Register(ctx)
		switch {
		case err != nil:
			logger.Warn("agent registration failed, will retry", "error", err.Error())
		case resp.Agent.Status == "approved":
			logger.Info("agent approved — starting work")
			return true
		default:
			logger.Info("agent awaiting approval", "status", resp.Agent.Status)
		}

		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
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

// ── HTTP server ───────────────────────────────────────────────────────────

var readyz atomic.Bool

func startServer(addr string, prom *metrics.Collectors, logger *slog.Logger) {
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

	mux.Handle("/metrics", promhttp.HandlerFor(prom.Registry(), promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server crashed", "error", err.Error())
		}
	}()
}

// ── Leader flag ──────────────────────────────────────────────────────────

var leaderFlag atomic.Bool

type leaderProxy struct{}

func (leaderProxy) IsLeader() bool { return leaderFlag.Load() }
