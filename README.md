# SentinelHQ Outpost

Kubernetes agent that watches `CronJob` and `Job` resources and reports their
lifecycle to the SentinelHQ backend or exposes them as Prometheus metrics.
Runs as a **Deployment** with `replicas: 2` and **leader election** so
failover is sub-second with no event duplication.

[![Release](https://img.shields.io/github/v/release/sentinelhq-sh/outpost)](https://github.com/sentinelhq-sh/outpost/releases)
[![License](https://img.shields.io/github/license/sentinelhq-sh/outpost)](LICENSE)

## Installation

### Helm (recommended)

```sh
helm install outpost oci://ghcr.io/sentinelhq-sh/charts/outpost \
  --version 0.1.1 \
  --namespace sentinelhq --create-namespace \
  --set config.clusterName=prod \
  --set config.apiUrl=https://api.sentinelhq.cloud \
  --set config.apiKey=<token>
```

Prometheus-only mode (no SentinelHQ account required):

```sh
helm install outpost oci://ghcr.io/sentinelhq-sh/charts/outpost \
  --version 0.1.1 \
  --namespace sentinelhq --create-namespace \
  --set config.clusterName=prod \
  --set config.agentMode=prometheus
```

### Helm values

| Value | Default | Description |
| --- | --- | --- |
| `config.agentMode` | `sentinel` | Output mode: `sentinel` or `prometheus`. |
| `config.clusterName` | `default` | Logical cluster label sent on every event. |
| `config.apiUrl` | `""` | SentinelHQ backend URL. Required in `sentinel` mode. |
| `config.apiKey` | `""` | API key. Mount via `existingSecret` in production. |
| `config.addr` | `:8080` | Listen address for `/healthz`, `/readyz`, and `/metrics`. |
| `config.pollInterval` | `5m` | Re-sync interval. Minimum 30s. |
| `config.agentName` | `""` | Name shown in the UI. Defaults to `<clusterName>/<release>`, or `<release>` while `clusterName` is `default`. |
| `config.fingerprint` | `""` | Pins the agent identity. Empty means one agent per cluster and release (see below). |
| `replicaCount` | `2` | Number of replicas. Keep at 2+ for HA. |
| `podDisruptionBudget.enabled` | `false` | Enable PodDisruptionBudget (`minAvailable: 1`). |
| `existingSecret` | `""` | Use an existing Secret for the API key. |
| `serviceMonitor.enabled` | `false` | Create a Prometheus Operator ServiceMonitor. |

Full reference: [`charts/outpost/values.yaml`](charts/outpost/values.yaml).

### Agent identity

All pods of a release are one agent for the backend. Its identity is
`k8s/<cluster>/<namespace>/<release>`, where `<cluster>` is the UID of the
`kube-system` namespace, so the same release name in two clusters gives two
agents. The agent refuses to start if it cannot read that namespace.

## Configuration

All settings come from environment variables (12-factor):

| Variable | Default | Description |
| --- | --- | --- |
| `SENTINELHQ_AGENT_MODE` | — | Output mode: `sentinel` or `prometheus`. Required. |
| `SENTINELHQ_RUNTIME` | `auto` | Cluster attachment: `auto`, `kubernetes`, or `standalone`. See [Runtime](#runtime). |
| `SENTINELHQ_API_URL` | — | SentinelHQ API root (`https://api.sentinelhq.cloud`, or `http://host/api` without a dedicated API domain) — the agent appends `/v1/...`. Required in `sentinel` mode. |
| `SENTINELHQ_API_KEY` | — | Project API key. Required in `sentinel` mode. |
| `SENTINELHQ_CLUSTER_NAME` | `default` | Logical label sent on every event. |
| `SENTINELHQ_INSTANCE` | — | Deployment name in its cluster (`namespace/release`), set by the chart. Makes the identity cluster-scoped. |
| `SENTINELHQ_FINGERPRINT` | machine-id, else hostname | Pins the agent identity. Outside Kubernetes, the default is per host. |
| `SENTINELHQ_ADDR` | `:8080` | Listen address for `/healthz`, `/readyz`, and `/metrics`. |
| `SENTINELHQ_POLL_INTERVAL` | `5m` | Re-sync interval. Minimum 30s. |
| `SENTINELHQ_HTTP_TIMEOUT` | `10s` | Per-request timeout. |

The agent refuses to start and lists every missing field at once.

## Output modes

`/healthz`, `/readyz`, and `/metrics` are always served on `SENTINELHQ_ADDR`
(default `:8080`) regardless of mode.

| Mode | Description |
| --- | --- |
| `sentinel` | Pushes events to the SentinelHQ backend. Requires `API_URL` + `API_KEY`. |
| `prometheus` | Metrics only, no push to the SentinelHQ backend. |

## Metrics (Prometheus, Alloy, Mimir)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `sentinelhq_cronjob_runs_total` | counter | `namespace`, `cronjob`, `status` | Finished runs by outcome (`complete`, `fail`) |
| `sentinelhq_cronjob_duration_seconds` | histogram | `namespace`, `cronjob` | Duration of finished runs |
| `sentinelhq_cronjob_last_run_timestamp_seconds` | gauge | `namespace`, `cronjob` | Last run start or end, from the Job |
| `sentinelhq_cronjob_last_success_timestamp_seconds` | gauge | `namespace`, `cronjob` | Last successful run, from the Job |
| `sentinelhq_outpost_leader` | gauge | | 1 on the pod holding the lease |
| `sentinelhq_outpost_monitors_active` | gauge | | Annotated CronJobs tracked |
| `sentinelhq_outpost_info` | gauge | `version`, `commit`, `cluster` | Build info, always 1 |

Standard `go_*` and `process_*` metrics are exposed too. Active probes report to
the SentinelHQ backend only.

Two things to keep in mind when scraping:

- **Honor labels.** `namespace` is the CronJob's namespace. Without `honor_labels`,
  it is renamed `exported_namespace` and replaced by the agent's namespace. The
  chart's ServiceMonitor sets it.
- **Every pod exposes the same series.** Aggregate with `max by`, not `sum`, or
  counts double with two replicas.
- **Use one cluster name.** `sentinelhq_outpost_info` carries `cluster` from
  `config.clusterName`, and it wins over Alloy's `external_labels`: set both to the
  same value.

At startup, Jobs still in the cluster refresh the timestamps with their real
time but are not counted again. Runs that finish while every pod is down are
missing from the counters; the timestamps catch up.

### Alloy with the ServiceMonitor

Install with `--set serviceMonitor.enabled=true` (requires the prometheus-operator CRDs):

```alloy
prometheus.operator.servicemonitors "outpost" {
  forward_to = [prometheus.remote_write.mimir.receiver]
}

prometheus.remote_write "mimir" {
  // Metrics carry no cluster label: add it here for multi-cluster setups.
  external_labels = { cluster = "homelab" }

  endpoint {
    url = "http://mimir:9009/api/v1/push"
  }
}
```

### Alloy without the CRDs

```alloy
discovery.kubernetes "pods" {
  role = "pod"
}

discovery.relabel "outpost" {
  targets = discovery.kubernetes.pods.targets

  rule {
    source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name", "__meta_kubernetes_pod_container_port_name"]
    regex         = "outpost;http"
    action        = "keep"
  }
}

prometheus.scrape "outpost" {
  targets      = discovery.relabel.outpost.output
  honor_labels = true
  forward_to   = [prometheus.remote_write.mimir.receiver]
}
```

### Useful queries

```promql
# No success for 26 hours (daily job)
time() - max by (namespace, cronjob) (sentinelhq_cronjob_last_success_timestamp_seconds) > 93600

# A run failed in the last hour
max by (namespace, cronjob) (increase(sentinelhq_cronjob_runs_total{status="fail"}[1h])) > 0

# p95 duration
histogram_quantile(0.95, max by (namespace, cronjob, le) (rate(sentinelhq_cronjob_duration_seconds_bucket[1d])))
```

The timestamp series appear after a CronJob's first run: combine with `absent()`
to catch one that never ran.

## Runtime

`SENTINELHQ_RUNTIME` controls whether the agent attaches to a Kubernetes
cluster. This is independent of the output mode above.

| Runtime | Description |
| --- | --- |
| `auto` (default) | Try Kubernetes; fall back to standalone if no cluster is reachable. |
| `kubernetes` | Require a cluster — the agent exits if Kubernetes is unreachable. |
| `standalone` | Never touch Kubernetes. No CronJob watcher, no leader election. |

In **Kubernetes** runtime the agent watches `CronJob`/`Job` resources and
runs as a Deployment with leader election. In **standalone** runtime it skips
all of that: it is the sole instance (always leader) and only executes active
probes (HTTP/TCP/DNS) pulled from the backend, pushing the results. Use it on
a VM, bare-metal host, Docker, or a laptop — anywhere without Kubernetes.

Standalone requires `sentinel` mode (probes push to the backend):

```sh
docker run --rm \
  -e SENTINELHQ_AGENT_MODE=sentinel \
  -e SENTINELHQ_RUNTIME=standalone \
  -e SENTINELHQ_API_URL=https://api.sentinelhq.cloud \
  -e SENTINELHQ_API_KEY=sentinel_live_xxxxxxxxxxxx \
  ghcr.io/sentinelhq-sh/outpost:latest
```

## Run locally

```sh
go build -o bin/outpost ./cmd/outpost

SENTINELHQ_AGENT_MODE=sentinel \
SENTINELHQ_API_URL=http://127.0.0.1:8000 \
SENTINELHQ_API_KEY=sentinel_live_xxxxxxxxxxxx \
./bin/outpost
```

```sh
curl localhost:8080/healthz
curl localhost:8080/readyz
```

## Development

```sh
make build   # build ./bin/outpost
make test    # run tests
make lint    # vet + gofmt check
```

## Layout

```
cmd/outpost/          # entry point: wiring, signal handling, healthz
internal/config/      # env -> typed Config + validation
internal/emitter/     # fan-out to push client and Prometheus collectors
internal/k8sclient/   # Kubernetes client init
internal/leaderelector/  # leader election via coordination.k8s.io leases
internal/metrics/     # Prometheus collectors
internal/registrar/   # CronJob sync with SentinelHQ backend
internal/reporter/    # HTTP client with Bearer auth + retry + backoff
internal/version/     # build-time metadata (overridden by -ldflags)
internal/watcher/     # Kubernetes watch loops for CronJobs and Jobs
charts/outpost/       # Helm chart
```

## License

Apache 2.0 -- see [LICENSE](LICENSE).
