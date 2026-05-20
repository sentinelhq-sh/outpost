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

Standalone Prometheus mode (no SentinelHQ account required):

```sh
helm install outpost oci://ghcr.io/sentinelhq-sh/charts/outpost \
  --version 0.1.1 \
  --namespace sentinelhq --create-namespace \
  --set config.clusterName=prod \
  --set config.prometheusAddr=":9091"
```

### Helm values

| Value | Default | Description |
| --- | --- | --- |
| `config.agentMode` | `sentinel` | Output mode: `sentinel`, `prometheus`, or `both`. |
| `config.clusterName` | `default` | Logical cluster label sent on every event. |
| `config.apiUrl` | `""` | SentinelHQ backend URL. Required in `sentinel` and `both` modes. |
| `config.apiKey` | `""` | API key. Mount via `existingSecret` in production. |
| `config.prometheusAddr` | `""` | `/metrics` listen address. Required in `prometheus` and `both` modes. |
| `config.pollInterval` | `5m` | Re-sync interval. Minimum 30s. |
| `replicaCount` | `2` | Number of replicas. Keep at 2+ for HA. |
| `podDisruptionBudget.enabled` | `false` | Enable PodDisruptionBudget (`minAvailable: 1`). |
| `existingSecret` | `""` | Use an existing Secret for the API key. |
| `serviceMonitor.enabled` | `false` | Create a Prometheus Operator ServiceMonitor. |

Full reference: [`charts/outpost/values.yaml`](charts/outpost/values.yaml).

## Configuration

All settings come from environment variables (12-factor):

| Variable | Default | Description |
| --- | --- | --- |
| `SENTINELHQ_AGENT_MODE` | — | Output mode: `sentinel`, `prometheus`, or `both`. Required. |
| `SENTINELHQ_API_URL` | — | SentinelHQ backend URL. Required in `sentinel` and `both` modes. |
| `SENTINELHQ_API_KEY` | — | Project API key. Required in `sentinel` and `both` modes. |
| `SENTINELHQ_CLUSTER_NAME` | `default` | Logical label sent on every event. |
| `SENTINELHQ_HEALTHZ_ADDR` | `:9090` | Listen address for `/healthz` and `/readyz`. |
| `SENTINELHQ_PROMETHEUS_ADDR` | `""` | Listen address for `/metrics`. Required in `prometheus` and `both` modes. |
| `SENTINELHQ_POLL_INTERVAL` | `5m` | Re-sync interval. Minimum 30s. |
| `SENTINELHQ_HTTP_TIMEOUT` | `10s` | Per-request timeout. |

The agent refuses to start and lists every missing field at once.

## Output modes

`/metrics` is always exposed on `SENTINELHQ_PROMETHEUS_ADDR` (default `:9091`) regardless of mode.

| Mode | Description |
| --- | --- |
| `sentinel` | Pushes events to the SentinelHQ backend. Requires `API_URL` + `API_KEY`. |
| `prometheus` | Metrics only, no push to the SentinelHQ backend. |

## Run locally

```sh
go build -o bin/outpost ./cmd/outpost

SENTINELHQ_API_URL=http://127.0.0.1:8000 \
SENTINELHQ_API_KEY=sentinel_live_xxxxxxxxxxxx \
./bin/outpost
```

```sh
curl localhost:9090/healthz
curl localhost:9090/readyz
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
