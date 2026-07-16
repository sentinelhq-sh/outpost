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
| `replicaCount` | `2` | Number of replicas. Keep at 2+ for HA. |
| `podDisruptionBudget.enabled` | `false` | Enable PodDisruptionBudget (`minAvailable: 1`). |
| `existingSecret` | `""` | Use an existing Secret for the API key. |
| `serviceMonitor.enabled` | `false` | Create a Prometheus Operator ServiceMonitor. |

Full reference: [`charts/outpost/values.yaml`](charts/outpost/values.yaml).

## Configuration

All settings come from environment variables (12-factor):

| Variable | Default | Description |
| --- | --- | --- |
| `SENTINELHQ_AGENT_MODE` | — | Output mode: `sentinel` or `prometheus`. Required. |
| `SENTINELHQ_RUNTIME` | `auto` | Cluster attachment: `auto`, `kubernetes`, or `standalone`. See [Runtime](#runtime). |
| `SENTINELHQ_API_URL` | — | SentinelHQ server root (e.g. `https://api.sentinelhq.cloud`), no `/api` suffix — the agent appends `/api/v1/...`. Required in `sentinel` mode. |
| `SENTINELHQ_API_KEY` | — | Project API key. Required in `sentinel` mode. |
| `SENTINELHQ_CLUSTER_NAME` | `default` | Logical label sent on every event. |
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
