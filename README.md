# SentinelHQ Outpost

Kubernetes agent that watches your `CronJob`, `Job`, and `Pod` resources
and reports their lifecycle to the SentinelHQ server. Runs as a
**Deployment** with `replicas: 2+` and **leader election** so failover is
sub-second. Read-only `ClusterRole` on `batch/v1` and `core/v1`.

> Why Deployment with HA and not DaemonSet? The agent talks to the Kubernetes
> API for cluster-scoped objects — it does not need per-node access. Two pods
> in active/standby (only the leader emits events) gives sub-second failover
> with no event duplication, for a fraction of the resources a DaemonSet
> would consume on a 50-node cluster.

Currently in **Phase 1**: configuration loading, HTTP reporter, healthz.
Kubernetes watch loops and leader election land in Phase 2.

## Configuration

All settings come from environment variables (12-factor):

| Variable | Required | Default | Description |
| -- | -- | -- | -- |
| `SENTINELHQ_API_URL` | yes | — | Root URL of the SentinelHQ server (no trailing slash). |
| `SENTINELHQ_API_KEY` | yes | — | Project API key from the dashboard. Mount via a Secret. |
| `SENTINELHQ_CLUSTER_NAME` | no | `default` | Logical label sent on every event. |
| `SENTINELHQ_HEALTHZ_ADDR` | no | `:9090` | Listen address for `/healthz` and `/readyz`. |
| `SENTINELHQ_POLL_INTERVAL` | no | `5m` | How often the registrar re-syncs. Minimum 30s. |
| `SENTINELHQ_HTTP_TIMEOUT` | no | `10s` | Per-request timeout. |

The agent will refuse to start if a required variable is missing, with a
structured error listing every problem at once.

## Run locally

```sh
go build -o outpost ./cmd/outpost
SENTINELHQ_API_URL=http://127.0.0.1:8000 \
SENTINELHQ_API_KEY=sentinel_live_xxxxxxxxxxxx \
./outpost
```

Then probe the local endpoints:

```sh
curl localhost:9090/healthz   # always 200 once the process is up
curl localhost:9090/readyz    # 200 only after the server smoke test passes
```

## Tests

```sh
go test ./...
```

## Layout

```
cmd/outpost/         # entry point: wiring + signal handling + healthz
internal/config/     # env → typed Config + validation
internal/reporter/   # HTTP client with Bearer auth + retry + backoff
internal/version/    # build-time metadata (overridden by -ldflags)
```

## Roadmap

- [ ] Phase 2: k8s informers (`CronJob`, `Job`, `Pod`) + annotation filter
- [ ] Phase 3: discovery → register loop
- [ ] Phase 4: event emission (Run / Complete / Fail / DurationExceeded)
- [ ] Phase 5: Helm chart + Docker image
- [ ] Phase 6: GitHub Actions release pipeline
