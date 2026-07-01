# Operations Runbook

How to run, observe, load-test, back up, and validate failure behavior. Implements the operational
half of roadmap Phase 10.

## Running the full stack

```bash
make stack-up     # apps + Postgres + Redpanda + Prometheus + Grafana + Alertmanager (--profile full)
make stack-down
```

Host dev mode (apps via `go run`) still works with `make up` (infra only) + `make run-api` /
`make run-worker`. Redpanda advertises `localhost:9092` (host) and `redpanda:29092` (in-compose).

> Note: Prometheus/Grafana/Alertmanager **bake their configs into images** (`deploy/{prometheus,
> alertmanager,grafana}/Dockerfile`) instead of bind-mounting — so `make stack-up` works even on
> rootless Docker where `/home` bind mounts fail with `permission denied`. To change a config/dashboard,
> rebuild (`make stack-up` re-runs `--build`). Validate alert rules standalone with
> `docker run -i --entrypoint promtool prom/prometheus:v2.54.1 check rules /dev/stdin < deploy/prometheus/alerts.yml`.

| Service | URL |
|---|---|
| cdp-api | http://localhost:18080 (`/healthz`, `/metrics`) |
| cdp-worker metrics | scraped at `cdp-worker:9100/metrics` |
| Prometheus | http://localhost:9090 (Status → Targets, Alerts) |
| Grafana | http://localhost:3000 (anonymous admin; **CDP Overview** dashboard) |
| Alertmanager | http://localhost:9093 |

## Observability

- **Metrics:** cdp-api exposes ingress counters (`events_received/validated/rejected/rate_limited`);
  cdp-worker exposes pipeline/identity/profile/segment/activation + `processing_lag_seconds`.
- **Dashboard:** Grafana auto-provisions *CDP Overview* (ingress, pipeline throughput, p95 lag, DLQ &
  retries, identity/profile/segment, activation).
- **Alerts:** `deploy/prometheus/alerts.yml` — validation-failure rate, DLQ growth, publish failures,
  high lag, activation failures, circuit-open. Validate with `make promtool`.

## Load test

```bash
make loadtest API_KEY=cdp_...    # k6 ramps to 50 VUs; thresholds p95<250ms, error<1%
```

## Backup & restore

```bash
make backup                                  # → backups/cdp-<ts>.dump (pg_dump -Fc)
make restore FILE=backups/cdp-<ts>.dump      # restores into scratch DB cdp_restore
```
Verify by comparing row counts between `cdp` and `cdp_restore`.

## Failure tests

- **Worker crash / recovery:** `docker compose ... stop cdp-worker`. Ingress keeps returning `202`
  (events commit to `event_outbox` as `pending`). Restart → the relay drains them to `published` and
  the consumers populate `raw_event` and downstream. No events lost (durable outbox).
- **Bus (Redpanda) outage:** stop `redpanda`. Ingress still `202`s (writes to the outbox); the relay
  logs publish failures and retries; `kafka_publish_failed_total` rises (alert fires). Restart → the
  relay catches up.
- **Destination outage:** point a destination at a failing URL. Deliveries become `failed_retryable`
  with backoff; after `CIRCUIT_THRESHOLD` failures the **circuit breaker** opens
  (`activation_circuit_open_total` rises, alert fires) and tasks defer rather than hammer the endpoint;
  they resume after `CIRCUIT_COOLDOWN`.
- **DLQ:** a poison message lands in `dlq_event`; retry/discard via the admin API
  (`POST …/dlq/{id}/retry|discard`).

## Release checklist

See `12-testing-and-release-checklist.md`. Before production: run `make test`, `make loadtest`,
`make promtool`, a backup/restore round-trip, and the failure tests above.
