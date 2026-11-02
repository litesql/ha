# HA Observability

This example runs HA with OpenTelemetry enabled and a local observability stack:

- Grafana for dashboards and exploration
- Prometheus for metrics
- Loki for logs
- Tempo for traces
- OpenTelemetry Collector to receive OTLP/HTTP from HA and route each signal

The provisioned Grafana dashboard uses metrics emitted by `go-ha` and
`go-sqlite-ha`: cluster readiness/leadership and NATS state, replication
sequences/lag/changesets/errors/durations, snapshots, and SQLite operations,
durations, and backup sizes. It also shows HTTP server/client and gRPC RPC
latency. HA instruments incoming HTTP and Connect RPC requests and the CLI's
outgoing HTTP and native gRPC calls when `--observability` (or
`HA_OBSERVABILITY=true`) is enabled. The app does not expose a Prometheus
`/metrics` endpoint; the Collector exports received OTLP metrics for
Prometheus to scrape.

## Start

From this directory:

```sh
docker compose up -d
```

Open [Grafana](http://localhost:3000) and sign in with `admin` / `admin`.
The **HA / HA Observability** dashboard and Prometheus, Loki, and Tempo
datasources are provisioned automatically. For traces, open Grafana Explore
and select Tempo; the datasource also links traces to matching Loki logs.

HA's HTTP API is available at [http://localhost:8080](http://localhost:8080).
For example, create a table and insert a row to generate database activity:

```sh
curl -sS http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, name TEXT)"}'

curl -sS http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"INSERT INTO events (name) VALUES (:name)","params":{"name":"example"}}'
```

The Grafana, Prometheus, and HA ports are bound to localhost. Grafana's
`admin` / `admin` credentials are for this local example only; change them
before exposing the stack to a network.

## Stop

```sh
docker compose down
```

Persisted data remains in Docker volumes. To remove the example's data too:

```sh
docker compose down --volumes
```
