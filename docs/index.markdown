---
layout: home
title: HA - Highly Available SQLite Cluster
---

![HA logo](https://github.com/litesql/ha/blob/main/ha.png?raw=true)

HA runs SQLite databases as a highly available service. It exposes SQLite over HTTP, gRPC, MySQL and PostgreSQL wire protocols, and replicates changes between nodes using NATS JetStream. Choose leaderless replication, leader-based write routing, or synchronous two-phase commit (2PC), depending on the consistency and availability your application needs.

- [Quick start](#quick-start)
- [Features](#features)
- [Cluster modes](#cluster-modes)
- [Known limitations and unsupported features](#known-limitations-and-unsupported-features)
- [Installation](#installation)
- [Clients and protocols](#clients-and-protocols)
- [HTTP API](#http-api)
- [Replication and CDC](#replication-and-cdc)
- [Proxy and source replication](#proxy-and-source-replication)
- [Cross-database queries](#cross-database-queries)
- [Transaction history and undo](#transaction-history-and-undo)
- [Configuration](#configuration)

## Features

- SQLite databases replicated across nodes using embedded or external NATS JetStream.
- Leaderless writes with last-writer-wins conflict handling, leader-based write routing, and 2PC-coordinated transactions.
- Embedded read/write replicas with the [go-ha database/sql driver](https://github.com/litesql/go-ha) and read-only replicas with the [ha-sync SQLite extension](https://github.com/litesql/ha-sync).
- HTTP and gRPC APIs, plus MySQL and PostgreSQL wire-protocol endpoints.
- HTTP API, OpenAPI description, and an MCP endpoint with `databases` and `query` tools.
- Database snapshots, transaction history, undo operations, and cross-database queries.
- Proxying and replicating MySQL or PostgreSQL sources, and consuming Debezium change events from Kafka.

## Quick start

Start a node with an on-disk database and PostgreSQL wire protocol enabled:

```sh
mkdir db1 && mkdir db2
```

```sh
ha -n node1 --pg-port 5432 file:db1/mydatabase.db
```

Start a second node connected to the first node's NATS server:

```sh
ha -n node2 --port 8081 --pg-port 5433 --nats-port 0 \
  --replication-url nats://localhost:4222 file:db2/mydatabase.db
```

Create and query a table through the HTTP API:

```sh
curl -H 'Content-Type: application/json' -d '[
  {"sql":"CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"},
  {"sql":"INSERT INTO users (id, name) VALUES (1, :name)","params":{"name":"HA user"}}
]' http://localhost:8080/query
```

Check that the change has replicated:

```sh
curl -H 'Content-Type: application/json' -d \
  '{"sql":"SELECT * FROM users"}' http://localhost:8081/query
```

The default HTTP and gRPC port is `8080`. PostgreSQL and MySQL wire servers are disabled unless their respective ports are configured. See [Installation](#installation) for a multi-node Docker Compose example.

## Cluster modes

Choose one mode based on the guarantees and failure behavior your application needs. The ready-to-run topologies are in the [leaderless example](https://github.com/litesql/ha/tree/main/examples/leaderless), [leader-based example](https://github.com/litesql/ha/tree/main/examples/leader-based), and [2PC example](https://github.com/litesql/ha/tree/main/examples/2pc).

### Leaderless replication

Nodes consume changes from a shared NATS JetStream stream, and clients can write to any node. JetStream delivers messages at least once; HA makes supported DML operations replayable and uses last-writer-wins conflict handling by default. Concurrent writes to the same data can still produce conflicts or temporary differences between nodes. Use a custom `--interceptor` when application-specific conflict handling is required.

Start from the [leaderless Compose example](https://github.com/litesql/ha/blob/main/examples/leaderless/docker-compose.yml). Nodes must be configured to use the same NATS deployment and compatible replication stream settings.

### Leader-based replication

Leader-based mode routes writes through a leader to reduce concurrent-write conflicts. Configure `--leader-addr` to enable leader election, or `--leader-static` on a node to route writes to a fixed leader. Leader communication uses the Connect/gRPC service on the HTTP server port; set `--grpc-insecure` only on a trusted network.

Start from the [leader-based Compose example](https://github.com/litesql/ha/blob/main/examples/leader-based/docker-compose.yml). Leader-based routing does not turn SQLite into a multi-writer database: write availability depends on the configured leader behavior and reachable cluster services.

### Synchronous two-phase commit (2PC)

2PC coordinates a transaction with every configured peer before reporting it committed. It is intended for applications that need coordinated commits across the participating HA nodes rather than leaderless last-writer-wins conflict resolution.

Enable 2PC through the SQLite DSN query parameters supplied to HA. Set `2pcPeers` to the comma-separated HTTP(S) addresses of the other nodes; configure each node with the other participants in the same cluster. Peer addresses must be reachable from the HA containers or processes. The coordination uses the HA Connect/gRPC service exposed on each node's HTTP port.

The provided Compose topology demonstrates three nodes:

```sh
docker compose -f examples/2pc/docker-compose.yml up
```

Its DSNs use the same pattern as this abbreviated two-node example:

```text
file:/data/db.sqlite?_journal=WAL&2pcPeers=http://node2:8080
```

On node 2, configure `2pcPeers=http://node1:8080`. For three or more nodes, list every other participant, separated by commas, as in the Compose example.

The `2pcTimeout` DSN parameter sets the coordination timeout; it defaults to `60s`. For example:

```text
file:/data/db.sqlite?_journal=WAL&2pcPeers=http://node2:8080&2pcTimeout=15s
```

The coordinator persists commit decisions locally and retries recovery on startup. A transaction can commit locally but return `ErrTwoPhaseCommitPending` if a remote commit cannot be confirmed; treat that as a pending outcome, not proof that the transaction was rolled back. Ensure every peer address remains stable and reachable, and protect peer traffic with appropriate network controls and authentication in production.

## Known limitations and unsupported features

HA speaks SQLite SQL. MySQL and PostgreSQL wire compatibility lets compatible clients connect; it does not provide those databases' SQL dialects or feature sets.

- MySQL- and PostgreSQL-specific SQL functions are not supported unless SQLite provides an equivalent.
- Tables declared `WITHOUT ROWID` are not replicated.
- Replication does not emit changes for conflicting rows removed by `ON CONFLICT REPLACE`.
- `CREATE IF NOT EXISTS` and `DROP IF EXISTS` are idempotent for replicated DDL. `ALTER TABLE` replication is less predictable, so validate schema changes across the full cluster before relying on them.
- MySQL's deprecated `COM_FIELD_LIST` protocol command is not supported.
- Undo operations reverse data changes, not schema changes such as `CREATE`, `ALTER`, or `DROP`.
- Leaderless writes to multiple nodes can conflict; default last-writer-wins behavior may not fit every application.
- 2PC requires all participating peers to be configured and reachable for coordination. If commit confirmation is interrupted after the local decision is durable, the operation can remain pending until recovery completes.

For workloads that need database-specific behavior, verify each SQL statement and client feature against SQLite and the relevant wire-protocol compatibility before deploying.

## Installation

### Install from source

Install with Go:

```sh
go install github.com/litesql/ha@latest
```

### Install with Docker

```sh
docker run --name ha \
  -e HA_MEMORY=true \
  -p 5432:5432 -p 8080:8080 -p 4222:4222 \
  ghcr.io/litesql/ha:latest
```

Mount persistent storage for the database and NATS JetStream state when running a durable deployment. For complete multi-node setups, use the Compose files under `examples/`.

### Install with Helm

```sh
helm repo add litesql https://litesql.github.io/helm-charts
helm repo update
helm install ha litesql/ha
```

See the [litesql Helm charts repository](https://litesql.github.io/helm-charts) for chart configuration.

## Clients and protocols

### HA command-line client

Connect to a running node:

```sh
ha -r http://localhost:8080
```

The client accepts SQL and these additional commands:

| Command | Description |
|---|---|
| `SHOW DATABASES;` | List loaded databases |
| `CREATE DATABASE <dsn>;` | Create a database |
| `DROP DATABASE <id>;` | Drop a database |
| `SET DATABASE TO <id>;` | Select a database for subsequent commands |
| `UNSET DATABASE;` | Return to the default database |
| `EXIT;` | Quit the client |

### PostgreSQL and MySQL wire protocols

Enable a wire server with `--pg-port` or `--mysql-port`, then connect with a compatible client:

```sh
PGPASSWORD=ha psql -h localhost -U ha -p 5432
mysql -h localhost --port 3306 -u ha
```

Queries are executed by SQLite. Protocol support is intended for client interoperability, not as a drop-in replacement for PostgreSQL or MySQL; see [Known limitations](#known-limitations-and-unsupported-features).

For GUI access, use the [JDBC HA driver](https://github.com/litesql/jdbc-ha). Go applications can use the [go-ha `database/sql` driver](https://github.com/litesql/go-ha).

### Local replicas

- Use the [go-ha `database/sql` driver](https://github.com/litesql/go-ha) to create embedded read/write replicas for Go applications.
- Use the [ha-sync SQLite extension](https://github.com/litesql/ha-sync) to create local read-only replicas from a remote HA database.

## HTTP API

The OpenAPI description is available at `http://localhost:8080/openapi.yaml`; interactive API documentation is available at `http://localhost:8080/docs`. The [gRPC API and client SDKs](https://buf.build/litesql/sqlite-ha/sdks/main:grpc) are also available.

Common HTTP endpoints include:

| Method and path | Purpose |
|---|---|
| `GET /healthz` | Health check |
| `GET /databases` | List loaded databases |
| `POST /databases` | Create a database |
| `DELETE /databases/{id}` | Drop a database |
| `POST /query` | Execute SQL against the default database |
| `POST /databases/{id}` | Execute SQL against a selected database |
| `GET /download` | Download a database backup |
| `POST /snapshot` / `GET /snapshot` | Create or download the latest snapshot |
| `GET /replications` | List replication subscriptions |
| `GET /replications/{name}` | Get replication status |
| `DELETE /replications/{name}` | Remove a replication |
| `GET /history/{param}` | Read transaction history |
| `POST /undo/{param}`, `/undoe/{param}`, `/undot/{param}` | Undo transactions |

### Execute a parameterized query

```sh
curl -H 'Content-Type: application/json' -d '{
  "sql": "INSERT INTO users(name) VALUES(:name)",
  "params": {"name": "HA user"}
}' http://localhost:8080/query
```

### Execute multiple statements in one transaction

Send an array of SQL statements to `/query`; HA executes the commands as a transaction:

```sh
curl -H 'Content-Type: application/json' -d '[
  {"sql":"INSERT INTO users(name) VALUES(:name)","params":{"name":"temporary"}},
  {"sql":"DELETE FROM users WHERE name = :name","params":{"name":"temporary"}},
  {"sql":"SELECT * FROM users"}
]' http://localhost:8080/query
```

### MCP endpoint

HA exposes a streamable HTTP MCP endpoint at `/mcp` with two tools:

- `databases`: list loaded database identifiers.
- `query`: execute a query against a selected database, optionally with parameters.

## Replication and CDC

HA uses NATS JetStream for HA-to-HA replication. It can use an embedded NATS server or connect to an external server with `--replication-url`. JetStream delivers at least once; replicated DML is designed to be idempotent. DDL replication is enabled by default and can be disabled with `--disable-ddl-sync`.

The change-data-capture messages contain a node name, a list of row changes, and a timestamp. A row change includes the database and table names, column names, operation, and old/new row identifiers and values where applicable.

Replication can be configured with a delivery policy (`--replication-policy`), stream name (`--replication-stream`), stream retention age (`--replication-max-age`), JetStream replica count (`--replicas`), and row identification strategy (`--row-identify`: `pk`, `rowid`, or `full`). `--async-replication` enables asynchronous replication publishing.

### Custom conflict handling

The default conflict policy is last-writer-wins. Use `--interceptor` to load a Go script implementing custom change-set behavior. See the [interceptor example](https://github.com/litesql/ha/blob/main/internal/interceptor/testdata/ignore_alter_table_errors.go).

### Snapshots and recovery

Use `--snapshot-interval` to publish snapshots to the NATS JetStream Object Store, and `--from-latest-snapshot` to load the latest available snapshot at startup. Snapshots support initialization and recovery; they do not replace persistent backups or operational testing of restore procedures.

## Proxy and source replication

HA can keep a local SQLite copy of an external data source and, depending on configuration, proxy queries or redirect writes to that source.

### MySQL source

Set `--mysql-proxied` to the source MySQL DSN. `--mysql-include` and `--mysql-exclude` filter replicated tables. `--mysql-dump-bin`, `--mysql-dump-db`, and `--mysql-dump-include` configure optional initial data import with `mysqldump`; `--mysql-proxy-id` identifies the replication connection.

### PostgreSQL source

Set `--pg-proxied` to the source PostgreSQL DSN. Configure logical replication with `--pg-publication` and `--pg-slot`; the source must be set up for PostgreSQL logical replication.

### Debezium sink

Set `--debezium-brokers`, `--debezium-group`, and `--debezium-topics` to consume Debezium change events from Kafka. Set `--debezium-source-dsn` if writes should be redirected to a source database. Supported source DSN drivers include MySQL/MariaDB, Oracle, PostgreSQL, and SQL Server.

Use `--proxy-local` to choose the local SQLite file, `--proxy-use-schema` to create local tables from source schema, `--proxy-disable-redirect` to keep query execution local, and `--proxy-read-your-writes` to enable read-your-writes behavior for proxied queries.

Examples: [PostgreSQL](https://github.com/litesql/ha/tree/main/examples/proxy-postgresql), [MySQL](https://github.com/litesql/ha/tree/main/examples/proxy-mysql), and [Oracle via Debezium](https://github.com/litesql/ha/tree/main/examples/proxy-oracle-debezium).

## Cross-database queries

Query across multiple SQLite databases loaded on the same HA node without `ATTACH DATABASE`. Use the `/*+ db=<regex> */` SQL hint to choose participating database IDs:

```sql
SELECT id, name /*+ db=.* */ FROM users;
SELECT id, name /*+ db=users_.* */ FROM users;
```

Find database IDs with `SHOW DATABASES;`. This feature selects databases on one node; it is not a distributed join across remote HA nodes.

## Transaction history and undo

Read recent transaction history by count or age:

```sql
HISTORY 100;
HISTORY '5m';
```

Undo commands accept a sequence number or a time duration:

| Command | Behavior |
|---|---|
| `UNDO n` | Revert transactions from sequence `n` onward |
| `UNDOE n` | Revert entity modifications from sequence `n` onward |
| `UNDOT n` | Revert transactions affecting entities modified by sequence `n` |

For example: `UNDO 150;`, `UNDO '10m';`, `UNDOE 200;`, or `UNDOT 250;`. Undo does not restore schema changes.

## Configuration

Run `ha --help` for current command-line options. Most options can also be set through the corresponding `HA_...` environment variable or a config file. Database-specific 2PC options (`2pcPeers` and `2pcTimeout`) are SQLite DSN query parameters, not command-line flags.

| Flag | Environment variable | Default | Description |
|---|---|---:|---|
| `-n`, `--name` | `HA_NAME` | hostname | Node name |
| `-p`, `--port` | `HA_PORT` | `8080` | HTTP and gRPC server port |
| `--token` | `HA_TOKEN` | unset | API and gRPC authentication token |
| `--log-level` | `HA_LOG_LEVEL` | `info` | Log level: `info`, `warn`, `error`, or `debug` |
| `-m`, `--memory` | `HA_MEMORY` | `false` | Store the database in memory |
| `--db-params` | `HA_DB_PARAMS` | build-dependent | SQLite parameters appended to database DSNs |
| `--create-db-dir` | `HA_CREATE_DB_DIR` | unset | Directory for newly created database files |
| `--config` | `HA_CONFIG` | unset | Optional config file |
| `--version` | `HA_VERSION` | | Print version and exit |
| `--from-latest-snapshot` | `HA_FROM_LATEST_SNAPSHOT` | `false` | Load the latest available JetStream snapshot at startup |
| `--snapshot-interval` | `HA_SNAPSHOT_INTERVAL` | `0` | Automatic snapshot interval; zero disables snapshots |
| `--disable-ddl-sync` | `HA_DISABLE_DDL_SYNC` | `false` | Disable publishing DDL changes |
| `--extensions` | `HA_EXTENSIONS` | unset | Comma-separated SQLite extensions to load |
| `--nats-port` | `HA_NATS_PORT` | `4222` | Embedded NATS port; `0` disables the embedded server |
| `--nats-store-dir` | `HA_NATS_STORE_DIR` | unset | Embedded NATS storage directory |
| `--nats-user` | `HA_NATS_USER` | unset | Embedded NATS username |
| `--nats-pass` | `HA_NATS_PASS` | unset | Embedded NATS password |
| `--nats-config` | `HA_NATS_CONFIG` | unset | Embedded NATS configuration file |
| `--nats-logs` | `HA_NATS_LOGS` | `false` | Enable embedded NATS logs |
| `--replication-url` | `HA_REPLICATION_URL` | unset | External NATS URL; otherwise use embedded NATS |
| `--replication-stream` | `HA_REPLICATION_STREAM` | `ha_replication` | JetStream replication stream name |
| `--replication-policy` | `HA_REPLICATION_POLICY` | default | Subscriber delivery policy |
| `--replication-timeout` | `HA_REPLICATION_TIMEOUT` | `15s` | Replication publisher timeout |
| `--replication-max-age` | `HA_REPLICATION_MAX_AGE` | `24h` | Maximum replication message age |
| `--replicas` | `HA_REPLICAS` | `1` | JetStream replicas, from 1 to 5 |
| `--async-replication` | `HA_ASYNC_REPLICATION` | `false` | Publish replication messages asynchronously |
| `--row-identify` | `HA_ROW_IDENTIFY` | `pk` | Row identification strategy: `pk`, `rowid`, or `full` |
| `--leader-addr` | `HA_LEADER_ADDR` | unset | Local target address; enables leader election |
| `--leader-static` | `HA_LEADER_STATIC` | unset | Fixed remote leader address; disables election |
| `--grpc-insecure` | `HA_GRPC_INSECURE` | `false` | Use plaintext gRPC for leader messages; trusted networks only |
| `--interceptor` | `HA_INTERCEPTOR` | unset | Go script for custom replication behavior |
| `--pg-port` | `HA_PG_PORT` | `0` | PostgreSQL wire-protocol port; zero disables it |
| `--pg-user` | `HA_PG_USER` | `ha` | PostgreSQL wire-protocol username |
| `--pg-pass` | `HA_PG_PASS` | `ha` | PostgreSQL wire-protocol password |
| `--pg-cert` | `HA_PG_CERT` | unset | PostgreSQL server TLS certificate |
| `--pg-key` | `HA_PG_KEY` | unset | PostgreSQL server TLS key |
| `--mysql-port` | `HA_MYSQL_PORT` | `0` | MySQL wire-protocol port; zero disables it |
| `--mysql-user` | `HA_MYSQL_USER` | `ha` | MySQL wire-protocol username |
| `--mysql-pass` | `HA_MYSQL_PASS` | unset | MySQL wire-protocol password |
| `--pg-proxied` | `HA_PG_PROXIED` | unset | PostgreSQL source DSN |
| `--pg-publication` | `HA_PG_PUBLICATION` | `ha_publication` | Source logical-replication publication |
| `--pg-slot` | `HA_PG_SLOT` | `ha_slot` | Source logical-replication slot |
| `--mysql-proxied` | `HA_MYSQL_PROXIED` | unset | MySQL source DSN |
| `--mysql-include` | `HA_MYSQL_INCLUDE` | `^db.*` | Table include regexp; empty includes all |
| `--mysql-exclude` | `HA_MYSQL_EXCLUDE` | unset | Table exclude regexp |
| `--mysql-dump-bin` | `HA_MYSQL_DUMP_BIN` | unset | `mysqldump` executable path |
| `--mysql-dump-db` | `HA_MYSQL_DUMP_DB` | unset | Database name used by `mysqldump` |
| `--mysql-dump-include` | `HA_MYSQL_DUMP_INCLUDE` | unset | Table filter passed to `mysqldump` |
| `--mysql-proxy-id` | `HA_MYSQL_PROXY_ID` | `sqlite-ha` | MySQL replication metadata identifier |
| `--proxy-local` | `HA_PROXY_LOCAL` | `ha.db` | Local SQLite proxy file |
| `--proxy-use-schema` | `HA_PROXY_USE_SCHEMA` | `false` | Create local tables using source schema |
| `--proxy-disable-redirect` | `HA_PROXY_DISABLE_REDIRECT` | `false` | Execute queries on local SQLite instead of redirecting |
| `--proxy-read-your-writes` | `HA_PROXY_READ_YOUR_WRITES` | `false` | Enable read-your-writes behavior for proxied queries |
| `--debezium-brokers` | `HA_DEBEZIUM_BROKERS` | unset | Comma-separated Kafka brokers |
| `--debezium-group` | `HA_DEBEZIUM_GROUP` | unset | Kafka consumer group |
| `--debezium-topics` | `HA_DEBEZIUM_TOPICS` | unset | Kafka topics to consume |
| `--debezium-source-dsn` | `HA_DEBEZIUM_SOURCE_DSN` | unset | Source DSN for redirected writes |
| `--concurrent-queries` | `HA_CONCURRENT_QUERIES` | `50` | Maximum concurrent queries |

Replication delivery policy values include `all`, `last`, `new`, `by_start_sequence=X`, and `by_start_time=X`. See [2PC configuration](#synchronous-two-phase-commit-2pc) for peer-list DSN syntax.

## More information

- [Source repository](https://github.com/litesql/ha)
- [gRPC API and SDKs](https://buf.build/litesql/sqlite-ha/sdks/main:grpc)
- [PostgreSQL proxy example](https://github.com/litesql/ha/tree/main/examples/proxy-postgresql)
- [MySQL proxy example](https://github.com/litesql/ha/tree/main/examples/proxy-mysql)
- [Oracle Debezium example](https://github.com/litesql/ha/tree/main/examples/proxy-oracle-debezium)
