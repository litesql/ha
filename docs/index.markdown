---
# Feel free to add content and custom Front Matter to this file.
# To modify the layout, see https://jekyllrb.com/docs/themes/#overriding-theme-defaults

layout: home
---

![HA logo](https://github.com/litesql/ha/blob/main/ha.png?raw=true)

Highly available SQLite cluster with embedded NATS JetStream.

- Connect using HTTP API, [gRPC API](https://buf.build/litesql/sqlite-ha/sdks/main:grpc), [database/sql Go driver](https://github.com/litesql/go-ha), [JDBC driver](https://github.com/litesql/jdbc-ha), MySQL, or PostgreSQL Wire Protocol
- Create live local **read/write** replicas with [go-ha database/sql driver](https://github.com/litesql/go-ha)
- Create live local read replicas with [ha-sync SQLite extension](https://github.com/litesql/ha-sync)
- Change Data Capture (CDC)
- Execute cross-database queries without `ATTACH DATABASE`
- Proxy and replicate PostgreSQL, MySQL, or any Debezium Source Connector–compatible database to build a resilient, faster **edge data service**.
- [Open Source](https://github.com/litesql/ha)

## Overview

- [1. Installation](#installation)
  - [1.1 Install from source](#install-from-source)
  - [1.2 Install with Docker](#install-with-docker)
  - [1.3 Install with Helm](#install-with-helm)
- [2. Quick Start](#quick-start)
  - [2.1 Load an existing database into memory](#load-an-existing-database-into-memory)
  - [2.2 Use a database on disk](#use-a-database-on-disk)
  - [2.3 Load from the latest snapshot](#load-from-the-latest-snapshot)
  - [2.4 Load multiple databases](#load-multiple-databases)
- [3. Local Replicas](#local-replicas)
  - [3.1 Read/write replicas](#readwrite-replicas)
  - [3.2 Read-only replicas](#read-only-replicas)
- [4. HA Client, PostgreSQL and MySQL Wire Protocol](#wire-protocols)
  - [4.1 HA client mode](#ha-client-mode)
- [5. HTTP API](#http-api)
  - [5.1 Bind parameters](#bind-parameters)
  - [5.2 Multiple commands in one transaction](#multiple-commands-in-one-transaction)
  - [5.3 Backup the database](#backup-the-database)
  - [5.4 Take a snapshot](#take-a-snapshot)
  - [5.5 Download the latest snapshot](#download-the-latest-snapshot)
  - [5.6 List replications](#list-replications)
  - [5.7 Replication status](#replication-status)
  - [5.8 Remove replication](#remove-replication)
- [6. Replication](#replication)
  - [6.1 CDC message format](#cdc-message-format)
  - [6.2 Replication limitations](#replication-limitations)
  - [6.3 Conflict resolution](#conflict-resolution)
  - [6.4 Proxy and source replication](#proxy-and-source-replication)
- [7. Cross-shard Queries](#cross-shard-queries)
- [8. Transaction Operations](#transaction-operations)
- [9. Configuration](#configuration)

## 1. Installation<a id='installation'></a>

Download the latest release from the [GitHub releases page](https://github.com/litesql/ha/releases).

### 1.1 Install from source<a id='install-from-source'></a>

```sh
go install github.com/litesql/ha@latest
```

### 1.2 Install with Docker<a id='install-with-docker'></a>

```sh
docker run --name ha \
  -e HA_MEMORY=true \
  -p 5432:5432 -p 8080:8080 -p 4222:4222 \
  ghcr.io/litesql/ha:latest
```

> Mount a volume at `/data` to persist NATS JetStream state.

#### Cluster example

Use the provided Docker Compose example:

```sh
cd examples/leader-based
docker compose up
```

| Instance | HTTP | PostgreSQL Wire | NATS | MySQL Wire |
|----------|------|-----------------|------|------------|
| node1    | 8080 | 5432            | 4222 | 3306       |
| node2    | 8081 | 5433            | 4223 | 3307       |
| node3    | 8082 | 5434            | 4224 | 3308       |

### 1.3 Install with Helm<a id='install-with-helm'></a>

```sh
helm repo add litesql https://litesql.github.io/helm-charts
helm repo update
helm install ha litesql/ha
```

Visit the [litesql Helm charts repository](https://litesql.github.io/helm-charts) for deployment options.

## 2. Quick Start<a id='quick-start'></a>

### Start a local HA node

```sh
ha -n node1 -m --pg-port 5432
```

Start a second node that connects to the first node:

```sh
ha -n node2 -m --port 8081 --pg-port 5433 --nats-port 0 --replication-url nats://localhost:4222
```

### Create a table

```sh
curl -d '[
  {
    "sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
  }
]' \
http://localhost:8080
```

### Insert data using the HTTP API

```sh
curl -d '[
  {
    "sql": "INSERT INTO users(name) VALUES(:name)",
    "params": {"name": "HA User"}
  }
]' \
http://localhost:8080
```

### Connect with PostgreSQL Wire Protocol

```sh
PGPASSWORD="ha" psql -h localhost -U ha
```

```sql
INSERT INTO users(name) VALUES('HA user from PostgreSQL Wire Protocol');
SELECT * FROM users;
```

### Check another node

```sh
PGPASSWORD="ha" psql -h localhost -U ha -p 5433
```

```sql
SELECT * FROM users;
```

### 2.1 Load an existing database into memory<a id='load-an-existing-database-into-memory'></a>

```sh
ha -m mydatabase.db
```

### 2.2 Use a database on disk<a id='use-a-database-on-disk'></a>

```sh
ha "file:mydatabase.db"
```

### 2.3 Load from the latest snapshot<a id='load-from-the-latest-snapshot'></a>

```sh
ha --from-latest-snapshot
```

### 2.4 Load multiple databases<a id='load-multiple-databases'></a>

```sh
ha *.db
```

## 3. Local Replicas<a id='local-replicas'></a>

### 3.1 Read/write replicas<a id='readwrite-replicas'></a>

- Use the [go-ha](https://github.com/litesql/go-ha) `database/sql` driver to create embedded read/write replicas.
- Ideal for Go applications that require a local writable HA replica.

### 3.2 Read-only replicas<a id='read-only-replicas'></a>

- Use the [ha-sync](https://github.com/litesql/ha-sync) SQLite extension to create local read-only replicas from a remote HA database.
- Works with any language that supports SQLite.

## 4. HA Client, PostgreSQL and MySQL Wire Protocol<a id='wire-protocols'></a>

- Connect with any HA, PostgreSQL, or MySQL compatible client.
- SQL is parsed by SQLite.
- MySQL- and PostgreSQL-specific functions are not supported.
- For GUI tools such as DBeaver, use the [JDBC HA driver](https://github.com/litesql/jdbc-ha).

### 4.1 HA client mode<a id='ha-client-mode'></a>

```sh
ha -r http://localhost:8080
```

The HA client accepts standard SQL plus these special commands:

| Command | Description |
|---------|-------------|
| `SHOW DATABASES;` | List all available databases |
| `CREATE DATABASE <dsn>;` | Create a new database |
| `DROP DATABASE <id>;` | Drop a database |
| `SET DATABASE TO <id>;` | Run subsequent commands against a specific database |
| `UNSET DATABASE;` | Reset to the default database |
| `EXIT;` | Quit the client (`Ctrl+D`) |

## 5. HTTP API<a id='http-api'></a>

Access the OpenAPI definition at [http://localhost:8080/openapi.yaml](http://localhost:8080/openapi.yaml).

### 5.1 Bind parameters<a id='bind-parameters'></a>

```sh
curl -d '{
  "sql": "INSERT INTO users(name) VALUES(:name)",
  "params": {"name": "HA user"}
}' \
http://localhost:8080/query
```

Example response:

```json
{
  "columns": ["rows_affected", "last_insert_id"],
  "rows": [[1, 3]]
}
```

### 5.2 Multiple commands in one transaction<a id='multiple-commands-in-one-transaction'></a>

```sh
curl -d '[
  {
    "sql": "INSERT INTO users(name) VALUES(:name)",
    "params": {"name": "new HA user"}
  },
  {
    "sql": "DELETE FROM users WHERE name = :name",
    "params": {"name": "new HA user"}
  },
  {
    "sql": "SELECT * FROM users"
  }
]' \
http://localhost:8080/query
```

Example response:

```json
{
  "results": [
    {
      "columns": ["rows_affected", "last_insert_id"],
      "rows": [[1, 2]]
    },
    {
      "columns": ["rows_affected", "last_insert_id"],
      "rows": [[1, 2]]
    },
    {
      "columns": ["id", "name"],
      "rows": [[1, "HA user"]]
    }
  ]
}
```

### 5.3 Backup the database<a id='backup-the-database'></a>

```sh
curl -O -J http://localhost:8080/download
```

### 5.4 Take a snapshot<a id='take-a-snapshot'></a>

```sh
curl -X POST http://localhost:8080/snapshot
```

### 5.5 Download the latest snapshot<a id='download-the-latest-snapshot'></a>

```sh
curl -O -J http://localhost:8080/snapshot
```

### 5.6 List replications<a id='list-replications'></a>

```sh
curl http://localhost:8080/replications
```

### 5.7 Replication status<a id='replication-status'></a>

```sh
curl http://localhost:8080/replications/{name}
```

### 5.8 Remove replication<a id='remove-replication'></a>

```sh
curl -X DELETE http://localhost:8080/replications/{name}
```

## 6. Replication<a id='replication'></a>

- Support writing to any server in leaderless mode.
- Works with embedded or external NATS JetStream.
- NATS JetStream delivers at least once.
- DML operations (INSERT, UPDATE, DELETE) are idempotent.
- Last writer wins.
- DDL commands are replicated since v0.0.7.

### 6.1 CDC message format<a id='cdc-message-format'></a>

```json
{
  "node": "ha_node_name",
  "changes": [
    {
      "database": "main",
      "table": "users",
      "columns": ["id", "name"],
      "operation": "INSERT",
      "new_rowid": 2,
      "new_values": [2, "new HA user"]
    },
    {
      "database": "main",
      "table": "users",
      "columns": ["id", "name"],
      "operation": "DELETE",
      "old_rowid": 2,
      "old_values": [2, "new HA user"]
    }
  ],
  "timestamp_ns": 1758574275504509677
}
```

### 6.2 Replication limitations<a id='replication-limitations'></a>

- Tables without `ROWID` are not replicated.
- Replication is not triggered when conflicting rows are removed by `ON CONFLICT REPLACE`.
- DDL idempotency is automatic for `CREATE IF NOT EXISTS` and `DROP IF EXISTS`, but `ALTER TABLE` replication is less predictable.
- Writing to multiple nodes improves availability, but may reduce consistency in some edge cases. If consistency is required, route writes through a single node or use `--leader-static` / `--leader-addr`.

### 6.3 Conflict resolution<a id='conflict-resolution'></a>

HA applies the following rules:

1. **Last Writer Wins**: the most recent write is retained.
2. **Idempotent operations**: supported DML and DDL commands can be replayed safely.
3. **Custom conflict handling**: use `--interceptor` with a Go script to implement application-specific logic.

Example interceptor: [ignore_alter_table_errors.go](https://github.com/litesql/ha/blob/main/internal/interceptor/testdata/ignore_alter_table_errors.go).

### 6.4 Proxy and source replication<a id='proxy-and-source-replication'></a>

HA can proxy reads and writes to an external MySQL or PostgreSQL source database while maintaining a local SQLite cache.

- Use `--mysql-proxied` to connect to a source MySQL database.
- Use `--pg-proxied` to connect to a source PostgreSQL database.
- The local SQLite proxy file is configured with `--proxy-local`.
- `--proxy-disable-redirect` forces all queries to run on the local SQLite database instead of redirecting them to the source.
- `--proxy-read-your-writes` enables read-your-writes semantics for proxied queries.

For MySQL source replication, provide optional import settings:

- `--mysql-include` and `--mysql-exclude` filter source MySQL tables by regexp.
- `--mysql-dump-bin` - path to the `mysqldump` executable for initial data import.
- `--mysql-dump-db` - database name used by `mysqldump`.
- `--mysql-dump-include` - table filter passed to `mysqldump`.
- `--mysql-proxy-id` - identifier for MySQL replication metadata.

For PostgreSQL source replication, configure logical replication:

- `--pg-publication` - publication name in the source PostgreSQL database.
- `--pg-slot` - replication slot name created on the source.

### 6.5 Debezium sink mode<a id='debezium-sink-mode'></a>

HA can act as a Debezium sink for Kafka topics.

- `--debezium-brokers` specifies Kafka brokers.
- `--debezium-group` sets the Kafka consumer group.
- `--debezium-topics` selects the Kafka topics to consume.
- `--debezium-source-dsn` redirects writes back to the source database.

This mode is useful when HA is consuming Debezium change events and storing them locally while optionally forwarding writes back to the original source.

## 7. Cross-shard Queries<a id='cross-shard-queries'></a>

HA supports queries across multiple SQLite databases on the same node without `ATTACH DATABASE`.

Use the optimizer hint `/*+ db=<regex> */` to select participating databases:

```sql
SELECT id, name /*+ db=.* */ FROM users;
```

To target a subset, use a narrower regex, for example `db=users_.*`.

- Discover database IDs with `SHOW DATABASES;`
- Refine shard selection with the regex pattern

## 8. Transaction Operations<a id='transaction-operations'></a>

HA provides transaction history and undo commands for committed changes.

### Transaction History

```sql
HISTORY 100;
HISTORY '5m';
```

### Undo Operations

- `UNDO n`: revert transactions from sequence `n` onward.
- `UNDOE n`: revert entity modifications from sequence `n` onward.
- `UNDOT n`: revert transactions affecting entities modified by sequence `n`.

Examples:

```sql
UNDO 150;
UNDO '10m';
UNDOE 200;
UNDOT 250;
```

**Note:** Undo does not revert schema changes such as `CREATE`, `ALTER`, or `DROP`.

## 9. Configuration<a id='configuration'></a>

Use `ha --help` for the full list of options.

| Flag | Environment Variable | Default | Description |
|------|----------------------|---------|-------------|
| -n, --name | HA_NAME | hostname | Node name |
| -p, --port | HA_PORT | 8080 | Server port for HTTP and gRPC endpoints |
| --token | HA_TOKEN | | API authentication token |
| -m, --memory | HA_MEMORY | false | Store the database in memory |
| --db-params | HA_DB_PARAMS | default | SQLite DSN parameters appended to each database file |
| --create-db-dir | HA_CREATE_DB_DIR | | Directory for new database files |
| --from-latest-snapshot | HA_FROM_LATEST_SNAPSHOT | false | Load the latest snapshot from NATS JetStream Object Store if available |
| --snapshot-interval | HA_SNAPSHOT_INTERVAL | 0s | Interval for automatic snapshots to NATS JetStream Object Store |
| --disable-ddl-sync | HA_DISABLE_DDL_SYNC | false | Disable publishing DDL commands |
| --nats-logs | HA_NATS_LOGS | false | Enable embedded NATS server logging |
| --nats-port | HA_NATS_PORT | 4222 | Embedded NATS server port (0 disables embedded NATS) |
| --nats-store-dir | HA_NATS_STORE_DIR | | Embedded NATS server storage directory |
| --nats-user | HA_NATS_USER | | Embedded NATS server username |
| --nats-pass | HA_NATS_PASS | | Embedded NATS server password |
| --nats-config | HA_NATS_CONFIG | | Embedded NATS server configuration file |
| --leader-addr | HA_LEADER_ADDR | | Address used when this node becomes leader (enables leader election) |
| --leader-static | HA_LEADER_STATIC | | Static leader address (disables leader election) |
| --grpc-insecure | HA_GRPC_INSECURE | false | Use plaintext gRPC for leader messages |
| --mysql-port | HA_MYSQL_PORT | 0 | Port for MySQL wire protocol server |
| --mysql-user | HA_MYSQL_USER | ha | MySQL authentication user |
| --mysql-pass | HA_MYSQL_PASS | | MySQL authentication password |
| --mysql-proxied | HA_MYSQL_PROXIED | | Source MySQL DSN to replicate into the local HA instance and redirect writes |
| --mysql-include | HA_MYSQL_INCLUDE | ^db.* | Regexp to include tables from the proxied MySQL source |
| --mysql-exclude | HA_MYSQL_EXCLUDE | | Regexp to exclude tables from the proxied MySQL source |
| --mysql-dump-bin | HA_MYSQL_DUMP_BIN | | Path to mysqldump executable for proxied MySQL import |
| --mysql-dump-db | HA_MYSQL_DUMP_DB | | Database name used by mysqldump for proxied MySQL import |
| --mysql-dump-include | HA_MYSQL_DUMP_INCLUDE | | Table filter passed to mysqldump for proxied MySQL import |
| --mysql-proxy-id | HA_MYSQL_PROXY_ID | sqlite-ha | Identifier for proxied MySQL replication metadata |
| --pg-port | HA_PG_PORT | 0 | Port for PostgreSQL wire protocol server |
| --pg-user | HA_PG_USER | ha | PostgreSQL authentication user |
| --pg-pass | HA_PG_PASS | ha | PostgreSQL authentication password |
| --pg-cert | HA_PG_CERT | | TLS certificate file for PostgreSQL server |
| --pg-key | HA_PG_KEY | | TLS key file for PostgreSQL server |
| --pg-proxied | HA_PG_PROXIED | | Source PostgreSQL DSN to replicate from and proxy to |
| --pg-publication | HA_PG_PUBLICATION | ha_publication | Publication name for source PostgreSQL logical replication |
| --pg-slot | HA_PG_SLOT | ha_slot | Replication slot name for the source PostgreSQL database |
| --proxy-local | HA_PROXY_LOCAL | ha.db | Local SQLite proxy database file path |
| --proxy-use-schema | HA_PROXY_USE_SCHEMA | false | Create local tables based on source database schema |
| --proxy-disable-redirect | HA_PROXY_DISABLE_REDIRECT | false | Disable redirecting queries to the source database |
| --proxy-read-your-writes | HA_PROXY_READ_YOUR_WRITES | false | Enable read-your-writes behavior for proxied queries |
| --debezium-brokers | HA_DEBEZIUM_BROKERS | | Comma-separated Kafka brokers for Debezium sink mode |
| --debezium-group | HA_DEBEZIUM_GROUP | | Kafka consumer group for Debezium sink |
| --debezium-topics | HA_DEBEZIUM_TOPICS | | Kafka topics to consume in Debezium sink mode |
| --debezium-source-dsn | HA_DEBEZIUM_SOURCE_DSN | | Source DSN for Debezium write redirection |
| --concurrent-queries | HA_CONCURRENT_QUERIES | 50 | Maximum number of concurrent queries |
| --async-replication | HA_ASYNC_REPLICATION | false | Enable asynchronous replication message publishing |
| --async-replication-store-dir | HA_ASYNC_REPLICATION_STORE_DIR | | Directory for asynchronous replication outbox storage |
| --replicas | HA_REPLICAS | 1 | Number of JetStream replicas for stream and object store |
| --replication-timeout | HA_REPLICATION_TIMEOUT | 15s | Timeout for replication publisher operations |
| --replication-stream | HA_REPLICATION_STREAM | ha_replication | Replication stream name |
| --replication-max-age | HA_REPLICATION_MAX_AGE | 24h | Maximum age for messages in the replication stream |
| --replication-url | HA_REPLICATION_URL | | NATS URL for replication; defaults to embedded NATS when empty |
| --replication-policy | HA_REPLICATION_POLICY | | Replication subscriber delivery policy: all, last, new, by_start_sequence=X, or by_start_time=x |
| --row-identify | HA_ROW_IDENTIFY | pk | Row identification strategy for replication: pk, rowid, or full |
| --extensions | HA_EXTENSIONS | | Comma-separated list of SQLite extensions to load |
| --config | HA_CONFIG | | Path to an optional config file |
| --version | HA_VERSION | | Print version information and exit |
