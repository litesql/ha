# ha

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![LiberaPay](https://liberapay.com/assets/widgets/donate.svg)](https://liberapay.com/walterwanderley/donate)
[![receives](https://img.shields.io/liberapay/receives/walterwanderley.svg?logo=liberapay)](https://liberapay.com/walterwanderley/donate)
[![patrons](https://img.shields.io/liberapay/patrons/walterwanderley.svg?logo=liberapay)](https://liberapay.com/walterwanderley/donate)


Highly available leaderless SQLite cluster powered by embedded NATS JetStream server.

- Connect using HTTP API or PostgreSQL Wire Protocol
- Use [ha-sync SQLite extension](https://github.com/litesql/ha-sync) to create live local read replicas
- Change Data Capture (CDC)
- [https://litesql.github.io/ha/](https://litesql.github.io/ha/)

## Overview

- [1. Installation](#1)
  - [1.1 Install from source](#1.1)
- [2 . Usage](#2)
  - [2.1 Loading existed database to memory](#2.1)
  - [2.2 Use database in disk](#2.2)
  - [2.3 Load database from latest snapshot](#2.3)
- [3. Local Read Replicas](#3)
- [4. Using Docker](#4)
  - [4.1 Cluster example](#4.1)
- [5. PostgreSQL Wire Protocol](#5)
- [6. HTTP API](#6)
  - [6.1 Using bind parameters](#6.1)
  - [6.2 Multiple commands (one transaction)](#6.2)
  - [6.3 Backup database](#6.3)
  - [6.4 Take a snapshot and save on NATS Object Store](#6.4)
  - [6.5 Get latest snapshot from NATS Object Store](#6.5)
  - [6.6 List all replications status](#6.6)
  - [6.7 Get replication status](#6.7)
  - [6.8 Remove replication (consumer)](#6.8)
- [7. Replication](#7)
  - [7.1 CDC message format](#7.1)
  - [7.2 Replication limitations](#7.2)
- [8. Configuration](#8)


## 1. Installation<a id='1'></a>

- Download from [releases page](https://github.com/litesql/ha/releases).

### 1.1 Install from source<a id='1.1'></a>

```sh
git clone https://github.com/litesql/ha.git
cd ha
go install
```

## 2. Usage<a id='2'></a>

1. Start the first ha node (-m flag if you want to use in-memory)

```sh
ha -n node1 -m
```

2. Start an another ha node

```sh
ha -n node2 -m --port 8081 --pg-port 5433 --nats-port 0 --replication-url nats://localhost:4222
```

3. Create a table

```sh
curl -d '[
  {
    "sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"     
  }
]' \
http://localhost:8080
```

4. Insert some data using HTTP client

```sh
curl -d '[
  {
    "sql": "INSERT INTO users(name) VALUES(:name)", 
    "params": {"name": "HA User"} 
  }
]' \
http://localhost:8080
```

5. Or use a PostgreSQL client

```sh
PGPASSWORD="ha" psql -h localhost -U ha
```

```sql
INSERT INTO users(name) VALUES('HA user from PostgreSQL Wire Protocol');
SELECT * FROM users;
```

6. Connect to another server and check the values

```sh
PGPASSWORD="ha" psql -h localhost -U ha -p 5433
```

```sql
SELECT * FROM users;
```

### 2.1 Loading existed database to memory<a id='2.1'></a>

```sh
ha -m mydatabase.db
```

### 2.2 Use database in disk<a id='2.2'></a>

```sh
ha file:mydatabase.db?_journal=WAL&_busy_timeout=500
```

### 2.3 Load database from latest snapshot<a id='2.3'></a>

```sh
ha --from-latest-snapsot
```

## 3. Local Read Replicas<a id='3'></a>

- Use [ha-sync](https://github.com/litesql/ha-sync) SQLite extension to create local embedded replicas from a remote HA database.

## 4. Using Docker<a id='4'></a>

```sh
docker run --name ha \
-e HA_MEMORY=true \
-p 5432:5432 -p 8080:8080 -p 4222:4222 \
ghcr.io/litesql/ha:latest

- Set up a volume at /data to store the NATS streams state.

```
### 4.1 Cluster example<a id='4.1'></a>

- [Docker compose cluster](https://github.com/litesql/ha/blob/main/docker-compose.yml) example

```sh
docker compose up
```

- Services:

| Instance | HTTP | Pg Wire | NATS |
|----------|------|---------|------|
|node1     | 8080 | 5432    | 4222 |
|node2     | 8081 | 5433    | 4223 |
|node3     | 8082 | 5434    | 4224 |

## 5. PostgreSQL Wire Protocol<a id='5'></a>

- You can use any PostgreSQL driver to connect to ha.
- The SQLite parser engine will proccess the commands.
- PostgreSQL functions (and visual editors like pgadmim, dbeaver, etc) are not supported.

## 6. HTTP API<a id='6'></a>

### 6.1 Using bind parameters<a id='6.1'></a>

```sh
curl -d '[{
    "sql": "INSERT INTO users(name) VALUES(:name)", 
    "params": {"name": "HA user"}
}]' \
http://localhost:8080
```

```json
{
  "results": [
    {
      "columns": [
        "rows_affected",
        "last_insert_id"
      ],
      "rows": [
        [
          1,
          3
        ]
      ]
    }
  ]
}
```

### 6.2 Multiple commands (one transaction)<a id='6.2'></a>

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
http://localhost:8080
```

```json
{
  "results": [
    {
      "columns": [
        "rows_affected",
        "last_insert_id"
      ],
      "rows": [
        [
          1,
          2
        ]
      ]
    },
    {
      "columns": [
        "rows_affected",
        "last_insert_id"
      ],
      "rows": [
        [
          1,
          2
        ]
      ]
    },
    {
      "columns": [
        "id",
        "name"
      ],
      "rows": [
        [
          1,
          "HA user"
        ]        
      ]
    }
  ]
}
```

### 6.3 Backup database<a id='6.3'></a>

```sh
curl -O -J http://localhost:8080
```

### 6.4 Take a snapshot and save on NATS Object Store<a id='6.4'></a>

```sh
curl -X POST http://localhost:8080/snapshot
```

### 6.5 Get latest snapshot from NATS Object Store<a id='6.5'></a>

```sh
curl -O -J http://localhost:8080/snapshot
```

### 6.6 List all replications status<a id='6.6'></a>

```sh
curl http://localhost:8080/replications
```

### 6.7 Get replication status<a id='6.7'></a>

```sh
curl http://localhost:8080/replications/{name}
```

### 6.8 Remove replication (consumer)<a id='6.8'></a>

```sh
curl -X DELETE http://localhost:8080/replications/{name}
```

## 7. Replication<a id='7'></a>

- You can write to any server
- Uses embedded or external NATS JetStream cluster
- NATS JetStream guarantees "at-least-once" message delivery
- All DML (INSERT, UPDATE, DELETE) operations are idempotent
- Last writer wins
- DDL commands are replicated (since v0.0.7)

### 7.1 CDC message format<a id='7.1'></a>

```json
{
  "node": "ha_node_name",
  "changes": [
    {
      "database": "main",
      "table": "users",
      "columns": [
        "id",
        "name"
      ],
      "operation": "INSERT",      
      "new_rowid": 2,
      "new_values": [
        2,
        "new HA user"
      ]
    },
    {
      "database": "main",
      "table": "users",
      "columns": [
        "id",
        "name"
      ],
      "operation": "DELETE",
      "old_rowid": 2,
      "old_values": [
        2,
        "new HA user"
      ]
    }
  ],
  "timestamp_ns": 1758574275504509677
}
```

### 7.2 Replication limitations<a id='7.2'></a>

- Tables WITHOUT ROWID are not replicated
- The replication is not invoked when conflicting rows are deleted because of an ON CONFLICT REPLACE clause. 
- Use idempotents DDL commands (CREATE IF NOT EXISTS and DROP IF EXISTS)
- Writing to any node in the cluster improves availability, but it can lead to consistency issues in certain edge cases. If your application values Consistency more than Availability, it's better to route all write operations through a single cluster node.

## 8. Configuration<a id='8'></a>

| Flag | Environment Variable | Default | Description |
|------|----------------------|---------|-------------|
| -n, --name | HA_NAME        | random  | Node name   |
| -p, --port | HA_PORT        | 8080    | HTTP API tcp port |
| -m, --memory | HA_MEMORY    | false   | Store database in memory |
| --from-latest-snapsot | HA_FROM_LATEST_SNAPSHOT | false | Use the latest database snapshot from NATS JetStream Object Store (if available at startup) |
| --snapshot-interval | HA_SNAPSHOT_INTERVAL | 0s | Interval to create database snapshot to NATS JetStream Object Store (0 to disable) |
| --nats-logs | HA_NATS_LOGS | false | Enable embedded NATS Server logging |
| --nats-port | HA_NATS_PORT | 4222 | Embedded NATS server port (0 to disable) |
| --nats-store-dir | HA_NATS_STORE_DIR | /tmp/nats | Embedded NATS server store directory |
| --nats-user | HA_NATS_USER |  | Embedded NATS server user |
| --nats-pass | HA_NATS_PASS |  | Embedded NATS server password |
| --nats-config | HA_NATS_CONFIG | | Path to embedded NATS server config file (override other nats configurations) |
| --pg-port | HA_PG_PORT | 5432 | Port to PostgreSQL Wire Protocol server |
| --pg-user | HA_PG_USER | ha   | PostgreSQL Auth user |
| --pg-pass | HA_PG_PASS | ha   | PostgreSQL Auth password |
| --pg-cert | HA_PG_CERT |      | Path to PostgreSQL TLS certificate file |
| --pg-key  | HA_PG_KEY  |      | Path to PostgreSQL TLS key file |
| --concurrent-queries | HA_CONCURRENT_QUERIES | 50 | Number of concurrent queries (DB pool max) |
| --extensions | HA_EXTENSIONS |  | Comma-separated list of SQLite extensions path to load |
| --replicas | HA_REPLICAS | 1 | Number of replicas to keep for the stream and object store in clustered jetstream |
| --replication-timeout | HA_REPLICATION_TIMEOUT | 15s | Replication publisher timeout |
| --replication-stream | HA_REPLICATION_STREAM | ha_replication | Replication stream name |
| --replication-max-age | HA_REPLICATION_MAX_AGE | 24h | Replication stream max age |
| --replication-url | HA_REPLICATION_URL |  | Replication NATS url (defaults to embedded NATS server) |
| --replication-policy | HA_REPLICATION_POLICY | all | eplication subscriver delivery policy (all|last|new|by_start_sequence=X|by_start_time=x) |
| --version | HA_VERSION | false | Print version information and exit |
| -c, --config | | | config file (optional) |