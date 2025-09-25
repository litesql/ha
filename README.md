# ha

Highly available leaderless SQLite cluster powered by embedded NATS JetStream server.

- Connect using HTTP API or PostgreSQL Wire Protocol
- Use [ha-sync SQLite extension](https://github.com/litesql/ha-sync) to create live local read replicas
- Change Data Capture (CDC)

## Installation

- Download from [releases page](https://github.com/litesql/ha/releases).

### Install from source

```sh
git clone https://github.com/litesql/ha.git
cd ha
go install
```

## Usage

1. Start the first ha node (-m flag if you want to use in-memory)

```sh
ha -m
```

2. Start an another ha node

```sh
ha -m --port 8081 --pg-port 5433 --nats-port 0 --replication-url nats://localhost:4222
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

### Loading an database to memory

```sh
ha -m mydatabase.db
```

### Store database in disk

```sh
ha file:mydatabase.db?_journal=WAL&_busy_timeout=500
```

### Backup database

```sh
curl -O -J http://localhost:8080
```

### Take a snapshot and save on NATS Object Store

```sh
curl -X POST http://localhost:8080/snapshot
```

### Get latest snapshot from NATS Object Store

```sh
curl -O -J http://localhost:8080/snapshot
```

## Local Read Replicas

- Use [ha-sync](https://github.com/litesql/ha-sync) SQLite extension to create local embedded replicas from a remote HA database.

## Using Docker

```sh
docker run --name ha \
-e HA_MEMORY=true \
-p 5432:5432 -p 8080:8080 -p 4222:4222 \
ghcr.io/litesql/ha:latest
```

- Set up a volume at /data to store the NATS streams state.

## PostgreSQL Wire Protocol

- You can use any PostgreSQL driver to connect to ha.
- The SQLite parser engine will proccess the commands.
- PostgreSQL functions (and visual editors like pgadmim, dbeaver, etc) are not supported.

## HTTP API

- Using bind parameters:

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

- Multiple commands (one transaction)

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

## Replication

- You can write to any server
- Uses embedded or external NATS JetStream cluster
- NATS JetStream guarantees "at-least-once" message delivery
- All DML (INSERT, UPDATE, DELETE) operations are idempotent
- Last writer wins
- DDL commands are replicated (since v0.0.7)

### CDC message format

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

### Replication limitations

- Tables WITHOUT ROWID are not replicated
- The replication not invoked when conflicting rows are deleted because of an ON CONFLICT REPLACE clause. 
- Nor is the replication invoked when rows are deleted using the [truncate optimization](https://sqlite.org/lang_delete.html#truncateopt).
- Use idempotents DDL commands (CREATE IF NOT EXISTS and DROP IF EXISTS)