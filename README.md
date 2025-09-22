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

1. Create an sqlite database:

```sh
sqlite3 mydatabase.db 'CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);'
```

2. Start the first ha node 

```sh
ha mydatabase.db
```

3. Start an another ha node

```sh
ha --port 8081 --pg-port 5433 --nats-port 0 --replication-url nats://localhost:4222 mydatabase.db
```

4. Insert some data using HTTP client

```sh
curl -d '[{"sql": "INSERT INTO users(name) VALUES('\''HA user'\'')"}]' \
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

### Backup database

```sh
curl -O -J http://localhost:8080
```

## Local Read Replicas

- Use [ha-sync](https://github.com/litesql/ha-sync) SQLite extension to create local embedded replicas from a remote HA database.

## Using Docker

1. Create an initial sqlite database:

```sh
sqlite3 mydatabase.db 'CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);'
```

2. Start docker

```sh
docker run --name ha -e HA_ARGS=/tmp/ha.db \
-v $(pwd)/mydatabase.db:/tmp/ha.db \
-p 5432:5432 -p 8080:8080 ghcr.io/litesql/ha:latest
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
- All operations are idempotent
- Last writer wins

### CDC message format

```
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
- DDL (CREATE, ALTER, DROP) commands are not replicated
- Truncate table commands (DELETE without WHERE) are not replicated