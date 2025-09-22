# ha

Highly available leaderless SQLite cluster powered by embedded NATS JetStream server.

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