# ha

Highly available leaderless SQLite cluster powered by embedded NATS JetStream server.

## Installation

- Download from [releases page](https://github.com/litesql/ha/releases).

### Install from source

```sh
go install github.com/litesql/ha@latest
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
curl -d '[{"sql": "INSERT INTO users(name) VALUES('\''HA user'\'')}"]' \
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
curl http://localhost:8080 -o mybackup.db
```
