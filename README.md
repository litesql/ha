# ha

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![LiberaPay](https://liberapay.com/assets/widgets/donate.svg)](https://liberapay.com/walterwanderley/donate)
[![receives](https://img.shields.io/liberapay/receives/walterwanderley.svg?logo=liberapay)](https://liberapay.com/walterwanderley/donate)
[![patrons](https://img.shields.io/liberapay/patrons/walterwanderley.svg?logo=liberapay)](https://liberapay.com/walterwanderley/donate)

![](ha.png)


## Highly available SQLite cluster 

Powered by an embedded NATS JetStream server.

## Features

- 🔌 Connect using HTTP API, [gRPC API](https://buf.build/litesql/sqlite-ha/sdks/main:grpc), [database/sql go driver](https://github.com/litesql/go-ha), [JDBC driver](https://github.com/litesql/jdbc-ha), MySQL or PostgreSQL Wire Protocol
- 🔁 Replicate data using embedded or external NATS server
- 📝 Create live local **read/write** replicas with [go-ha database/sql driver](https://github.com/litesql/go-ha)
- 📚 Create live local read replicas with [ha-sync SQLite extension](https://github.com/litesql/ha-sync)
- 🔄 Supports Change Data Capture (CDC)
- ⚙️ Configure a leader-based or leaderless cluster (with custom strategies for resolving replication data conflicts)
- 📚 Execute cross-shard queries using SQL hint /*+ db=DSN */ (where DSN is a regexp to DataSource name)
- 📖 Full documentation: [https://litesql.github.io/ha/](https://litesql.github.io/ha/)

## 🚀 Getting Started

Download and install the [latest release](https://github.com/litesql/ha/releases/latest)

### 1. Start the first **ha** instance

```sh
mkdir db1
ha -n node1 --pg-port 5432 --mysql-port 3306 "file:db1/mydatabase.db"
```

This command launches:

- An embedded NATS server on port 4222
- A MySQL Wire Protocol compatible server on port 3306
- A PostgreSQL Wire Protocol compatible server on port 5432
- An HTTP API server on port 8080

### 2. Start a second **ha** instance

```sh
mkdir db2
ha -n node2 --nats-port 0 -p 8081 --pg-port 5433 --mysql-port 3307 --replication-url nats://localhost:4222 "file:db2/mydatabase.db"
```

This command starts:

- A PostgreSQL Wire Protocol server on port 5433
- A MySQL Wire Protocol server on port 3307
- An HTTP API server on port 8081.

It connects to the previously launched embedded NATS server for replication.

### 3. Connect using a client

Special HA Client commands:

|Command|Description|
|-------|-----------|
|SHOW DATABASES; | List all databases |
|CREATE DATABASE dsn; | Create a new database |
|DROP DATABASE id; | Drop a database |
|SET DATABASE TO id; | Send commands to a specific database |
|UNSET DATABASE; | Use default database |
|UNDO n; | Undo the last n transactions. Where n is a number or a time duration |
|EXIT; | Quit client (ctrl+d) |


#### 3.1 HA Client

```sh
ha -r http://localhost:8080
```

#### 3.2. PostgreSQL Client

```sh
PGPASSWORD=ha psql -h localhost -U ha
```

#### 3.3 MySQL Client

```sh
mysql -h localhost --port 3306 -u ha
```

Create and populate a table:

```sql
CREATE TABLE users(ID INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users(name) VALUES('HA user');
```

### 4. Verify replication on the second instance

```sh
ha -r http://localhost:8081
```

Using postgresql client:

```sh
PGPASSWORD=ha psql -h localhost -U ha -p 5433
```

```sql
SELECT * FROM users;
 ID |  name   
----+---------
 1  | HA user

```

Using a mysql client:

```sh
mysql -h localhost --port 3307 -u ha

MySQL [(none)]> show databases;
+---------------+
| Database      |
+---------------+
| mydatabase.db |
+---------------+
1 row in set (0,000 sec)

MySQL [(none)]> use mydatabase.db

MySQL [mydatabase.db]> select * from users;
+----+---------+
| ID | name    |
+----+---------+
|  1 | HA user |
+----+---------+
1 row in set (0,000 sec)
```

### 5. Please refer to the complete documentation for the HTTP API

[HTTP API](https://litesql.github.io/ha/#5)

## 🛠️ License & Contributions

This project is open-source. Contributions, issues, and feature requests are welcome via [GitHub](https://github.com/litesql/ha).