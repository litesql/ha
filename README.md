# ha

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![LiberaPay](https://liberapay.com/assets/widgets/donate.svg)](https://liberapay.com/walterwanderley/donate)
[![receives](https://img.shields.io/liberapay/receives/walterwanderley.svg?logo=liberapay)](https://liberapay.com/walterwanderley/donate)
[![patrons](https://img.shields.io/liberapay/patrons/walterwanderley.svg?logo=liberapay)](https://liberapay.com/walterwanderley/donate)

![](ha.png)


## Highly available SQLite cluster 

Powered by an embedded NATS JetStream server.

## Features

- 🔌 Connect via HTTP API, MySQL or PostgreSQL Wire Protocol  
- 🔁 Replicate data using embedded or external NATS server
- 📝 Create live local **read/write** replicas with [go-ha database/sql driver](https://github.com/litesql/go-ha)
- 📚 Create live local read replicas with [ha-sync SQLite extension](https://github.com/litesql/ha-sync)
- 🔄 Supports Change Data Capture (CDC)
- ⚙️ Configure a leader-based or leaderless cluster (with custom strategies for resolving replication data conflicts)
- 📚 Execute cross-shard queries sing SQL hint /*+ db=DSN */ using regexp
- 📖 Full documentation: [https://litesql.github.io/ha/](https://litesql.github.io/ha/)

## 🚀 Getting Started

Download and install the [latest release](https://github.com/litesql/ha/releases/latest)

### 1. Start the first **ha** instance

```sh
mkdir db1
ha -n node1 "file:db1/mydatabase.db?_journal=WAL&_busy_timeout=5000"
```

This command launches:

- An embedded NATS server on port 4222
- A MySQL Wire Protocol compatible server on port 3306
- A PostgreSQL Wire Protocol compatible server on port 5432
- An HTTP API server on port 8080

### 2. Start a second **ha** instance

```sh
mkdir db2
ha -n node2 -p 8081 --nats-port 0 --pg-port 5433 --mysql-port 3307 --replication-url nats://localhost:4222 "file:db2/mydatabase.db?_journal=WAL&_busy_timeout=5000"
```

This command starts:

- A PostgreSQL Wire Protocol server on port 5433
- A MySQL Wire Protocol server on port 3307
- An HTTP API server on port 8081.

It connects to the previously launched embedded NATS server for replication.

### 3. Connect using a PostgreSQL client

```sh
PGPASSWORD=ha psql -h localhost -U ha
```

Create and populate a table:

```sql
CREATE TABLE users(ID INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users(name) VALUES('HA user');
```

### 4. Verify replication on the second instance

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
+-----------------------+
| Database              |
+-----------------------+
|ha.db                  |
+-----------------------+
1 row in set (0,000 sec)

MySQL [(none)]> use ha.db

MySQL [ha.db]> select * from users;
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