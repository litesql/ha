## MySQL to HA SQLite Cluster Replication

This example demonstrates real-time data replication from a MySQL database to an HA SQLite cluster using Binary Log file position replication. The setup showcases proxying, replication, and high availability in action.

### Prerequisites

- Docker and Docker Compose
- `ha` CLI tool installed locally

```sh
go install github.com/litesql/ha@latest
```

### Step 1: Start Services

Launch all services (MySQL database and HA SQLite):

```sh
docker compose up -d
```

### Step 2: Insert Data via HA Cluster

Connect to the HA SQLite cluster proxy and insert test data:

```sh
ha -r http://localhost:8080
```

In the HA CLI, run:

```sql
INSERT INTO users(name, email) VALUES('Test User', 'test@user');
```

Alternatively, use the HTTP API:

```sh
curl http://localhost:8080/query \
-d '
{
  "sql": "INSERT INTO users(name, email) VALUES(?, ?)", 
  "params": {
    "name": "Test User", 
    "email": "test@user"
  }
}'
```

This query will be redirect to MySQL and replicated back to HA SQLite.

### Step 3: Verify Data in MySQL

Connect to the MySQL database and verify the data has been saved:

```sh
docker compose exec -it mysql bash -c 'mysql -u root -ppassword db'
```

In the SQL prompt, run:

```sql
SELECT * FROM users;
```

### Step 4: Test Bi-directional Sync

You can update data directly in MySQL and observe it being replicated back to the HA SQLite cluster:

```sql
UPDATE users SET email = 'updated@user.com' WHERE name = 'Test User';
```

Then verify the changes are reflected in the HA cluster by re-running the SELECT query through the `ha` CLI.

