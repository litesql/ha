## Oracle to HA SQLite Cluster Replication with Debezium

This example demonstrates real-time data replication from an Oracle database to an HA SQLite cluster using Debezium CDC (Change Data Capture). The setup showcases proxying, replication, and high availability in action.

### Prerequisites

- Docker and Docker Compose
- `curl` for API testing
- `ha` CLI tool installed locally

```sh
go install github.com/litesql/ha@latest
```

### Step 1: Start Services

Launch all services (Oracle database, Kafka, Debezium, and HA SQLite):

```sh
docker compose up -d
```

Monitor the Oracle database startup logs:

```sh
docker logs -f oracle-26ai
```

Wait for the "Database is ready" message:

```
#########################
DATABASE IS READY TO USE!
#########################
```

Once this message appears, you can proceed to the next step.

### Step 2: Register Debezium Oracle Connector

Register the Oracle connector with Kafka Connect to enable CDC:

```sh
curl -v -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d "@oracle-connector.json"
```

Verify the connector is registered and running:

```sh
curl http://localhost:8083/connectors/oracle-connector/status
```

You should see the connector in `RUNNING` state.

### Step 3: Insert Data via HA Cluster

Connect to the HA SQLite cluster proxy and insert test data:

```sh
ha -r http://localhost:8080
```

In the HA CLI, run:

```sql
INSERT INTO users(name, email) VALUES('Test User', 'test@user');
```

This query will be redirect to Oracle and replicated back to HA SQLite.

### Step 4: Verify Data in Oracle

Connect to the Oracle database and verify the data has been saved:

```sh
docker exec -it oracle-26ai sqlplus sys/Oracle123@FREEPDB1 as sysdba
```

In the SQL prompt, run:

```sql
SELECT * FROM users;
```

### Step 5: Test Bi-directional Sync (Optional)

You can update data directly in Oracle and observe it being replicated back to the HA SQLite cluster:

```sql
UPDATE users SET email = 'updated@user.com' WHERE name = 'Test User';
```

Then verify the changes are reflected in the HA cluster by re-running the SELECT query through the `ha` CLI.

