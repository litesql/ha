package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/litesql/go-ha"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

func startTestNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("nats server did not become ready")
	}
	t.Cleanup(func() {
		s.Shutdown()
	})
	return s
}

func waitForRowCount(t *testing.T, db *sql.DB, query string, expected int) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err == nil && count == expected {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for query %q to return %d", query, expected)
}

func waitForNamedValue(t *testing.T, db *sql.DB, query string, expected string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var got string
		if err := db.QueryRowContext(ctx, query).Scan(&got); err == nil && got == expected {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for query %q to return %q", query, expected)
}

func TestReplicationAcrossNodes(t *testing.T) {
	ctx := context.Background()
	nats := startTestNATSServer(t)
	t.Cleanup(func() { Shutdown() })

	leaderOpts := []ha.Option{
		ha.WithName("node1"),
		ha.WithReplicationID("integration-replication"),
		ha.WithReplicationURL(nats.ClientURL()),
		ha.WithReplicas(1),
	}
	followerOpts := []ha.Option{
		ha.WithName("node2"),
		ha.WithReplicationID("integration-replication"),
		ha.WithReplicationURL(nats.ClientURL()),
		ha.WithReplicas(1),
	}

	if err := Load(ctx, "file:/replication_leader.db?vfs=memdb", LoadConfig{MemDB: true, MaxConns: 5, Options: leaderOpts}); err != nil {
		t.Fatalf("load leader: %v", err)
	}
	leader, err := DB("replication_leader.db")
	if err != nil {
		t.Fatalf("open leader db: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table on leader: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "INSERT INTO users(name) VALUES ('alice'), ('bob')"); err != nil {
		t.Fatalf("insert rows on leader: %v", err)
	}

	if err := Load(ctx, "file:/replication_follower.db?vfs=memdb", LoadConfig{MemDB: true, MaxConns: 5, Options: followerOpts}); err != nil {
		t.Fatalf("load follower: %v", err)
	}
	follower, err := DB("replication_follower.db")
	if err != nil {
		t.Fatalf("open follower db: %v", err)
	}

	waitForRowCount(t, follower, "SELECT COUNT(*) FROM users", 2)

	var names []string
	rows, err := follower.QueryContext(ctx, "SELECT name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query follower rows: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration: %v", err)
	}
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected replicated rows: %#v", names)
	}
}

func TestReplicationSucceedsForTransactionalBatch(t *testing.T) {
	ctx := context.Background()
	nats := startTestNATSServer(t)
	t.Cleanup(func() { Shutdown() })

	leaderOpts := []ha.Option{
		ha.WithName("txn-node1"),
		ha.WithReplicationID("integration-transactional-replication"),
		ha.WithReplicationURL(nats.ClientURL()),
		ha.WithReplicas(1),
	}
	followerOpts := []ha.Option{
		ha.WithName("txn-node2"),
		ha.WithReplicationID("integration-transactional-replication"),
		ha.WithReplicationURL(nats.ClientURL()),
		ha.WithReplicas(1),
	}

	if err := Load(ctx, "file:/replication_txn_leader.db?vfs=memdb", LoadConfig{MemDB: true, MaxConns: 5, Options: leaderOpts}); err != nil {
		t.Fatalf("load leader: %v", err)
	}
	leader, err := DB("replication_txn_leader.db")
	if err != nil {
		t.Fatalf("open leader db: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "CREATE TABLE events (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table on leader: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "INSERT INTO events(value) VALUES ('one')"); err != nil {
		t.Fatalf("insert first row: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "INSERT INTO events(value) VALUES ('two')"); err != nil {
		t.Fatalf("insert second row: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	if err := Load(ctx, "file:/replication_txn_follower.db?vfs=memdb", LoadConfig{MemDB: true, MaxConns: 5, Options: followerOpts}); err != nil {
		t.Fatalf("load follower: %v", err)
	}
	follower, err := DB("replication_txn_follower.db")
	if err != nil {
		t.Fatalf("open follower db: %v", err)
	}

	waitForRowCount(t, follower, "SELECT COUNT(*) FROM events", 2)
	var values []string
	rows, err := follower.QueryContext(ctx, "SELECT value FROM events ORDER BY id")
	if err != nil {
		t.Fatalf("query follower rows: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan value: %v", err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration: %v", err)
	}
	if len(values) != 2 || values[0] != "one" || values[1] != "two" {
		t.Fatalf("unexpected transactional rows: %#v", values)
	}
}

func TestReplicationPropagatesLaterWritesAfterFollowerStarts(t *testing.T) {
	ctx := context.Background()
	nats := startTestNATSServer(t)
	t.Cleanup(func() { Shutdown() })

	leaderOpts := []ha.Option{
		ha.WithName("late-node1"),
		ha.WithReplicationID("integration-late-write-replication"),
		ha.WithReplicationURL(nats.ClientURL()),
		ha.WithReplicas(1),
	}
	followerOpts := []ha.Option{
		ha.WithName("late-node2"),
		ha.WithReplicationID("integration-late-write-replication"),
		ha.WithReplicationURL(nats.ClientURL()),
		ha.WithReplicas(1),
	}

	if err := Load(ctx, "file:/late_replication_leader.db?vfs=memdb", LoadConfig{MemDB: true, MaxConns: 5, Options: leaderOpts}); err != nil {
		t.Fatalf("load leader: %v", err)
	}
	leader, err := DB("late_replication_leader.db")
	if err != nil {
		t.Fatalf("open leader db: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "CREATE TABLE checkpoints (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table on leader: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "INSERT INTO checkpoints(name) VALUES ('initial')"); err != nil {
		t.Fatalf("insert initial row: %v", err)
	}

	if err := Load(ctx, "file:/late_replication_follower.db?vfs=memdb", LoadConfig{MemDB: true, MaxConns: 5, Options: followerOpts}); err != nil {
		t.Fatalf("load follower: %v", err)
	}
	follower, err := DB("late_replication_follower.db")
	if err != nil {
		t.Fatalf("open follower db: %v", err)
	}
	waitForRowCount(t, follower, "SELECT COUNT(*) FROM checkpoints", 1)
	waitForNamedValue(t, follower, "SELECT name FROM checkpoints WHERE id = 1", "initial")

	if _, err := leader.ExecContext(ctx, "UPDATE checkpoints SET name = 'updated' WHERE id = 1"); err != nil {
		t.Fatalf("update leader row: %v", err)
	}
	if _, err := leader.ExecContext(ctx, "INSERT INTO checkpoints(name) VALUES ('later')"); err != nil {
		t.Fatalf("insert later row: %v", err)
	}

	waitForRowCount(t, follower, "SELECT COUNT(*) FROM checkpoints", 2)
	waitForNamedValue(t, follower, "SELECT name FROM checkpoints WHERE id = 1", "updated")
	waitForNamedValue(t, follower, "SELECT name FROM checkpoints WHERE id = 2", "later")
}
