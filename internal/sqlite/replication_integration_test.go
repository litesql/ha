package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/litesql/go-ha"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
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

func startTestNATSCluster(t *testing.T, size int) (*nats.Conn, []*natsserver.Server) {
	t.Helper()
	routes := make([]*url.URL, 0, size)
	clusterPorts := make([]int, size)
	for i := range clusterPorts {
		clusterPorts[i] = freePort(t)
		routes = append(routes, &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", clusterPorts[i])})
	}

	servers := make([]*natsserver.Server, size)
	for i := range servers {
		opts := natsserver.Options{
			ServerName: fmt.Sprintf("node%d", i+1),
			Host:       "127.0.0.1",
			Port:       -1,
			JetStream:  true,
			StoreDir:   t.TempDir(),
			NoLog:      true,
			NoSigs:     true,
			Cluster: natsserver.ClusterOpts{
				Name: "test-cluster",
				Host: "127.0.0.1",
				Port: clusterPorts[i],
			},
			Routes: routes,
		}
		ns, err := natsserver.NewServer(&opts)
		if err != nil {
			t.Fatalf("new nats cluster node %d: %v", i+1, err)
		}
		go ns.Start()
		servers[i] = ns
	}

	for _, ns := range servers {
		if !ns.ReadyForConnections(20 * time.Second) {
			t.Fatalf("nats cluster node %q did not become ready", ns.Name())
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, ns := range servers {
			if !ns.JetStreamEnabled() || !ns.JetStreamIsCurrent() {
				ready = false
				break
			}
			if ns.JetStreamIsLeader() && len(ns.JetStreamClusterPeers()) < len(servers) {
				ready = false
				break
			}
		}
		if ready && metaLeader(servers) != nil {
			nc, err := nats.Connect(servers[0].ClientURL())
			if err != nil {
				t.Fatalf("connect to cluster: %v", err)
			}
			t.Cleanup(nc.Close)
			return nc, servers
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, ns := range servers {
		ns.Shutdown()
	}
	t.Fatal("nats jetstream cluster is not ready")
	return nil, nil
}

func metaLeader(servers []*natsserver.Server) *natsserver.Server {
	for _, ns := range servers {
		if ns.JetStreamIsLeader() {
			return ns
		}
	}
	return nil
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
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

func waitForLeader(t *testing.T, providers ...ha.LeaderProvider) ha.LeaderProvider {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, provider := range providers {
			if provider != nil && provider.IsLeader() {
				return provider
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for a leader among %d provider(s)", len(providers))
	return nil
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

func TestLeaderElectionSelectsExactLeader(t *testing.T) {
	_, servers := startTestNATSCluster(t, 2)
	conns := make([]*nats.Conn, 0, len(servers))
	for _, s := range servers {
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("connect to nats server %q: %v", s.Name(), err)
		}
		conns = append(conns, nc)
	}
	t.Cleanup(func() {
		for _, nc := range conns {
			nc.Close()
		}
		for _, ns := range servers {
			ns.Shutdown()
		}
	})

	providers := make([]ha.LeaderProvider, 0, len(servers))
	for i, nc := range conns {
		dl, err := ha.StartLeaderElection(
			context.Background(),
			fmt.Sprintf("127.0.0.1:%d", 5200+i),
			nc,
			"leader.election.test",
			2,
			2,
			filepath.Join(t.TempDir(), fmt.Sprintf("ha-election-%d.log", i+1)),
		)
		if err != nil {
			t.Fatalf("start leader election for node %d: %v", i+1, err)
		}
		providers = append(providers, dl)
	}

	leader := waitForLeader(t, providers...)
	if leader == nil {
		t.Fatal("expected a leader to be elected")
	}
	if leader.RedirectTarget() == "" {
		t.Fatal("elected leader must expose a redirect target")
	}

	leaderCount := 0
	for _, provider := range providers {
		if provider.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly one leader, got %d", leaderCount)
	}
}

