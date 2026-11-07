package postgresql_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/litesql/go-ha"
	"github.com/litesql/ha/internal/sqlite"
	"github.com/litesql/ha/internal/wire/postgresql"
)

func TestMain(m *testing.M) {
	err := sqlite.Load(context.TODO(), "file:/test.db?vfs=memdb", sqlite.LoadConfig{
		MemDB:    true,
		MaxConns: 10,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load sqlite databases: %v\n", err)
		os.Exit(1)
	}
	defer ha.Shutdown()
	os.Exit(m.Run())
}

func TestServe(t *testing.T) {
	type command struct {
		sql      string
		args     []any
		wantRows [][]any
		complete string
	}

	type testCase struct {
		commands []command
		expect   error
	}

	tt := map[string]testCase{
		"crud": {
			commands: []command{
				{
					sql:      "CREATE TABLE user(ID INT, Name TEXT)",
					complete: "CREATE TABLE",
				},
				{
					sql:      "INSERT INTO user VALUES(1, 'User 1')",
					complete: "INSERT 0 1",
				},
				{
					sql:      "SELECT name FROM user",
					wantRows: [][]any{{"User 1"}},
					complete: "SELECT 1",
				},
				{
					sql:      "UPDATE user SET name = 'User 2' WHERE id = 2",
					complete: "UPDATE 0",
				},
				{
					sql:      "SELECT name FROM user",
					wantRows: [][]any{{"User 1"}},
					complete: "SELECT 1",
				},
				{
					sql:      "UPDATE user SET name = 'User one' WHERE id = 1",
					complete: "UPDATE 1",
				},
				{
					sql:      "SELECT name FROM user WHERE id = 1",
					wantRows: [][]any{{"User one"}},
					complete: "SELECT 1",
				},
				{
					sql:      "DELETE FROM user WHERE id = 2",
					complete: "DELETE 0",
				},
				{
					sql:      "DELETE FROM user WHERE id = 1",
					complete: "DELETE 1",
				},
				{
					sql:      "SELECT name FROM user",
					complete: "SELECT 0",
				},
				{
					sql:      "INSERT INTO user VALUES(3, NULL)",
					complete: "INSERT 0 1",
				},
				{
					sql:      "SELECT id, name FROM user WHERE id = 3",
					wantRows: [][]any{{"3", nil}},
					complete: "SELECT 1",
				},
			},
			expect: nil,
		},
		"crud bind parameter": {
			commands: []command{
				{
					sql:      "CREATE TABLE user_bind(ID INT, Name TEXT)",
					complete: "CREATE TABLE",
				},
				{
					sql:      "INSERT INTO user_bind VALUES($1, $2)",
					args:     []any{1, "User 1"},
					complete: "INSERT 0 1",
				},
				{
					sql:      "SELECT name FROM user_bind WHERE id = :id",
					args:     []any{1},
					wantRows: [][]any{{"User 1"}},
					complete: "SELECT 1",
				},
			},
			expect: nil,
		},
	}

	for name, tc := range tt {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatalf("failed to create listener: %v", err)
			}
			defer listener.Close()

			server, err := postgresql.NewServer(postgresql.Config{
				User: "test", Pass: "test",
			})
			if err != nil {
				t.Fatalf("failed to create server: %v", err)
			}

			go func() {
				if err := server.Serve(listener); err != nil {
					t.Errorf("server failed: %v", err)
				}
			}()
			port := listener.Addr().(*net.TCPAddr).Port
			connString := fmt.Sprintf("postgresql://%s:%s@localhost:%d/ha", "test", "test", port)

			pgPool, err := pgxpool.New(context.TODO(), connString)
			if err != nil {
				t.Fatalf("failed to connect to database: %v", err)
			}
			defer pgPool.Close()

			for _, cmd := range tc.commands {
				if len(cmd.wantRows) > 0 {
					rows, err := pgPool.Query(context.TODO(), cmd.sql, cmd.args...)
					if err != nil {
						t.Errorf("failed to execute query %q: %v", cmd.sql, err)
						return
					}

					var gotRows [][]any
					for rows.Next() {
						values, err := rows.Values()
						if err != nil {
							t.Errorf("failed to scan row: %v", err)
							return
						}
						gotRows = append(gotRows, values)
					}
					if err := rows.Err(); err != nil {
						t.Errorf("rows error: %v", err)
						return
					}
					if len(gotRows) != len(cmd.wantRows) {
						t.Errorf("unexpected number of rows: want %d got %d", len(cmd.wantRows), len(gotRows))
						return
					}
					for rowIndex, row := range gotRows {
						for cellIndex, cell := range row {
							wantCell := cmd.wantRows[rowIndex][cellIndex]
							if (cell == nil) != (wantCell == nil) {
								t.Errorf("unexpected nil mismatch at row %d column %d: want %#v got %#v", rowIndex, cellIndex, wantCell, cell)
								return
							}
							if fmt.Sprint(cell) != fmt.Sprint(wantCell) {
								t.Errorf("unexpected cell at row %d column %d: want %v got %v", rowIndex, cellIndex, wantCell, fmt.Sprint(cell))
								return
							}
						}
					}
					continue
				}
				commandTag, err := pgPool.Exec(context.TODO(), cmd.sql, cmd.args...)
				if err != nil {
					t.Errorf("failed to execute command %q: %v", cmd.sql, err)
					return
				}
				if commandTag.String() != cmd.complete {
					t.Errorf("unexpected command tag: want %q got %q", cmd.complete, commandTag.String())
					return
				}
			}
		})
	}
}

func TestTransaction(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	server, err := postgresql.NewServer(postgresql.Config{
		User: "test", Pass: "test",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer server.Shutdown(context.TODO())

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("server failed: %v", err)
		}
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	connString := fmt.Sprintf("postgresql://%s:%s@localhost:%d/ha", "test", "test", port)

	pgPool, err := pgxpool.New(context.TODO(), connString)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pgPool.Close()

	_, err = pgPool.Exec(context.TODO(), "CREATE TABLE user_transaction(ID INT, Name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tx, err := pgPool.BeginTx(context.TODO(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(context.TODO())

	_, err = tx.Exec(context.TODO(), "INSERT INTO user_transaction VALUES(1, 'User 1')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	var name string
	err = tx.QueryRow(context.TODO(), "SELECT name FROM user_transaction WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("failed to select row: %v", err)
		return
	}
	if name != "User 1" {
		t.Fatalf("unexpected name: want %q got %q", "User 1", name)
	}

	err = tx.Commit(context.TODO())
	if err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	err = pgPool.QueryRow(context.TODO(), "SELECT name FROM user_transaction WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("failed to select row after commit: %v", err)
	}
	if name != "User 1" {
		t.Fatalf("unexpected name after commit: want %q got %q", "User 1", name)
	}

	tx2, err := pgPool.BeginTx(context.TODO(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		t.Fatalf("failed to begin transaction 2: %v", err)
	}
	defer tx2.Rollback(context.TODO())

	newUserName := "changed User"
	_, err = tx2.Exec(context.TODO(), "UPDATE user_transaction SET name = $1 WHERE id = $2", newUserName, 1)
	if err != nil {
		t.Fatalf("failed to update row: %v", err)
	}

	var name2 string
	// change query to ignore cached value
	err = tx2.QueryRow(context.TODO(), "SELECT name FROM user_transaction WHERE id = 1 LIMIT 1").Scan(&name2)
	if err != nil {
		t.Fatalf("failed to select row: %v", err)
	}
	if name2 != newUserName {
		t.Fatalf("unexpected name: want %q got %q", newUserName, name2)
	}

	err = tx2.Rollback(context.TODO())
	if err != nil {
		t.Fatalf("failed to rollback transaction 2: %v", err)
	}

	name2 = ""
	err = pgPool.QueryRow(context.TODO(), "SELECT name FROM user_transaction WHERE id = 1").Scan(&name2)
	if err != nil {
		t.Fatalf("failed to select row: %v", err)
	}
	if name2 != name {
		t.Fatalf("unexpected name: want %q got %q", name, name2)
	}
}

func TestTransactionPrepared(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	server, err := postgresql.NewServer(postgresql.Config{
		User: "test", Pass: "test",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer server.Shutdown(context.TODO())

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("server failed: %v", err)
		}
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	connString := fmt.Sprintf("postgresql://%s:%s@localhost:%d/ha", "test", "test", port)

	pgPool, err := pgxpool.New(context.TODO(), connString)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pgPool.Close()

	_, err = pgPool.Exec(context.TODO(), "CREATE TABLE users(ID INT, Name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tx, err := pgPool.BeginTx(context.TODO(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(context.TODO())

	_, err = tx.Exec(context.TODO(), "INSERT INTO users VALUES(1, 'User 1')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	var name string
	err = tx.QueryRow(context.TODO(), "SELECT name FROM users WHERE id = $1", 1).Scan(&name)
	if err != nil {
		t.Fatalf("failed to select row: %v", err)
	}
	if name != "User 1" {
		t.Fatalf("unexpected name: want %q got %q", "User 1", name)
	}

	err = tx.Commit(context.TODO())
	if err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	err = pgPool.QueryRow(context.TODO(), "SELECT name FROM users WHERE id = $1", 1).Scan(&name)
	if err != nil {
		t.Fatalf("failed to select row after commit: %v", err)
	}
	if name != "User 1" {
		t.Fatalf("unexpected name after commit: want %q got %q", "User 1", name)
	}

	tx2, err := pgPool.BeginTx(context.TODO(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		t.Fatalf("failed to begin transaction 2: %v", err)
	}
	defer tx2.Rollback(context.TODO())

	newUserName := "changed User"
	_, err = tx2.Exec(context.TODO(), "UPDATE users SET name = $1 WHERE id = $2", newUserName, 1)
	if err != nil {
		t.Fatalf("failed to update row: %v", err)
	}

	var name2 string
	// Reuse the exact same prepared statement across transactions
	err = tx2.QueryRow(context.TODO(), "SELECT name FROM users WHERE id = $1", 1).Scan(&name2)
	if err != nil {
		t.Fatalf("failed to select row: %v", err)
	}
	if name2 != newUserName {
		t.Fatalf("unexpected name: want %q got %q", newUserName, name2)
	}

	err = tx2.Rollback(context.TODO())
	if err != nil {
		t.Fatalf("failed to rollback transaction 2: %v", err)
	}

	name2 = ""
	err = pgPool.QueryRow(context.TODO(), "SELECT name FROM users WHERE id = 1").Scan(&name2)
	if err != nil {
		t.Fatalf("failed to select row: %v", err)
	}
	if name2 != name {
		t.Fatalf("unexpected name: want %q got %q", name, name2)
	}
}

func TestHandshakeDatabaseAndSetUnsetDatabase(t *testing.T) {
	ctx := context.Background()
	err := sqlite.Load(ctx, "file:/second.db?vfs=memdb", sqlite.LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	})
	if err != nil {
		t.Fatalf("failed to load second.db: %v", err)
	}
	defer sqlite.Drop(ctx, "second.db")

	secondDB, err := sqlite.DB("second.db")
	if err != nil {
		t.Fatalf("failed to get second.db: %v", err)
	}
	if _, err := secondDB.ExecContext(ctx, "CREATE TABLE only_in_second(val TEXT); INSERT INTO only_in_second VALUES('from-second')"); err != nil {
		t.Fatalf("failed to setup second.db: %v", err)
	}

	defaultDB, err := sqlite.DB("")
	if err != nil {
		t.Fatalf("failed to get default db: %v", err)
	}
	if _, err := defaultDB.ExecContext(ctx, "CREATE TABLE only_in_default(val TEXT); INSERT INTO only_in_default VALUES('from-default')"); err != nil {
		t.Fatalf("failed to setup default db: %v", err)
	}

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	server, err := postgresql.NewServer(postgresql.Config{
		User: "test", Pass: "test",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer server.Shutdown(ctx)

	go func() {
		_ = server.Serve(listener)
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	// Connect specifying "second" (without .db extension) in handshake URL
	connString := fmt.Sprintf("postgresql://test:test@localhost:%d/second", port)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	var val string
	if err := conn.QueryRow(ctx, "SELECT val FROM only_in_second").Scan(&val); err != nil {
		t.Fatalf("expected handshake to select second.db, got error: %v", err)
	}
	if val != "from-second" {
		t.Fatalf("want from-second, got %q", val)
	}

	// UNSET DATABASE should revert to default database
	if _, err := conn.Exec(ctx, "UNSET DATABASE;"); err != nil {
		t.Fatalf("UNSET DATABASE failed: %v", err)
	}
	if err := conn.QueryRow(ctx, "SELECT val FROM only_in_default").Scan(&val); err != nil {
		t.Fatalf("expected default db after UNSET DATABASE, got error: %v", err)
	}
	if val != "from-default" {
		t.Fatalf("want from-default, got %q", val)
	}

	// SET DATABASE TO second (without .db) should switch back to second.db
	if _, err := conn.Exec(ctx, "SET DATABASE TO second"); err != nil {
		t.Fatalf("SET DATABASE TO second failed: %v", err)
	}
	if err := conn.QueryRow(ctx, "SELECT val FROM only_in_second").Scan(&val); err != nil {
		t.Fatalf("expected second.db after SET DATABASE TO second, got error: %v", err)
	}
}
