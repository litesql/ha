package pgwire_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/litesql/go-ha"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
	"github.com/litesql/ha/internal/pgwire"
)

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
			},
			expect: nil,
		},
		"crud bind parameter": {
			commands: []command{
				{
					sql:      "CREATE TABLE user(ID INT, Name TEXT)",
					complete: "CREATE TABLE",
				},
				{
					sql:      "INSERT INTO user VALUES($1, $2)",
					args:     []any{1, "User 1"},
					complete: "INSERT 0 1",
				},
				{
					sql:      "SELECT name FROM user WHERE id = :id",
					args:     []any{1},
					wantRows: [][]any{{"User 1"}},
					complete: "SELECT 1",
				},
			},
			expect: nil,
		},
	}

	pub := new(fakePublisher)
	connector, err := sqlite3ha.NewConnector("file:/test.db?vfs=memdb", ha.WithCDCPublisher(pub))
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	for name, tc := range tt {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatalf("failed to create listener: %v", err)
			}
			defer listener.Close()
			db := sql.OpenDB(connector)
			defer db.Close()

			server, err := pgwire.NewServer(pgwire.Config{
				User: "test", Pass: "test",
			}, db)
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
							if fmt.Sprint(cell) != fmt.Sprint(cmd.wantRows[rowIndex][cellIndex]) {
								t.Errorf("unexpected cell at row %d column %d: want %v got %v", rowIndex, cellIndex, cmd.wantRows[rowIndex][cellIndex], fmt.Sprint(cell))
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
	pub := new(fakePublisher)
	connector, err := sqlite3ha.NewConnector("file:/test.db?vfs=memdb", ha.WithCDCPublisher(pub))
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	db := sql.OpenDB(connector)
	defer db.Close()

	server, err := pgwire.NewServer(pgwire.Config{
		User: "test", Pass: "test",
	}, db)
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

	_, err = pgPool.Exec(context.TODO(), "CREATE TABLE user(ID INT, Name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tx, err := pgPool.BeginTx(context.TODO(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(context.TODO())

	_, err = tx.Exec(context.TODO(), "INSERT INTO user VALUES(1, 'User 1')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	var name string
	err = tx.QueryRow(context.TODO(), "SELECT name FROM user WHERE id = 1").Scan(&name)
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

	err = pgPool.QueryRow(context.TODO(), "SELECT name FROM user WHERE id = 1").Scan(&name)
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
	_, err = tx2.Exec(context.TODO(), "UPDATE user SET name = $1 WHERE id = $2", newUserName, 1)
	if err != nil {
		t.Fatalf("failed to update row: %v", err)
	}

	var name2 string
	// change query to ignore cached value
	err = tx2.QueryRow(context.TODO(), "SELECT name FROM user WHERE id = 1 LIMIT 1").Scan(&name2)
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
	err = pgPool.QueryRow(context.TODO(), "SELECT name FROM user WHERE id = 1").Scan(&name2)
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
	pub := new(fakePublisher)
	connector, err := sqlite3ha.NewConnector("file:/test.db?vfs=memdb", ha.WithCDCPublisher(pub))
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	db := sql.OpenDB(connector)
	defer db.Close()

	server, err := pgwire.NewServer(pgwire.Config{
		User: "test", Pass: "test",
	}, db)
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

	_, err = pgPool.Exec(context.TODO(), "CREATE TABLE user(ID INT, Name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tx, err := pgPool.BeginTx(context.TODO(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(context.TODO())

	_, err = tx.Exec(context.TODO(), "INSERT INTO user VALUES(1, 'User 1')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	var name string
	err = tx.QueryRow(context.TODO(), "SELECT name FROM user WHERE id = $1", 1).Scan(&name)
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

	err = pgPool.QueryRow(context.TODO(), "SELECT name FROM user WHERE id = $1 LIMIT 1", 1).Scan(&name)
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
	_, err = tx2.Exec(context.TODO(), "UPDATE user SET name = $1 WHERE id = $2", newUserName, 1)
	if err != nil {
		t.Fatalf("failed to update row: %v", err)
	}

	var name2 string
	// change query to ignore cached value
	err = tx2.QueryRow(context.TODO(), "SELECT name FROM user WHERE id = $1 LIMIT $2", 1, 2).Scan(&name2)
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
	err = pgPool.QueryRow(context.TODO(), "SELECT name FROM user WHERE id = 1").Scan(&name2)
	if err != nil {
		t.Fatalf("failed to select row: %v", err)
	}
	if name2 != name {
		t.Fatalf("unexpected name: want %q got %q", name, name2)
	}
}

type fakePublisher struct {
	err     error
	changes []ha.Change
}

func (f *fakePublisher) Publish(cs *ha.ChangeSet) error {
	f.changes = cs.Changes
	return f.err
}
