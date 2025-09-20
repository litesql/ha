package pgwire_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/litesql/ha/internal/pgwire"
	"github.com/litesql/ha/internal/sqlite"
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
					complete: "SELECT 0",
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
					complete: "SELECT 0",
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

	for name, tc := range tt {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatalf("failed to create listener: %v", err)
			}
			defer listener.Close()
			sqlite.RegisterDriver(nil, "", nil)
			db, err := sql.Open("sqlite3-ha", "file:test.db?vfs=memdb")
			if err != nil {
				t.Fatalf("open database: %v", err)
			}
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
