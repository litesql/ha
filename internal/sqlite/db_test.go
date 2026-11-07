package sqlite

import (
	"context"
	"database/sql"
	"testing"
)

func TestBuildCreateDSN(t *testing.T) {
	cfg := LoadConfig{
		Dir:              "/data/dbs",
		DefaultDSNParams: "_journal=WAL",
	}

	t.Run("valid simple name", func(t *testing.T) {
		got, err := BuildCreateDSN("app", cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := "file:/data/dbs/app.db?_journal=WAL"
		if got != want {
			t.Fatalf("want %q, got %q", want, got)
		}
	})

	t.Run("custom query params preserved", func(t *testing.T) {
		got, err := BuildCreateDSN("app.db?_timeout=5000", cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := "file:/data/dbs/app.db?_timeout=5000"
		if got != want {
			t.Fatalf("want %q, got %q", want, got)
		}
	})

	t.Run("rejects path traversal", func(t *testing.T) {
		badInputs := []string{
			"../evil.db",
			"../../etc/passwd",
			"foo/../../bar.db",
			"..",
			".",
			"/",
			"",
			"   ",
		}
		for _, in := range badInputs {
			if got, err := BuildCreateDSN(in, cfg); err == nil {
				t.Fatalf("expected error for input %q, got DSN %q", in, got)
			}
		}
	})

	t.Run("requires create-db-dir when not memdb", func(t *testing.T) {
		if _, err := BuildCreateDSN("app.db", LoadConfig{}); err == nil {
			t.Fatal("expected error when Dir is empty and MemDB is false")
		}
	})
}

func TestApplyProxiedChangeEscapesIdentifiers(t *testing.T) {
	ctx := context.Background()
	err := Load(ctx, "file:/proxied_escape_test.db?vfs=memdb", LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	})
	if err != nil {
		t.Fatalf("failed to load memdb: %v", err)
	}
	defer Drop(ctx, "proxied_escape_test.db")

	db, err := DB("proxied_escape_test.db")
	if err != nil {
		t.Fatalf("failed to get db: %v", err)
	}

	// Table and column names using SQL reserved keywords ("order", "group", "check")
	_, err = db.ExecContext(ctx, "CREATE TABLE `order` (`id` INT PRIMARY KEY, `group` TEXT, `check` TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	runTx := func(change proxiedChange) {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		defer tx.Rollback()
		if err := applyProxiedChange(ctx, tx, change); err != nil {
			t.Fatalf("applyProxiedChange(%s) failed: %v", change.Kind, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit tx: %v", err)
		}
	}

	// INSERT with reserved column names
	runTx(proxiedChange{
		Kind:         "INSERT",
		Table:        "order",
		ColumnNames:  []string{"id", "group", "check"},
		ColumnValues: []any{1, "admin", "ok"},
	})

	// UPDATE with reserved column and key names
	runTx(proxiedChange{
		Kind:         "UPDATE",
		Table:        "order",
		ColumnNames:  []string{"id", "group", "check"},
		ColumnValues: []any{1, "superadmin", "verified"},
		OldKeyNames:  []string{"id", "check"},
		OldKeyValues: []any{1, "ok"},
	})

	var grp string
	if err := db.QueryRowContext(ctx, "SELECT `group` FROM `order` WHERE `id` = 1").Scan(&grp); err != nil {
		t.Fatalf("query after update: %v", err)
	}
	if grp != "superadmin" {
		t.Fatalf("want superadmin, got %q", grp)
	}

	// DELETE with reserved column names
	runTx(proxiedChange{
		Kind:         "DELETE",
		Table:        "order",
		ColumnNames:  []string{"id", "group"},
		ColumnValues: []any{1, "superadmin"},
	})

	// Re-insert and TRUNCATE table with reserved name
	runTx(proxiedChange{
		Kind:         "INSERT",
		Table:        "order",
		ColumnNames:  []string{"id", "group", "check"},
		ColumnValues: []any{2, "guest", nil},
	})
	runTx(proxiedChange{
		Kind:  "TRUNCATE",
		Table: "order",
	})

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `order`").Scan(&count); err != nil {
		t.Fatalf("count after truncate: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after truncate, got %d", count)
	}
}

func TestIsQueryAndExecReturningCTEPragma(t *testing.T) {
	ctx := context.Background()
	err := Load(ctx, "file:/query_types_test.db?vfs=memdb", LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	})
	if err != nil {
		t.Fatalf("failed to load memdb: %v", err)
	}
	defer Drop(ctx, "query_types_test.db")

	db, err := DB("query_types_test.db")
	if err != nil {
		t.Fatalf("failed to get db: %v", err)
	}

	if _, err := Exec(ctx, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// INSERT ... RETURNING
	res, err := Exec(ctx, db, "INSERT INTO items (name) VALUES ('book') RETURNING id, name", nil)
	if err != nil {
		t.Fatalf("INSERT RETURNING failed: %v", err)
	}
	if res.NoReturning || len(res.Columns) != 2 || len(res.Rows) != 1 {
		t.Fatalf("unexpected RETURNING response: %+v", res)
	}

	// WITH (CTE) SELECT
	res, err = Exec(ctx, db, "WITH cte AS (SELECT id, name FROM items) SELECT name FROM cte", nil)
	if err != nil {
		t.Fatalf("CTE SELECT failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != "book" {
		t.Fatalf("unexpected CTE response: %+v", res)
	}

	// PRAGMA returning rows
	res, err = Exec(ctx, db, "PRAGMA table_info(items)", nil)
	if err != nil {
		t.Fatalf("PRAGMA table_info failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 columns in table_info, got %d", len(res.Rows))
	}
}

func TestDeserializeInMemoryDB(t *testing.T) {
	ctx := context.Background()
	err := Load(ctx, "file:../../testdata/example.db?vfs=memdb", LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	})
	if err != nil {
		t.Fatalf("failed to load example.db into memdb: %v", err)
	}
	defer Drop(ctx, "example.db")

	db, err := DB("example.db")
	if err != nil {
		t.Fatalf("failed to get example.db: %v", err)
	}

	res, err := Exec(ctx, db, "SELECT name FROM sqlite_schema WHERE type = 'table'", nil)
	if err != nil {
		t.Fatalf("failed to query deserialized db: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected tables in deserialized example.db")
	}
}

func TestGetArgsNamedWithDollarPrefixAndEmptyKey(t *testing.T) {
	ctx := context.Background()
	err := Load(ctx, "file:/args_test.db?vfs=memdb", LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	})
	if err != nil {
		t.Fatalf("failed to load memdb: %v", err)
	}
	defer Drop(ctx, "args_test.db")

	db, err := DB("args_test.db")
	if err != nil {
		t.Fatalf("failed to get db: %v", err)
	}

	if _, err := Exec(ctx, db, "CREATE TABLE t (id INT, name TEXT)", nil); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Named parameter prefixed with '$' and an empty key "" should not panic or lose "$name"
	_, err = Exec(ctx, db, "INSERT INTO t (id, name) VALUES (1, $name)", map[string]any{
		"$name": "Alice",
		"":      "ignored",
	})
	if err != nil {
		t.Fatalf("insert with $name failed: %v", err)
	}

	// Positional parameters "$1", "$2"
	res, err := Exec(ctx, db, "SELECT name FROM t WHERE id = $1 AND name = $2", map[string]any{
		"$1": 1,
		"$2": "Alice",
	})
	if err != nil {
		t.Fatalf("select with positional $1, $2 failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != "Alice" {
		t.Fatalf("unexpected rows: %+v", res.Rows)
	}
}



