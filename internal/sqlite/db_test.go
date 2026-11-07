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
