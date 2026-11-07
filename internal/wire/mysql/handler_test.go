package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/litesql/go-ha"
	"github.com/litesql/ha/internal/sqlite"
)

func TestUseDB(t *testing.T) {
	dummyDB1 := &sql.DB{}
	dummyDB2 := &sql.DB{}
	dbs := map[string]*sql.DB{
		"mydb.db":  dummyDB1,
		"other.db": dummyDB2,
	}

	h := &Handler{
		dbProvider: func(name string) (*sql.DB, bool) {
			db, ok := dbs[name]
			return db, ok
		},
	}

	t.Run("exact match with extension", func(t *testing.T) {
		err := h.UseDB("mydb.db")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if h.db != dummyDB1 {
			t.Fatalf("expected h.db to be dummyDB1")
		}
	})

	t.Run("match without extension (fallback to .db)", func(t *testing.T) {
		err := h.UseDB("other")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if h.db != dummyDB2 {
			t.Fatalf("expected h.db to be dummyDB2")
		}
	})

	t.Run("quoted and backtick trimming", func(t *testing.T) {
		err := h.UseDB("`mydb`")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if h.db != dummyDB1 {
			t.Fatalf("expected h.db to be dummyDB1")
		}

		err = h.UseDB("'mydb'")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if h.db != dummyDB1 {
			t.Fatalf("expected h.db to be dummyDB1")
		}

		err = h.UseDB("\"other.db\"")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if h.db != dummyDB2 {
			t.Fatalf("expected h.db to be dummyDB2")
		}
	})

	t.Run("non-existent database", func(t *testing.T) {
		err := h.UseDB("nonexistent")
		if err == nil {
			t.Fatal("expected error for non-existent database, got nil")
		}
		var myErr *mysql.MyError
		if !errors.As(err, &myErr) {
			t.Fatalf("expected *mysql.MyError, got %T: %v", err, err)
		}
		if myErr.Code != mysql.ER_BAD_DB_ERROR {
			t.Fatalf("expected ER_BAD_DB_ERROR (%d), got %d", mysql.ER_BAD_DB_ERROR, myErr.Code)
		}
	})

	t.Run("empty database name", func(t *testing.T) {
		err := h.UseDB("  ")
		if err == nil {
			t.Fatal("expected error for empty database, got nil")
		}
		var myErr *mysql.MyError
		if !errors.As(err, &myErr) {
			t.Fatalf("expected *mysql.MyError, got %T: %v", err, err)
		}
		if myErr.Code != mysql.ER_NO_DB_ERROR {
			t.Fatalf("expected ER_NO_DB_ERROR (%d), got %d", mysql.ER_NO_DB_ERROR, myErr.Code)
		}
	})
}

func TestHandleQueryUse(t *testing.T) {
	dummyDB := &sql.DB{}
	dbs := map[string]*sql.DB{
		"mydb.db": dummyDB,
	}

	newHandler := func() *Handler {
		return &Handler{
			dbProvider: func(name string) (*sql.DB, bool) {
				db, ok := dbs[name]
				return db, ok
			},
		}
	}

	successQueries := []string{
		"USE mydb",
		"USE mydb;",
		"use mydb;",
		"use mydb;;",
		"Use mydb",
		"USE `mydb`",
		"USE `mydb`;",
		"USE `mydb.db`;",
		"USE 'mydb';",
		"USE \"mydb\";",
		"  USE   mydb   ;  ",
		"use\tmydb;",
		"USE mydb\n;",
	}

	for _, query := range successQueries {
		t.Run(query, func(t *testing.T) {
			h := newHandler()
			res, err := h.HandleQuery(query)
			if err != nil {
				t.Fatalf("query %q failed: %v", query, err)
			}
			if res != nil {
				t.Fatalf("expected nil result for USE command, got %v", res)
			}
			if h.db != dummyDB {
				t.Fatalf("expected h.db to be set after %q", query)
			}
		})
	}

	t.Run("empty USE commands", func(t *testing.T) {
		emptyQueries := []string{
			"USE",
			"USE;",
			"USE ;",
			"use   ;",
		}
		for _, query := range emptyQueries {
			h := newHandler()
			_, err := h.HandleQuery(query)
			if err == nil {
				t.Fatalf("expected error for %q, got nil", query)
			}
			var myErr *mysql.MyError
			if !errors.As(err, &myErr) {
				t.Fatalf("expected *mysql.MyError for %q, got %T: %v", query, err, err)
			}
			if myErr.Code != mysql.ER_NO_DB_ERROR {
				t.Fatalf("expected ER_NO_DB_ERROR (%d) for %q, got %d", mysql.ER_NO_DB_ERROR, query, myErr.Code)
			}
		}
	})

	t.Run("nonexistent database USE", func(t *testing.T) {
		h := newHandler()
		_, err := h.HandleQuery("USE unknown_db;")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var myErr *mysql.MyError
		if !errors.As(err, &myErr) {
			t.Fatalf("expected *mysql.MyError, got %T: %v", err, err)
		}
		if myErr.Code != mysql.ER_BAD_DB_ERROR {
			t.Fatalf("expected ER_BAD_DB_ERROR (%d), got %d", mysql.ER_BAD_DB_ERROR, myErr.Code)
		}
	})

	t.Run("non-USE commands starting with USE", func(t *testing.T) {
		h := newHandler()
		// Queries like USER or USER_TABLE are not USE commands and should not trigger UseDB.
		// Since h.db is nil and h.dbProvider("") doesn't find "", they will fail with database not selected or similar,
		// but NOT with ER_NO_DB_ERROR or ER_BAD_DB_ERROR.
		_, err := h.HandleQuery("USER;")
		if err != nil {
			var myErr *mysql.MyError
			if errors.As(err, &myErr) && (myErr.Code == mysql.ER_NO_DB_ERROR || myErr.Code == mysql.ER_BAD_DB_ERROR) {
				t.Fatalf("USER; should not be treated as USE command, got %v", myErr)
			}
		}
	})
}

func TestNewServerSetsConnectorProvider(t *testing.T) {
	dummyConnector := &ha.Connector{}
	srv, err := NewServer(Config{
		Port: 3306,
		ConnectorProvider: func(dbName string) (*ha.Connector, bool) {
			if dbName == "mydb.db" {
				return dummyConnector, true
			}
			return nil, false
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.ConnectorProvider == nil {
		t.Fatal("expected ConnectorProvider to be set on Server")
	}
	got, ok := srv.ConnectorProvider("mydb.db")
	if !ok || got != dummyConnector {
		t.Fatalf("unexpected ConnectorProvider result: got %v, ok=%v", got, ok)
	}
}

func TestUndoAndHistoryWithoutSelectedDatabase(t *testing.T) {
	h := &Handler{}

	if _, err := h.HandleQuery("UNDO 1;"); err == nil {
		t.Fatal("expected error for UNDO when no connector is set, got nil")
	}
	if _, err := h.HandleQuery("HISTORY 1;"); err == nil {
		t.Fatal("expected error for HISTORY when no connector is set, got nil")
	}
}

func TestUnsetDatabaseAndPreparedStmtInTransaction(t *testing.T) {
	ctx := context.Background()
	if err := sqlite.Load(ctx, "file:/mysql_default.db?vfs=memdb", sqlite.LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	}); err != nil {
		t.Fatalf("failed to load default db: %v", err)
	}
	defer sqlite.Shutdown()

	if err := sqlite.Load(ctx, "file:/mysql_other.db?vfs=memdb", sqlite.LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	}); err != nil {
		t.Fatalf("failed to load other db: %v", err)
	}

	defaultDB, _ := sqlite.DB("")
	defaultConn, _ := sqlite.Connector("")

	h := &Handler{
		db:        defaultDB,
		connector: defaultConn,
		dbProvider: func(name string) (*sql.DB, bool) {
			db, err := sqlite.DB(name)
			return db, err == nil
		},
		connectorProvider: func(name string) (*ha.Connector, bool) {
			c, err := sqlite.Connector(name)
			return c, err == nil
		},
	}

	// Switch to mysql_other and then UNSET DATABASE
	if _, err := h.HandleQuery("USE mysql_other"); err != nil {
		t.Fatalf("USE mysql_other failed: %v", err)
	}
	if h.db == defaultDB {
		t.Fatal("expected h.db to change after USE mysql_other")
	}

	if _, err := h.HandleQuery("UNSET DATABASE;"); err != nil {
		t.Fatalf("UNSET DATABASE failed: %v", err)
	}
	if h.db != defaultDB {
		t.Fatal("expected h.db to reset to defaultDB after UNSET DATABASE")
	}

	// Prepare statement BEFORE starting transaction
	if _, err := h.HandleQuery("CREATE TABLE items (id INT, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	params, _, stmtCtx, err := h.HandleStmtPrepare("INSERT INTO items (id, name) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("HandleStmtPrepare failed: %v", err)
	}
	defer h.HandleStmtClose(stmtCtx)
	if params != 2 {
		t.Fatalf("expected 2 params, got %d", params)
	}

	// Start transaction, execute prepared statement, and rollback
	if _, err := h.HandleQuery("BEGIN"); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := h.HandleStmtExecute(stmtCtx, "INSERT INTO items (id, name) VALUES (?, ?)", []any{1, "in-tx"}); err != nil {
		t.Fatalf("HandleStmtExecute in tx failed: %v", err)
	}
	if _, err := h.HandleQuery("ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	// Row must not exist because prepared statement was executed inside the rolled-back transaction
	var count int
	if err := defaultDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after rollback of prepared statement, got %d", count)
	}

	// Verify SHOW DATABASES;, SHOW TABLES;, version query, SQL comments, and CREATE/DROP DATABASE without .db
	if _, err := h.HandleQuery("/* comment */ SHOW DATABASES;"); err != nil {
		t.Fatalf("SHOW DATABASES; failed: %v", err)
	}
	if _, err := h.HandleQuery("-- comment\nSHOW TABLES;"); err != nil {
		t.Fatalf("SHOW TABLES; failed: %v", err)
	}
	verRes, err := h.HandleQuery("select concat(@@version, ' ', @@version_comment);")
	if err != nil || verRes == nil || verRes.Resultset == nil {
		t.Fatalf("version query failed: res=%v, err=%v", verRes, err)
	}

	h.createDatabaseOptions = sqlite.LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	}
	if _, err := h.HandleQuery("CREATE DATABASE temp_app;"); err != nil {
		t.Fatalf("CREATE DATABASE temp_app; failed: %v", err)
	}
	if _, err := h.HandleQuery("DROP DATABASE `temp_app`;"); err != nil {
		t.Fatalf("DROP DATABASE `temp_app`; failed: %v", err)
	}
}
