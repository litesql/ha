package mcp

import (
	"context"
	"testing"

	"github.com/litesql/ha/internal/sqlite"
)

func TestMCPQueryAndSchema(t *testing.T) {
	ctx := context.Background()
	err := sqlite.Load(ctx, "file:/mcp_test.db?vfs=memdb", sqlite.LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	})
	if err != nil {
		t.Fatalf("failed to load memdb: %v", err)
	}
	defer sqlite.Drop(ctx, "mcp_test.db")

	_, _, err = Query(ctx, nil, QueryInput{
		DatabaseID: "mcp_test.db",
		Query:      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
	})
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, _, err = Query(ctx, nil, QueryInput{
		DatabaseID: "mcp_test.db",
		Query:      "INSERT INTO users (name) VALUES ('Alice')",
	})
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, qOut, err := Query(ctx, nil, QueryInput{
		DatabaseID: "mcp_test.db",
		Query:      "SELECT id, name FROM users",
	})
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(qOut.Columns) != 2 || qOut.Columns[0] != "id" || qOut.Columns[1] != "name" {
		t.Fatalf("unexpected columns: %v", qOut.Columns)
	}
	if len(qOut.Results) != 1 || qOut.Results[0][1] != "Alice" {
		t.Fatalf("unexpected results: %v", qOut.Results)
	}

	_, sOut, err := Schema(ctx, nil, SchemaInput{DatabaseID: "mcp_test.db"})
	if err != nil {
		t.Fatalf("Schema failed: %v", err)
	}
	if len(sOut.Items) != 1 || sOut.Items[0].Name != "users" || sOut.Items[0].Type != "table" {
		t.Fatalf("unexpected schema items: %+v", sOut.Items)
	}
}
