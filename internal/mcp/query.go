package mcp

import (
	"context"

	"github.com/litesql/ha/internal/sqlite"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type QueryInput struct {
	DatabaseID string `json:"database_id" jsonschema:"The identifier of the database to query.,example=ha.db"`
	// The query string to be executed.
	Query string `json:"query" jsonschema:"The query string to be executed.,example=SELECT * FROM users WHERE active = true"`
	// Optional parameters for the query.
	Params map[string]any `json:"params,omitempty" jsonschema:"Optional parameters for the query.,example={\"limit\": 10, \"offset\": 0}"`
}

type QueryOutput struct {
	// The column names of the query result.
	Columns []string `json:"columns" jsonschema:"The column names of the query result.,example=[\"id\", \"name\"]"`
	// The results of the query.
	Results [][]any `json:"results" jsonschema:"The results of the query.,example=[[1, \"Alice\"], [2, \"Bob\"]]"`
}

func Query(ctx context.Context, req *mcp.CallToolRequest, input QueryInput) (result *mcp.CallToolResult, output QueryOutput, err error) {
	db, err := sqlite.DB(input.DatabaseID)
	if err != nil {
		return
	}
	res, err := sqlite.Exec(ctx, db, input.Query, input.Params)
	if err != nil {
		return
	}
	output.Columns = res.Columns
	output.Results = res.Rows
	return
}
