package mcp

import (
	"context"

	"github.com/litesql/ha/internal/sqlite"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type DatabasesInput struct {
}

type DatabasesOutput struct {
	Databases []string `json:"databases" jsonschema:"The list of database identifiers.,example=[\"ha.db\", \"test.db\"]"`
}

func Databases(ctx context.Context, req *mcp.CallToolRequest, input DatabasesInput) (result *mcp.CallToolResult, output DatabasesOutput, err error) {
	output.Databases = sqlite.Databases()
	return
}
