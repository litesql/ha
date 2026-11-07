package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func NewServer(version string) *mcp.Server {
	if version == "" {
		version = "dev"
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "ha", Version: version}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "databases", Description: "list loaded databases"}, Databases)
	mcp.AddTool(server, &mcp.Tool{Name: "schema", Description: "inspect tables and schema objects in a database"}, Schema)
	mcp.AddTool(server, &mcp.Tool{Name: "query", Description: "execute a query"}, Query)
	mcp.AddTool(server, &mcp.Tool{Name: "history", Description: "retrieve transaction history by sequence or duration"}, History)
	mcp.AddTool(server, &mcp.Tool{Name: "undo", Description: "undo transactions by sequence or duration"}, Undo)
	return server
}

func NewHTTPHandler(version string) *mcp.StreamableHTTPHandler {
	server := NewServer(version)
	return mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, nil)
}
