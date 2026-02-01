package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func NewServer() *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "ha", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "databases", Description: "list loaded databases"}, Databases)
	mcp.AddTool(server, &mcp.Tool{Name: "query", Description: "execute a query"}, Query)
	return server
}

func NewHTTPHandler() *mcp.StreamableHTTPHandler {
	server := NewServer()
	return mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, nil)
}
