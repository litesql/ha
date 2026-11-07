package mcp

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	haconnect "github.com/litesql/go-ha/connect"
	"github.com/litesql/ha/internal/sqlite"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type SchemaInput struct {
	DatabaseID string `json:"database_id,omitempty" jsonschema:"The identifier of the database to inspect (empty uses default).,example=ha.db"`
}

type SchemaItem struct {
	Name string `json:"name" jsonschema:"Object name"`
	Type string `json:"type" jsonschema:"Object type (table, view, index, trigger)"`
	SQL  string `json:"sql" jsonschema:"DDL statement used to create the object"`
}

type SchemaOutput struct {
	Items []SchemaItem `json:"items" jsonschema:"Schema objects in the database."`
}

func Schema(ctx context.Context, req *mcp.CallToolRequest, input SchemaInput) (result *mcp.CallToolResult, output SchemaOutput, err error) {
	db, err := sqlite.DB(input.DatabaseID)
	if err != nil {
		return
	}
	res, err := sqlite.Exec(ctx, db, "SELECT name, type, COALESCE(sql, '') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name", nil)
	if err != nil {
		return
	}
	output.Items = make([]SchemaItem, 0, len(res.Rows))
	for _, row := range res.Rows {
		if len(row) >= 3 {
			output.Items = append(output.Items, SchemaItem{
				Name: fmt.Sprint(row[0]),
				Type: fmt.Sprint(row[1]),
				SQL:  fmt.Sprint(row[2]),
			})
		}
	}
	return
}

type HistoryInput struct {
	DatabaseID string `json:"database_id,omitempty" jsonschema:"The identifier of the database (empty uses default).,example=ha.db"`
	Param      string `json:"param" jsonschema:"Sequence number (e.g. '10') or duration (e.g. '5m').,example=5m"`
}

type HistoryOutput struct {
	History []haconnect.HistoryItem `json:"history" jsonschema:"List of committed transaction history items."`
}

func History(ctx context.Context, req *mcp.CallToolRequest, input HistoryInput) (result *mcp.CallToolResult, output HistoryOutput, err error) {
	c, err := sqlite.Connector(input.DatabaseID)
	if err != nil {
		return
	}
	param := strings.TrimSpace(input.Param)
	seq, parseErr := strconv.Atoi(param)
	if parseErr != nil {
		duration, durErr := time.ParseDuration(param)
		if durErr != nil {
			err = fmt.Errorf("invalid param: %w", durErr)
			return
		}
		if duration <= 0 {
			err = fmt.Errorf("duration must be greater than 0")
			return
		}
		output.History, err = c.HistoryByTime(ctx, duration)
		return
	}
	if seq < 0 {
		err = fmt.Errorf("sequence must be a non-negative integer")
		return
	}
	output.History, err = c.HistoryBySeq(ctx, uint64(seq))
	return
}

type UndoInput struct {
	DatabaseID string `json:"database_id,omitempty" jsonschema:"The identifier of the database (empty uses default).,example=ha.db"`
	Param      string `json:"param" jsonschema:"Sequence number (e.g. '0' for last transaction, '10' for until seq 10) or duration (e.g. '5m').,example=0"`
	Filter     string `json:"filter,omitempty" jsonschema:"Optional undo filter when undoing by sequence: 'none', 'entity', or 'transaction'.,example=none"`
}

type UndoOutput struct {
	Status string `json:"status" jsonschema:"Undo result status message."`
}

func Undo(ctx context.Context, req *mcp.CallToolRequest, input UndoInput) (result *mcp.CallToolResult, output UndoOutput, err error) {
	c, err := sqlite.Connector(input.DatabaseID)
	if err != nil {
		return
	}
	var undoType haconnect.UndoFilter
	switch strings.ToLower(strings.TrimSpace(input.Filter)) {
	case "", "none":
		undoType = haconnect.UndoFilterNone
	case "e", "entity":
		undoType = haconnect.UndoFilterEntity
	case "t", "transaction":
		undoType = haconnect.UndoFilterTransaction
	default:
		err = fmt.Errorf("invalid undo filter %q: use 'none', 'entity', or 'transaction'", input.Filter)
		return
	}

	param := strings.TrimSpace(input.Param)
	seq, parseErr := strconv.Atoi(param)
	if parseErr != nil {
		if undoType != haconnect.UndoFilterNone {
			err = fmt.Errorf("cannot specify entity or transaction undo filter with a duration")
			return
		}
		duration, durErr := time.ParseDuration(param)
		if durErr != nil {
			err = fmt.Errorf("invalid param: %w", durErr)
			return
		}
		if duration <= 0 {
			err = fmt.Errorf("duration must be greater than 0")
			return
		}
		if err = c.UndoByTime(ctx, duration, haconnect.UndoFilterNone, nil); err != nil {
			return
		}
		output.Status = fmt.Sprintf("undone transactions in the last %s", duration)
		return
	}
	if seq < 0 {
		err = fmt.Errorf("sequence must be a non-negative integer")
		return
	}
	if err = c.UndoBySeq(ctx, uint64(seq), undoType, nil); err != nil {
		return
	}
	if seq == 0 {
		output.Status = "undone the last transaction"
	} else {
		output.Status = fmt.Sprintf("undone transactions until sequence %d", seq)
	}
	return
}
