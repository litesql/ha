package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/litesql/go-sqlite3"
)

var (
	nodeName     string
	publisher    CDCPublisher
	registerOnce sync.Once
)

type CDCPublisher interface {
	Publish(cs *ChangeSet) error
}

func RegisterDriver(extensions []string, name string, pub CDCPublisher) {
	registerOnce.Do(func() {
		nodeName = name
		publisher = pub
		sql.Register("sqlite3-ha",
			&sqlite3.SQLiteDriver{
				Extensions: extensions,
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					enableCDCHooks(conn)
					return nil
				},
			})

	})
}

var (
	changeSetSessions   = make(map[*sqlite3.SQLiteConn]*ChangeSet)
	changeSetSessionsMu sync.Mutex
)

func AddSQLChange(conn *sqlite3.SQLiteConn, sql string, args []any) error {
	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	cs.AddChange(Change{
		Operation: "SQL",
		SQL:       sql,
		SQLArgs:   args,
	})
	return nil
}

func RemoveLastChange(conn *sqlite3.SQLiteConn) error {
	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	if len(cs.Changes) > 0 {
		cs.Changes = cs.Changes[:len(cs.Changes)-1]
	}
	return nil
}

type tableInfo struct {
	columns []string
	types   []string
}

func enableCDCHooks(conn *sqlite3.SQLiteConn) {
	changeSetSessionsMu.Lock()
	defer changeSetSessionsMu.Unlock()

	cs := NewChangeSet(nodeName)
	changeSetSessions[conn] = cs
	tableColumns := make(map[string]tableInfo)
	conn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
		change, ok := getChange(&d)
		if !ok {
			return
		}
		fullTableName := fmt.Sprintf("%s.%s", change.Database, change.Table)
		var types []string
		if ti, ok := tableColumns[fullTableName]; ok {
			change.Columns = ti.columns
			types = ti.types
		} else {
			rows, err := conn.Query(fmt.Sprintf("SELECT name, type FROM %s.PRAGMA_TABLE_INFO('%s')", change.Database, change.Table), nil)
			if err != nil {
				slog.Error("failed to read columns", "error", err, "database", change.Database, "table", change.Table)
				return
			}
			defer rows.Close()
			var columns []string
			for {
				dataRow := []driver.Value{new(string), new(string)}

				err := rows.Next(dataRow)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						slog.Error("failed to read table columns", "error", err, "table", change.Table)
					}
					break
				}
				if v, ok := dataRow[0].(string); ok {
					columns = append(columns, v)
				}
				if v, ok := dataRow[1].(string); ok {
					types = append(types, v)
				}
			}
			change.Columns = columns
			tableColumns[fullTableName] = tableInfo{
				columns: columns,
				types:   types,
			}
		}
		for i, t := range types {
			if t != "BLOB" {
				if i < len(change.OldValues) && change.OldValues[i] != nil {
					change.OldValues[i] = convert(change.OldValues[i])
				}
				if i < len(change.NewValues) && change.NewValues[i] != nil {
					change.NewValues[i] = convert(change.NewValues[i])
				}
			}
		}

		cs.AddChange(change)
	})

	conn.RegisterCommitHook(func() int {
		if err := cs.Send(publisher); err != nil {
			slog.Error("failed to send changeset", "error", err)
			return 1
		}
		return 0
	})
	conn.RegisterRollbackHook(func() {
		cs.Clear()
	})
}

func disableCDCHooks(conn *sqlite3.SQLiteConn) {
	conn.RegisterPreUpdateHook(nil)
	conn.RegisterCommitHook(nil)
	conn.RegisterRollbackHook(nil)
}

func convert(src any) any {
	switch v := src.(type) {
	case []byte:
		return string(v)
	default:
		return src
	}
}
