package sqlite

import (
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	"github.com/mattn/go-sqlite3"
)

var (
	Driver       = "sqlite3-dq"
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

func enableCDCHooks(conn *sqlite3.SQLiteConn) {
	cs := NewChangeSet(nodeName)
	tableColumns := make(map[string][]string)
	conn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
		change, ok := getChange(&d)
		if !ok {
			return
		}
		fullTableName := fmt.Sprintf("%s.%s", change.Database, change.Table)
		if cols, ok := tableColumns[fullTableName]; ok {
			change.Columns = cols
		} else {
			rows, err := conn.Query(fmt.Sprintf("select * from %s.%s LIMIT 0", change.Database, change.Table), nil)
			if err != nil {
				slog.Error("failed to read columns", "error", err, "database", change.Database, "table", change.Table)
				return
			}
			defer rows.Close()
			change.Columns = rows.Columns()
			tableColumns[fullTableName] = change.Columns
		}
		for i := range len(change.Columns) {
			if len(change.OldValues) > 0 && i < len(change.OldValues) {
				change.OldValues[i] = convert(change.OldValues[i])
			}
			if len(change.NewValues) > 0 && i < len(change.NewValues) {
				change.NewValues[i] = convert(change.NewValues[i])
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
