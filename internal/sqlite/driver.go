package sqlite

import (
	"database/sql"
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
	conn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
		change, ok := getChange(&d)
		if !ok {
			return
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
