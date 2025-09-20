package sqlite

import (
	"database/sql"
	"log/slog"
	"sync"

	"github.com/mattn/go-sqlite3"
)

var (
	Driver       = "sqlite3-dq"
	registerOnce sync.Once
)

type CDCPublisher interface {
	Publish(cs *ChangeSet) error
}

func RegisterDriver(extensions []string, nodeName string, pub CDCPublisher) {
	registerOnce.Do(func() {
		sql.Register("sqlite3-ha",
			&sqlite3.SQLiteDriver{
				Extensions: extensions,
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {

					cs := NewChangeSet(nodeName)
					conn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
						change, ok := getChange(&d)
						if !ok {
							return
						}
						cs.AddChange(change)
					})

					conn.RegisterCommitHook(func() int {
						if err := cs.Send(pub); err != nil {
							slog.Error("failed to send changeset", "error", err)
							return 1
						}
						return 0
					})
					conn.RegisterRollbackHook(func() {
						cs.Clear()
					})
					return nil
				},
			})
	})
}
