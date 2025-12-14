//go:build !cgo

package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/litesql/go-ha"
	sqliteha "github.com/litesql/go-sqlite-ha"
)

func Backup(ctx context.Context, db *sql.DB, w io.Writer) error {
	return sqliteha.Backup(ctx, db, w)
}

func newConnector(dsn string, options ...ha.Option) (*ha.Connector, error) {
	return sqliteha.NewConnector(dsn, options...)
}

func deserializerConn(conn driver.Conn) (deserializer, error) {
	switch c := conn.(type) {
	case deserializer:
		return c, nil
	default:
		return nil, fmt.Errorf("not a sqlite3 connection")
	}

}
