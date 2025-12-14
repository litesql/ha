//go:build cgo

package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/litesql/go-ha"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
)

func Backup(ctx context.Context, db *sql.DB, w io.Writer) error {
	return sqlite3ha.Backup(ctx, db, w)
}

func newConnector(dsn string, options ...ha.Option) (*ha.Connector, error) {
	return sqlite3ha.NewConnector(dsn, options...)
}

func deserializerConn(conn driver.Conn) (deserializer, error) {
	switch c := conn.(type) {
	case *sqlite3ha.Conn:
		return c.SQLiteConn, nil
	case deserializer:
		return c, nil
	default:
		return nil, fmt.Errorf("not a sqlite3 connection")
	}

}
