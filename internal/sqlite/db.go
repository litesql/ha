package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/litesql/go-ha"
	"github.com/litesql/go-sqlite3"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
)

var (
	db *sql.DB
)

type Request struct {
	Sql    string         `json:"sql"`
	Params map[string]any `json:"params"`
}

type Response struct {
	Columns      []string `json:"columns"`
	Rows         [][]any  `json:"rows"`
	RowsAffected int64    `json:"-"`
	NoReturning  bool     `json:"-"`
}

type execerQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func SetGlobalDB(database *sql.DB) {
	db = database
}

func Exec(ctx context.Context, eq execerQuerier, stmt *ha.Statement, params map[string]any) (*Response, error) {
	slog.Info("Executing statement", "type", stmt.Type(), "sql", stmt.Source(), "params", params)
	if stmt.IsSelect() || stmt.IsExplain() || stmt.HasReturning() {
		return doQuery(ctx, eq, stmt.Source(), params)
	}

	return doExec(ctx, eq, stmt.Source(), params)
}

func Transaction(ctx context.Context, req []Request) ([]*Response, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var list []*Response
	for _, query := range req {
		stmt, err := ha.ParseStatement(ctx, query.Sql)
		if err != nil {
			return nil, err
		}
		res, err := Exec(ctx, tx, stmt, query.Params)
		if err != nil {
			return nil, err
		}
		list = append(list, res)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return list, nil
}

func DeserializeFromReader(ctx context.Context, r io.Reader) error {
	dest, err := os.CreateTemp("", "ha-*.db")
	if err != nil {
		return err
	}
	defer os.Remove(dest.Name())

	_, err = io.Copy(dest, r)
	if err != nil {
		return err
	}
	dest.Close()

	return Deserialize(ctx, dest.Name())
}

func Deserialize(ctx context.Context, file string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	sqlite3Conn, err := sqliteConn(conn)
	if err != nil {
		return err
	}
	buf := make([]byte, len(data)*2)
	data = append(data, buf...)
	return sqlite3Conn.Deserialize(data, "")
}

func Filename(ctx context.Context) (string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	sqlite3Conn, err := sqliteConn(conn)
	if err != nil {
		return "", err
	}
	return sqlite3Conn.GetFilename(""), nil

}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func doQuery(ctx context.Context, querier querier, query string, args map[string]any) (*Response, error) {
	rows, err := querier.QueryContext(ctx, query, getArgs(args)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnsCount := len(columns)
	if columnsCount == 0 {
		return nil, fmt.Errorf("zero columns")
	}

	dataRows := make([][]any, 0)
	for rows.Next() {
		values := make([]any, columnsCount)
		for i := range values {
			values[i] = &values[i]
		}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		dataRows = append(dataRows, values)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &Response{
		Columns: columns,
		Rows:    dataRows,
	}, nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func doExec(ctx context.Context, execer execer, query string, params map[string]any) (*Response, error) {
	args := getArgs(params)
	res, err := execer.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	rowsAffected, _ := res.RowsAffected()
	lastInsertID, _ := res.LastInsertId()

	return &Response{
		Columns:      []string{"rows_affected", "last_insert_id"},
		Rows:         [][]any{{rowsAffected, lastInsertID}},
		RowsAffected: rowsAffected,
		NoReturning:  true}, nil
}

func getArgs(params map[string]any) []any {
	if len(params) == 0 {
		return nil
	}
	for k := range params {
		if isPositional(rune(k[0])) {
			return getPositionalArgs(params)
		}
		break
	}
	var args []any = make([]any, 0, len(params))
	for k, v := range params {
		if len(k) > 1 && (strings.HasPrefix(k, "$") || (strings.HasPrefix(k, "?") ||
			strings.HasPrefix(k, ":") || strings.HasPrefix(k, "@"))) {
			k = k[1:]
		}
		args = append(args, sql.Named(k, v))
	}
	return args
}

func isPositional(r rune) bool {
	return r == '$'
}

func getPositionalArgs(params map[string]any) []any {
	total := len(params)
	args := make([]any, total)
	for i := range total {
		args[i] = params[fmt.Sprintf("$%d", i+1)]
	}
	return args
}

func sqliteConn(conn *sql.Conn) (*sqlite3.SQLiteConn, error) {
	var sqlite3Conn *sqlite3.SQLiteConn
	err := conn.Raw(func(driverConn any) error {
		switch c := driverConn.(type) {
		case *sqlite3ha.Conn:
			sqlite3Conn = c.SQLiteConn
			return nil
		case *sqlite3.SQLiteConn:
			sqlite3Conn = c
			return nil
		default:
			return fmt.Errorf("not a sqlite3 connection")
		}
	})
	return sqlite3Conn, err
}
