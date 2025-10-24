package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/litesql/go-ha"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
	"github.com/nats-io/nats.go/jetstream"
)

type connectorDB struct {
	db        *sql.DB
	connector *ha.Connector
}

var dbs = make(map[string]*connectorDB)

var reDateTime = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`)

func Load(dsnList []string, memDB bool, fromLatestSnapshot bool, deliverPolicy string, maxConns int, opts ...ha.Option) error {
	for i, dsn := range dsnList {
		defaultDB := i == 0
		id := idFromDSN(dsn)
		if _, exists := dbs[id]; exists {
			return fmt.Errorf("database with id %q already added", id)
		}
		options := slices.Clone(opts)
		waitFor := make(chan struct{})
		options = append(options, ha.WithWaitFor(waitFor))
		var connector *ha.Connector
		if fromLatestSnapshot {
			slog.Info("loading latest snapshot from NATS JetStream Object Store", "dsn", dsn)
			sequence, reader, err := ha.LatestSnapshot(context.Background(), dsn, opts...)
			if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
				return fmt.Errorf("failed to load latest snapshot: %w", err)
			}

			if sequence > 0 && deliverPolicy == "" {
				policy := fmt.Sprintf("by_start_sequence=%d", sequence)
				options = append(options, ha.WithDeliverPolicy(policy))
			}
			if reader != nil {
				if memDB {
					connector, err = sqlite3ha.NewConnector(dsn, options...)
					if err != nil {
						return err
					}
					err = deserializeFromReader(context.Background(), connector, reader)
					if err != nil {
						return fmt.Errorf("failed to load latest snapshot: %w", err)
					}
				} else {
					filename := filenameFromDSN(dsn)
					f, err := os.Create(filename)
					if err != nil {
						return fmt.Errorf("failed to create snapshot file %q: %w", filename, err)
					}
					_, err = io.Copy(f, reader)
					if err != nil {
						f.Close()
						return fmt.Errorf("failed to write snapshot file %q: %w", filename, err)
					}
					f.Close()
					slog.Info("loading snapshot", "filename", filename)
					connector, err = sqlite3ha.NewConnector(dsn, options...)
					if err != nil {
						return err
					}
				}
			}
		} else {
			if memDB {
				slog.Info("using in-memory database")
				filename := filenameFromDSN(dsn)
				options := slices.Clone(opts)
				if filename != "" && deliverPolicy == "" {
					matched := reDateTime.MatchString(filepath.Base(filename))
					if matched {
						dateTime := filepath.Base(filename)[0:len(time.DateTime)]
						_, err := time.Parse(time.DateTime, dateTime)
						if err == nil {
							policy := fmt.Sprintf("by_start_time=%s", dateTime)
							options = append(options, ha.WithDeliverPolicy(policy))
						}
					}
				}
				var err error
				connector, err = sqlite3ha.NewConnector(dsn, options...)
				if err != nil {
					return err
				}

				if filename != "" {
					fi, err := os.Stat(filename)
					if err == nil && !fi.IsDir() {
						slog.Info("loading database", "file", filename)
						err := deserialize(context.Background(), connector, filename)
						if err != nil {
							return fmt.Errorf("failed to load database %q: %w", filename, err)
						}
					}
				}
			} else {
				slog.Info("using data source name", "dsn", dsn)
				var err error
				connector, err = sqlite3ha.NewConnector(dsn, opts...)
				if err != nil {
					return err
				}
			}
		}

		db := sql.OpenDB(connector)
		db.SetConnMaxIdleTime(0)
		db.SetConnMaxLifetime(0)
		db.SetMaxOpenConns(maxConns)
		db.SetMaxIdleConns(maxConns)
		if connector.Subscriber() != nil {
			connector.Subscriber().SetDB(db)
		}
		if connector.Snapshotter() != nil {
			connector.Snapshotter().SetDB(db)
		}
		close(waitFor)
		connDB := &connectorDB{
			db:        db,
			connector: connector,
		}
		dbs[id] = connDB
		if defaultDB {
			dbs[""] = connDB
		}
	}
	return nil
}

func Close() {
	for _, connDB := range dbs {
		connDB.db.Close()
		connDB.connector.Close()
	}
}

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

func Exec(ctx context.Context, eq execerQuerier, stmt *ha.Statement, params map[string]any) (*Response, error) {
	slog.Info("Executing statement", "type", stmt.Type(), "sql", stmt.Source(), "params", params)
	if stmt.IsSelect() || stmt.IsExplain() || stmt.HasReturning() {
		return doQuery(ctx, eq, stmt.Source(), params)
	}

	return doExec(ctx, eq, stmt.Source(), params)
}

func Databases() []string {
	var list []string
	for id := range dbs {
		if id == "" {
			continue
		}
		list = append(list, id)
	}
	return list
}

func DB(id string) (*sql.DB, error) {
	dbConnector, ok := dbs[id]
	if !ok {
		return nil, fmt.Errorf("database with id %q not found", id)
	}
	return dbConnector.db, nil
}

func Connector(id string) (*ha.Connector, error) {
	dbConnector, ok := dbs[id]
	if !ok {
		return nil, fmt.Errorf("database with id %q not found", id)
	}
	return dbConnector.connector, nil
}

func Transaction(ctx context.Context, db *sql.DB, queries []Request) ([]*Response, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var list []*Response
	for _, query := range queries {
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

func deserializeFromReader(ctx context.Context, connector driver.Connector, r io.Reader) error {
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

	return deserialize(ctx, connector, dest.Name())
}

func deserialize(ctx context.Context, connector driver.Connector, file string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	conn, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	deserializerConn, err := deserializerConn(conn)
	if err != nil {
		return err
	}
	buf := make([]byte, len(data))
	data = append(data, buf...)
	return deserializerConn.Deserialize(data, "")
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

type deserializer interface {
	Deserialize(b []byte, schema string) error
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

func filenameFromDSN(dsn string) string {
	var filename string
	u, err := url.Parse(dsn)
	if err == nil {
		filename = u.Path
	}
	if filename == "" {
		filename = strings.TrimPrefix(dsn, "file:")
		if i := strings.Index(filename, "?"); i > 0 {
			filename = filename[0:i]
		}
	}
	return filename
}

func idFromDSN(dsn string) string {
	var filename string
	u, err := url.Parse(dsn)
	if err == nil {
		filename = u.Path
	}
	if filename == "" {
		filename = strings.TrimPrefix(dsn, "file:")
		if i := strings.Index(filename, "?"); i > 0 {
			filename = filename[0:i]
		}
	}
	if filename != "" {
		return filepath.Base(filename)
	}
	return ""
}
