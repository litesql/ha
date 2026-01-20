package postgresql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/litesql/go-ha"

	"github.com/litesql/ha/internal/sqlite"
)

const (
	transactionAttribute = "tx"
	databaseIDAttribute  = "dbID"
)

type Config struct {
	User       string
	Pass       string
	TLSCert    string
	TLSKey     string
	CreateOpts CreateDatabaseOptions
}

type CreateDatabaseOptions struct {
	Dir                string
	MemDB              bool
	FromLatestSnapshot bool
	DeliverPolicy      string
	MaxConns           int
	Opts               []ha.Option
}

const columnWidth = 256

type Server struct {
	*wire.Server
}

func NewServer(cfg Config) (*Server, error) {
	var server Server
	opts := []wire.OptionFn{
		wire.Version("17.0"),
		wire.SessionMiddleware(server.session),
		wire.TerminateConn(server.terminateConn),
		wire.Logger(slog.Default()),
		wire.SessionAuthStrategy(
			wire.ClearTextPassword(func(ctx context.Context, database, username, password string) (context.Context, bool, error) {
				if username == cfg.User && password == cfg.Pass {
					slog.InfoContext(ctx, "pg-wire: authenticated", "database", database, "user", username, "remote", wire.RemoteAddress(ctx))
					return ctx, true, nil
				}
				return ctx, false, nil
			})),
	}

	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, err
		}
		config := &tls.Config{Certificates: []tls.Certificate{cert}}
		opts = append(opts, wire.TLSConfig(config))
	}

	wireServer, err := wire.NewServer(parseFn(cfg.CreateOpts), opts...)
	if err != nil {
		return nil, err
	}

	server.Server = wireServer
	return &server, nil
}

func (s *Server) ListenAndServe(port int) error {
	return s.Server.ListenAndServe(fmt.Sprintf(":%d", port))
}

func (s *Server) Serve(l net.Listener) error {
	return s.Server.Serve(l)
}

func (s *Server) Close() error {
	return s.Server.Close()
}

func (s *Server) session(ctx context.Context) (context.Context, error) {
	slog.InfoContext(ctx, "pg-wire: new session established", "remote", wire.RemoteAddress(ctx))
	return ctx, nil
}

func (s *Server) terminateConn(ctx context.Context) error {
	rollback(ctx)
	slog.InfoContext(ctx, "pg-wire: session terminated", "remote", wire.RemoteAddress(ctx))
	return nil
}

var reSetDatabase = regexp.MustCompile(`(?i)^SET\s+DATABASE\s*(=|TO)\s*([^;\s]+)`)

func parseFn(createDatabaseOptions CreateDatabaseOptions) wire.ParseFn {
	return func(ctx context.Context, sql string) (wire.PreparedStatements, error) {
		slog.InfoContext(ctx, "pg-wire: query received", "remote", wire.RemoteAddress(ctx), "sql", sql)
		upper := strings.ToUpper(strings.TrimSpace(sql))
		if strings.HasPrefix(upper, "-- PING") {
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete("pong")
			})), nil
		}

		var dbID string
		if id, ok := wire.GetAttribute(ctx, databaseIDAttribute); ok {
			dbID = id.(string)
		}

		if strings.TrimSpace(strings.ReplaceAll(upper, ";", "")) == "SHOW DATABASES" {
			handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				var count int
				for _, id := range sqlite.Databases() {
					count++
					status := "0"
					if id == dbID {
						status = "1"
					}
					writer.Row([]any{id, status})
				}
				return writer.Complete(fmt.Sprintf("SELECT %d", count))
			}

			return wire.Prepared(wire.NewStatement(handle,
				wire.WithColumns(wire.Columns{
					wire.Column{
						Table: 0,
						Name:  "database",
						Oid:   pgtype.TextOID,
						Width: columnWidth,
					},
					wire.Column{
						Table: 0,
						Name:  "active",
						Oid:   pgtype.TextOID,
						Width: columnWidth,
					},
				}))), nil
		}

		if strings.HasPrefix(upper, "SET ") {
			if match := reSetDatabase.FindStringSubmatch(sql); len(match) == 3 {
				dbID := match[2]
				if slices.Contains((sqlite.Databases()), dbID) {
					rollback(ctx)
					wire.SetAttribute(ctx, databaseIDAttribute, dbID)
					return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
						return writer.Complete("OK, " + dbID)
					})), nil
				}
				return nil, fmt.Errorf("database %q not found", dbID)
			}
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete("ignored")
			})), nil
		}

		if strings.HasPrefix(upper, "CREATE DATABASE ") {
			if !createDatabaseOptions.MemDB && createDatabaseOptions.Dir == "" {
				return nil, fmt.Errorf("create database is disabled, inform flag --create-db-dir at startup")
			}
			dsn := strings.TrimSpace(sql[16:])
			dsn = strings.TrimSuffix(dsn, ";")

			destPath := sqlite.IdFromDSN(dsn)
			if destPath == "" {
				return nil, fmt.Errorf("invalid dsn")
			}
			var params string
			if idx := strings.Index(dsn, "?"); idx != -1 {
				params = dsn[idx:]
			}

			if !strings.HasSuffix(destPath, ".db") {
				destPath += ".db"
			}
			destPath = filepath.Join(createDatabaseOptions.Dir, filepath.Base(destPath))
			dsn = fmt.Sprintf("file:%s%s", destPath, params)

			err := sqlite.Load(ctx, dsn, createDatabaseOptions.MemDB, createDatabaseOptions.FromLatestSnapshot,
				createDatabaseOptions.DeliverPolicy, createDatabaseOptions.MaxConns, createDatabaseOptions.Opts...)
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				if err != nil {
					return writer.Complete(fmt.Sprintf("failed to create database: %v", err))
				}
				return writer.Complete("DATABASE CREATED")
			})), nil
		} else if strings.HasPrefix(upper, "DROP DATABASE ") {
			id := strings.TrimSpace(sql[14:])
			id = strings.TrimSuffix(id, ";")

			dbfile, err := sqlite.Drop(ctx, id)
			if dbfile != "" {
				err = os.Remove(dbfile)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					return nil, fmt.Errorf("failed to remove database file: %v", err)
				}
				os.Remove(dbfile + "-shm")
				os.Remove(dbfile + "-wal")
			}
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete("OK")
			})), nil
		}

		db, err := sqlite.DB(dbID)
		if err != nil {
			return nil, err
		}

		stmt, err := ha.ParseStatement(ctx, sql)
		if err != nil {
			return nil, psqlerr.WithCode(err, codes.SyntaxErrorOrAccessRuleViolation)
		}

		switch {
		case stmt.Begin():
			err = begin(ctx, db)
			if err != nil {
				return nil, err
			}
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Empty()
			})), nil
		case stmt.Commit():
			err = commit(ctx)
			if err != nil {
				return nil, err
			}
			return wire.Prepared(wire.NewStatement(func(ctxHandler context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Empty()
			})), nil
		case stmt.Rollback():
			err = rollback(ctx)
			if err != nil {
				return nil, err
			}
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Empty()
			})), nil
		}
		return handler(ctx, stmt, db)
	}
}

type execerQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func handler(ctx context.Context, stmt *ha.Statement, db *sql.DB) (wire.PreparedStatements, error) {
	if len(stmt.Parameters()) > 0 {
		return handlerPrepared(ctx, stmt, db)
	}
	var (
		eq  execerQuerier
		err error
	)
	if tx, ok := wire.GetAttribute(ctx, transactionAttribute); ok && tx != nil {
		ctxTx := tx.(*sql.Tx)
		eq = ctxTx
	} else {
		eq = db
	}
	resp, err := sqlite.Exec(ctx, eq, stmt, nil)
	if err != nil {
		return nil, err
	}

	if resp.NoReturning {
		switch {
		case stmt.IsInsert():
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete(fmt.Sprintf("INSERT 0 %d", resp.RowsAffected))
			})), nil
		case stmt.IsDelete():
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete(fmt.Sprintf("DELETE %d", resp.RowsAffected))
			})), nil
		case stmt.IsUpdate():
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete(fmt.Sprintf("UPDATE %d", resp.RowsAffected))
			})), nil
		case stmt.Type() != ha.TypeOther:
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete(stmt.Type())
			})), nil
		default:
			return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Empty()
			})), nil
		}
	}

	columns := make([]wire.Column, len(resp.Columns))
	for i, col := range resp.Columns {
		columns[i] = wire.Column{
			Table: 0,
			Name:  col,
			Oid:   pgtype.TextOID,
			Width: columnWidth,
		}
	}

	handle := func(_ context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		for _, row := range resp.Rows {
			err := writeRow(writer, row)
			if err != nil {
				slog.ErrorContext(ctx, "pg-wire: write row", "error", err)
				return err
			}
		}
		err := writer.Complete(fmt.Sprintf("SELECT %d", len(resp.Rows)))
		if err != nil {
			slog.ErrorContext(ctx, "pg-wire: write data", "error", err, "query", stmt.Source())
			return err
		}
		return nil
	}
	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(columns))), nil
}

func handlerPrepared(ctx context.Context, stmt *ha.Statement, db *sql.DB) (wire.PreparedStatements, error) {
	bindParameters := stmt.Parameters()
	parameters := make([]uint32, len(bindParameters))
	for i := range parameters {
		parameters[i] = 0
	}
	options := []wire.PreparedOptionFn{wire.WithParameters(parameters)}

	cols := stmt.Columns()
	if len(cols) > 0 {
		columns := make([]wire.Column, len(cols))
		for i, col := range cols {
			if strings.Contains(col, "*") {
				return nil, psqlerr.WithCode(errors.New("cannot use '*' in prepared statements, please specify column names"), codes.SyntaxErrorOrAccessRuleViolation)
			}
			columns[i] = wire.Column{
				Table: 0,
				Name:  col,
				Oid:   pgtype.TextOID,
				Width: columnWidth,
			}
		}
		options = append(options, wire.WithColumns(columns))
	}
	var (
		eq execerQuerier
	)
	if tx, ok := wire.GetAttribute(ctx, transactionAttribute); ok && tx != nil {
		eq = tx.(*sql.Tx)
	} else {
		eq = db
	}
	handle := func(ctxHandle context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		params := make(map[string]any)
		for i, p := range parameters {
			value, err := p.Scan(25) // postgresql OID type text -> oid.T_text
			if err != nil {
				slog.ErrorContext(ctx, "pg-wire: parameter scan", "error", err)
				return err
			}
			params[bindParameters[i]] = value
		}
		resp, err := sqlite.Exec(ctxHandle, eq, stmt, params)
		if err != nil {
			slog.ErrorContext(ctx, "pg-wire: local exec", "error", err, "query", stmt.Source())
			return err
		}

		if resp.NoReturning {
			switch {
			case stmt.IsInsert():
				return writer.Complete(fmt.Sprintf("INSERT 0 %d", resp.RowsAffected))
			case stmt.IsDelete():
				return writer.Complete(fmt.Sprintf("DELETE %d", resp.RowsAffected))
			case stmt.IsUpdate():
				return writer.Complete(fmt.Sprintf("UPDATE %d", resp.RowsAffected))
			case stmt.Type() != ha.TypeOther:
				return writer.Complete(stmt.Type())
			default:
				return writer.Empty()
			}
		}

		for _, row := range resp.Rows {
			err := writeRow(writer, row)
			if err != nil {
				slog.ErrorContext(ctx, "pg-wire: write row", "error", err)
				return err
			}
		}
		err = writer.Complete(fmt.Sprintf("SELECT %d", len(resp.Rows)))
		if err != nil {
			slog.ErrorContext(ctx, "pg-wire: write data", "error", err)
			return err
		}
		return nil
	}

	return wire.Prepared(wire.NewStatement(handle, options...)), nil
}

func begin(ctx context.Context, db *sql.DB) error {
	existsTx, ok := wire.GetAttribute(ctx, transactionAttribute)
	if ok && existsTx != nil {
		return nil
	}
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
	wire.SetAttribute(ctx, transactionAttribute, tx)
	return nil
}

func commit(ctx context.Context) error {
	txContext, ok := wire.GetAttribute(ctx, transactionAttribute)
	if ok && txContext != nil {
		tx := txContext.(*sql.Tx)
		wire.SetAttribute(ctx, transactionAttribute, nil)
		err := tx.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func rollback(ctx context.Context) error {
	txContext, ok := wire.GetAttribute(ctx, transactionAttribute)
	if ok && txContext != nil {
		tx := txContext.(*sql.Tx)
		wire.SetAttribute(ctx, transactionAttribute, nil)
		err := tx.Rollback()
		if err != nil {
			return err
		}
	}
	return nil
}

func writeRow(writer wire.DataWriter, row []any) error {
	strs := make([]any, len(row))
	for i, v := range row {
		if row == nil {
			continue
		}
		switch v := v.(type) {
		case *any:
			strs[i] = fmt.Sprintf("%v", *v)
			continue
		}
		strs[i] = fmt.Sprintf("%v", v)
	}
	return writer.Row(strs)
}
