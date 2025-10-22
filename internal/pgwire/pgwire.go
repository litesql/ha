package pgwire

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/lib/pq/oid"
	"github.com/litesql/go-ha"

	"github.com/litesql/ha/internal/sqlite"
)

const (
	transactionAttribute = "tx"
)

type Config struct {
	User    string
	Pass    string
	TLSCert string
	TLSKey  string
}

const columnWidth = 256

type Server struct {
	*wire.Server
}

func NewServer(cfg Config, db *sql.DB) (*Server, error) {
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

	wireServer, err := wire.NewServer(parseFn(db), opts...)
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

func parseFn(db *sql.DB) wire.ParseFn {
	return func(ctx context.Context, sql string) (statements wire.PreparedStatements, err error) {
		slog.InfoContext(ctx, "pg-wire: query received", "remote", wire.RemoteAddress(ctx), "sql", sql)

		upper := strings.ToUpper(strings.TrimSpace(sql))
		if strings.HasPrefix(upper, "-- PING") {
			statements = wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete("pong")
			}))
			return
		}
		if strings.HasPrefix(upper, "SET ") {
			statements = wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Complete("OK")
			}))
		}

		stmt, err := ha.ParseStatement(ctx, sql)
		if err != nil {
			err = psqlerr.WithCode(err, codes.SyntaxErrorOrAccessRuleViolation)
			return
		}

		switch {
		case stmt.Begin():
			err = begin(ctx, db)
			if err != nil {
				return
			}
			statements = wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Empty()
			}))
			return
		case stmt.Commit():
			err = commit(ctx)
			if err != nil {
				return
			}
			statements = wire.Prepared(wire.NewStatement(func(ctxHandler context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Empty()
			}))

			return
		case stmt.Rollback():
			err = rollback(ctx)
			if err != nil {
				return
			}
			statements = wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
				return writer.Empty()
			}))
			return
		}
		statements, err = handler(ctx, stmt, db)
		return

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
			Oid:   oid.T_text,
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
	parameters := make([]oid.Oid, len(bindParameters))
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
				Oid:   oid.T_text,
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
