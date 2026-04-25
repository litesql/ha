package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/litesql/go-ha"
	haconnect "github.com/litesql/go-ha/connect"

	"github.com/litesql/ha/internal/sqlite"
)

type Handler struct {
	connector             *ha.Connector
	db                    *sql.DB
	tx                    *sql.Tx
	dbProvider            DBProvider
	connectorProvider     ConnectorProvider
	createDatabaseOptions CreateDatabaseOptions
}

type DBProvider func(dbName string) (*sql.DB, bool)

type ConnectorProvider func(dbName string) (*ha.Connector, bool)

func (h *Handler) UseDB(dbName string) error {
	slog.Debug("Received: UseDB", "dbname", dbName)
	db, ok := h.dbProvider(dbName)
	if ok {
		h.db = db
	}
	connector, ok := h.connectorProvider(dbName)
	if ok {
		h.connector = connector
	}

	return nil
}

var (
	reComments = regexp.MustCompile(`(?s)//.*?\\n|/\\*.*?\\*/`)
	reUndo     = regexp.MustCompile(`(?i)^UNDO(\s|E|T)\s*([^;\s]+)`)
)

func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
	slog.Debug("Received: Query", "query", query)
	cleanQuery := reComments.ReplaceAllString(query, "")
	keepCaseQuery := strings.TrimSpace(cleanQuery)
	cleanQuery = strings.ToUpper(keepCaseQuery)
	// These queries are implemented for minimal support for MySQL Shell
	if len(cleanQuery) > 4 && strings.HasPrefix(strings.ToUpper(cleanQuery[0:4]), "SET ") {
		return mysql.NewResultReserveResultset(0), nil
	}
	if cleanQuery == `select concat(@@version, ' ', @@version_comment)` {
		r, err := mysql.BuildSimpleResultset([]string{"concat(@@version, ' ', @@version_comment)"}, [][]any{
			{"8.4.7"},
		}, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(r), nil
	}

	if strings.HasPrefix(cleanQuery, "USE ") {
		dbName := strings.ReplaceAll(strings.TrimSpace(query[strings.Index(query, "use ")+4:]), "`", "")
		return nil, h.UseDB(dbName)
	}

	if cleanQuery == "SHOW DATABASES" {
		dbs := sqlite.Databases()
		vals := make([][]any, 0, len(dbs))
		for _, db := range dbs {
			vals = append(vals, []any{db})
		}
		resultSet, err := mysql.BuildSimpleResultset([]string{"Database"}, vals, false)
		if err != nil {
			slog.Debug("BuildSimpleResultset error", "error", err)
			return nil, err
		}
		return mysql.NewResult(resultSet), nil
	}

	if cleanQuery == "SHOW TABLES" {
		rows, err := h.query("SELECT name as tables FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
		if err != nil {
			slog.Debug("Query error", "error", err)
			return nil, err
		}
		resultSet, err := rowsToResultset(rows, false)
		if err != nil {
			slog.Debug("rowsToResultset error", "error", err)
			return nil, err
		}
		return mysql.NewResult(resultSet), nil
	}

	if strings.HasPrefix(cleanQuery, "CREATE DATABASE ") {
		if !h.createDatabaseOptions.MemDB && h.createDatabaseOptions.Dir == "" {
			return nil, fmt.Errorf("create database is disabled, inform flag --create-db-dir at startup")
		}
		dsn := strings.TrimSpace(keepCaseQuery[16:])
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
		destPath = filepath.Join(h.createDatabaseOptions.Dir, filepath.Base(destPath))
		dsn = fmt.Sprintf("file:%s%s", destPath, params)

		err := sqlite.Load(context.Background(), dsn, h.createDatabaseOptions.MemDB, h.createDatabaseOptions.FromLatestSnapshot,
			h.createDatabaseOptions.DeliverPolicy, h.createDatabaseOptions.MaxConns, h.createDatabaseOptions.Opts...)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(nil), nil
	}
	if strings.HasPrefix(cleanQuery, "DROP DATABASE ") {
		id := strings.TrimSpace(keepCaseQuery[14:])
		id = strings.TrimSuffix(id, ";")

		dbfile, err := sqlite.Drop(context.Background(), id)
		if dbfile != "" {
			err = os.Remove(dbfile)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("failed to remove database file: %v", err)
			}
			os.Remove(dbfile + "-shm")
			os.Remove(dbfile + "-wal")
		}
		return mysql.NewResult(nil), nil
	}

	if strings.HasPrefix(cleanQuery, "UNDO") {
		reUndoMatch := reUndo.FindStringSubmatch(keepCaseQuery)
		if len(reUndoMatch) != 3 {
			return nil, fmt.Errorf("invalid undo syntax")
		}
		var undoType haconnect.UndoFilter
		switch strings.TrimSpace(reUndoMatch[1]) {
		case "E", "e":
			undoType = haconnect.UndoFilterEntity
		case "T", "t":
			undoType = haconnect.UndoFilterNone
		default:
			undoType = haconnect.UndoFilterNone
		}

		undoParam := strings.TrimSpace(reUndoMatch[2])
		undoParam = strings.TrimSuffix(undoParam, ";")
		seq, err := strconv.Atoi(undoParam)
		if err != nil {
			if undoType != haconnect.UndoFilterNone {
				return nil, fmt.Errorf("invalid undo syntax: cannot specify undo type without a valid sequence or duration")
			}
			duration, err := time.ParseDuration(undoParam)
			if err != nil {
				return nil, fmt.Errorf("invalid undo argument: %v", err)
			}
			if duration <= 0 {
				return nil, fmt.Errorf("duration must be greater than 0")
			}
			err = h.connector.UndoByTime(context.Background(), duration, haconnect.UndoFilterNone, nil)
			if err != nil {
				return nil, fmt.Errorf("undo failed: %v", err)
			}
			return mysql.NewResult(nil), nil
		}
		if seq < 0 {
			return nil, fmt.Errorf("sequence must be a non-negative integer")
		}
		err = h.connector.UndoBySeq(context.Background(), uint64(seq), undoType, nil)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(nil), nil
	}

	if strings.HasPrefix(cleanQuery, "HISTORY ") {
		historyParam := strings.TrimSpace(keepCaseQuery[8:])
		historyParam = strings.TrimSuffix(historyParam, ";")
		var items []haconnect.HistoryItem
		seq, err := strconv.Atoi(historyParam)
		if err != nil {
			duration, err := time.ParseDuration(historyParam)
			if err != nil {
				return nil, fmt.Errorf("invalid history argument: %v", err)
			}
			if duration <= 0 {
				return nil, fmt.Errorf("duration must be greater than 0")
			}
			items, err = h.connector.HistoryByTime(context.Background(), duration)
			if err != nil {
				return nil, fmt.Errorf("history retrieval failed: %v", err)
			}
		} else {
			if seq < 0 {
				return nil, fmt.Errorf("sequence must be a non-negative integer")
			}
			items, err = h.connector.HistoryBySeq(context.Background(), uint64(seq))
			if err != nil {
				return nil, fmt.Errorf("history retrieval failed: %v", err)
			}
		}
		vals := make([][]any, 0, len(items))
		for _, item := range items {
			sqls := strings.Join(item.SQL, ";\n")
			vals = append(vals, []any{item.Seq, sqls, time.Unix(0, item.Timestamp).Format(time.RFC3339Nano)})
		}
		resultSet, err := mysql.BuildSimpleResultset([]string{"Sequence", "SQL", "Timestamp"}, vals, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(resultSet), nil
	}

	if isSelect(cleanQuery) {
		rows, err := h.query(query)
		if err != nil {
			slog.Debug("Query error", "error", err)
			return nil, err
		}
		resultSet, err := rowsToResultset(rows, false)
		if err != nil {
			slog.Debug("rowsToResultset error", "error", err)
			return nil, err
		}
		return mysql.NewResult(resultSet), nil
	}

	res, err := h.exec(query)
	if err != nil {
		slog.Debug("Exec error", "error", err)
		return nil, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		slog.Debug("RowsAffected error", "error", err)
		return nil, err
	}
	lastInsertID, err := res.LastInsertId()
	if err != nil {
		slog.Debug("LastInsertId error", "error", err)
		return nil, err
	}
	result := mysql.NewResultReserveResultset(0)
	result.AffectedRows = uint64(affected)
	result.InsertId = uint64(lastInsertID)
	return result, nil
}

// HandleFieldList is called for COM_FIELD_LIST packets
// Note that COM_FIELD_LIST has been deprecated since MySQL 5.7.11
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_field_list.html
func (h *Handler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	slog.Debug("Received: FieldList", "table", table, "fieldWildcard", fieldWildcard)
	return nil, fmt.Errorf("not supported now")
}

func (h *Handler) HandleStmtPrepare(query string) (int, int, any, error) {
	slog.Debug("Received: StmtPrepare", "query", query)
	if h.db == nil {
		return 0, 0, nil, fmt.Errorf("no database selected")
	}
	stmt, err := h.db.Prepare(query)
	if err != nil {
		return 0, 0, nil, err
	}
	if h.tx != nil {
		stmt = h.tx.Stmt(stmt)
	}
	params := strings.Count(query, "?")
	columns := 0
	return params, columns, stmt, nil
}

func (h *Handler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	slog.Debug("Received: StmtExecute", "query", query, "args", args, "context", context)
	switch stmt := context.(type) {
	case *sql.Stmt:
		if isSelect(query) {
			rows, err := stmt.Query(args...)
			if err != nil {
				return nil, err
			}
			resultSet, err := rowsToResultset(rows, true)
			if err != nil {
				return nil, err
			}
			return mysql.NewResult(resultSet), nil
		}
		res, err := stmt.Exec(args...)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		lastInsertID, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}
		result := mysql.NewResultReserveResultset(0)
		result.AffectedRows = uint64(affected)
		result.InsertId = uint64(lastInsertID)
		return result, nil
	default:
		return nil, fmt.Errorf("unknown statement context type")
	}
}

func (h *Handler) HandleStmtClose(context any) error {
	slog.Debug("Received: StmtClose", "context", context)
	switch stmt := context.(type) {
	case *sql.Stmt:
		return stmt.Close()
	default:
		return fmt.Errorf("unknown statement context type")
	}
}

func (h *Handler) HandleOtherCommand(cmd byte, data []byte) error {
	slog.Warn("Received: OtherCommand", "cmd", cmd, "data", data)
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}

type sqlResult struct {
	affectedRows uint64
	insertId     uint64
}

func (r *sqlResult) LastInsertId() (int64, error) {
	return int64(r.insertId), nil
}

func (r *sqlResult) RowsAffected() (int64, error) {
	return int64(r.affectedRows), nil
}

func (h *Handler) exec(query string) (sql.Result, error) {
	if strings.HasPrefix(strings.ToUpper(query), "BEGIN") {
		if h.tx != nil {
			return nil, fmt.Errorf("transaction already started")
		}
		if h.db == nil {
			return nil, fmt.Errorf("no database selected")
		}
		tx, err := h.db.Begin()
		if err != nil {
			return nil, err
		}
		h.tx = tx
	}
	if strings.HasPrefix(strings.ToUpper(query), "COMMIT") {
		if h.tx == nil {
			return nil, fmt.Errorf("no transaction started")
		}
		err := h.tx.Commit()
		if err != nil {
			return nil, err
		}
		h.tx = nil
		return &sqlResult{}, nil
	}
	if strings.HasPrefix(strings.ToUpper(query), "ROLLBACK") {
		if h.tx == nil {
			return nil, fmt.Errorf("no transaction started")
		}
		err := h.tx.Rollback()
		if err != nil {
			return nil, err
		}
		h.tx = nil
		return &sqlResult{}, nil
	}

	if h.tx != nil {
		return h.tx.Exec(query)
	}
	if h.db == nil {
		return nil, fmt.Errorf("no database selected")
	}
	return h.db.Exec(query)
}

func (h *Handler) query(query string) (*sql.Rows, error) {
	if h.tx != nil {
		return h.tx.Query(query)
	}
	if h.db == nil {
		return nil, fmt.Errorf("no database selected")
	}
	return h.db.Query(query)
}

func rowsToResultset(rows *sql.Rows, binary bool) (*mysql.Resultset, error) {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columns := make([]any, len(cols))
	columnPointers := make([]any, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}
	vals := make([][]any, 0)
	for rows.Next() {
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		row := make([]any, len(cols))
		for i, c := range columnPointers {
			if c == nil {
				row[i] = nil
				continue
			}
			row[i] = *c.(*any)
		}
		vals = append(vals, row)
	}
	return mysql.BuildSimpleResultset(cols, vals, binary)
}

func isSelect(query string) bool {
	if len(query) > 6 {
		return strings.HasPrefix(strings.ToLower(query), "select")
	}
	return false
}
