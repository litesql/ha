package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type ChangeSet struct {
	Node      string   `json:"node"`
	Changes   []Change `json:"changes"`
	Timestamp int64    `json:"timestamp_ns"`
}

func NewChangeSet(node string) *ChangeSet {
	return &ChangeSet{
		Node: node,
	}
}

func (cs *ChangeSet) AddChange(change Change) {
	cs.Changes = append(cs.Changes, change)
}

func (cs *ChangeSet) Clear() {
	cs.Changes = nil
}

func (cs *ChangeSet) Send(pub CDCPublisher) error {
	if len(cs.Changes) == 0 || pub == nil {
		return nil
	}
	defer cs.Clear()

	slog.Info("Sending changeset", "changes", len(cs.Changes))

	cs.Timestamp = time.Now().UnixNano()
	cs.setTableColumns(db)

	return pub.Publish(cs)
}

func (cs *ChangeSet) Apply() error {
	if len(cs.Changes) == 0 {
		return nil
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	sconn, err := sqliteConn(conn)
	if err != nil {
		return err
	}
	disableCDCHooks(sconn)
	defer enableCDCHooks(sconn)

	cs.setTableColumns(conn)
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, change := range cs.Changes {
		var sql string
		switch change.Operation {
		case "INSERT":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?%d", col, i+1)
			}
			sql = fmt.Sprintf("INSERT INTO %s.%s (%s, rowid) VALUES (%s) ON CONFLICT (rowid) DO UPDATE SET %s;", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)+1), strings.Join(setClause, ", "))
			_, err = tx.Exec(sql, append(change.NewValues, change.NewRowID)...)
		case "UPDATE":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?", col)
			}
			sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE rowid = ?;", change.Database, change.Table, strings.Join(setClause, ", "))
			args := append(change.NewValues, change.OldRowID)
			_, err = tx.Exec(sql, args...)
		case "DELETE":
			sql = fmt.Sprintf("DELETE FROM %s.%s WHERE rowid = ?;", change.Database, change.Table)
			_, err = tx.Exec(sql, change.OldRowID)
		default:
			slog.Warn("unknown operation", "operation", change.Operation)
			continue
		}
		if err != nil {
			slog.Error("failed to apply change", "error", err, "operation", change.Operation, "table", change.Table)
			err = errors.Join(err, tx.Rollback())
			return err
		}
	}
	return tx.Commit()
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	for i := range n {
		b.WriteString(fmt.Sprintf("?%d,", i+1))
	}
	return strings.TrimRight(b.String(), ",")
}

func (cs *ChangeSet) setTableColumns(q querier) {
	tableColumns := make(map[string][]string)
	tableTypes := make(map[string][]string)
	for i, change := range cs.Changes {
		if len(change.Columns) > 0 {
			continue
		}
		if _, ok := tableColumns[change.Table]; !ok {
			columns, types, err := getTableColumnsAndTypes(q, change.Table)
			if err != nil {
				slog.Error("failed to get table columns", "table", change.Table, "error", err)
				continue
			}
			tableTypes[change.Table] = types
			tableColumns[change.Table] = columns
			cs.Changes[i].Columns = columns
		} else {
			cs.Changes[i].Columns = tableColumns[change.Table]
		}
		types := tableTypes[change.Table]
		for j, ctype := range types {
			if strings.Contains(ctype, "TEXT") || strings.Contains(ctype, "CHAR") ||
				strings.Contains(ctype, "CLOB") || strings.Contains(ctype, "JSON") ||
				strings.Contains(ctype, "DATE") || strings.Contains(ctype, "TIME") {
				if len(change.OldValues) > 0 && j < len(change.OldValues) {
					change.OldValues[j] = convert(change.OldValues[j])
				}
				if len(change.NewValues) > 0 && j < len(change.NewValues) {
					change.NewValues[j] = convert(change.NewValues[j])
				}
			}
		}
	}
}

func getTableColumnsAndTypes(q querier, table string) ([]string, []string, error) {
	rows, err := q.QueryContext(context.Background(), "SELECT name, type FROM PRAGMA_table_info(?)", table)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var columns, types []string
	for rows.Next() {
		var name, ctype string
		if err := rows.Scan(&name, &ctype); err != nil {
			return nil, nil, err
		}
		columns = append(columns, name)
		types = append(types, ctype)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return columns, types, nil
}

type Change struct {
	Database  string   `json:"database"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	Operation string   `json:"operation"` // "INSERT", "UPDATE", "DELETE"
	OldRowID  int64    `json:"old_rowid,omitempty"`
	NewRowID  int64    `json:"new_rowid,omitempty"`
	OldValues []any    `json:"old_values,omitempty"`
	NewValues []any    `json:"new_values,omitempty"`
}

func convert(src any) string {
	switch v := src.(type) {
	case []byte:
		var dst []byte
		n, err := base64.StdEncoding.Decode(dst, v)
		if err != nil {
			slog.Warn("converter from []byte", "error", err)
			return string(v)
		}
		return string(dst[0:n])
	case string:
		dst, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			slog.Error("converter from string", "error", err)
			return v
		}
		return string(dst)
	default:
		return fmt.Sprintf("%s", src)
	}

}
