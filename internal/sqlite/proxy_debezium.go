package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/litesql/debezium-sink/consumer"
	"github.com/litesql/go-ha"
)

func handleDebeziumProxiedChanges(db *sql.DB) consumer.HandlerFn {
	return func(changeset []consumer.Change, source map[string]any) error {
		ctx := ha.ContextLocalDB(context.Background(), true)
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var serverTime time.Time
		for _, change := range changeset {
			slog.Debug("received proxied db data", "change", change, "position", source)
			serverTime = change.ServerTime
			var sql string
			switch change.Kind {
			case "INSERT":
				sql = fmt.Sprintf("REPLACE INTO `%s` (%s) VALUES (%s)", change.Table, strings.Join(change.ColumnNames, ", "), placeholders(len(change.ColumnValues)))
				_, err = tx.ExecContext(ctx, sql, change.ColumnValues...)
			case "UPDATE":
				setClauses := make([]string, len(change.ColumnNames))
				for i, name := range change.ColumnNames {
					setClauses[i] = fmt.Sprintf("%s = ?", name)
				}
				var args []any
				args = append(args, change.ColumnValues...)
				whereClause := make([]string, len(change.OldKeys.KeyNames))
				for i, col := range change.OldKeys.KeyNames {
					if change.OldKeys.KeyValues[i] == nil {
						whereClause[i] = fmt.Sprintf("%s IS NULL", col)
					} else {
						whereClause[i] = fmt.Sprintf("%s = ?", col)
						args = append(args, change.OldKeys.KeyValues[i])
					}
				}
				sql = fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", change.Table, strings.Join(setClauses, ", "), strings.Join(whereClause, " AND "))
				_, err = tx.ExecContext(ctx, sql, args...)
			case "DELETE":
				whereClause := make([]string, len(change.ColumnNames))
				var args []any
				for i, col := range change.ColumnNames {
					if change.ColumnValues[i] == nil {
						whereClause[i] = fmt.Sprintf("%s IS NULL", col)
					} else {
						whereClause[i] = fmt.Sprintf("%s = ?", col)
						args = append(args, change.ColumnValues[i])
					}
				}
				sql = fmt.Sprintf("DELETE FROM `%s` WHERE %s", change.Table, strings.Join(whereClause, " AND "))
				_, err = tx.ExecContext(ctx, sql, args...)
			case "TRUNCATE":
				sql = fmt.Sprintf("DELETE FROM %s", change.Table)
				_, err = tx.ExecContext(ctx, sql)
			case "SQL":
				_, err = tx.ExecContext(ctx, change.SQL)
			}
			if err != nil {
				return fmt.Errorf("apply change: %s: %w", sql, err)
			}
		}
		position, _ := json.Marshal(source)
		_, err = tx.ExecContext(ctx, "REPLACE INTO ha_proxied_tracker(`rowid`, position, server_time) VALUES (1, ?, ?)", string(position), serverTime)
		if err != nil {
			return fmt.Errorf("update proxied tracker: %w", err)
		}
		return tx.Commit()
	}
}
