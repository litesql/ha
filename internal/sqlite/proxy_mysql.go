package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/litesql/go-ha"
	"github.com/litesql/mysql/replication"
)

func handleMysqlProxiedChanges(db *sql.DB) replication.HandleChanges {
	return func(changeset []replication.Change, currentPosition mysql.Position) error {
		ctx := ha.ContextLocalDB(context.Background(), true)
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var serverTime time.Time
		for _, change := range changeset {
			slog.Debug("received proxied db data", "change", change)
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
			case "SQL":
				_, err = tx.ExecContext(ctx, change.SQL)
			}
			if err != nil {
				return fmt.Errorf("apply change: %s: %w", sql, err)
			}
		}
		positionJson, _ := json.Marshal(currentPosition)
		_, err = tx.ExecContext(ctx, "REPLACE INTO ha_proxied_tracker(`rowid`, position, server_time) VALUES (1, ?, ?)", string(positionJson), serverTime)
		if err != nil {
			return fmt.Errorf("update proxied tracker: %w", err)
		}
		return tx.Commit()
	}
}

func mysqlCheckpointLoader(db *sql.DB) replication.CheckpointLoader {
	return func() (mysql.Position, error) {
		var positionJson string
		err := db.QueryRowContext(ha.ContextLocalDB(context.Background(), true), "SELECT position FROM ha_proxied_tracker WHERE rowid = 1").Scan(&positionJson)
		if err != nil {
			if err == sql.ErrNoRows {
				return mysql.Position{}, nil
			}
			return mysql.Position{}, fmt.Errorf("load checkpoint: %w", err)
		}
		var position mysql.Position
		err = json.Unmarshal([]byte(positionJson), &position)
		return position, err
	}
}
