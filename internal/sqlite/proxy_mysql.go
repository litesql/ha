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

type mysqlPositionTracker struct {
	sourceDB  *sql.DB
	replicaDB *sql.DB
}

func (t *mysqlPositionTracker) SetReplicaDB(db *sql.DB) {
	t.replicaDB = db
}

func (t *mysqlPositionTracker) ReplicaPosition(ctx context.Context) (uint64, error) {
	if t.replicaDB == nil {
		return 0, nil
	}
	var positionJson string
	err := t.replicaDB.QueryRowContext(ctx, "SELECT position FROM ha_proxied_tracker WHERE rowid = 1").Scan(&positionJson)
	if err != nil {
		return 0, err
	}
	var position mysql.Position
	err = json.Unmarshal([]byte(positionJson), &position)
	slog.Debug("replica position", "pos", position.Pos)
	return uint64(position.Pos), nil
}

func (t *mysqlPositionTracker) SourcePosition(ctx context.Context) (uint64, error) {
	var position string
	err := t.sourceDB.QueryRowContext(ctx, "SELECT local FROM performance_schema.log_status LIMIT 1").Scan(&position)
	if err != nil {
		return 0, err
	}
	var logStatus mysqlLogStatus
	err = json.Unmarshal([]byte(position), &logStatus)
	if err != nil {
		return 0, err
	}
	slog.Debug("source position", "pos", logStatus.BinaryLogPosition)
	return logStatus.BinaryLogPosition, nil
}

type mysqlLogStatus struct {
	GTID              string `json:"gtid_executed"`
	BinaryLogFile     string `json:"binary_log_file"`
	BinaryLogPosition uint64 `json:"binary_log_position"`
}

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
			var query string
			switch change.Kind {
			case "INSERT":
				query = fmt.Sprintf("REPLACE INTO `%s` (%s) VALUES (%s)", change.Table, strings.Join(change.ColumnNames, ", "), placeholders(len(change.ColumnValues)))
				_, err = tx.ExecContext(ctx, query, change.ColumnValues...)
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
				query = fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", change.Table, strings.Join(setClauses, ", "), strings.Join(whereClause, " AND "))
				var res sql.Result
				res, err = tx.ExecContext(ctx, query, args...)
				if err == nil {
					affected, _ := res.RowsAffected()
					if affected == 0 {
						query = fmt.Sprintf("REPLACE INTO `%s` (%s) VALUES (%s)", change.Table, strings.Join(change.ColumnNames, ", "), placeholders(len(change.ColumnValues)))
						_, err = tx.ExecContext(ctx, query, change.ColumnValues...)
					}
				}
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
				query = fmt.Sprintf("DELETE FROM `%s` WHERE %s", change.Table, strings.Join(whereClause, " AND "))
				_, err = tx.ExecContext(ctx, query, args...)
			case "SQL":
				query = change.SQL
				_, err = tx.ExecContext(ctx, query)
			}
			if err != nil {
				return fmt.Errorf("apply change: %s: %w", query, err)
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
