package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/litesql/go-ha"
	"github.com/litesql/postgresql/replication"
)

type baseProxiedPositionTracker interface {
	ha.ProxiedPositionProvider
	SetReplicaDB(*sql.DB)
}

type pgPositionTracker struct {
	sourceDB  *sql.DB
	replicaDB *sql.DB
}

func (t *pgPositionTracker) SetReplicaDB(db *sql.DB) {
	t.replicaDB = db
}

func (t *pgPositionTracker) ReplicaPosition(ctx context.Context) (uint64, error) {
	if t.replicaDB == nil {
		return 0, nil
	}
	var position pglogrepl.LSN
	err := t.replicaDB.QueryRowContext(ctx, "SELECT position FROM ha_proxied_tracker LIMIT 1").Scan(&position)
	if err != nil {
		return 0, err
	}
	slog.Warn("replica position", "pos", position)
	return uint64(position), nil
}

func (t *pgPositionTracker) SourcePosition(ctx context.Context) (uint64, error) {
	var position pglogrepl.LSN
	err := t.sourceDB.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()").Scan(&position)
	if err != nil {
		return 0, err
	}
	slog.Warn("source position", "pos", position)
	return uint64(position), nil
}

func handlePgProxiedChanges(db *sql.DB) replication.HandleChanges {
	return func(changeset []replication.Change, currentPosition pglogrepl.LSN) error {
		ctx := ha.ContextLocalDB(context.Background(), true)
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var serverTime time.Time
		for _, change := range changeset {
			slog.Debug("received proxied db data", "change", change, "currentPosition", currentPosition)
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
		_, err = tx.ExecContext(ctx, "REPLACE INTO ha_proxied_tracker(`rowid`, position, server_time) VALUES (1, ?, ?)", currentPosition, serverTime)
		if err != nil {
			return fmt.Errorf("update proxied tracker: %w", err)
		}
		return tx.Commit()
	}
}

func pgCheckpointLoader(db *sql.DB) replication.CheckpointLoader {
	return func() (pglogrepl.LSN, error) {
		var position string
		err := db.QueryRowContext(ha.ContextLocalDB(context.Background(), true), "SELECT position FROM ha_proxied_tracker WHERE rowid = 1").Scan(&position)
		if err != nil {
			if err == sql.ErrNoRows {
				return 0, nil
			}
			return 0, fmt.Errorf("load checkpoint: %w", err)
		}
		return pglogrepl.ParseLSN(position)
	}
}

func placeholders(count int) string {
	var b strings.Builder
	for i := range count {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("?")
	}
	return b.String()
}
