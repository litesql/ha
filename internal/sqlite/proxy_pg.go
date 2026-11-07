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
	err := t.replicaDB.QueryRowContext(ha.ContextLocalDB(ctx, true), "SELECT position FROM ha_proxied_tracker LIMIT 1").Scan(&position)
	if err != nil {
		return 0, err
	}
	return uint64(position), nil
}

func (t *pgPositionTracker) SourcePosition(ctx context.Context) (uint64, error) {
	var position pglogrepl.LSN
	err := t.sourceDB.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()").Scan(&position)
	if err != nil {
		return 0, err
	}
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
			err = applyProxiedChange(ctx, tx, proxiedChange{
				Kind:         change.Kind,
				Table:        change.Table,
				ColumnNames:  change.ColumnNames,
				ColumnValues: change.ColumnValues,
				OldKeyNames:  change.OldKeys.KeyNames,
				OldKeyValues: change.OldKeys.KeyValues,
				SQL:          change.SQL,
			})
			if err != nil {
				return err
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
