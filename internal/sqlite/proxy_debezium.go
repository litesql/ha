package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
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
		position, _ := json.Marshal(source)
		_, err = tx.ExecContext(ctx, "REPLACE INTO ha_proxied_tracker(`rowid`, position, server_time) VALUES (1, ?, ?)", string(position), serverTime)
		if err != nil {
			return fmt.Errorf("update proxied tracker: %w", err)
		}
		return tx.Commit()
	}
}
