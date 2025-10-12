package ha

import (
	"database/sql"
	"strings"

	"github.com/litesql/go-ha"
)

func Before(cs *ha.ChangeSet, db *sql.DB) (skip bool, err error) {
	return false, nil
}

func After(cs *ha.ChangeSet, db *sql.DB, err error) error {
	if err != nil {
		for _, change := range cs.Changes {
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(change.Command)), "ALTER TABLE") {
				// ignore ALTER TABLE errors
				return nil
			}
		}
	}
	return err
}
