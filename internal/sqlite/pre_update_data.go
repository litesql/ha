package sqlite

import (
	"fmt"

	"github.com/mattn/go-sqlite3"
)

func getChange(d *sqlite3.SQLitePreUpdateData) (c Change, ok bool) {
	ok = true
	c = Change{
		Database: d.DatabaseName,
		Table:    d.TableName,
		OldRowID: d.OldRowID,
		NewRowID: d.NewRowID,
	}
	count := d.Count()
	if d.Op == sqlite3.SQLITE_UPDATE {
		c.Operation = "UPDATE"
		c.OldValues = make([]any, count)
		c.NewValues = make([]any, count)
		for i := range count {
			c.OldValues[i] = &c.OldValues[i]
			c.NewValues[i] = &c.NewValues[i]
		}
		d.Old(c.OldValues...)
		d.New(c.NewValues...)
	} else if d.Op == sqlite3.SQLITE_INSERT {
		c.Operation = "INSERT"
		c.NewValues = make([]any, count)
		for i := range count {
			c.NewValues[i] = &c.NewValues[i]
		}
		d.New(c.NewValues...)
	} else if d.Op == sqlite3.SQLITE_DELETE {
		c.Operation = "DELETE"
		c.OldValues = make([]any, count)
		for i := range count {
			c.OldValues[i] = &c.OldValues[i]
		}
		d.Old(c.OldValues...)
	} else {
		c.Operation = fmt.Sprintf("UNKNOWN - %d", d.Op)
	}

	return
}
