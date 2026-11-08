package clickhouse

import (
	"context"
	"fmt"
)

// ResyncMarker is the comment of the shadow table of a resync. EXCHANGE TABLES moves the comment with
// the table, so which of two tables is the new one is told by the comment, and a swap that a crash
// interrupted can be finished without guessing.
const ResyncMarker = "binwave: resync shadow"

// SwapRequest is the replacement of a table by its shadow table, already decided.
type SwapRequest struct {
	Source   string
	Database string
	Table    string
	Shadow   string
	// DryRun logs the statements instead of executing them.
	DryRun bool
}

// tableComment reads the comment of a table; exists is false for a table that is not there.
func (c *Client) tableComment(ctx context.Context, database, table string) (comment string, exists bool, err error) {
	rows, err := c.conn.Query(ctx, "SELECT comment FROM system.tables WHERE database = ? AND name = ?", database, table)
	if err != nil {
		return "", false, fmt.Errorf("read the table %s.%s: %w", database, table, err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		if err := rows.Scan(&comment); err != nil {
			return "", false, fmt.Errorf("read the table %s.%s: %w", database, table, err)
		}
		exists = true
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("read the table %s.%s: %w", database, table, err)
	}
	return comment, exists, nil
}

// SwapShadow puts the shadow table of a resync in the place of the table, and drops what was there.
// Every step can be repeated after a crash, and it is decided from what ClickHouse holds: the shadow table
// has the marker in its comment, and the marker travels with the table.
//
//   - the shadow table has the marker and the table does not: they are exchanged (or, when the table
//     does not exist, the shadow table is renamed to its name);
//   - the table has the marker: the swap is done, and what is left is to drop the old table, which
//     now has the shadow table's name, and to remove the marker;
//   - there is no shadow table but there is the table: the swap was finished before (see below);
//   - neither exists: nothing to swap in, which is an error.
func (c *Client) SwapShadow(ctx context.Context, req SwapRequest) error {
	table := quote(req.Database) + "." + quote(req.Table)
	shadow := quote(req.Database) + "." + quote(req.Shadow)

	tComment, tExists, err := c.tableComment(ctx, req.Database, req.Table)
	if err != nil {
		return err
	}
	sComment, sExists, err := c.tableComment(ctx, req.Database, req.Shadow)
	if err != nil {
		return err
	}

	switch {
	case tExists && tComment == ResyncMarker:
		// The swap was made; clean up.
	case sExists && sComment == ResyncMarker && tExists:
		if req.DryRun {
			c.log.Info("dry run: statement not executed", "statement", "EXCHANGE TABLES "+table+" AND "+shadow)
			return nil
		}
		if err := c.exec(ctx, "EXCHANGE TABLES "+table+" AND "+shadow); err != nil {
			return c.classifyResync(req, err)
		}
	case sExists && sComment == ResyncMarker:
		if req.DryRun {
			c.log.Info("dry run: statement not executed", "statement", "RENAME TABLE "+shadow+" TO "+table)
			return nil
		}
		if err := c.exec(ctx, "RENAME TABLE "+shadow+" TO "+table); err != nil {
			return c.classifyResync(req, err)
		}
	case !sExists && tExists:
		// A swap that was finished, and whose end (the position saved) a crash interrupted, is found like
		// this: the marker is gone with the shadow table. It cannot be told from a shadow table that
		// somebody dropped, which is why it is said in the log.
		c.log.Warn("swap skipped: no shadow table",
			"table", table, "shadow", shadow)
		return nil
	default:
		return &ConflictError{Err: fmt.Errorf("the shadow table %s of the resync of %s is not there or is not the one binwave made, "+
			"so there is nothing to put in the place of %s; nothing was changed", shadow, req.Source, table)}
	}

	if req.DryRun {
		return nil
	}
	c.log.Info("table swapped in", "table", table)
	if err := c.exec(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
		return c.classifyResync(req, err)
	}
	if err := c.exec(ctx, "ALTER TABLE "+table+" MODIFY COMMENT ''"); err != nil {
		return c.classifyResync(req, err)
	}
	return nil
}

func (c *Client) classifyResync(req SwapRequest, err error) error {
	if IsTransient(err) {
		return err
	}
	if _, permanent := Permanent(err); permanent {
		return err
	}
	return &TableError{Table: req.Source, Err: fmt.Errorf("replace the table by its resynchronized copy: %w", err)}
}
