package clickhouse

import (
	"context"
	"errors"
	"fmt"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// SwapRequest is the replacement of a table by its shadow table, already decided.
type SwapRequest struct {
	Source   string
	Database string
	Table    string
	Shadow   string
	// Exchanged says the exchange may have been made already: an earlier attempt sent it, or a restart
	// found the marker on the table. Only then is a shadow table that is gone a swap that was finished.
	Exchanged bool
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
//   - there is no shadow table but there is the table: the swap was finished before, if it may have been
//     made (req.Exchanged), and otherwise the shadow table was lost, which is an error of the table;
//   - neither exists: nothing to swap in, which is an error.
//
// sent reports that the exchange (or the rename) was sent, whatever its answer: a repeat of the swap
// passes it on as req.Exchanged.
func (c *Client) SwapShadow(ctx context.Context, req SwapRequest) (sent bool, err error) {
	table := quote(req.Database) + "." + quote(req.Table)
	shadow := quote(req.Database) + "." + quote(req.Shadow)

	tComment, tExists, err := c.tableComment(ctx, req.Database, req.Table)
	if err != nil {
		return false, err
	}
	sComment, sExists, err := c.tableComment(ctx, req.Database, req.Shadow)
	if err != nil {
		return false, err
	}

	// EXCHANGE TABLES moves the marker with the table, so which of two tables is the new one is told by
	// the marker, and a swap that a crash interrupted is finished without guessing.
	marker := ResyncMarker(req.Table)
	switch {
	case tExists && isMarker(tComment, marker):
		// The swap was made; clean up.
	case sExists && isMarker(sComment, marker) && req.Exchanged:
		// An exchange sent before, whose answer was lost, may still be running on the server: sent again,
		// it would exchange the tables back. Whether it was made is for a restart to read.
		return true, fmt.Errorf("the exchange of %s and %s was sent and its answer lost, and it is not made yet; "+
			"it is not sent again", table, shadow)
	case sExists && isMarker(sComment, marker) && tExists:
		if req.DryRun {
			c.log.Info("dry run: statement not executed", "statement", "EXCHANGE TABLES "+table+" AND "+shadow)
			return false, nil
		}
		if err := c.exec(ctx, "EXCHANGE TABLES "+table+" AND "+shadow); err != nil {
			return !answered(err), c.classifyResync(req, err)
		}
	case sExists && isMarker(sComment, marker):
		if req.DryRun {
			c.log.Info("dry run: statement not executed", "statement", "RENAME TABLE "+shadow+" TO "+table)
			return false, nil
		}
		if err := c.exec(ctx, "RENAME TABLE "+shadow+" TO "+table); err != nil {
			return !answered(err), c.classifyResync(req, err)
		}
	case !sExists && tExists && req.Exchanged:
		// A swap that was finished, and repeated because its answer was lost, is found like this: the
		// marker is gone with the shadow table.
		c.log.Warn("swap skipped: no shadow table",
			"table", table, "shadow", shadow)
		return false, nil
	case !sExists && tExists:
		// Nothing of this swap was made, and what was to take the table's place is gone.
		return false, &TableError{Table: req.Source, Recoverable: true, Err: fmt.Errorf(
			"the shadow table %s of the resync is gone, so there is nothing to put in the place of %s; nothing was changed", shadow, table)}
	default:
		return false, &ConflictError{Err: fmt.Errorf("the shadow table %s of the resync of %s is not there or is not the one binwave made, "+
			"so there is nothing to put in the place of %s; nothing was changed", shadow, req.Source, table)}
	}

	// From here the exchange is made: what is left is the cleanup.
	if req.DryRun {
		return true, nil
	}
	c.log.Info("table swapped in", "table", table)
	if err := c.exec(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
		return true, c.classifyResync(req, err)
	}
	if err := c.exec(ctx, "ALTER TABLE "+table+" MODIFY COMMENT ''"); err != nil {
		return true, c.classifyResync(req, err)
	}
	return true, nil
}

// answered reports whether the server answered a statement with an error: it was refused, not made.
// Any other failure (a timeout, a broken connection) leaves open whether it was made.
func answered(err error) bool {
	var exception *ch.Exception
	return errors.As(err, &exception)
}

// SwapMarks says which of the table and its shadow table carries the marker of the resync.
type SwapMarks struct {
	// Table: the exchange of the swap was made, EXCHANGE TABLES moved the marker to the table. The name
	// of the shadow table then holds the old table, or nothing once it is dropped.
	Table bool
	// Shadow: the shadow table is the one binwave made for the resync, and was not swapped in.
	Shadow bool
	// ShadowExists: a table has the name of the shadow table, binwave's or not.
	ShadowExists bool
}

// SwapMarks reads which of the table and its shadow table carries the marker of the resync.
func (c *Client) SwapMarks(ctx context.Context, req SwapRequest) (SwapMarks, error) {
	marker := ResyncMarker(req.Table)
	tComment, tExists, err := c.tableComment(ctx, req.Database, req.Table)
	if err != nil {
		return SwapMarks{}, err
	}
	sComment, sExists, err := c.tableComment(ctx, req.Database, req.Shadow)
	if err != nil {
		return SwapMarks{}, err
	}
	return SwapMarks{Table: tExists && isMarker(tComment, marker), Shadow: sExists && isMarker(sComment, marker), ShadowExists: sExists}, nil
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
