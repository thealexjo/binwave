package clickhouse

import (
	"context"
	"fmt"
)

// SwapRequest is the replacement of a table by its shadow table, already decided.
type SwapRequest struct {
	Source   string
	Database string
	Table    string
	Shadow   string
	// Copy is the UUID of the shadow table an earlier attempt sent an exchange for, whose answer was lost
	// (what SwapShadow returned): only a table of that UUID in the place of the table, without the marker,
	// is a swap that was finished.
	Copy string
	// ShadowUUID is the UUID the resync made its shadow table with (empty in a resync saved before it was
	// kept): only a table of that UUID is the shadow table.
	ShadowUUID string
}

// tableComment reads the comment of a table, empty for a table that is not there.
func (c *Client) tableComment(ctx context.Context, database, table string) (string, error) {
	got, err := clientOps{c}.read(ctx, database, table)
	return got[table].comment, err
}

// SwapShadow puts the shadow table of a resync in the place of the table, and drops what was there
// (swapCopy). Every step can be repeated after a crash. A shadow table that is gone before an exchange
// may have been made is an error of the table; a shadow table that is not binwave's, neither table there,
// or another table than the copy in the place of the table after an exchange, a conflict.
//
// copied is the UUID of the shadow table an exchange may have been made with, whatever its answer, and
// empty when none may have been: a repeat of the swap passes it on as req.Copy.
func (c *Client) SwapShadow(ctx context.Context, req SwapRequest) (copied string, err error) {
	table := quote(req.Database) + "." + quote(req.Table)
	shadow := quote(req.Database) + "." + quote(req.Shadow)
	copied, outcome, err := swapCopy(ctx, clientOps{c}, c.log, swapSpec{
		database: req.Database, table: req.Table, copy: req.Shadow, marker: ResyncMarker(req.Table),
		copied: req.Copy, copyUUID: req.ShadowUUID,
	})
	if err != nil {
		return copied, c.classifyResync(req, err)
	}
	switch outcome {
	case swapLost:
		return "", &TableError{Table: req.Source, Recoverable: true, Err: fmt.Errorf(
			"the shadow table %s of the resync is gone, so there is nothing to put in the place of %s; nothing was changed", shadow, table)}
	case swapNotTheCopy:
		return copied, &ConflictError{Err: fmt.Errorf("the table %s is not the copy binwave made, after an exchange with it may have been made: "+
			"another table took the name of %s during the swap; nothing more is changed, see which table is which in ClickHouse", table, shadow)}
	case swapNoCopy:
		return copied, &ConflictError{Err: fmt.Errorf("the shadow table %s of the resync of %s is not there or is not the one binwave made, "+
			"so there is nothing to put in the place of %s; nothing was changed", shadow, req.Source, table)}
	}
	return copied, nil
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
	// Swapped: the table is the shadow table, by its UUID: the exchange was made, and with no marker on the
	// table the swap was finished.
	Swapped bool
}

// SwapMarks reads which of the table and its shadow table carries the marker of the resync. Like the
// swap, it waits while an exchange of the table runs: what it reads is then not undone by one.
func (c *Client) SwapMarks(ctx context.Context, req SwapRequest) (SwapMarks, error) {
	ops := clientOps{c}
	if err := waitExchanges(ctx, ops, req.Database, req.Table); err != nil {
		return SwapMarks{}, err
	}
	got, err := ops.read(ctx, req.Database, req.Table, req.Shadow)
	if err != nil {
		return SwapMarks{}, err
	}
	marker := ResyncMarker(req.Table)
	t, s := got[req.Table], got[req.Shadow]
	return SwapMarks{
		Table: t.exists && isMarker(t.comment, marker), Shadow: isCopy(s, marker, req.ShadowUUID), ShadowExists: s.exists,
		Swapped: req.ShadowUUID != "" && t.exists && t.uuid == req.ShadowUUID,
	}, nil
}

// DropShadow drops the shadow table of a resync if it is binwave's, by its identity, and what drops a
// crash interrupted left in the database (dropCopy). dropped is false when there was nothing of
// binwave's to drop.
func (c *Client) DropShadow(ctx context.Context, req SwapRequest) (dropped bool, err error) {
	return dropCopy(ctx, clientOps{c}, c.log, swapSpec{
		database: req.Database, table: req.Table, copy: req.Shadow, marker: ResyncMarker(req.Table), copyUUID: req.ShadowUUID,
	})
}

func (c *Client) classifyResync(req SwapRequest, err error) error {
	if IsTransient(err) || IsConflict(err) {
		return err
	}
	if _, permanent := Permanent(err); permanent {
		return err
	}
	return &TableError{Table: req.Source, Err: fmt.Errorf("replace the table by its resynchronized copy: %w", err)}
}
