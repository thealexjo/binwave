package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// The swap of a copy (the shadow table of a resync, the copy of a rebuild) into the place of a table is
// decided step by step from what ClickHouse holds, and two rules make every step safe to repeat:
//
//   - nothing is read or done while an exchange of the table may still run. Every exchange of a table is
//     sent with the same query id (ExchangeQueryID), and a swap first makes sure no query of that id runs:
//     an exchange whose answer was lost, by this process or by one that crashed, is over before anything
//     is read, and what is read stays true until this swap's own exchange;
//   - a table is dropped only by its identity: it is renamed to a name made of its own UUID, which nothing
//     else uses, and dropped there once the UUID is checked. A table that took the name in between is
//     never dropped: it is renamed back;
//
// and what is in the place of the table is the copy only by its identity: the marker on it names the UUID
// of the copy, or its UUID is that of the copy an earlier attempt exchanged.
//
// A dry run is the swap run on planOps, a model of the tables that logs every query instead of sending
// it: the plan it logs is the algorithm itself.

// ExchangeQueryID is the query id every EXCHANGE TABLES of a table is sent with. ClickHouse refuses a
// query while another of the same id runs, so no two exchanges of a table ever run at once.
func ExchangeQueryID(database, table string) string {
	return "binwave-exchange-" + database + "." + table
}

// errExchangeRunning is a swap that waits: an exchange of the table sent before still runs.
var errExchangeRunning = errors.New("an exchange of the table sent before still runs")

// errNoIdentity is a table without a UUID (in a database that gives its tables none, such as an Ordinary
// one): nothing tells it from a table that takes its name, so binwave neither swaps nor drops it.
var errNoIdentity = errors.New("the database gives its tables no UUID (it is not an Atomic database), " +
	"and binwave swaps and drops its tables only by their UUID")

// dropName is the name a table is renamed to before it is dropped: its own UUID, so a table under it is
// the one that was checked.
func dropName(uuid string) string {
	return dropPrefix + strings.ReplaceAll(uuid, "-", "")
}

const dropPrefix = "binwave_drop_"

// swapTable is what the swap reads of a table: whether it is there, its comment and its UUID.
type swapTable struct {
	exists  bool
	comment string
	uuid    string
}

// swapOps is what the swap does in ClickHouse, each by the name of the table.
type swapOps interface {
	// running reports whether a query of the id runs.
	running(ctx context.Context, queryID string) (bool, error)
	// read reads the tables of the names, all in one query.
	read(ctx context.Context, database string, names ...string) (map[string]swapTable, error)
	setComment(ctx context.Context, database, table, comment string) error
	// exchange exchanges the names of a and b, or renames b to a when a does not exist, under the query
	// id of the exchanges of a.
	exchange(ctx context.Context, database, a, b string, rename bool) error
	rename(ctx context.Context, database, from, to string) error
	drop(ctx context.Context, database, table string) error
	// dropping lists the tables of the database under a name of a drop (dropName).
	dropping(ctx context.Context, database string) ([]string, error)
}

// swapSpec is a swap of a copy into the place of a table.
type swapSpec struct {
	database, table, copy string
	marker                string // the marker of the job, naming the table
	// copied is the UUID of the copy an earlier attempt sent an exchange for, whose answer was lost: only
	// a table of that UUID in the place of the table, without the marker, is a swap finished before.
	copied string
	// copyUUID is the UUID the job made its copy with, when it knows one: only a table of that UUID is the
	// copy, and one of it in the place of the table, without the marker, is a swap finished before.
	copyUUID string
}

// swapOutcome is how a swap ended without an error.
type swapOutcome int

const (
	swapDone       swapOutcome = iota // the copy is in the place of the table
	swapLost                          // nothing was made, and the copy is gone
	swapNoCopy                        // there is no copy binwave made: neither table, or the copy is another table
	swapNotTheCopy                    // an exchange may have been made, and the table in the place of the table is not the copy
)

// maxSwapSteps bounds the exchanges of one attempt.
const maxSwapSteps = 3

// waitExchanges fails with errExchangeRunning while an exchange of the table runs.
func waitExchanges(ctx context.Context, ops swapOps, database, table string) error {
	running, err := ops.running(ctx, ExchangeQueryID(database, table))
	if err != nil {
		return err
	}
	if running {
		return fmt.Errorf("%s.%s: %w", database, table, errExchangeRunning)
	}
	return nil
}

// swapCopy puts the copy in the place of the table and drops what was there:
//
//   - the copy has the marker and the table does not: the marker is made to name the UUIDs of the
//     table (none when there is no table) and of the copy, and they are exchanged (or, when the table does
//     not exist, the copy is renamed to its name);
//   - the table has the marker: the swap is made, and what is left is to drop the old table, now under
//     the name of the copy, by the UUID the marker names, and to remove the marker. A table with a marker
//     that names another copy is not the copy, and nothing is done;
//   - the table is the copy an earlier attempt exchanged (spec.copied), without the marker: the swap was
//     finished before;
//   - the copy is gone and the table is there: the copy was lost, if no exchange may have been made, and
//     otherwise what is in the place of the table is not the copy.
//
// copied is the UUID of the copy an exchange may have been made with, by this attempt (whatever its
// answer) or one before, and empty when none may have been: a repeat of the swap passes it on as
// spec.copied. What ClickHouse holds is read again after every exchange.
func swapCopy(ctx context.Context, ops swapOps, log *slog.Logger, spec swapSpec) (copied string, outcome swapOutcome, err error) {
	copied = spec.copied
	if err := waitExchanges(ctx, ops, spec.database, spec.table); err != nil {
		return copied, swapDone, err
	}
	for range maxSwapSteps {
		got, err := ops.read(ctx, spec.database, spec.table, spec.copy)
		if err != nil {
			return copied, swapDone, err
		}
		t, c := got[spec.table], got[spec.copy]
		if t.exists && t.uuid == zeroUUID || c.exists && c.uuid == zeroUUID {
			return copied, swapDone, errNoIdentity
		}
		switch {
		case t.exists && isMarker(t.comment, spec.marker) && !isCopy(t, spec.marker, spec.copyUUID):
			// The marker was put on another table under the name of the copy, and it was exchanged in.
			return copied, swapNotTheCopy, nil
		case t.exists && isMarker(t.comment, spec.marker):
			return t.uuid, swapDone, cleanUpSwap(ctx, ops, log, spec, t)
		case t.exists && (copied != "" && t.uuid == copied || spec.copyUUID != "" && t.uuid == spec.copyUUID):
			log.Info("swap finished before", "table", spec.database+"."+spec.table)
			return t.uuid, swapDone, nil
		case isCopy(c, spec.marker, spec.copyUUID):
			replaced := zeroUUID
			if t.exists {
				replaced = t.uuid
			}
			if want := Replacing(spec.marker, replaced, c.uuid); c.comment != want {
				if err := ops.setComment(ctx, spec.database, spec.copy, want); err != nil {
					return copied, swapDone, err
				}
			}
			if err := ops.exchange(ctx, spec.database, spec.table, spec.copy, !t.exists); err != nil {
				if !answered(err) {
					copied = c.uuid
				}
				return copied, swapDone, err
			}
			copied = c.uuid
			log.Info("table swapped in", "table", spec.database+"."+spec.table)
		case t.exists && copied != "":
			// An exchange may have been made, and what is in the place of the table is not the copy: the copy
			// was moved, and another table took its name.
			return copied, swapNotTheCopy, nil
		case !c.exists && t.exists:
			return "", swapLost, nil
		default:
			return copied, swapNoCopy, nil
		}
	}
	return copied, swapDone, fmt.Errorf("the swap of %s.%s did not settle after %d exchanges", spec.database, spec.table, maxSwapSteps)
}

// cleanUpSwap drops the table the exchange put under the name of the copy, by the UUID the marker on the
// table names, drops what drops a crash interrupted left in the database, and removes the marker. A
// marker that names none (written before markers named the replaced table) cannot tell the old table from
// another: what is under the name of the copy is left.
func cleanUpSwap(ctx context.Context, ops swapOps, log *slog.Logger, spec swapSpec, t swapTable) error {
	if uuid, ok := replacedBy(t.comment); ok {
		if err := dropByIdentity(ctx, ops, log, spec.database, spec.copy, uuid); err != nil {
			return err
		}
	} else if got, err := ops.read(ctx, spec.database, spec.copy); err != nil {
		return err
	} else if got[spec.copy].exists {
		log.Warn("table not dropped: which table it is cannot be told; drop it by hand if it is the one the swap replaced",
			"table", spec.database+"."+spec.table, "left", spec.database+"."+spec.copy)
	}
	if err := sweep(ctx, ops, spec.database); err != nil {
		return err
	}
	return ops.setComment(ctx, spec.database, spec.table, "")
}

// dropCopy drops the copy of a job that is given up, if it is binwave's, by its identity, and what drops
// a crash interrupted left in the database. A table under the name of the copy without the marker of the
// job, or with a marker that names another copy, is left. dropped is false when there was nothing of
// binwave's to drop.
func dropCopy(ctx context.Context, ops swapOps, log *slog.Logger, spec swapSpec) (dropped bool, err error) {
	if err := waitExchanges(ctx, ops, spec.database, spec.table); err != nil {
		return false, err
	}
	got, err := ops.read(ctx, spec.database, spec.copy)
	if err != nil {
		return false, err
	}
	switch c := got[spec.copy]; {
	case !c.exists:
	case c.uuid == zeroUUID:
		log.Warn("table not dropped: it has no UUID to tell it by; drop it by hand", "copy", spec.database+"."+spec.copy)
	case !isCopy(c, spec.marker, spec.copyUUID):
		log.Warn("table not dropped: it has the name of a copy, and is not binwave's", "table", spec.database+"."+spec.table, "copy", spec.database+"."+spec.copy)
	default:
		if err := dropByIdentity(ctx, ops, log, spec.database, spec.copy, c.uuid); err != nil {
			return false, err
		}
		dropped = true
	}
	return dropped, sweep(ctx, ops, spec.database)
}

// sweep drops what drops by identity a crash interrupted left in the database: a table under the name
// made of its own UUID (dropName) is one binwave renamed to drop it. Each is read again right before its
// drop, as dropByIdentity does.
func sweep(ctx context.Context, ops swapOps, database string) error {
	names, err := ops.dropping(ctx, database)
	if err != nil {
		return err
	}
	for _, name := range names {
		got, err := ops.read(ctx, database, name)
		if err != nil {
			return err
		}
		if t := got[name]; !t.exists || t.uuid == zeroUUID || name != dropName(t.uuid) {
			continue
		}
		if err := ops.drop(ctx, database, name); err != nil {
			return err
		}
	}
	return nil
}

// dropByIdentity drops the table of the UUID if it is under the name, or under the name a drop of it
// renamed it to: it is renamed to that name, checked there, and dropped there. Any other table is left.
func dropByIdentity(ctx context.Context, ops swapOps, log *slog.Logger, database, name, uuid string) error {
	if uuid == zeroUUID {
		return errNoIdentity
	}
	trash := dropName(uuid)
	got, err := ops.read(ctx, database, name, trash)
	if err != nil {
		return err
	}
	if t := got[trash]; t.exists {
		if t.uuid != uuid {
			// A table took the name before this drop renamed anything there, or a crash came between a rename
			// of this drop and its check: which, nothing says, and it is not moved.
			return &ConflictError{Err: fmt.Errorf("ClickHouse has the table %s.%s, whose name binwave uses to drop the table %s.%s, "+
				"and it is another table; rename it (back to %s if it was there) and resume", database, trash, database, name, name)}
		}
		// A drop that a crash interrupted renamed it already.
		return ops.drop(ctx, database, trash)
	}
	switch at := got[name]; {
	case !at.exists:
		return nil
	case at.uuid != uuid:
		log.Warn("table not dropped: it is not the table binwave made", "left", database+"."+name)
		return nil
	}
	if err := ops.rename(ctx, database, name, trash); err != nil {
		return err
	}
	if got, err = ops.read(ctx, database, trash); err != nil {
		return err
	}
	if got[trash].uuid != uuid {
		// Another table took the name between the read and the rename: this rename moved it, and it goes back.
		log.Warn("table not dropped: it is not the table binwave made", "left", database+"."+name)
		return ops.rename(ctx, database, trash, name)
	}
	return ops.drop(ctx, database, trash)
}

// answered reports whether the server answered a statement with an error: it was refused, not made.
// Any other failure (a timeout, a broken connection) leaves open whether it was made.
func answered(err error) bool {
	var exception *ch.Exception
	return errors.As(err, &exception)
}

// clientOps does the swap in the ClickHouse of the client.
// The queries of the swap, the same for clientOps, which sends them, and planOps, which logs them.

func runningQuery(queryID string) string {
	return "SELECT count() FROM system.processes WHERE query_id = " + stringLiteral(queryID)
}

func readQuery(database string, names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = stringLiteral(n)
	}
	return "SELECT name, comment, toString(uuid) FROM system.tables WHERE database = " + stringLiteral(database) +
		" AND name IN (" + strings.Join(quoted, ", ") + ")"
}

func droppingQuery(database string) string {
	return "SELECT name FROM system.tables WHERE database = " + stringLiteral(database) + " AND startsWith(name, " + stringLiteral(dropPrefix) + ")"
}

func commentStatement(database, table, comment string) string {
	return "ALTER TABLE " + quote(database) + "." + quote(table) + " MODIFY COMMENT " + stringLiteral(comment)
}

func exchangeStatement(database, a, b string, rename bool) string {
	if rename {
		return renameStatement(database, b, a)
	}
	return "EXCHANGE TABLES " + quote(database) + "." + quote(a) + " AND " + quote(database) + "." + quote(b)
}

func renameStatement(database, from, to string) string {
	return "RENAME TABLE " + quote(database) + "." + quote(from) + " TO " + quote(database) + "." + quote(to)
}

func dropStatement(database, table string) string {
	return "DROP TABLE IF EXISTS " + quote(database) + "." + quote(table)
}

type clientOps struct{ c *Client }

func (o clientOps) running(ctx context.Context, queryID string) (bool, error) {
	var n uint64
	if err := o.c.conn.QueryRow(ctx, runningQuery(queryID)).Scan(&n); err != nil {
		return false, fmt.Errorf("read the queries that run: %w", err)
	}
	return n > 0, nil
}

func (o clientOps) read(ctx context.Context, database string, names ...string) (map[string]swapTable, error) {
	rows, err := o.c.conn.Query(ctx, readQuery(database, names))
	if err != nil {
		return nil, fmt.Errorf("read the tables %s of %s: %w", strings.Join(names, ", "), database, err)
	}
	defer func() { _ = rows.Close() }()
	got := map[string]swapTable{}
	for rows.Next() {
		var name string
		t := swapTable{exists: true}
		if err := rows.Scan(&name, &t.comment, &t.uuid); err != nil {
			return nil, fmt.Errorf("read the tables of %s: %w", database, err)
		}
		got[name] = t
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the tables of %s: %w", database, err)
	}
	return got, nil
}

func (o clientOps) setComment(ctx context.Context, database, table, comment string) error {
	return o.c.exec(ctx, commentStatement(database, table, comment))
}

func (o clientOps) exchange(ctx context.Context, database, a, b string, rename bool) error {
	return o.c.exec(ch.Context(ctx, ch.WithQueryID(ExchangeQueryID(database, a))), exchangeStatement(database, a, b, rename))
}

func (o clientOps) rename(ctx context.Context, database, from, to string) error {
	return o.c.exec(ctx, renameStatement(database, from, to))
}

func (o clientOps) drop(ctx context.Context, database, table string) error {
	return o.c.exec(ctx, dropStatement(database, table))
}

func (o clientOps) dropping(ctx context.Context, database string) ([]string, error) {
	rows, err := o.c.conn.Query(ctx, droppingQuery(database))
	if err != nil {
		return nil, fmt.Errorf("read the tables of %s: %w", database, err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read the tables of %s: %w", database, err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the tables of %s: %w", database, err)
	}
	return names, nil
}

// newPlan is a plan of the swap from the tables as they are, logged to log. made is a copy that is to be
// made first, with the marker of the job, whose UUID the plan shows by its description.
func newPlan(log *slog.Logger, spec swapSpec, tables map[string]swapTable, made bool) *planOps {
	plan := &planOps{log: log, tables: tables, shown: map[string]string{}}
	if made {
		plan.tables[spec.copy] = swapTable{exists: true, comment: spec.marker, uuid: standInUUID}
		plan.shown[standInUUID] = "{uuid of " + quote(spec.database) + "." + quote(spec.copy) + "}"
	}
	return plan
}

// standInUUID is the UUID of a table in a plan that is known only once the table is made.
const standInUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

// planOps runs the swap on a model of the tables instead of ClickHouse: it changes nothing, and logs, in
// order, every query the swap would send, each read with what it is to find there. A UUID that is known
// only when the swap runs (of a copy not made yet) is a stand-in in the model, shown by its description.
type planOps struct {
	log    *slog.Logger
	tables map[string]swapTable
	shown  map[string]string // a stand-in UUID, and what the plan shows instead of it
}

func (o *planOps) show(s string) string {
	for uuid, as := range o.shown {
		s = strings.ReplaceAll(s, uuid, as)
		s = strings.ReplaceAll(s, strings.ReplaceAll(uuid, "-", ""), strings.TrimSuffix(as, "}")+" without dashes}")
	}
	return s
}

func (o *planOps) check(query, expect string) {
	o.log.Info("dry run: query not executed", "query", o.show(query), "expect", o.show(expect))
}

func (o *planOps) statement(stmt string, attrs ...any) {
	o.log.Info("dry run: statement not executed", append([]any{"statement", o.show(stmt)}, attrs...)...)
}

func (o *planOps) running(_ context.Context, queryID string) (bool, error) {
	o.check(runningQuery(queryID), "0, or wait until it is: an exchange of the table sent before still runs")
	return false, nil
}

func (o *planOps) read(_ context.Context, database string, names ...string) (map[string]swapTable, error) {
	got := map[string]swapTable{}
	expect := make([]string, len(names))
	for i, n := range names {
		t, ok := o.tables[n]
		if !ok {
			expect[i] = "no table " + n
			continue
		}
		got[n] = t
		expect[i] = fmt.Sprintf("%s with uuid %s and comment %s", n, t.uuid, stringLiteral(t.comment))
	}
	o.check(readQuery(database, names), strings.Join(expect, "; "))
	return got, nil
}

func (o *planOps) setComment(_ context.Context, database, table, comment string) error {
	o.statement(commentStatement(database, table, comment))
	t := o.tables[table]
	t.comment = comment
	o.tables[table] = t
	return nil
}

func (o *planOps) exchange(_ context.Context, database, a, b string, rename bool) error {
	o.statement(exchangeStatement(database, a, b, rename), "query_id", ExchangeQueryID(database, a))
	ta, aok := o.tables[a]
	o.tables[a] = o.tables[b]
	delete(o.tables, b)
	if aok && !rename {
		o.tables[b] = ta
	}
	return nil
}

func (o *planOps) rename(_ context.Context, database, from, to string) error {
	o.statement(renameStatement(database, from, to))
	o.tables[to] = o.tables[from]
	delete(o.tables, from)
	return nil
}

func (o *planOps) drop(_ context.Context, database, table string) error {
	o.statement(dropStatement(database, table))
	delete(o.tables, table)
	return nil
}

func (o *planOps) dropping(_ context.Context, database string) ([]string, error) {
	o.check(droppingQuery(database), "the tables a drop renamed and did not drop: each whose name is "+dropPrefix+
		" followed by its own UUID without dashes is read again and dropped, as above")
	var names []string
	for n := range o.tables {
		if strings.HasPrefix(n, dropPrefix) {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	return names, nil
}
