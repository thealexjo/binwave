package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/gtid"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/typemap"
)

// A table that cannot be replicated is set aside instead of stopping the service: quarantine.
// What the pipeline does for a quarantined table is nothing: its row changes are read and
// dropped, so the position moves on and the other tables are unaffected. The table falls behind
// the source until an operator resynchronizes it and releases it. Every entry is loud (a log
// line, a metric, /status) and saved with the position, so a restart keeps it.
//
// tables.on_replication_error: stop restores the strict behaviour: the problem stops the service.

// quarantineSet is the set of quarantined tables. It is shared with the goroutine that runs the
// checks after start, hence the lock.
//
// A released table stays in the set with a fence (Until) until the stream has passed it: the
// changes of the table up to the fence are still dropped, and the first transaction beyond it is
// replicated. That is what "replicated again from now on" means, without replaying the time
// before the cause was fixed.
type quarantineSet struct {
	mu      sync.Mutex
	entries map[string]checkpoint.Quarantined
	fences  map[string]*gtid.Set // parsed Until of released tables
	// since is the GTID set committed so far, recorded in new entries.
	since string
	// fresh names the tables quarantined since the pipeline last looked, so that a resync of such a
	// table can be given up without walking every resync there is: with an entry per table of the
	// source (replication.initial_position: snapshot) that walk is what makes a transaction cost
	// O(tables). Drained by drainFresh.
	fresh []string
}

// skip reports whether a change of the table, made by the transaction, is dropped. changed
// reports that a fence was passed and the entry removed, which must be saved.
func (q *quarantineSet) skip(table, transaction string) (skip, changed bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.entries[table]
	if !ok {
		return false, false, nil
	}
	if !e.Released() {
		return true, false, nil
	}
	fence := q.fences[table]
	if fence == nil {
		if fence, err = gtid.Parse(e.Until); err != nil {
			return false, false, fmt.Errorf("the fence of released table %s is damaged: %w", table, err)
		}
		if q.fences == nil {
			q.fences = map[string]*gtid.Set{}
		}
		q.fences[table] = fence
	}
	inside, err := fence.Contains(transaction)
	if err != nil {
		return false, false, fmt.Errorf("check table %s against its fence: %w", table, err)
	}
	if inside {
		return true, false, nil
	}
	delete(q.entries, table)
	delete(q.fences, table)
	return false, true, nil
}

// add records the table as quarantined; it reports false when it was quarantined already. A
// released table that fails again is quarantined again. recoverable is what tables.autoresync acts
// on: see checkpoint.Quarantined.Recoverable.
func (q *quarantineSet) add(table string, code errcode.Code, reason string, recoverable bool, at time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if e, ok := q.entries[table]; ok && !e.Released() {
		return false
	}
	if q.entries == nil {
		q.entries = map[string]checkpoint.Quarantined{}
	}
	delete(q.fences, table)
	q.entries[table] = checkpoint.Quarantined{Table: table, Since: q.since, Code: string(code), Reason: reason, At: at, Recoverable: recoverable}
	q.fresh = append(q.fresh, table)
	return true
}

// drainFresh returns the tables quarantined since the last call, and forgets them.
func (q *quarantineSet) drainFresh() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	names := q.fresh
	q.fresh = nil
	return names
}

func (q *quarantineSet) setSince(gtid string) {
	q.mu.Lock()
	q.since = gtid
	q.mu.Unlock()
}

// merge adds the saved entries to what is already known. It reports whether the set holds
// something the saved entries lack, which must be saved.
func (q *quarantineSet) merge(saved []checkpoint.Quarantined) (added []checkpoint.Quarantined, unsaved bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.entries == nil {
		q.entries = map[string]checkpoint.Quarantined{}
	}
	inSaved := make(map[string]bool, len(saved))
	for _, e := range saved {
		inSaved[e.Table] = true
		if _, ok := q.entries[e.Table]; !ok {
			q.entries[e.Table] = e
			added = append(added, e)
			if !e.Released() {
				// A table the saved state has both quarantined and resynchronizing (it was quarantined
				// while its resync ran) gives that resync up, the same as a fresh quarantine does.
				q.fresh = append(q.fresh, e.Table)
			}
		}
	}
	for table := range q.entries {
		if !inSaved[table] {
			unsaved = true
		}
	}
	return added, unsaved
}

// move carries the quarantine of a renamed table to its new name: the table is the same, and
// still behind the source. With keep false (the new name is not replicated) the entry is removed.
// It reports whether the table was quarantined.
func (q *quarantineSet) move(from, to string, keep bool) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.entries[from]
	if !ok {
		return false
	}
	delete(q.entries, from)
	delete(q.fences, from)
	if keep {
		e.Table = to
		q.entries[to] = e
		delete(q.fences, to)
	}
	return true
}

// release fences a quarantined table: its changes up to the fence stay dropped. It reports
// whether the table was quarantined.
func (q *quarantineSet) release(table, fence string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.entries[table]
	if !ok || e.Released() {
		return false
	}
	e.Until = fence
	q.entries[table] = e
	delete(q.fences, table)
	return true
}

// remove takes a table out of the set at once, whatever its state, and returns the reason it was
// quarantined for. It is for a table that a resync takes over.
func (q *quarantineSet) remove(table string) (reason string, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.entries[table]
	if !ok {
		return "", false
	}
	delete(q.entries, table)
	delete(q.fences, table)
	return e.Reason, !e.Released()
}

// isQuarantined reports whether the table is quarantined now (not released).
func (q *quarantineSet) isQuarantined(table string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.entries[table]
	return ok && !e.Released()
}

// active lists the tables that are quarantined, not the released ones waiting for their fence.
func (q *quarantineSet) active() []checkpoint.Quarantined {
	var out []checkpoint.Quarantined
	for _, e := range q.list() {
		if !e.Released() {
			out = append(out, e)
		}
	}
	return out
}

func (q *quarantineSet) list() []checkpoint.Quarantined {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]checkpoint.Quarantined, 0, len(q.entries))
	for _, e := range q.entries {
		out = append(out, e)
	}
	return out
}

func (q *quarantineSet) encode() string { return checkpoint.EncodeQuarantine(q.list()) }

// Quarantine sets a table aside. It is for the checks that run after start, which find problems
// of tables while the stream is already being read; it is safe to call from any goroutine. The
// table is quarantined only when tables.on_replication_error is quarantine: with stop, the
// caller decides what a problem means. It returns whether the table was quarantined. The reason is
// never one tables.autoresync acts on: a caller that knows better uses quarantine directly.
func (p *Pipeline) Quarantine(table string, code errcode.Code, reason string) bool {
	return p.quarantine(table, code, reason, false)
}

// quarantine is Quarantine with recoverable decided by the caller: true marks a reason
// tables.autoresync may fix on its own (see checkpoint.Quarantined.Recoverable and
// chsink.Recoverable). Not safe to call from another goroutine unless recoverable is always false
// (Quarantine's contract) — a true value belongs to the pipeline's own goroutine, which is the only
// one that starts a resync.
//
// The log line is short on purpose: code identifies the kind of problem
// (docs/reference/error-codes.md has the full explanation and what to do), reason is only the
// variable detail (which column, which type) and does not repeat the table name, which is already
// its own attribute.
func (p *Pipeline) quarantine(table string, code errcode.Code, reason string, recoverable bool) bool {
	if p.cfg.Tables.OnReplicationError != config.OnErrorQuarantine {
		return false
	}
	if !p.q.add(table, code, reason, recoverable, p.d.Now()) {
		return true
	}
	p.log.Error("["+string(code)+"] table quarantined", "table", table, "code", code, "detail", reason, "recoverable", recoverable)
	p.d.Observer.QuarantineChanged(p.q.active())
	p.qChanged.Store(true)
	return true
}

// causeCode names the kind of a data-path error (docs/reference/error-codes.md) from its Go type,
// independently of whether it ends up quarantining the table (tableFailed) or failing one
// transaction (annotate, in failure.go): the same underlying cause can become either, depending on
// tables.on_replication_error and where in the pipeline it was found.
func causeCode(err error) errcode.Code {
	var (
		conflict *chsink.ConflictError
		mismatch *chsink.SchemaMismatchError
		table    *chsink.TableError
		write    *WriteError
		conv     *typemap.ConversionError
	)
	switch {
	case errors.As(err, &conflict):
		return errcode.REPL101
	case errors.As(err, &mismatch):
		return errcode.REPL102
	case errors.As(err, &table), errors.As(err, &write):
		return errcode.REPL103
	case errors.As(err, &conv):
		return errcode.CONV101
	default:
		return ""
	}
}

// syncQuarantine brings the position that will be saved up to date with the quarantine set,
// which may have changed on another goroutine. It runs on the pipeline's goroutine.
func (p *Pipeline) syncQuarantine() {
	if p.qChanged.Swap(false) {
		p.committed.Quarantine = p.q.encode()
		p.markDirty()
	}
}

// tableFailed handles a problem of one table found while replicating it. With quarantine the
// table is set aside and replication goes on (nil); with stop the problem is returned.
func (p *Pipeline) tableFailed(table string, err error) error {
	if p.cfg.Tables.OnReplicationError != config.OnErrorQuarantine {
		return err
	}
	// A missing table or column is a case of its own: the changes made until it exists are lost
	// if the table is set aside, while stopping loses nothing. Setting it aside needs to be asked for.
	if chsink.IsMissingTable(err) && p.cfg.DDL.OnMissingStructure != config.OnMissingQuarantine {
		return err
	}
	p.quarantine(table, causeCode(err), err.Error(), chsink.Recoverable(err))
	p.dropBuffers(table)
	p.syncQuarantine()
	return nil
}

// dropBuffers discards what is buffered for a table that will not be written any more.
func (p *Pipeline) dropBuffers(table string) {
	kept := p.order[:0]
	for _, t := range p.order {
		if t.Source.FullName() != table {
			kept = append(kept, t)
			continue
		}
		b := p.buffers[t]
		p.memBytes -= b.bytes
		p.memRows -= len(b.rows)
		delete(p.buffers, t)
	}
	p.order = kept
	p.d.Observer.BufferChanged(p.memRows, p.memBytes)
}

// loadQuarantine reads the quarantined tables of the saved state and says so. Tables set aside
// before the stream started (by the checks that run at start) stay, and are saved with the
// position.
func (p *Pipeline) loadQuarantine(pos checkpoint.Position) error {
	entries, err := checkpoint.DecodeQuarantine(pos.Quarantine)
	if err != nil {
		return fmt.Errorf("load position: %w", err)
	}
	added, unsaved := p.q.merge(entries)
	if unsaved {
		p.qChanged.Store(true)
	}
	for _, e := range added {
		p.log.Warn("["+e.Code+"] table still quarantined", "table", e.Table, "code", e.Code, "since", e.Since, "detail", e.Reason)
	}
	p.d.Observer.QuarantineChanged(p.q.active())
	return nil
}

// Quarantined returns the tables that are set aside, in the running pipeline or, while
// replication is paused, as it was when it stopped.
func (p *Pipeline) Quarantined() []checkpoint.Quarantined {
	list := p.q.list()
	sort.Slice(list, func(i, j int) bool { return list[i].Table < list[j].Table })
	return list
}

// Release takes tables out of quarantine. Their changes are replicated again from the first
// transaction that is not in fence, the source's GTID set at the moment of the release; the
// changes made before it stay missing in ClickHouse and are not replayed. unknown are the tables
// that were not quarantined. Safe to call from any goroutine; a running pipeline saves the change
// with its next position, an idle one needs SaveQuarantine.
func (p *Pipeline) Release(tables []string, fence string) (released, unknown []string) {
	for _, t := range tables {
		if p.q.release(t, fence) {
			released = append(released, t)
			p.log.Info("table released, changes during quarantine lost", "table", t, "fence", fence)
		} else {
			unknown = append(unknown, t)
		}
	}
	if len(released) > 0 {
		p.d.Observer.QuarantineChanged(p.q.active())
		p.qChanged.Store(true)
	}
	return released, unknown
}

// Prepare settles where replication starts from, and loads the quarantine list, before the
// stream is read. On the first run the configured start point ("now") is decided and saved here,
// so a service that starts paused starts from the moment it was started, not from the moment it
// is resumed: what happens in the source while it is paused is not skipped.
func (p *Pipeline) Prepare(ctx context.Context) error {
	pos, err := p.startPosition(ctx)
	if err != nil {
		return err
	}
	return p.loadQuarantine(pos)
}

// SaveQuarantine writes the quarantine list to the saved state while the pipeline is not running,
// keeping the saved position as it is.
func (p *Pipeline) SaveQuarantine(ctx context.Context) error {
	pos, found, err := p.d.Store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load position: %w", err)
	}
	if !found {
		return nil // nothing has been saved yet; the list is saved with the first position
	}
	// Cleared before the list is taken: a table set aside meanwhile sets it again, for the next save.
	p.qChanged.Store(false)
	pos.Quarantine = p.q.encode()
	if err := p.d.Store.Save(ctx, pos); err != nil {
		p.qChanged.Store(true)
		return fmt.Errorf("save the quarantine list: %w", err)
	}
	return nil
}
