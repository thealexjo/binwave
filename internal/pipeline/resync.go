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
	"github.com/thealexjo/binwave/internal/ddl"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/gtid"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/snapshot"
	"github.com/thealexjo/binwave/internal/typemap"
)

// A resync reads a table again from the source and replaces its ClickHouse table with the result,
// while the stream goes on. It is how a table that fell behind (quarantine, a skipped transaction, a
// change of type MySQL converts by its own rules) catches up, and it is also how the initial load
// (replication.initial_position: snapshot, on the first run) loads every table: the same mechanism,
// for every table the catalog shows at that moment, distinguished only by checkpoint.Resync.InitialLoad
// (which just says "don't hold a `tables resync` up behind this, and don't count it against the same
// parallelism budget" — everything else, including giving up, is identical).
//
// The algorithm:
//
//  1. At a transaction boundary, a shadow table <t>__resync is created, and from then on the changes of
//     the table go to it, not to the table (which, for the initial load, does not exist yet: the shadow
//     is all there is until the swap). Their versions are above Version, that of the last transaction
//     before the boundary.
//  2. The snapshot reads the table in chunks by primary key, and the rows of a chunk are written to the
//     shadow table with Version: below the version of any change that reaches the table after the
//     boundary, so a change wins over the row of a chunk, and a row that no change touched stays as it
//     was read.
//  3. When the last chunk is read the stream still has to reach the point at which it was read (the
//     watermark, the GTID set the source had executed before it): only then have all the changes
//     that the chunks may lack been applied. At that point, at a transaction boundary, the shadow table
//     replaces the table.
//
// Nobody outside binwave ever sees a table half filled: it is either not there yet, or it is the
// complete shadow table swapped into its place. The shadow table starts empty, so a row deleted in the
// source while the table was behind is not in it. Nothing is dropped before the swap, and at the swap
// the old table goes (for the initial load, there is no old table to drop).

// ErrNotRunning is returned to a request for a resync while replication is not running: the resync
// begins at a transaction boundary of the stream.
var ErrNotRunning = errors.New("replication is not running")

// Snapshotter is what a resync and the initial load need of the source.
type Snapshotter interface {
	snapshot.Source
	// TableDefinition describes a table as the binlog would, for reading it.
	TableDefinition(ctx context.Context, database, table string) (*schema.Table, error)
	// Catalog lists the tables the initial load reads, at the moment replication starts for the
	// first time.
	Catalog(ctx context.Context, database, table string) (*schema.Catalog, error)
}

// maxReadFailures is how many times in a row the read of a chunk may fail, with an error that is not
// known to pass with time, before the resync gives up. A source that is down (Deps.SourceTransient) is
// waited out instead, however long it takes: if it is really gone, the stream breaks too, the process
// restarts, and the resync continues from its saved cursor.
const maxReadFailures = 5

type resyncJob struct {
	state   checkpoint.Resync
	tbl     *schema.Table // the definition the snapshot reads with; nil once the read is done
	target  *chsink.Table // the shadow table
	results chan chunkResult
	cancel  context.CancelFunc
	stopped chan struct{} // closed when the reading goroutine has ended
	status  snapshot.Status
	// final is state.Final parsed, and finalCount how many transactions it holds. Parsed once, when
	// the read is done, because the question "has the stream reached it" is asked on every transaction
	// boundary: parsing it there, for thousands of tables, is what the garbage collector then lives on.
	final      *gtid.Set
	finalCount int64
	lostFinal  error // state.Final does not parse: the resync is given up when the queue reaches it
	doneSaved  bool  // a saved position has the job read in full (Done, Final)
}

type chunkResult struct {
	// chunk carries what the pipeline needs of the chunk: its Rows are converted by the reading
	// goroutine and left out (see rows, nrows).
	chunk    snapshot.Chunk
	rows     []chsink.Row
	nrows    int
	progress snapshot.Progress
	// err is a read that failed for good; convert is a row the shadow table cannot take. Both end the
	// resync, but for different reasons, and the operator is told which.
	err     error
	convert error
}

// resyncRequest asks the pipeline goroutine to begin a resync.
type resyncRequest struct {
	table string
	def   *schema.Table
	reply chan error
}

type resyncSet struct {
	mu       sync.Mutex // guards queue and statuses; jobs belong to the pipeline goroutine
	queue    []resyncRequest
	statuses []snapshot.Status
}

// StartResync begins a resync of the table. It returns when the pipeline has begun it (at the next
// transaction boundary or heartbeat of the stream), or with the reason it cannot. When ctx ends first,
// the request is withdrawn, unless the pipeline has taken it already: then its answer is returned, so
// the caller is never told a resync did not begin that does. Safe to call from any goroutine.
func (p *Pipeline) StartResync(ctx context.Context, table string) error {
	if p.d.Snapshot == nil {
		return errors.New("this service has no source to read a snapshot from")
	}
	db, name, ok := cutQualified(table)
	if !ok || !p.router.Match(db, name) {
		return fmt.Errorf("%s is not a replicated table (see tables.include and tables.exclude)", table)
	}
	if !p.running.Load() {
		return fmt.Errorf("%w: resume replication first, a resync begins at a transaction boundary of the stream", ErrNotRunning)
	}
	def, err := p.d.Snapshot.TableDefinition(ctx, db, name)
	if err != nil {
		return err
	}
	req := resyncRequest{table: table, def: def, reply: make(chan error, 1)}
	p.rs.mu.Lock()
	p.rs.queue = append(p.rs.queue, req)
	p.rs.mu.Unlock()
	select {
	case err := <-req.reply:
		return err
	case <-ctx.Done():
	}
	p.rs.mu.Lock()
	for i, q := range p.rs.queue {
		if q.reply == req.reply {
			p.rs.queue = append(p.rs.queue[:i], p.rs.queue[i+1:]...)
			p.rs.mu.Unlock()
			return fmt.Errorf("wait for the resync to begin: %w", ctx.Err())
		}
	}
	p.rs.mu.Unlock()
	return <-req.reply
}

// SetLoadRate changes the combined speed limit of the initial load while it runs; 0 removes it. It
// takes effect at once, for every table the load is currently reading, and does not affect a
// `tables resync`, which the limit never applied to. It has no effect once the load has finished:
// nothing is left to pace.
func (p *Pipeline) SetLoadRate(rowsPerSecond int) error {
	if !p.running.Load() {
		return ErrNotRunning
	}
	p.loadBudget.SetRate(rowsPerSecond)
	p.log.Info("snapshot rate changed", "rows_per_second", rowsPerSecond)
	return nil
}

// sizerOptions is how the chunks of every snapshot read of this run are sized: from the
// configuration, and through the override an operator can set while they run.
func (p *Pipeline) sizerOptions() snapshot.SizerOptions {
	sn := p.cfg.Replication.Snapshot
	maxBytes := int(sn.MaxChunkBytes())
	if maxBytes == 0 {
		maxBytes = -1 // asked for: no bound on the weight of a chunk
	}
	return snapshot.SizerOptions{
		Initial:  sn.ChunkSize,
		Fixed:    !sn.AdaptiveChunksEnabled(),
		MaxBytes: maxBytes,
		Override: p.chunkSize,
	}
}

// SetChunkSize changes the size of the chunks of every snapshot read while they run: rows rows in
// every chunk, or adaptive sizing when rows is 0 or less, whatever the configuration says. Unlike the
// rate limit, this applies to a `binwave tables resync` as well as to the initial load: it is a knob
// for measuring a source, and readers living by different rules is what makes a measurement useless.
// The bound on the weight of a chunk (replication.snapshot.chunk_max_bytes) still holds. Takes effect
// on the next chunk of every table being read, and is not saved: after a restart the configuration
// decides again.
func (p *Pipeline) SetChunkSize(rows int) error {
	if !p.running.Load() {
		return ErrNotRunning
	}
	p.chunkSize.Set(rows)
	if rows > 0 {
		p.log.Info("chunk size fixed", "rows", rows)
	} else {
		p.log.Info("chunk size adaptive")
	}
	return nil
}

// Resyncs lists the resyncs that are running, sorted by table. Safe to call from any goroutine.
func (p *Pipeline) Resyncs() []snapshot.Status {
	p.rs.mu.Lock()
	defer p.rs.mu.Unlock()
	return append([]snapshot.Status(nil), p.rs.statuses...)
}

// encodeResyncs renders the state of the resyncs for the position: the registered jobs, and while a
// restart resumes them, the saved entries it has not finished with.
func (p *Pipeline) encodeResyncs() string {
	entries := make([]checkpoint.Resync, 0, len(p.resyncs)+len(p.resumeLeft))
	for _, j := range p.resyncs {
		entries = append(entries, j.state)
	}
	for name, st := range p.resumeLeft {
		if _, registered := p.resyncs[name]; !registered {
			entries = append(entries, st)
		}
	}
	return checkpoint.EncodeResyncs(entries)
}

// syncResyncs records that the resyncs moved on, so the position that is saved next carries it.
// The text itself is built at save time (materializeResyncs): it holds every entry there is — one
// per table of the source during the initial load — and building it on every chunk that is read
// costs far more than the save it feeds. Everything a cursor stands for is written before a
// position is saved (flush writes all buffers first), so the position never has a cursor that is
// ahead of the rows.
func (p *Pipeline) syncResyncs() {
	p.resyncsStale = true
	p.markDirty()
}

// materializeResyncs puts the resyncs into the position about to be saved, if they have moved since
// it was last built. It runs on the pipeline's goroutine, from flush, just before the save.
func (p *Pipeline) materializeResyncs() {
	if !p.resyncsStale {
		return
	}
	p.resyncsStale = false
	if text := p.encodeResyncs(); p.committed.Resync != text {
		p.committed.Resync = text
	}
	if p.committed.LoadTotal > 0 && !p.loading() {
		// The last table of the initial load is done: the total has nothing left to be the total of.
		p.committed.LoadTotal = 0
	}
}

// loading reports whether any table of the initial load is still to be loaded.
func (p *Pipeline) loading() bool {
	for _, j := range p.resyncs {
		if j.state.InitialLoad {
			return true
		}
	}
	for _, st := range p.resumeLeft {
		if st.InitialLoad {
			return true
		}
	}
	return false
}

// resyncPublishEvery is how often the statuses of the resyncs are rebuilt for `binwave replication
// status` and the metrics. The list holds an entry per registered resync — one per table of the
// source during the initial load — and every chunk read and every table finished changes it, so
// rebuilding and sorting it each time is exactly the per-transaction cost that the indexes above
// exist to avoid. A progress display does not need more than this.
const resyncPublishEvery = time.Second

// publishResyncs records that the statuses changed. They go out from publishStaleResyncs, which
// every transaction boundary and every heartbeat reaches (serviceResyncs).
func (p *Pipeline) publishResyncs() {
	p.resyncsPublishStale = true
}

// publishStaleResyncs hands the statuses over when they changed and the last time was long enough ago.
// A resync that appeared or ended does not wait for the throttle: which tables are being resynchronized
// is what an operator and the metrics are asked about, and a table that is finished must not be reported
// as still reading. The throttle is for the progress of the tables that are reading, which changes with
// every chunk. Publishing costs one pass over the resyncs, and it happens once per pass here, not once
// per table, so a burst of swaps at the end of an initial load pays for one.
func (p *Pipeline) publishStaleResyncs() {
	if p.resyncSetChanged {
		p.publishResyncsNow()
		return
	}
	if !p.resyncsPublishStale || p.d.Now().Sub(p.resyncsPublished) < resyncPublishEvery {
		return
	}
	p.publishResyncsNow()
}

// publishResyncsNow hands the statuses over whatever the throttle says: for a pipeline that stops.
func (p *Pipeline) publishResyncsNow() {
	statuses := make([]snapshot.Status, 0, len(p.resyncs))
	for _, j := range p.resyncs {
		statuses = append(statuses, j.status)
	}
	sort.Slice(statuses, func(i, k int) bool { return statuses[i].Table < statuses[k].Table })
	p.rs.mu.Lock()
	p.rs.statuses = statuses
	p.rs.mu.Unlock()
	p.resyncsPublishStale, p.resyncSetChanged = false, false
	p.resyncsPublished = p.d.Now()
	p.d.Observer.ResyncChanged(statuses)
}

// trackResync indexes a registered job: reading when the pipeline has to look at it on every
// transaction boundary (it was launched, or it is read and waits for the stream), waiting when it is
// an initial load without a slot yet. See Pipeline.reading and Pipeline.waiting.
func (p *Pipeline) trackResync(name string, job *resyncJob) {
	switch {
	case job.state.Done:
		p.awaitSwap(job)
	case job.results != nil:
		p.reading[name] = job
	default:
		p.waiting = append(p.waiting, name)
		p.waitingSorted = false
	}
}

// awaitSwap puts a job that is read in full in the queue of tables waiting for the stream, with its
// Final parsed once. The queue is kept in the order of how much of the source each Final covers, which
// is the order the stream reaches them in: @@gtid_executed only grows, so a table read later has a
// Final that includes the Final of a table read earlier. Appending keeps that order while the service
// runs (each read finishes later than the last); a restart, which registers the saved entries in name
// order, is what the sort is for.
func (p *Pipeline) awaitSwap(job *resyncJob) {
	delete(p.reading, job.state.Table)
	if job.final == nil && job.state.Final != "" {
		set, err := gtid.Parse(job.state.Final)
		if err != nil {
			job.lostFinal = err // a damaged Final cannot say when the shadow table is complete
		} else {
			job.final, job.finalCount = set, set.Count()
		}
	}
	p.awaiting = append(p.awaiting, job)
	if n := len(p.awaiting); n > 1 && p.awaiting[n-2].finalCount > job.finalCount {
		sort.SliceStable(p.awaiting, func(i, k int) bool { return p.awaiting[i].finalCount < p.awaiting[k].finalCount })
	}
}

// untrackResync forgets a job that is no longer registered. A name left in waiting is skipped when
// fillLoadSlots walks it, so it needs no removal there.
func (p *Pipeline) untrackResync(name string) {
	delete(p.reading, name)
	for i, job := range p.awaiting {
		if job.state.Table == name {
			p.awaiting = append(p.awaiting[:i], p.awaiting[i+1:]...)
			return
		}
	}
}

// resyncResumeReportEvery bounds how often resumeResyncs reports progress while walking a large
// saved list (thousands of tables at the very first start of replication.initial_position:
// snapshot can take minutes): often enough that an operator watching the log, and /healthz
// (internal/observability, stale after 30s without progress), both see it is working and not
// stuck; rare enough that it does not become the same one-line-per-table spam this quiets in the
// first place.
const resyncResumeReportEvery = 200

// resumeResyncs continues the resyncs (and the initial load's own resyncs) that the saved position
// records. With replication.initial_position: snapshot, every table of the source becomes one such
// entry at the very first start, so this can be hundreds at once: one line per table at every start
// until each finishes would drown everything else in the log, so resumeResync itself is quiet
// (Debug) and this logs periodic progress instead — silence for minutes on a large source would
// look exactly like a hang, both in the log and on /healthz.
func (p *Pipeline) resumeResyncs(ctx context.Context, pos checkpoint.Position) error {
	entries, err := checkpoint.DecodeResyncs(pos.Resync)
	if err != nil {
		return fmt.Errorf("load position: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	if p.cfg.DDL.DryRun {
		// A resync creates, swaps and drops tables, and the first of them may be the next step. Replication
		// stops before it, with every entry saved as it is, to go on once dry_run is off.
		return p.holdResync(entries[0].Table, errcode.SYNC101, errors.New("the saved position has resyncs to continue, and ddl.dry_run is set: "+
			"a resync creates, swaps and drops tables; turn dry_run off to continue them"))
	}
	// A position saved on the way (a resync that begins again flushes, one that reads again saves that it
	// does) has the entries not resumed yet: each can be resumed again after another crash.
	p.resumeLeft = make(map[string]checkpoint.Resync, len(entries))
	for _, st := range entries {
		p.resumeLeft[st.Table] = st
	}
	defer func() { p.resumeLeft = nil }()
	var initialLoad, resync int
	for i, st := range entries {
		if st.InitialLoad {
			initialLoad++
		} else {
			resync++
		}
		if err := p.resumeResync(ctx, st); err != nil {
			return err
		}
		delete(p.resumeLeft, st.Table)
		if _, kept := p.resyncs[st.Table]; !kept {
			p.syncResyncs() // swapped in, or given up: the saved list must lose it
		}
		if p.hooks.Progress != nil {
			p.hooks.Progress()
		}
		if done := i + 1; done%resyncResumeReportEvery == 0 && done < len(entries) {
			p.log.Info("resyncs resuming", "done", done, "total", len(entries))
		}
	}
	p.resumeLeft = nil
	if p.resyncsStale {
		// A swap was finished or a resync began again: saved at once, as after a swap in the stream.
		if err := p.flush(ctx); err != nil {
			return err
		}
	}
	p.log.Info("resyncs resumed", "total", len(entries), "initial_load", initialLoad, "resync", resync)
	p.publishResyncs()
	return nil
}

// resumeResync continues one resync after a restart. An entry from the initial load (InitialLoad) is
// resumed exactly the same way as one from `tables resync`; the only difference is that it is not
// launched here if it would exceed replication.snapshot.parallelism — fillLoadSlots decides that,
// once every entry has been registered.
func (p *Pipeline) resumeResync(ctx context.Context, st checkpoint.Resync) error {
	db, name, _ := cutQualified(st.Table)
	tdb, _ := chsink.TargetName(p.cfg, db, name)
	job := &resyncJob{state: st}
	job.status = p.statusOf(job, snapshot.Progress{Fraction: -1})
	if st.Pending {
		return p.beginPending(ctx, st)
	}

	shape, err := p.shape(ctx, tdb, st.Shadow)
	if err != nil {
		return err
	}
	marks, err := p.swapMarks(ctx, job)
	if err != nil {
		return err
	}
	exchanged := marks.Table
	if !exchanged && marks.ShadowExists && !marks.Shadow {
		// The name of the shadow table is taken by a table binwave did not make (the old table under it
		// after an exchange carries no marker either, but then the table carries it): nothing of it is
		// written to, swapped in or dropped.
		p.resyncs[st.Table] = job
		return p.holdResync(st.Table, errcode.SYNC104, &chsink.ConflictError{Err: fmt.Errorf(
			"ClickHouse has the table %s.%s without the marker of the resync of %s, which binwave did not make; "+
				"rename or drop it in ClickHouse to continue the resync", tdb, st.Shadow, st.Table)})
	}
	if !exchanged && p.q.isQuarantined(st.Table) {
		// A resync takes its table out of quarantine when it begins: one that is in it again was
		// quarantined during the resync, and saved before the resync was given up. Its shadow table may
		// miss the rows that were dropped with the quarantine.
		p.log.Warn("resync given up: the table was quarantined", "table", st.Table)
		p.giveUpShadow(ctx, st)
		p.syncResyncs()
		return nil
	}
	if !exchanged && !shape.Exists && (st.Done || readSome(st)) {
		// The rows read so far were in the shadow table, and it is gone: they would be missing. Read in full,
		// with the table never given its marker, the swap was not made, or it was finished and its end not
		// saved. Which, nothing in ClickHouse says, and taking it for finished would leave the table without
		// what the shadow table had: the table is read again.
		p.log.Warn("resync restarted: shadow table gone", "table", st.Table, "shadow", tdb+"."+st.Shadow)
		job.state.Done, job.state.Final = false, ""
		readFromStart(&job.state)
		if err := p.saveResumed(ctx, job.state); err != nil {
			return err
		}
		st = job.state
	}
	if st.Done || exchanged {
		// Only the stream reaching the last watermark, and the swap, are left. An exchange is made only
		// once the shadow table holds everything, also when the entry (saved by an earlier release) does
		// not say the read is done.
		job.doneSaved = st.Done
		p.forgetTable(st.Table)
		p.resyncs[st.Table] = job
		p.trackResync(st.Table, job)
		if exchanged {
			// The swap a crash interrupted is finished before the stream goes on: its changes of the
			// table would go to the name of the shadow table, which holds the old table or nothing.
			if _, err := p.swapIn(ctx, job, true); err != nil {
				return err
			}
			p.syncResyncs()
			return nil
		}
		p.log.Debug("resync continues, waiting for the stream",
			"table", st.Table, "final", st.Final, "initial_load", st.InitialLoad)
		return nil
	}
	if p.d.Snapshot == nil {
		return fmt.Errorf("the saved state has a resync of %s, but this service has no source to read a snapshot from", st.Table)
	}
	def, err := p.tableDefinition(ctx, db, name)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("continue the resync of %s: %w", st.Table, ctx.Err())
		}
		if p.holdsResyncs() {
			p.resyncs[st.Table] = job
			return p.holdResync(st.Table, errcode.SYNC101, fmt.Errorf("could not continue after a restart: %w", err))
		}
		p.giveUpShadow(ctx, st)
		p.restoreQuarantine(st, "could not continue after a restart: "+err.Error())
		return nil //nolint:nilerr // the resync is what failed; the stream goes on and the table is quarantined
	}
	target, err := p.shadowTable(def, st.Shadow)
	if err != nil {
		return err
	}
	ensure := func() error {
		return p.withRetry(ctx, "prepare the shadow table "+st.Shadow, func(actx context.Context) error {
			return p.d.Sink.Ensure(actx, target, chsink.DDL{Mode: p.cfg.DDL.Mode})
		})
	}
	err = ensure()
	var mismatch *chsink.SchemaMismatchError
	if errors.As(err, &mismatch) {
		// The table was changed while the service was down: what was read is in the old structure. The
		// shadow table is dropped first, by its identity, then made again. A drop that fails leaves the
		// entry as it was saved.
		p.log.Warn("resync restarted: table changed while stopped", "table", st.Table)
		if _, err := p.dropShadow(ctx, st); err != nil {
			return fmt.Errorf("resync %s: the shadow table of the old structure: %w", st.Table, err)
		}
		readFromStart(&job.state)
		job.state.Restarts = st.Restarts + 1
		if err := p.saveResumed(ctx, job.state); err != nil {
			return err
		}
		err = ensure()
	}
	if err != nil {
		return fmt.Errorf("resync %s: %w", st.Table, err)
	}
	job.tbl, job.target = def, target
	p.forgetTable(st.Table)
	p.resyncs[st.Table] = job
	p.trackResync(st.Table, job)
	if st.InitialLoad {
		// fillLoadSlots launches it once there is room, so hundreds of tables resumed at once do not
		// all start reading in the same instant.
		p.log.Debug("initial load continues after a restart", "table", st.Table, "rows_read", job.state.Rows)
		return nil
	}
	if err := p.launch(ctx, job); err != nil {
		return err
	}
	p.log.Debug("resync continues after a restart", "table", st.Table, "rows_read", job.state.Rows)
	return nil
}

// saveResumed saves the position with a resync that a restart makes read the table again, before its
// shadow table is made again: a crash after that would find an empty shadow table, and the saved place
// would skip what is before it.
func (p *Pipeline) saveResumed(ctx context.Context, st checkpoint.Resync) error {
	p.resumeLeft[st.Table] = st
	p.syncResyncs()
	return p.flush(ctx)
}

// readSome reports whether a resync read anything into its shadow table: a cursor, or a partition it
// goes on from (which is all there is at the boundary of two partitions).
func readSome(st checkpoint.Resync) bool {
	return len(st.Cursor) > 0 || st.Partition != "" || st.Rows > 0
}

// readFromStart makes a resync read the table from the start: the reader goes on from a partition even
// without a cursor.
func readFromStart(st *checkpoint.Resync) {
	st.Cursor, st.Partition, st.Rows = nil, "", 0
}

// tablesToLoad decides, once, at the moment replication starts for the first time (initial_position:
// snapshot), every table the initial load resynchronizes: every table the router matches that a chunk
// reader can read by key. A table it cannot (no primary key, a prefix key, a key type a chunk cannot
// resume from) is quarantined right here, loudly, with the saved state of the very first position:
// with the load promising the operator "every table gets its full history", a table that silently kept
// replicating from this moment on, missing everything before it, would be exactly the silent gap
// binwave never leaves. With tables.on_replication_error: stop, such a table refuses the start instead
// (the operator asked for zero tolerance, the same as any other table problem).
func (p *Pipeline) tablesToLoad(ctx context.Context, since string) ([]checkpoint.Resync, error) {
	var cat *schema.Catalog
	err := p.fromSource(ctx, "list the tables of the initial load", func() error {
		var err error
		cat, err = p.d.Snapshot.Catalog(ctx, "", "")
		return err
	})
	if err != nil {
		return nil, err
	}
	now := p.d.Now()
	var entries []checkpoint.Resync
	for i, t := range cat.Tables {
		// A read per table, for every table of the source: minutes on a large one, and /healthz calls a
		// pipeline that has not reported progress for half a minute stuck (internal/observability).
		p.progress()
		if done := i + 1; done%resyncResumeReportEvery == 0 {
			p.log.Info("deciding what the initial load covers", "done", done, "total", len(cat.Tables))
		}
		if _, err := p.tableDefinition(ctx, t.Database, t.Name); err != nil {
			if ctx.Err() != nil {
				// The read was cut short by the context, not refused because of this table's key: a
				// shutdown or a cancelled retry. Attributing it to the table here would quarantine it,
				// and every table after it in the catalog the loop never even reached, for a reason
				// that is not permanent and has nothing to do with any of them.
				return nil, fmt.Errorf("list the tables of the initial load: %w", ctx.Err())
			}
			if !p.Quarantine(t.FullName(), errcode.TBL106, err.Error()) {
				return nil, fmt.Errorf("%w: %s cannot be part of the initial load (tables.on_replication_error is stop): %w",
					ErrUnsupported, t.FullName(), err)
			}
			continue
		}
		shadow := chsink.ShadowName(p.cfg, t.Database, t.Name)
		tdb, ttable := chsink.TargetName(p.cfg, t.Database, t.Name)
		marks, err := p.marksOf(ctx, chsink.SwapRequest{Source: t.FullName(), Database: tdb, Table: ttable, Shadow: shadow})
		if err != nil {
			return nil, fmt.Errorf("list the tables of the initial load: %w", err)
		}
		if marks.ShadowExists || marks.Table {
			// What a resync checks before it begins (startResync): a table under the name of the shadow
			// table, or the marker of an unfinished swap on the table, is left from before, and the load
			// would reuse it or take it for its own swap.
			reason := fmt.Sprintf("ClickHouse has %s.%s, or %s.%s carries the marker of a resync, left from before; "+
				"drop the leftover (or remove the marker) and resync the table", tdb, shadow, tdb, ttable)
			if !p.Quarantine(t.FullName(), errcode.SYNC104, reason) {
				return nil, &chsink.ConflictError{Err: fmt.Errorf("%s cannot be part of the initial load: %s", t.FullName(), reason)}
			}
			continue
		}
		entries = append(entries, checkpoint.Resync{
			Table: t.FullName(), Shadow: shadow, Since: since, Started: now, InitialLoad: true,
			Estimate: t.EstimatedRows,
		})
	}
	if p.cfg.Replication.Snapshot.LargestFirstEnabled() {
		// The biggest tables first: the slots are few (replication.snapshot.parallelism) and the sizes
		// differ by orders of magnitude, so the order decides when the load ends. A table of a hundred
		// million rows that starts last leaves every other table waiting for it alone; started first, it
		// is read while hundreds of small ones come and go beside it. Ties keep the order of the catalog,
		// which is the order of the names.
		sort.SliceStable(entries, func(i, k int) bool { return entries[i].Estimate > entries[k].Estimate })
	}
	return entries, nil
}

// sourceDown reports whether a failed read of the source is one that passes with time.
func (p *Pipeline) sourceDown(err error) bool {
	return p.d.SourceTransient != nil && p.d.SourceTransient(err)
}

// fromSource runs a read of the source on the pipeline goroutine, waiting out a source that is down
// (a restart of MySQL takes longer than any fixed number of quick retries) for as long as it takes; any
// other failure is returned at once, as it was. The stream cannot go on meanwhile, but it could not
// anyway: the source it reads from is the one that is down.
func (p *Pipeline) fromSource(ctx context.Context, what string, read func() error) error {
	delay := p.retry.Initial
	for attempt := 1; ; attempt++ {
		err := read()
		if err == nil || ctx.Err() != nil || !p.sourceDown(err) {
			return err
		}
		p.d.Observer.SourceRetry(what, err)
		p.log.Warn("the source did not answer, retrying", "operation", what, "attempt", attempt, "retry_in", delay, "error", err)
		if p.d.Sleep(ctx, delay) != nil {
			return err
		}
		delay = min(delay*2, p.retry.Max)
	}
}

// tableDefinition is Snapshot.TableDefinition through fromSource.
func (p *Pipeline) tableDefinition(ctx context.Context, database, table string) (*schema.Table, error) {
	var def *schema.Table
	err := p.fromSource(ctx, "describe "+database+"."+table, func() error {
		var err error
		def, err = p.d.Snapshot.TableDefinition(ctx, database, table)
		return err
	})
	return def, err
}

// shadowTable resolves the shadow table of a resync from the definition that is read.
func (p *Pipeline) shadowTable(def *schema.Table, shadow string) (*chsink.Table, error) {
	t, err := chsink.Resolve(def, p.cfg, p.d.Reporter)
	if err != nil {
		return nil, fmt.Errorf("resync %s: %w", def.FullName(), err)
	}
	t.Name, t.Comment = shadow, chsink.ResyncMarker(t.Name)
	return t, nil
}

func (p *Pipeline) shape(ctx context.Context, database, table string) (chsink.TableShape, error) {
	var shape chsink.TableShape
	err := p.withRetry(ctx, "read "+database+"."+table, func(actx context.Context) error {
		var err error
		shape, err = p.d.Sink.Shape(actx, database, table)
		return err
	})
	return shape, err
}

// dropShadow drops the shadow table of a resync, if it is binwave's (by its identity,
// chsink.Client.DropShadow): a table that took its name is not touched. dropped is false when there was
// none of binwave's.
func (p *Pipeline) dropShadow(ctx context.Context, st checkpoint.Resync) (dropped bool, err error) {
	name, shadow := st.Table, st.Shadow
	db, table, _ := cutQualified(name)
	tdb, ttable := chsink.TargetName(p.cfg, db, table)
	err = p.withRetry(ctx, "drop the shadow table "+shadow, func(actx context.Context) error {
		dropped, err = p.d.Sink.DropShadow(actx, chsink.SwapRequest{Source: name, Database: tdb, Table: ttable, Shadow: shadow})
		return err
	})
	if err != nil {
		return false, fmt.Errorf("drop the shadow table %s.%s: %w", tdb, shadow, err)
	}
	return dropped, nil
}

// giveUpShadow drops the shadow table of a resync that is given up, its entry going with it: one that
// cannot be dropped is left for the operator.
func (p *Pipeline) giveUpShadow(ctx context.Context, st checkpoint.Resync) {
	if _, err := p.dropShadow(ctx, st); err != nil {
		p.log.Error("shadow table not dropped, drop it by hand", "table", st.Table, "error", err)
	}
}

// restoreQuarantine puts a table back in quarantine after a resync (or an initial load) that gave up.
// Recoverable is inherited from the job: if tables.autoresync started it, it gets to try again
// (bounded, see autoresyncQuarantined); an operator's own `tables resync` failing is not silently
// retried.
func (p *Pipeline) restoreQuarantine(st checkpoint.Resync, why string) {
	// The changes of the table since the resync began went to the shadow table, which is gone.
	reason := why + "; its changes since the resync began are not in the table"
	if st.Quarantined != "" {
		reason = st.Quarantined + " (" + reason + ")"
	}
	p.quarantine(st.Table, errcode.SYNC101, reason, st.Autoresync)
}

// holdsResyncs reports whether a resync that cannot go on stops replication instead of being given up:
// with tables.on_replication_error: stop the table cannot be quarantined.
func (p *Pipeline) holdsResyncs() bool {
	return p.cfg.Tables.OnReplicationError != config.OnErrorQuarantine
}

// holdResync stops replication on a resync that cannot go on, keeping it as it is in the position,
// so that resuming replication continues it.
func (p *Pipeline) holdResync(name string, code errcode.Code, err error) error {
	if job, ok := p.resyncs[name]; ok {
		p.stopJob(job)
	}
	f := failure.Failure{Time: p.d.Now(), Kind: failure.KindResync, Table: name, Code: string(code), Message: err.Error()}
	var conversion *typemap.ConversionError
	if errors.As(err, &conversion) {
		f.Detail = conversion.Detail()
	}
	p.log.Error("["+string(code)+"] resync stopped, replication stops with it", "table", name, "code", code, "error", err)
	return &FailedError{Failure: f, Err: err}
}

// registerPending keeps a resync that has to begin again in the position, as one that has to: a
// restart begins it then, and until it begins its table is written as it is.
func (p *Pipeline) registerPending(j restartJob) {
	db, table, _ := cutQualified(j.name)
	st := checkpoint.Resync{
		Table: j.name, Shadow: chsink.ShadowName(p.cfg, db, table), Version: p.versions.Last(), Since: p.committed.GTIDSet,
		Quarantined: j.quarantined, Restarts: j.restarts, Started: p.d.Now(),
		InitialLoad: j.origin.initialLoad, Autoresync: j.origin.autoresync, Pending: true,
	}
	job := &resyncJob{state: st}
	job.status = p.statusOf(job, snapshot.Progress{Fraction: -1})
	p.resyncs[j.name] = job
	p.resyncSetChanged = true
	p.syncResyncs()
}

// holdPending keeps a resync that has to begin again in the position, and stops replication on it.
func (p *Pipeline) holdPending(j restartJob, err error) error {
	p.registerPending(j)
	return p.holdResync(j.name, errcode.SYNC101, fmt.Errorf("could not begin again after a DDL statement: %w", err))
}

// launch starts the goroutine that reads the chunks of a job.
func (p *Pipeline) launch(ctx context.Context, job *resyncJob) error {
	cursor, err := checkpoint.DecodeCursor(job.state.Cursor)
	if err != nil {
		return err
	}
	rctx, cancel := context.WithCancel(ctx)
	job.cancel = cancel
	job.results = make(chan chunkResult, 2)
	job.stopped = make(chan struct{})
	// From here the pipeline has to look at this job on every transaction boundary: a chunk can be
	// waiting in results. A name left behind in waiting is not a candidate any more (results is set).
	p.reading[job.state.Table] = job
	job.status = p.statusOf(job, snapshot.Progress{Fraction: -1})
	p.publishResyncs()
	opts := snapshot.Options{Sizer: p.sizerOptions()}
	if job.state.InitialLoad {
		// Every table the initial load is reading at once shares one budget; a `tables resync` does not.
		opts.Budget = p.loadBudget
	}
	opts.Resume = snapshot.Scope{Strategy: snapshot.Strategy(job.state.Strategy), Partition: job.state.Partition}
	reader := snapshot.NewReader(p.d.Snapshot, job.tbl, cursor, opts)
	if cursor != nil && !reader.Resumed() {
		// The saved place cannot be continued: the table is read another way now, or the partition the
		// cursor belongs to is gone. Reading from the start is safe (the rows go to the shadow table
		// again with the same versions) but it is not free, so it is said out loud.
		p.log.Warn("snapshot cursor unusable, reading from start",
			"table", job.state.Table, "strategy", job.state.Strategy, "partition", job.state.Partition,
			"rows_read", job.state.Rows)
		job.state.Cursor, job.state.Partition, job.state.Rows = nil, "", 0
	}
	scope := reader.Scope()
	job.state.Strategy, job.state.Partition = string(scope.Strategy), scope.Partition
	// The reading goroutine converts what it read: conversion is the expensive half of a chunk, and
	// the pipeline goroutine has the whole stream to serve. The definitions it works with are its own
	// (a structure change interrupts the resync, it does not rewrite them under it), and conversion
	// itself only reads them, so parallel readers and the stream never race over one.
	tbl, target, version := job.tbl, job.target, job.state.Version
	go func() {
		defer close(job.stopped)
		failures, attempt, delay := 0, 0, p.retry.Initial
		for {
			chunk, err := reader.Next(rctx)
			if err != nil {
				if rctx.Err() != nil {
					return
				}
				attempt++
				down := p.sourceDown(err)
				if !down {
					failures++
				}
				if failures > maxReadFailures {
					select {
					case job.results <- chunkResult{err: err}:
					case <-rctx.Done():
					}
					return
				}
				p.d.Observer.SourceRetry("read "+job.state.Table, err)
				p.log.Warn("chunk read failed, retrying", "table", job.state.Table,
					"attempt", attempt, "retry_in", delay, "source_down", down, "error", err)
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-rctx.Done():
					timer.Stop()
					return
				}
				delay = min(delay*2, p.retry.Max)
				continue
			}
			failures, attempt, delay = 0, 0, p.retry.Initial
			res := chunkResult{nrows: len(chunk.Rows), progress: reader.Progress()}
			rows := make([]event.Row, len(chunk.Rows))
			for i, r := range chunk.Rows {
				rows[i] = event.Row{After: r}
			}
			res.rows, res.convert = target.Rows(event.Rows{Table: tbl, Op: event.Insert, Rows: rows}, version)
			chunk.Rows = nil // converted; the source images are not needed any more
			res.chunk = chunk
			select {
			case job.results <- res:
			case <-rctx.Done():
				return
			}
			if chunk.Done || res.convert != nil {
				return
			}
		}
	}()
	return nil
}

func (p *Pipeline) stopJob(job *resyncJob) {
	if job.cancel != nil {
		job.cancel()
		<-job.stopped
		job.cancel = nil
	}
}

// stopResyncs ends the goroutines that read chunks; the state stays, and the next run continues.
func (p *Pipeline) stopResyncs() {
	for _, j := range p.resyncs {
		p.stopJob(j)
	}
}

func (p *Pipeline) statusOf(j *resyncJob, pr snapshot.Progress) snapshot.Status {
	remaining := pr.Remaining.Seconds()
	kind := "resync"
	switch {
	case j.state.InitialLoad:
		kind = "initial_load"
	case j.state.Autoresync:
		kind = "autoresync"
	}
	return snapshot.Status{
		Table: j.state.Table, Kind: kind, Rows: j.state.Rows, Fraction: pr.Fraction, RowsPerSecond: pr.RowsPerSecond,
		RemainingSeconds: remaining, ChunkSize: pr.ChunkSize, Restarts: j.state.Restarts,
		Partitions: pr.Partitions, PartitionsDone: pr.PartitionsDone,
		Reading: j.results != nil && !j.state.Done, Done: j.state.Done, Started: j.state.Started,
	}
}

// fillLoadSlots starts initial-load resyncs that are waiting for a slot, up to
// replication.snapshot.parallelism running at once: reading hundreds of tables in chunks at the same
// time would overwhelm the source and ClickHouse alike. A `binwave tables resync` is not held to this:
// an operator who asks for one does not expect it queued behind an unrelated initial load, so it is
// always launched at once (see startResync).
// It walks the waiting names, not every registered resync: with one entry per table of the source,
// counting the running ones over all of them is work every transaction boundary would pay for
// thousands of tables that are only waiting their turn.
func (p *Pipeline) fillLoadSlots(ctx context.Context) error {
	if len(p.waiting) == 0 {
		return nil
	}
	limit := max(p.cfg.Replication.Snapshot.Parallelism, 1)
	running := 0
	for _, job := range p.reading {
		if job.state.InitialLoad && !job.state.Done && job.cancel != nil {
			running++
		}
	}
	if !p.waitingSorted {
		p.sortWaiting()
		p.waitingSorted = true
	}
	kept := p.waiting[:0]
	for i, name := range p.waiting {
		job, ok := p.resyncs[name]
		switch {
		case !ok || job.results != nil:
			continue // gone, or launched already: not waiting any more
		case running >= limit:
			kept = append(kept, p.waiting[i:]...)
			p.waiting = kept
			return nil
		}
		if err := p.launch(ctx, job); err != nil {
			// Whatever is left keeps its turn; the failure ends this pass.
			p.waiting = append(kept, p.waiting[i:]...)
			return err
		}
		running++
	}
	p.waiting = kept
	return nil
}

// sortWaiting puts the tables waiting for a slot in the order they are loaded in: the biggest first
// (replication.snapshot.largest_first), or by name when that is off. Names break ties, so the order is
// the same on every run and after a restart, where the queue comes back from the saved state in name
// order and the sizes come with it (checkpoint.Resync.Estimate).
func (p *Pipeline) sortWaiting() {
	largestFirst := p.cfg.Replication.Snapshot.LargestFirstEnabled()
	sort.Slice(p.waiting, func(i, k int) bool {
		a, b := p.waiting[i], p.waiting[k]
		if largestFirst {
			if ea, eb := p.estimateOf(a), p.estimateOf(b); ea != eb {
				return ea > eb
			}
		}
		return a < b
	})
}

// estimateOf is what the server believed a table waiting for a slot holds; 0 for one that is gone.
func (p *Pipeline) estimateOf(name string) int64 {
	if job, ok := p.resyncs[name]; ok {
		return job.state.Estimate
	}
	return 0
}

// maxAutoresyncAttempts caps how many times tables.autoresync retries the same table, within one run
// of the pipeline, before it stops and leaves the table in ordinary quarantine for an operator: a
// table whose problem is not actually one a resync fixes (the classification in
// chsink.Recoverable turned out wrong, or the resync fails for an unrelated reason such as the
// source being unreachable) would otherwise be retried every heartbeat, forever. Not persisted: a
// restart of the process gives a table a fresh budget of attempts.
const maxAutoresyncAttempts = 3

// autoresyncQuarantined starts a resync, without an operator, for every quarantined table whose
// reason is one a resync fixes on its own (checkpoint.Quarantined.Recoverable, decided when the
// table was quarantined; see chsink.Recoverable). Off by default (tables.autoresync).
func (p *Pipeline) autoresyncQuarantined(ctx context.Context) error {
	if !p.cfg.Tables.Autoresync {
		return nil
	}
	for _, q := range p.q.active() {
		if !q.Recoverable {
			continue
		}
		if _, active := p.resyncs[q.Table]; active {
			continue
		}
		if p.autoresyncAttempts[q.Table] >= maxAutoresyncAttempts {
			continue // tried and failed enough times already; stays quarantined for an operator to look at
		}
		// Counted before anything is tried: a table that fails even to be read must count against
		// the budget too, or an unreachable source would retry it every heartbeat forever.
		p.autoresyncAttempts[q.Table]++
		attempt := p.autoresyncAttempts[q.Table]
		lastAttempt := attempt >= maxAutoresyncAttempts
		giveUpCode := errcode.SYNC202
		if lastAttempt {
			giveUpCode = errcode.SYNC203
		}
		p.log.Info("["+string(errcode.SYNC201)+"] autoresync started", "table", q.Table, "code", errcode.SYNC201, "attempt", attempt, "of", maxAutoresyncAttempts)
		db, table, _ := cutQualified(q.Table)
		def, err := p.tableDefinition(ctx, db, table)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("autoresync %s: %w", q.Table, ctx.Err())
			}
			p.log.Error("["+string(giveUpCode)+"] autoresync could not read the table", "table", q.Table, "code", giveUpCode, "detail", err.Error())
			p.d.Observer.AutoresyncResolved(q.Table, false, attempt)
			continue
		}
		if err := p.startResync(ctx, q.Table, def, "", 0, resyncOrigin{autoresync: true}); err != nil {
			p.log.Error("["+string(giveUpCode)+"] autoresync could not begin", "table", q.Table, "code", giveUpCode, "detail", err.Error())
			p.d.Observer.AutoresyncResolved(q.Table, false, attempt)
			continue
		}
	}
	return nil
}

// serviceResyncs does what the resyncs need of the pipeline goroutine, between transactions: begins
// the ones asked for or that tables.autoresync starts on its own, gives initial-load resyncs their
// slots, takes the chunks that were read, and finishes the ones that are ready.
func (p *Pipeline) serviceResyncs(ctx context.Context) error {
	changed, swapped := false, false
	// A table quarantined since the last pass gives its resync up: its changes cannot be applied, so
	// neither can the snapshot's. Only those tables are looked at, not every resync there is. Drained
	// even when nothing is being resynchronized, so the list does not keep names nobody will ask for.
	for _, name := range p.q.drainFresh() {
		if _, resyncing := p.resyncs[name]; !resyncing || !p.q.isQuarantined(name) {
			continue
		}
		p.abortResync(ctx, name, "the table was quarantined again", false)
		changed = true
	}
	if len(p.resyncs) == 0 && len(p.restarting) == 0 && !p.requested() && !p.cfg.Tables.Autoresync {
		return nil
	}
	if p.tx == nil {
		// Beginning a resync, and beginning one again after a statement, are done between transactions.
		if err := p.restartInterrupted(ctx); err != nil {
			return err
		}
		if err := p.startRequested(ctx); err != nil {
			return err
		}
		if err := p.autoresyncQuarantined(ctx); err != nil {
			return err
		}
		if err := p.fillLoadSlots(ctx); err != nil {
			return err
		}
	}
	// The chunks that were read: only the tables with a reader, at most parallelism plus the resyncs an
	// operator asked for.
	names := make([]string, 0, len(p.reading))
	for name := range p.reading {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		job, ok := p.resyncs[name]
		if !ok {
			continue // given up while this pass ran
		}
		did, err := p.takeChunks(ctx, job)
		if err != nil {
			return err
		}
		changed = changed || did
	}
	if p.tx == nil {
		did, err := p.swapReady(ctx)
		if err != nil {
			return err
		}
		changed = changed || did
		swapped = swapped || did
	}
	if changed {
		p.syncResyncs()
		p.publishResyncs()
	}
	p.publishStaleResyncs()
	if swapped {
		// The position is saved at once after a swap (after syncResyncs above, so it carries the state
		// without the resync that finished), although the flush interval may not have passed. The rows do
		// not need it — they were written before the swap — but the entry of a finished resync does: a
		// restart that still finds it, with the shadow table gone, cannot tell a finished swap from a
		// shadow table someone dropped, so it reads the whole table again (it says so in the log). One
		// save per pass, not per table, so a burst of swaps at the end of an initial load pays for one.
		if err := p.flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

// swapReady replaces the tables whose snapshot the stream has caught up with, from the head of the
// queue of tables waiting for it.
//
// It stops at the first table the stream has not reached, and that is what keeps the cost of a
// transaction the cost of the work in flight rather than the cost of what the source has: the queue is
// ordered the way the stream reaches the watermarks (see awaitSwap), so a head that is not reached
// means none behind it is either. The alternative — asking every waiting table on every transaction —
// parsed a GTID set per table per transaction: with thousands of tables waiting at the end of an
// initial load that was tens of megabytes of garbage per second, and the service spent its time in
// the garbage collector instead of on the stream.
func (p *Pipeline) swapReady(ctx context.Context) (bool, error) {
	if len(p.awaiting) == 0 {
		return false, nil
	}
	// One parse of the committed set for the whole pass, however many tables are replaced by it.
	committed, err := gtid.Parse(p.committed.GTIDSet)
	if err != nil {
		return false, fmt.Errorf("the committed position cannot be read back: %w", err)
	}
	var saved *gtid.Set // the saved position, parsed once a table is ready to be swapped in
	did := false
	for len(p.awaiting) > 0 {
		job := p.awaiting[0]
		if _, still := p.resyncs[job.state.Table]; !still {
			p.awaiting = p.awaiting[1:] // given up while this pass ran
			continue
		}
		if job.lostFinal != nil {
			if p.holdsResyncs() {
				return did, p.holdResync(job.state.Table, errcode.SYNC101, fmt.Errorf("its watermark cannot be read: %w", job.lostFinal))
			}
			p.abortResync(ctx, job.state.Table, fmt.Sprintf("its watermark cannot be read: %v", job.lostFinal), true)
			did = true
			continue
		}
		if !committed.Covers(job.final) {
			break
		}
		if saved == nil {
			if saved, err = gtid.Parse(p.saved.GTIDSet); err != nil {
				saved = nil // nothing it covers is known: the save below replaces it
			}
		}
		if !job.doneSaved || saved == nil || !saved.Covers(job.final) {
			// The exchange comes after a saved position that has the table read in full, with the stream
			// at its watermark and every row written: a restart that found the read still going would
			// write the changes of the table under the name of the shadow table, which holds the old one.
			// One save covers every table the stream has reached, so a burst of swaps pays for one.
			p.syncResyncs()
			if err := p.flush(ctx); err != nil {
				return did, err
			}
			for _, j := range p.awaiting {
				j.doneSaved = true
			}
			saved = committed
		}
		finished, err := p.finishIfReady(ctx, job, committed)
		if err != nil {
			return did, err
		}
		did = did || finished
		if len(p.awaiting) > 0 && p.awaiting[0] == job {
			// The swap did not happen and did not fail either (the table was quarantined, or the entry
			// was given up); nothing more to do with it in this pass.
			break
		}
	}
	return did, nil
}

// requested reports whether a resync is waiting to be begun.
func (p *Pipeline) requested() bool {
	p.rs.mu.Lock()
	defer p.rs.mu.Unlock()
	return len(p.rs.queue) > 0
}

// refuseRequests answers the requests for a resync that will not be served, because Run ends.
func (p *Pipeline) refuseRequests() {
	p.rs.mu.Lock()
	queue := p.rs.queue
	p.rs.queue = nil
	p.rs.mu.Unlock()
	for _, req := range queue {
		req.reply <- ErrNotRunning
	}
}

// startRequested begins the resyncs that were asked for.
func (p *Pipeline) startRequested(ctx context.Context) error {
	p.rs.mu.Lock()
	queue := p.rs.queue
	p.rs.queue = nil
	p.rs.mu.Unlock()
	began := false
	for _, req := range queue {
		// What the operator asked for and cannot be done is answered to the operator; it does not stop replication.
		err := p.startResync(ctx, req.table, req.def, "", 0, resyncOrigin{})
		began = began || err == nil
		req.reply <- err
	}
	if began {
		// The state is saved at once: a resync that a restart forgets would leave its shadow table behind.
		return p.flush(ctx)
	}
	return nil
}

// resyncOrigin says why a resync began. It changes nothing about how the resync itself works — only
// how the pipeline schedules it (fillLoadSlots) and reports it (status, metrics, logs).
type resyncOrigin struct {
	// initialLoad is true for a table the initial load (replication.initial_position: snapshot)
	// began; see checkpoint.Resync.InitialLoad.
	initialLoad bool
	// autoresync is true for a table tables.autoresync began on its own, from quarantine; see
	// checkpoint.Resync.Autoresync and autoresyncQuarantined.
	autoresync bool
}

// startResync begins the resync of a table: an operator's `tables resync`, or a table
// tables.autoresync began on its own — both origin.initialLoad false, always launched at once — or
// one table of the initial load (origin.initialLoad true, launched once fillLoadSlots gives it a
// slot: the only origin held to replication.snapshot.parallelism, since it is the only one that can
// mean hundreds of tables at once). Its definition is what the snapshot reads with.
func (p *Pipeline) startResync(ctx context.Context, name string, def *schema.Table, quarantined string, restarts int, origin resyncOrigin) error {
	if job, active := p.resyncs[name]; active && !job.state.Pending {
		return fmt.Errorf("%s is being resynchronized already", name)
	}
	if p.cfg.DDL.DryRun {
		return errors.New("ddl.dry_run is set, and a resync creates a table: run the statements yourself or turn dry_run off")
	}
	db, table, _ := cutQualified(name)
	// The stream and the snapshot must see one structure: a change of it that the stream has not
	// reached yet would make the rows of the two differ. A currently quarantined table is not behind
	// in this sense — the opposite: the stream already reached and failed on exactly the statement
	// that changed it, which is why it never updated the registry, so its (stale) entry there is not
	// a structure the stream still has to deliver.
	if b, ok := p.registry[name]; ok && b.sig != def.Signature() && !p.q.isQuarantined(name) {
		return fmt.Errorf("the structure of %s in the source is not the one the stream has reached (a change of it is still waiting in the binlog): "+
			"try again when the stream has caught up", name)
	}
	tdb, ttable := chsink.TargetName(p.cfg, db, table)
	shadow := chsink.ShadowName(p.cfg, db, table)
	// A table that appeared after the start was never checked by the preflight: its swap must not put
	// its rows in the place of another source table's target or of another resync's shadow table.
	for _, held := range []string{ttable, shadow} {
		full := (&chsink.Table{Database: tdb, Name: held}).FullName()
		if owner, taken := p.ownerOf(full); taken && owner != name {
			return fmt.Errorf("source tables %s and %s would both be written to ClickHouse table %s; "+
				"give one of them another name with tables.overrides.<database.table>.target_table", owner, name, full)
		}
	}

	// A resync creates a table, fills it, and drops the old one in the swap. Whether the user may is
	// found out now, not in the middle of the swap, where a refusal would stop the service.
	if err := p.d.Sink.CheckAccess(ctx, tdb, "CREATE TABLE", "DROP TABLE", "INSERT", "SELECT"); err != nil {
		return fmt.Errorf("a resync needs these privileges on %s: %w", tdb, err)
	}
	shape, err := p.shape(ctx, tdb, shadow)
	if err != nil {
		return err
	}
	if shape.Exists {
		return &chsink.ConflictError{Err: fmt.Errorf("ClickHouse has the table %s.%s, left by a resync of %s that did not finish or was given up; "+
			"binwave does not reuse or drop it: drop it in ClickHouse and start the resync again", tdb, shadow, name)}
	}
	// The marker on the table says a swap of it was made and not cleaned up: the swap of this resync
	// would take it for its own, and drop the shadow table it filled.
	marks, err := p.swapMarks(ctx, &resyncJob{state: checkpoint.Resync{Table: name, Shadow: shadow}})
	if err != nil {
		return err
	}
	if marks.Table {
		return &chsink.ConflictError{Err: fmt.Errorf("the table %s.%s carries the marker of a resync whose swap was not finished; "+
			"remove it (ALTER TABLE ... MODIFY COMMENT '') once the table is the one to keep, and start the resync again", tdb, ttable)}
	}
	target, err := p.shadowTable(def, shadow)
	if err != nil {
		return err
	}
	// Everything before this point goes to the table, and will be replaced with it.
	if err := p.flush(ctx); err != nil {
		return err
	}
	if err := p.withRetry(ctx, "create the shadow table "+shadow, func(actx context.Context) error {
		return p.d.Sink.Ensure(actx, target, chsink.DDL{Mode: p.cfg.DDL.Mode})
	}); err != nil {
		return fmt.Errorf("create the shadow table of %s: %w", name, err)
	}

	if reason, ok := p.q.remove(name); ok {
		quarantined = reason
		p.qChanged.Store(true)
		p.syncQuarantine()
		p.d.Observer.QuarantineChanged(p.q.active())
	}
	job := &resyncJob{
		state: checkpoint.Resync{
			Table: name, Shadow: shadow, Version: p.versions.Last(), Since: p.committed.GTIDSet,
			Quarantined: quarantined, Restarts: restarts, Started: p.d.Now(),
			InitialLoad: origin.initialLoad, Autoresync: origin.autoresync,
		},
		tbl: def, target: target,
	}
	job.status = p.statusOf(job, snapshot.Progress{Fraction: -1})
	p.resyncs[name] = job
	p.resyncSetChanged = true
	p.trackResync(name, job)
	p.forgetTable(name) // the next rows of the table bind to the shadow table
	if !origin.initialLoad {
		if err := p.launch(ctx, job); err != nil {
			return err
		}
	}
	p.syncResyncs()
	p.publishResyncs()
	p.log.Info("resync started",
		"table", name, "shadow", tdb+"."+shadow, "version", job.state.Version, "restarts", restarts,
		"initial_load", origin.initialLoad, "autoresync", origin.autoresync)
	return nil
}

// takeChunks writes the chunks that the reader has ready.
func (p *Pipeline) takeChunks(ctx context.Context, job *resyncJob) (bool, error) {
	did := false
	for {
		select {
		case res := <-job.results:
			did = true
			if res.err != nil {
				if p.holdsResyncs() {
					return true, p.holdResync(job.state.Table, errcode.SYNC101, fmt.Errorf("the table could not be read: %w", res.err))
				}
				p.log.Error("resync failed: source read", "table", job.state.Table, "error", res.err)
				p.abortResync(ctx, job.state.Table, fmt.Sprintf("the table could not be read: %v", res.err), true)
				return true, nil //nolint:nilerr // the resync is what failed; the stream goes on and the table is quarantined
			}
			if err := p.writeChunk(ctx, job, res); err != nil {
				return did, err
			}
			// One event can drain every chunk the readers have ready, each a converted batch of up to
			// SizerOptions.Max rows with its insert and its saved position: minutes of real work under a
			// single handle call. Progress belongs to the work, not to the event, or the watchdog takes
			// the work for a hang.
			p.progress()
			if _, still := p.resyncs[job.state.Table]; !still {
				return did, nil // the chunk could not be converted, and the resync was given up
			}
		default:
			return did, nil
		}
	}
}

func (p *Pipeline) writeChunk(ctx context.Context, job *resyncJob, res chunkResult) error {
	name := job.state.Table
	if err := res.convert; err != nil {
		if p.holdsResyncs() {
			return p.holdResync(name, causeCode(err), fmt.Errorf("resync of %s: %w", name, err))
		}
		p.log.Error("resync failed: conversion", "table", name, "error", err)
		p.abortResync(ctx, name, err.Error(), false)
		return p.tableFailed(name, fmt.Errorf("resync of %s: %w", name, err))
	}
	p.add(job.target, res.rows)
	if res.nrows > 0 {
		p.d.Observer.ResyncRowsRead(name, res.nrows)
	}
	// The saved place must be the one the next chunk starts from, cursor and scope together: inside a
	// partition that is the key of the last row, and at the end of one it is the beginning of the next.
	switch {
	case res.chunk.PartitionDone:
		job.state.Cursor, job.state.Partition = nil, res.chunk.NextPartition
	case res.chunk.Last != nil:
		cursor, err := checkpoint.EncodeCursor(res.chunk.Last)
		if err != nil {
			return fmt.Errorf("resync %s: %w", name, err)
		}
		job.state.Cursor, job.state.Partition = cursor, res.chunk.Partition
	}
	job.state.Rows += int64(res.nrows)
	if res.chunk.Done {
		job.state.Done, job.state.Final = true, res.chunk.Watermark
		job.tbl = nil
		// From a reader to the queue of tables waiting for the stream, with Final parsed once (awaitSwap).
		p.awaitSwap(job)
		// What happens next (the swap once the stream reaches "final") belongs in the documentation,
		// not in a line that is written once per table of the initial load.
		p.log.Info("snapshot read, waiting for the stream",
			"table", name, "rows", job.state.Rows, "final", job.state.Final)
	}
	job.status = p.statusOf(job, res.progress)
	job.status.Rows = job.state.Rows
	job.status.Done = job.state.Done
	return p.flushIfNeeded(ctx)
}

// finishIfReady replaces the table with its shadow table once the snapshot is read and the stream has
// reached the point the last chunk was read at.
func (p *Pipeline) finishIfReady(ctx context.Context, job *resyncJob, committed *gtid.Set) (bool, error) {
	if !committed.Covers(job.final) {
		return false, nil
	}
	return p.swapIn(ctx, job, false)
}

// swapRequest is the swap of the shadow table of a job in the place of its table.
func (p *Pipeline) swapRequest(job *resyncJob) chsink.SwapRequest {
	name := job.state.Table
	db, table, _ := cutQualified(name)
	tdb, ttable := chsink.TargetName(p.cfg, db, table)
	return chsink.SwapRequest{Source: name, Database: tdb, Table: ttable, Shadow: job.state.Shadow}
}

// swapMarks reads which of the table and the shadow table of a job carries its marker.
func (p *Pipeline) swapMarks(ctx context.Context, job *resyncJob) (chsink.SwapMarks, error) {
	return p.marksOf(ctx, p.swapRequest(job))
}

// marksOf reads which of the table and the shadow table of a swap carries its marker.
func (p *Pipeline) marksOf(ctx context.Context, req chsink.SwapRequest) (chsink.SwapMarks, error) {
	var marks chsink.SwapMarks
	err := p.withRetry(ctx, "read the swap of "+req.Source, func(actx context.Context) error {
		var err error
		marks, err = p.d.Sink.SwapMarks(actx, req)
		return err
	})
	if err != nil {
		return chsink.SwapMarks{}, fmt.Errorf("resync %s: %w", req.Source, err)
	}
	return marks, nil
}

// swapIn replaces the table with the shadow table of a job that is complete, and ends the job. exchanged
// says a restart found the exchange made: only the cleanup after it is left.
func (p *Pipeline) swapIn(ctx context.Context, job *resyncJob, exchanged bool) (bool, error) {
	// The rows come first: the shadow table must hold everything before it takes the table's place. The
	// position that no longer has this resync is saved by serviceResyncs right after the pass, so a
	// restart does not find an entry whose shadow table is gone and read the whole table again.
	if err := p.flushBuffers(ctx); err != nil {
		return false, err
	}
	req := p.swapRequest(job)
	name, tdb, ttable := req.Source, req.Database, req.Table
	if !exchanged && p.q.isQuarantined(name) {
		// A write of the table failed in this pass, and its rows were dropped with the quarantine: the
		// shadow table does not hold everything, and does not take the table's place.
		p.log.Warn("resync given up: the table was quarantined", "table", name)
		p.abortResync(ctx, name, "the table was quarantined again", false)
		return true, nil
	}
	err := p.withRetry(ctx, "replace "+name+" with its resynchronized copy", func(actx context.Context) error {
		copied, err := p.d.Sink.SwapShadow(actx, req)
		if copied != "" {
			// An exchange may have been made: a repeat tells the swap finished from the copy's UUID.
			req.Copy, exchanged = copied, true
		}
		return err
	})
	if err != nil {
		if exchanged {
			// The table is the copy already, or may be: giving the resync up would drop what now has the
			// name of the shadow table and leave the marker on the table, where the next resync would take
			// it for its own swap. Replication stops, and resuming finishes the swap.
			return false, p.holdResync(name, errcode.SYNC101, fmt.Errorf("the exchange of the table with its copy was sent, and the swap did not finish: %w", err))
		}
		if chsink.IsTableProblem(err) {
			if p.holdsResyncs() {
				return false, p.holdResync(name, errcode.SYNC101, fmt.Errorf("the table could not be replaced with its copy: %w", err))
			}
			p.abortResync(ctx, name, err.Error(), true)
			return true, nil
		}
		return false, fmt.Errorf("resync %s: %w", name, err)
	}
	p.stopJob(job)
	delete(p.resyncs, name)
	p.resyncSetChanged = true
	p.untrackResync(name)
	p.forgetTable(name)
	p.manage(name, checkpoint.Target{Database: tdb, Table: ttable})
	p.d.Observer.ResyncFinished(name, true)
	if job.state.Autoresync {
		// tables.autoresync fixed this on its own: visible in a metric even though nobody had to look,
		// which is the whole point of counting it separately from an operator's own tables resync.
		p.d.Observer.AutoresyncResolved(name, true, p.autoresyncAttempts[name])
		delete(p.autoresyncAttempts, name)
	}
	p.log.Info("resync finished, table swapped in",
		"table", name, "rows", job.state.Rows, "took", p.d.Now().Sub(job.state.Started).Round(time.Second),
		"restarts", job.state.Restarts, "initial_load", job.state.InitialLoad, "autoresync", job.state.Autoresync)
	return true, nil
}

// abortResync gives a resync (or an initial-load resync) up: the reading stops, the shadow table is
// dropped and the state removed. With restore the table goes back to quarantine, with the reason it
// had and the one it was given up for.
func (p *Pipeline) abortResync(ctx context.Context, name, why string, restore bool) {
	job, ok := p.resyncs[name]
	if !ok {
		return
	}
	p.stopJob(job)
	delete(p.resyncs, name)
	p.resyncSetChanged = true
	p.untrackResync(name)
	p.giveUpShadow(ctx, job.state)
	p.forgetTable(name)
	p.dropShadowBuffers(name, job.state.Shadow)
	p.d.Observer.ResyncFinished(name, false)
	if job.state.Autoresync {
		// The count is kept (not deleted) so the next attempt, if any, still sees how many came before
		// it: that is what caps them at maxAutoresyncAttempts.
		p.d.Observer.AutoresyncResolved(name, false, p.autoresyncAttempts[name])
	}
	if restore {
		p.restoreQuarantine(job.state, "the resync was given up: "+why)
	}
	p.log.Warn("resync given up", "table", name, "reason", why, "rows_read", job.state.Rows,
		"initial_load", job.state.InitialLoad, "autoresync", job.state.Autoresync)
	p.syncResyncs()
	p.publishResyncs()
}

// dropShadowBuffers removes what is buffered for a shadow table that is going away: the rows of the
// snapshot and the ones of the stream, which are held apart because they are converted for the same table by
// different structures.
func (p *Pipeline) dropShadowBuffers(source, shadow string) {
	kept := p.order[:0]
	for _, t := range p.order {
		if t.Source.FullName() != source || t.Name != shadow {
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

// resyncImpact says what a DDL statement does to the resyncs.
type resyncImpact struct {
	table string
	// restart is true when the resync begins again after the statement: the structure changed, or the
	// rows were truncated. Otherwise the table is gone or renamed, and the resync ends.
	restart bool
	renamed string
}

// resyncImpacts lists the resyncs that a DDL statement touches.
func (p *Pipeline) resyncImpacts(e event.DDL) []resyncImpact {
	if len(p.resyncs) == 0 {
		return nil
	}
	op, err := ddl.Parse(e.Query, e.Database)
	if err != nil {
		return nil
	}
	var out []resyncImpact
	add := func(n ddl.Name, restart bool, renamed string) {
		if _, ok := p.resyncs[n.String()]; ok {
			out = append(out, resyncImpact{table: n.String(), restart: restart, renamed: renamed})
		}
	}
	switch o := op.(type) {
	case ddl.AlterTable:
		var renamedTo string
		for _, spec := range o.Specs {
			if r, ok := spec.(ddl.RenameTo); ok {
				renamedTo = r.Table.String()
			}
		}
		if renamedTo != "" {
			add(o.Table, false, renamedTo)
		} else if !onlyIgnorable(o) {
			add(o.Table, true, "")
		}
	case ddl.RenameTable:
		for _, pair := range o.Pairs {
			add(pair.From, false, pair.To.String())
		}
	case ddl.DropTable:
		for _, t := range o.Tables {
			add(t, false, "")
		}
	case ddl.TruncateTable:
		add(o.Table, true, "")
	case ddl.DropDatabase:
		for name := range p.resyncs {
			if db, _, ok := cutQualified(name); ok && db == o.Database {
				out = append(out, resyncImpact{table: name})
			}
		}
	}
	return out
}

// onlyIgnorable reports whether an ALTER TABLE changes nothing a snapshot depends on (an index, a table
// option, a partition operation that keeps the rows): the rows and the columns are what they were.
func onlyIgnorable(a ddl.AlterTable) bool {
	for _, spec := range a.Specs {
		switch s := spec.(type) {
		case ddl.Ignored:
		case ddl.PartitionOp:
			if s.Destructive {
				return false // its rows are gone from the source and stay in ClickHouse, the shadow table included
			}
		default:
			return false
		}
	}
	return true
}

// interruptResyncs gives up the resyncs a statement invalidates, and says so in the log.
func (p *Pipeline) interruptResyncs(ctx context.Context, e event.DDL, impacts []resyncImpact) {
	for _, im := range impacts {
		job := p.resyncs[im.table]
		kind := "resync"
		switch {
		case job.state.InitialLoad:
			kind = "initial load"
		case job.state.Autoresync:
			kind = "autoresync"
		}
		p.log.Warn("["+string(errcode.SYNC105)+"] "+kind+" discarded, DDL changed the table", "table", im.table, "code", errcode.SYNC105,
			"statement", e.Query, "rows_discarded", job.state.Rows, "restarts", job.state.Restarts)
		state := job.state
		p.abortResync(ctx, im.table, "a DDL statement changed the table", false)
		if !im.restart && im.renamed != "" && p.holdsResyncs() {
			// The renamed table holds the rows of before the resync, and nothing since: read it again.
			im.restart, im.table = true, im.renamed
		}
		if !im.restart {
			if im.renamed != "" {
				p.quarantine(im.renamed, errcode.SYNC105, "renamed from "+im.table+" during its resync", state.Autoresync)
			}
			continue
		}
		j := restartJob{
			name: im.table, quarantined: state.Quarantined, restarts: state.Restarts + 1,
			origin: resyncOrigin{initialLoad: state.InitialLoad, autoresync: state.Autoresync},
		}
		p.restarting = append(p.restarting, j)
		// Saved as one that has to begin again until it does: a position saved before that (the statement
		// itself, the flush the new resync begins with) must not forget it.
		p.registerPending(j)
	}
}

type restartJob struct {
	name        string
	quarantined string
	restarts    int
	// origin carries where the resync came from through the restart, so it is still an initial load
	// or an autoresync (not held up behind an operator's `tables resync`, still sharing the rate
	// budget if it is the initial load) the second time around.
	origin resyncOrigin
}

// restartInterrupted begins again the resyncs and initial loads that a statement discarded, with the
// structure it left.
func (p *Pipeline) restartInterrupted(ctx context.Context) error {
	jobs := p.restarting
	p.restarting = nil
	var held error
	for _, j := range jobs {
		db, table, _ := cutQualified(j.name)
		def, err := p.tableDefinition(ctx, db, table)
		if err == nil {
			err = p.startResync(ctx, j.name, def, j.quarantined, j.restarts, j.origin)
		}
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return fmt.Errorf("begin the resync of %s again: %w", j.name, ctx.Err())
		case p.holdsResyncs():
			if hold := p.holdPending(j, err); held == nil {
				held = hold
			}
		default:
			delete(p.resyncs, j.name)
			p.resyncSetChanged = true
			p.syncResyncs()
			p.quarantine(j.name, errcode.SYNC101, "could not begin again after a DDL statement: "+err.Error(), j.origin.autoresync)
		}
	}
	return held
}

// beginPending begins, after a restart, a resync that has to begin again from the start.
func (p *Pipeline) beginPending(ctx context.Context, st checkpoint.Resync) error {
	j := restartJob{
		name: st.Table, quarantined: st.Quarantined, restarts: st.Restarts,
		origin: resyncOrigin{initialLoad: st.InitialLoad, autoresync: st.Autoresync},
	}
	db, table, _ := cutQualified(st.Table)
	// A crash after the resync began again, before a position said so, leaves its shadow table: it is
	// binwave's (the marker says so) and holds nothing that is kept, since the read begins from the start.
	// A drop that fails leaves the entry as it was saved.
	dropped, err := p.dropShadow(ctx, st)
	if err != nil {
		return fmt.Errorf("begin the resync of %s again: %w", st.Table, err)
	}
	if dropped {
		p.log.Info("shadow table of a resync begun again dropped", "table", st.Table, "shadow", st.Shadow)
	}
	def, err := p.tableDefinition(ctx, db, table)
	if err == nil {
		err = p.startResync(ctx, j.name, def, j.quarantined, j.restarts, j.origin)
	}
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return fmt.Errorf("begin the resync of %s again: %w", st.Table, ctx.Err())
	case p.holdsResyncs():
		return p.holdPending(j, err)
	default:
		// As when it cannot begin again right after the statement (restartInterrupted).
		p.quarantine(j.name, errcode.SYNC101, "could not begin again after a DDL statement: "+err.Error(), j.origin.autoresync)
		return nil
	}
}
