// Package pipeline turns the source's change stream into ClickHouse writes: it routes tables,
// buffers rows with bounded memory, and commits the replication position only after the data up
// to that position is written.
//
// The delivery guarantee is at-least-once. Position and data live in different systems and cannot
// be committed together, so the order is fixed: write the data, and only then save the position.
// A crash between the two replays events from the older position, and replaying is harmless
// because every row carries a deterministic version (see clickhouse.Versioner).
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/snapshot"
	"github.com/thealexjo/binwave/internal/typemap"
)

// Timing constants. TimeoutStopSec of the systemd unit is 120s, so a graceful stop has to finish
// its final flush within ShutdownTimeout.
const (
	ShutdownTimeout       = 90 * time.Second
	errorExitFlushTimeout = 10 * time.Second
	keepaliveInterval     = time.Second
	sleepChunk            = 5 * time.Second
)

// ErrUnsupported marks a configuration the pipeline cannot honor yet. Restarting will not help,
// so callers treat it as a configuration error.
var ErrUnsupported = errors.New("unsupported configuration")

// Hooks report the pipeline's state to the process supervisor.
type Hooks struct {
	// Ready is called once the binlog stream has started.
	Ready func()
	// Progress is called while the pipeline is alive and doing its job.
	Progress func()
}

// Source is the change stream of a MySQL server.
type Source interface {
	// ExecutedGTIDSet returns the position "now".
	ExecutedGTIDSet(ctx context.Context) (string, error)
	// Stream delivers events after the GTID set from until ctx ends or something fails. Blocking
	// in handle pauses the reading: that is the backpressure.
	Stream(ctx context.Context, from string, started func(), handle func(context.Context, event.Event) error) error
}

// Sink writes into ClickHouse.
type Sink interface {
	Ensure(ctx context.Context, t *chsink.Table, ddl chsink.DDL) error
	// Change applies a decided schema change (columns added, dropped, renamed, widened) to a table.
	Change(ctx context.Context, req chsink.ChangeRequest) error
	// RenameTable renames a table, never overwriting one.
	RenameTable(ctx context.Context, req chsink.RenameRequest) error
	// DropTable drops a table that the source dropped, if it exists.
	DropTable(ctx context.Context, req chsink.DropRequest) error
	// Shape reads the sort key and the columns of a table.
	Shape(ctx context.Context, database, table string) (chsink.TableShape, error)
	// Rebuild replaces a table with a copy under a new sort key.
	Rebuild(ctx context.Context, req chsink.RebuildRequest) error
	// CheckAccess returns an *AccessError for the first privilege the user lacks on the database.
	CheckAccess(ctx context.Context, database string, privileges ...string) error
	// SwapShadow puts the shadow table of a resync in the place of the table, and drops the old one.
	SwapShadow(ctx context.Context, req chsink.SwapRequest) error
	// TruncateTable empties a table that the source truncated, if it exists.
	TruncateTable(ctx context.Context, req chsink.TruncateRequest) error
	Insert(ctx context.Context, t *chsink.Table, rows []chsink.Row) error
}

// Store persists the replication position.
type Store interface {
	Load(ctx context.Context) (pos checkpoint.Position, found bool, err error)
	Save(ctx context.Context, pos checkpoint.Position) error
}

// Guard vets a source table before its first row is written. It is where checks that need the
// source catalog run for tables that appear after start; the pipeline itself only knows the
// table definition from the binlog.
//
// problem is a problem of this table alone, which can be handled by setting the table aside; err
// is anything else: a mistake of the configuration, or a failure to look at the table at all.
type Guard interface {
	CheckTable(ctx context.Context, database, table string) (problem error, err error)
	// Forget makes the next CheckTable of the table look at it again.
	Forget(database, table string)
}

// RetryPolicy controls how transient ClickHouse failures are retried.
type RetryPolicy struct {
	Initial        time.Duration
	Max            time.Duration
	AttemptTimeout time.Duration
}

// DefaultRetry backs off from 500ms to 30s and gives one attempt two minutes.
var DefaultRetry = RetryPolicy{Initial: 500 * time.Millisecond, Max: 30 * time.Second, AttemptTimeout: 2 * time.Minute}

// Deps are the collaborators of a Pipeline. Now, Sleep and Retry may be left zero.
type Deps struct {
	Source   Source
	Sink     Sink
	Store    Store
	Logger   *slog.Logger
	Reporter typemap.Reporter
	// Guard is optional.
	Guard Guard
	// Observer is optional.
	Observer Observer
	// Snapshot is optional: without it a table cannot be resynchronized.
	Snapshot Snapshotter
	// SourceTransient tells a failure of a read of Snapshot that passes with time (the source is
	// restarting, the network is down) from one that does not; nil takes every failure for the latter.
	SourceTransient func(error) bool

	// Now and Sleep are injectable so tests are deterministic.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	Retry RetryPolicy
}

// Pipeline is the replication engine between a Source, a Sink and a Store.
type Pipeline struct {
	cfg    *config.Config
	d      Deps
	log    *slog.Logger
	router *Router
	retry  RetryPolicy

	hooks    Hooks
	versions *chsink.Versioner

	registry map[string]*binding
	recheck  map[string]bool // tables a statement named since they were vetted: vetted again before their next row
	tablesMu sync.Mutex
	tables   sourceTables // guarded by tablesMu
	// targets maps each ClickHouse table in use to the source table that feeds it, to refuse two
	// sources writing into one target.
	targets map[string]string
	// managed is the list of ClickHouse tables binwave manages, by source table (see managed.go), and
	// managedStale says it changed since it was last put into the position.
	managed      map[string]checkpoint.Target
	managedStale bool

	buffers  map[*chsink.Table]*tableBuffer
	order    []*chsink.Table // buffers in the order they were first filled
	memBytes int
	memRows  int

	q        quarantineSet
	qChanged atomic.Bool // the quarantine set changed on another goroutine

	running atomic.Bool // Run is in progress, so a request for a resync can be served
	// failing is set while requests to ClickHouse are failing and being retried, and cleared by the
	// first one that succeeds (inserts run in parallel, hence atomic).
	failing atomic.Bool
	rs      resyncSet
	resyncs map[string]*resyncJob // tables being resynchronized or initially loaded, by source name
	// reading and waiting index resyncs for the work every transaction boundary does (serviceResyncs,
	// fillLoadSlots), which must cost what the resyncs in flight cost and not what the source has
	// tables: with replication.initial_position: snapshot every table of the source is an entry in
	// resyncs from the first start until it is loaded, and thousands of them made a commit O(tables).
	//
	//   - reading: launched, a goroutine hands chunks over. Bounded by replication.snapshot.parallelism
	//     plus the resyncs an operator asked for, so walking it on every transaction boundary is cheap.
	//   - awaiting: read in full, waiting for the stream to reach the watermark of the last chunk
	//     (Final) so the table can be replaced. There can be thousands of these — every table of the
	//     source at the end of an initial load — so they are a queue in the order they became ready,
	//     with Final already parsed, and only the head is looked at (swapReady).
	//   - waiting: registered initial loads that have no slot yet (replication.snapshot.parallelism).
	//     Nothing to do for them until fillLoadSlots launches one, so they are only names, in name
	//     order (waitingSorted says whether that still holds), and a name whose job is gone is skipped.
	reading       map[string]*resyncJob
	awaiting      []*resyncJob
	waiting       []string
	waitingSorted bool
	restarting    []restartJob        // resyncs that a DDL statement discarded, to begin again after it
	loadBudget    *snapshot.Budget    // shared by every initial load this run; nil is unlimited
	chunkSize     *snapshot.ChunkSize // shared by every snapshot read this run; empty follows the configuration
	// autoresyncAttempts counts, per table, how many times tables.autoresync has tried it this run;
	// not persisted, see maxAutoresyncAttempts.
	autoresyncAttempts map[string]int

	skip        Skip // transactions to drop, set by the operator between runs
	tx          *txState
	committed   checkpoint.Position // position after the last Commit seen
	committedAt time.Time           // when the source committed the transaction that ended at committed
	// atHead says whether replication is at the head of the binlog, which decides how long rows may
	// wait in memory (flushEvery). It is true while the source has nothing more to send (a heartbeat)
	// or the last transaction was committed less than buffer.catchup_flush_interval ago, and false
	// while there is a backlog to work through: an initial load, an outage, a restart after one. A run
	// starts behind until a transaction or a heartbeat says otherwise.
	atHead     bool
	saved      checkpoint.Position // last position written to the store
	dirtySince time.Time           // when unsaved work first appeared
	// resyncsStale marks committed.Resync as older than the jobs; the text is built at save time
	// (see syncResyncs), not on every chunk, because it holds every entry there is.
	resyncsStale bool
	// resyncsPublished is when the statuses of the resyncs last went out, and resyncsPublishStale
	// whether they changed since: the throttle that keeps rebuilding the list of every entry there is
	// off the hot path (see resyncPublishEvery).
	resyncsPublished    time.Time
	resyncsPublishStale bool
	// resyncSetChanged marks that a resync appeared or ended since the statuses last went out, which
	// is published without waiting for the throttle (publishStaleResyncs).
	resyncSetChanged bool
}

type txState struct {
	gtid    string
	version uint64
	commit  time.Time // when the source committed it
	skipped bool      // dropped on the operator's request (see Skip): only its end is processed
}

// binding is the resolved ClickHouse table of a source table structure.
type binding struct {
	src   *schema.Table
	sig   string
	table *chsink.Table
}

type tableBuffer struct {
	table *chsink.Table
	rows  []chsink.Row
	bytes int
}

// New creates a Pipeline. It rejects configurations it cannot honor yet with ErrUnsupported.
func New(cfg *config.Config, d Deps) (*Pipeline, error) {
	if cfg.Replication.InitialPosition == config.StartFromSnapshot && d.Snapshot == nil {
		return nil, fmt.Errorf("%w: replication.initial_position snapshot needs a source that can read one", ErrUnsupported)
	}

	p := &Pipeline{
		cfg:      cfg,
		d:        d,
		log:      d.Logger,
		router:   NewRouter(cfg.Tables),
		retry:    d.Retry,
		registry: make(map[string]*binding),
		recheck:  make(map[string]bool),
		targets:  make(map[string]string),
		managed:  make(map[string]checkpoint.Target),
		buffers:  make(map[*chsink.Table]*tableBuffer),
		resyncs:  make(map[string]*resyncJob),
		reading:  make(map[string]*resyncJob),
	}
	if p.log == nil {
		p.log = slog.New(slog.DiscardHandler)
	}
	if p.d.Observer == nil {
		p.d.Observer = nopObserver{}
	}
	if p.d.Now == nil {
		p.d.Now = time.Now
	}
	if p.d.Sleep == nil {
		p.d.Sleep = p.sleepWithProgress
	}
	if p.retry == (RetryPolicy{}) {
		p.retry = DefaultRetry
	}
	return p, nil
}

// Run replicates until ctx is cancelled, which is a graceful stop that flushes and saves the
// position and returns nil, or until something fails. On failure it still makes a best effort
// to write what is buffered and save the last committed position.
func (p *Pipeline) Run(ctx context.Context, hooks Hooks) error {
	p.hooks = hooks

	pos, err := p.startPosition(ctx)
	if err != nil {
		return err
	}
	p.versions = chsink.NewVersioner(pos.Version)
	p.committed, p.saved = pos, pos
	p.q.setSince(pos.GTIDSet)
	if err := p.loadQuarantine(pos); err != nil {
		return err
	}
	if err := p.loadManaged(pos); err != nil {
		return err
	}
	p.resyncs = make(map[string]*resyncJob)
	p.reading = make(map[string]*resyncJob)
	p.awaiting = nil
	p.waiting, p.waitingSorted = nil, true
	p.resyncsStale, p.resyncsPublishStale, p.resyncSetChanged = false, false, false
	p.atHead = false // until a transaction or a heartbeat says otherwise: a restart usually has a backlog
	p.resyncsPublished = time.Time{}
	p.restarting = nil
	p.autoresyncAttempts = make(map[string]int)
	p.loadBudget = snapshot.NewBudget(p.cfg.Replication.Snapshot.MaxRowsPerSecond, p.d.Now, p.d.Sleep)
	p.chunkSize = snapshot.NewChunkSize(0)
	if err := p.resumeResyncs(ctx, pos); err != nil {
		p.stopResyncs()
		return err
	}
	if err := p.fillLoadSlots(ctx); err != nil {
		p.stopResyncs()
		return err
	}
	// What is being loaded or resynchronized is visible from the moment replication starts, not from
	// the first transaction the source happens to send: a service that came up with an initial load of
	// thousands of tables must not answer "nothing is going on" until something arrives.
	p.publishResyncsNow()
	p.running.Store(true)
	defer func() {
		p.running.Store(false)
		p.stopResyncs()
		p.refuseRequests()
	}()
	p.log.Info("replication starting", "gtid_set", pos.GTIDSet)

	streamErr := p.d.Source.Stream(ctx, pos.GTIDSet, p.ready, p.handle)
	return p.finish(ctx, streamErr)
}

// startPosition returns where to resume: the saved position, or the configured start on the
// first run. The configured start is saved before anything is read, so a restart before the
// first flush resumes from the same point instead of picking a new "now" and skipping data.
func (p *Pipeline) startPosition(ctx context.Context) (checkpoint.Position, error) {
	pos, err := p.loadOrDecidePosition(ctx)
	if err == nil {
		p.d.Observer.PositionLoaded(pos)
	}
	return pos, err
}

// loadOrDecidePosition is startPosition without telling the observer.
func (p *Pipeline) loadOrDecidePosition(ctx context.Context) (checkpoint.Position, error) {
	var (
		pos   checkpoint.Position
		found bool
	)
	err := p.withRetry(ctx, "load position", func(actx context.Context) error {
		var err error
		pos, found, err = p.d.Store.Load(actx)
		return err
	})
	if err != nil {
		return checkpoint.Position{}, fmt.Errorf("load position: %w", err)
	}
	if found {
		return pos, nil
	}

	switch p.cfg.Replication.InitialPosition {
	case config.StartFromNow:
		set, err := p.d.Source.ExecutedGTIDSet(ctx)
		if err != nil {
			return checkpoint.Position{}, fmt.Errorf("read the current position of the source: %w", err)
		}
		pos = checkpoint.Position{GTIDSet: set}
	case config.StartFromGTID:
		pos = checkpoint.Position{GTIDSet: p.cfg.Replication.GTID}
	case config.StartFromSnapshot:
		set, err := p.d.Source.ExecutedGTIDSet(ctx)
		if err != nil {
			return checkpoint.Position{}, fmt.Errorf("read the current position of the source: %w", err)
		}
		pos = checkpoint.Position{GTIDSet: set}
		p.q.setSince(set) // so a table tablesToLoad quarantines records the right Since
		// The tables to load are decided once, right here, and saved together with the starting
		// position: from then on a restart resumes them the same way it resumes any other resync
		// (resumeResyncs), with no separate bookkeeping of "has the initial load begun".
		entries, err := p.tablesToLoad(ctx, set)
		if err != nil {
			return checkpoint.Position{}, fmt.Errorf("list the tables of the initial load: %w", err)
		}
		pos.Resync = checkpoint.EncodeResyncs(entries)
		pos.LoadTotal = len(entries)  // the denominator of "N of M tables loaded", which the list alone loses
		pos.Quarantine = p.q.encode() // a table tablesToLoad could not include is quarantined already
	default:
		return checkpoint.Position{}, fmt.Errorf("%w: initial position %q", ErrUnsupported, p.cfg.Replication.InitialPosition)
	}
	pos.Tables = checkpoint.EncodeTables(nil) // nothing written yet, which is not a list to make

	if err := p.withRetry(ctx, "save initial position", func(actx context.Context) error {
		return p.d.Store.Save(actx, pos)
	}); err != nil {
		return checkpoint.Position{}, fmt.Errorf("save initial position: %w", err)
	}
	p.log.Info("no saved position, using initial_position", "start", p.cfg.Replication.InitialPosition, "gtid_set", pos.GTIDSet)
	return pos, nil
}

// finish flushes what is buffered and saves the last committed position, whatever ended the
// stream. It runs on a context that survives the cancellation that requested the shutdown.
func (p *Pipeline) finish(ctx context.Context, streamErr error) error {
	shutdown := ctx.Err() != nil
	timeout := errorExitFlushTimeout
	if shutdown {
		timeout = ShutdownTimeout
	}
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	var failed *FailedError
	if errors.As(streamErr, &failed) && failed.Failure.GTID != "" && p.tx != nil {
		p.dropTransactionRows(p.tx.version)
	}
	var flushErr error
	if errors.Is(streamErr, ErrOverloaded) {
		// The budget was just spent waiting on this very server; another round of retries would
		// only repeat it. What is buffered is replayed from the saved position after the restart.
		p.log.Warn("final flush skipped: ClickHouse overloaded")
	} else {
		flushErr = p.flush(fctx)
	}

	// The statuses of the resyncs are published on a throttle while running; a pipeline that stops
	// (a pause, a shutdown) must leave the last word of them, not one up to a second old.
	p.publishResyncsNow()

	if shutdown && errors.Is(streamErr, context.Canceled) {
		streamErr = nil
	}
	if streamErr != nil {
		if flushErr != nil {
			p.log.Error("final flush failed", "error", flushErr)
		}
		return streamErr
	}
	if flushErr != nil {
		return fmt.Errorf("final flush: %w", flushErr)
	}
	p.log.Info("stopped cleanly", "gtid_set", p.saved.GTIDSet)
	return nil
}

func (p *Pipeline) ready() {
	p.log.Info("ready")
	if p.hooks.Ready != nil {
		p.hooks.Ready()
	}
}

func (p *Pipeline) progress() {
	if p.hooks.Progress != nil {
		p.hooks.Progress()
	}
}

// handle processes one event of the stream. It runs on the source's goroutine, one event at a time.
// A failure of the data path leaves as a *FailedError that names the transaction and what failed.
func (p *Pipeline) handle(ctx context.Context, ev event.Event) error {
	p.progress()
	return p.annotate(ev, p.dispatch(ctx, ev))
}

func (p *Pipeline) dispatch(ctx context.Context, ev event.Event) error {
	switch e := ev.(type) {
	case event.Begin:
		return p.begin(e)
	case event.Rows:
		return p.rows(ctx, e)
	case event.DDL:
		return p.ddl(ctx, e)
	case event.Commit:
		return p.commit(ctx, e)
	case event.Heartbeat:
		// The source has nothing more to send, so whatever is buffered is the head of the binlog,
		// however old the last transaction is on a source that writes rarely.
		p.atHead = true
		p.syncQuarantine()
		if err := p.serviceResyncs(ctx); err != nil {
			return err
		}
		if err := p.flushIfNeeded(ctx); err != nil {
			return err
		}
		if p.tx == nil && len(p.order) == 0 && p.committed == p.saved {
			p.d.Observer.Idle()
		}
		return nil
	default:
		return fmt.Errorf("unknown event %T", ev)
	}
}

func (p *Pipeline) begin(e event.Begin) error {
	if p.tx != nil {
		if p.tx.gtid != e.GTID {
			return fmt.Errorf("transaction %s began inside transaction %s", e.GTID, p.tx.gtid)
		}
		// The source lost its connection in the middle of this transaction and sends it again
		// from the start. Rows of the first attempt may already be written, but they carry a lower
		// version than the replay will, so the replay supersedes them.
		p.log.Info("transaction redelivered, restarting it", "gtid", e.GTID)
		p.dropTransactionRows(p.tx.version) // the rows still buffered: the replay produces them again
	}
	// Every transaction takes a version, including those without replicated rows, so the
	// version chain does not depend on which tables are configured.
	v, err := p.versions.Next(e.CommitTime)
	if err != nil {
		return fmt.Errorf("transaction %s: %w", e.GTID, err)
	}
	// A transaction that is sent again (after a reconnect, or after a failure and a resume) keeps
	// its decision to be dropped, and can still be asked to be dropped by a skip set since.
	skipped := p.tx != nil && p.tx.gtid == e.GTID && p.tx.skipped
	if !skipped {
		if skipped, err = p.skipping(e.GTID); err != nil {
			return err
		}
	}
	p.tx = &txState{gtid: e.GTID, version: v, commit: e.CommitTime, skipped: skipped}
	if skipped {
		p.log.Warn("transaction skipped by operator", "gtid", e.GTID)
		p.d.Observer.TransactionSkipped()
	}
	p.d.Observer.TransactionBegan(e.CommitTime)
	return nil
}

func (p *Pipeline) rows(ctx context.Context, e event.Rows) error {
	if p.tx == nil {
		return errors.New("row change outside a transaction")
	}
	if p.tx.skipped {
		return nil
	}
	name := e.Table.FullName()
	skip, changed, err := p.q.skip(name, p.tx.gtid)
	if err != nil {
		return err
	}
	if changed {
		p.qChanged.Store(true) // the fence of a released table was passed
	}
	if skip {
		// Quarantined, or released and not yet past its fence: the change is read and dropped,
		// so the position moves on.
		p.d.Observer.RowsSkipped(name, len(e.Rows))
		return nil
	}
	b, err := p.bind(ctx, e.Table)
	if err != nil {
		if chsink.IsTableProblem(err) {
			return p.tableFailed(name, err)
		}
		return err
	}
	rows, err := b.table.Rows(e, p.tx.version)
	if err != nil {
		err = fmt.Errorf("transaction %s: %w", p.tx.gtid, err)
		var conversion *typemap.ConversionError
		if errors.As(err, &conversion) {
			return p.tableFailed(name, err)
		}
		return err
	}
	p.d.Observer.RowsRead(e.Table.FullName(), len(rows))
	p.add(b.table, rows)
	return p.flushIfNeeded(ctx)
}

func (p *Pipeline) commit(ctx context.Context, e event.Commit) error {
	if p.tx == nil {
		return errors.New("commit outside a transaction")
	}
	// The position becomes saveable only at a transaction boundary, and carries this
	// transaction's version so that a restart continues the version chain.
	// The quarantine and the resyncs are carried over as text: both are kept current by their own
	// change marks (syncQuarantine, materializeResyncs, both before the save), and re-encoding lists
	// that hold an entry per table on every single commit is what a commit must not cost.
	p.committed = checkpoint.Position{
		GTIDSet: e.GTIDSet, Version: p.tx.version, Quarantine: p.committed.Quarantine,
		Resync: p.committed.Resync, LoadTotal: p.committed.LoadTotal, Tables: p.committed.Tables,
	}
	p.q.setSince(e.GTIDSet)
	p.committedAt = p.tx.commit
	// Where this transaction stands in time says whether there is a backlog: one committed just now
	// means the stream is at the head, one committed hours ago means it is working through one.
	p.atHead = p.d.Now().Sub(p.tx.commit) < time.Duration(p.cfg.Buffer.CatchupFlushInterval)
	p.tx = nil
	p.d.Observer.TransactionCommitted()
	p.markDirty()
	if err := p.serviceResyncs(ctx); err != nil {
		return err
	}
	return p.flushIfNeeded(ctx)
}

// ddl handles a schema statement of the source. It is a barrier: everything converted under the
// old structure is written before later rows are read. Structure changes themselves are noticed
// from the rows (see bind), so a change made without a statement is caught as well.
func (p *Pipeline) ddl(ctx context.Context, e event.DDL) error {
	if p.tx != nil && p.tx.skipped {
		return nil
	}
	if err := p.flush(ctx); err != nil {
		return err
	}
	if impacts := p.resyncImpacts(e); len(impacts) > 0 {
		p.interruptResyncs(ctx, e, impacts)
	}
	p.forgetChecks(e)
	p.countTables(e)

	d, ok := classify(e.Query, e.Database)
	if !ok {
		p.log.Info("DDL statement in the source stream", "database", e.Database, "statement", e.Query)
		return p.applySchemaChange(ctx, e)
	}
	return p.destructiveDDL(ctx, e, d)
}

func cutQualified(name string) (db, table string, ok bool) {
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			return name[:i], name[i+1:], true
		}
	}
	return "", "", false
}

// bind returns the ClickHouse table for a source table structure. The source hands out the same
// *schema.Table while the structure is unchanged, so an unchanged pointer is the cheap path; a new
// pointer is compared by signature, and only a real change re-resolves and re-checks the table.
func (p *Pipeline) bind(ctx context.Context, src *schema.Table) (*binding, error) {
	name := src.FullName()
	if p.recheck[name] {
		delete(p.recheck, name)
		p.forgetTable(name) // bound again, so the guard looks at it again
	}

	if cur, ok := p.registry[name]; ok {
		if cur.src == src {
			return cur, nil
		}
		if cur.sig == src.Signature() {
			cur.src = src
			return cur, nil
		}
		p.log.Info("table structure changed", "table", name)
		// Rows converted under the old structure must be written before the new one is applied.
		if err := p.flush(ctx); err != nil {
			return nil, err
		}
	}

	t, err := chsink.Resolve(src, p.cfg, p.d.Reporter)
	if err != nil {
		return nil, err
	}
	job, resyncing := p.resyncs[name]
	if resyncing {
		// The table is being resynchronized: its changes go to the shadow table, which replaces it.
		t.Name, t.Comment = job.state.Shadow, chsink.ResyncMarker
	}
	// The target is one database, so tables of the same name in different source databases would
	// share a ClickHouse table and overwrite each other's rows without any error.
	if owner, taken := p.ownerOf(t.FullName()); taken && owner != name {
		return nil, fmt.Errorf("source tables %s and %s would both be written to ClickHouse table %s; "+
			"give one of them another name with tables.overrides.<database.table>.target_table", owner, name, t.FullName())
	}
	if _, known := p.registry[name]; !known && p.d.Guard != nil {
		problem, err := p.d.Guard.CheckTable(ctx, src.Database, src.Name)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", name, err)
		}
		if problem != nil {
			return nil, &chsink.TableError{Table: name, Err: fmt.Errorf("table %s: %w", name, problem)}
		}
	}
	ddl := chsink.DDL{Mode: p.cfg.DDL.Mode, DryRun: p.cfg.DDL.DryRun}
	if err := p.withRetry(ctx, "prepare table "+t.FullName(), func(actx context.Context) error {
		return p.d.Sink.Ensure(actx, t, ddl)
	}); err != nil {
		return nil, err
	}

	b := &binding{src: src, sig: src.Signature(), table: t}
	p.registry[name] = b
	p.tableBound(name)
	p.targets[t.FullName()] = name
	if !resyncing {
		p.manage(name, checkpoint.Target{Database: t.Database, Table: t.Name})
	}
	p.log.Info("replicating table", "source", name, "target", t.FullName(), "order_by", t.OrderBy)
	return b, nil
}

// ownerOf returns the source table that writes to a ClickHouse table: a bound one, or one being
// resynchronized, which holds its target and its shadow until the resync is swapped in or given up.
func (p *Pipeline) ownerOf(target string) (string, bool) {
	if owner, ok := p.targets[target]; ok {
		return owner, true
	}
	for name, job := range p.resyncs {
		db, table, _ := cutQualified(name)
		tdb, ttable := chsink.TargetName(p.cfg, db, table)
		for _, held := range []string{ttable, job.state.Shadow} {
			if target == (&chsink.Table{Database: tdb, Name: held}).FullName() {
				return name, true
			}
		}
	}
	return "", false
}

// add appends converted rows to the table's buffer.
func (p *Pipeline) add(t *chsink.Table, rows []chsink.Row) {
	if len(rows) == 0 {
		return
	}
	b, ok := p.buffers[t]
	if !ok {
		b = &tableBuffer{table: t}
		p.buffers[t] = b
		p.order = append(p.order, t)
	}
	for _, r := range rows {
		n := chsink.RowBytes(r)
		b.rows = append(b.rows, r)
		b.bytes += n
		p.memBytes += n
	}
	p.memRows += len(rows)
	p.d.Observer.BufferChanged(p.memRows, p.memBytes)
	p.markDirty()
}

func (p *Pipeline) markDirty() {
	if p.dirtySince.IsZero() {
		p.dirtySince = p.d.Now()
	}
}

// flushIfNeeded flushes when a buffer is full, memory is at its limit, or unsaved work has waited
// for the flush interval. Flushing here, on the reading goroutine, is the backpressure: nothing
// more is read until ClickHouse has accepted the data.
func (p *Pipeline) flushIfNeeded(ctx context.Context) error {
	if !p.shouldFlush() {
		return nil
	}
	return p.flushBuffers(ctx)
}

// insertResult is what one table's insert came back with, for the pipeline goroutine to apply.
type insertResult struct {
	table *chsink.Table
	rows  int // rows the insert carried, after Collapse
	held  int // rows the buffer held, for the memory accounting
	bytes int // bytes the buffer held
	took  time.Duration
	err   error
}

// writeBuffered writes the next buffers, up to buffer.insert_parallelism of them at the same time, and
// applies what came back. It returns when every insert it started has finished, so a caller that goes
// on to save the position saves it behind data that is already in ClickHouse.
//
// Why at the same time: a flush of a source whose changes are spread over many tables is a row or two
// per table, and such an insert costs a round-trip and the durability of the server, not the rows
// (tens of milliseconds for a handful of rows). One after another, a flush is as long as the number
// of tables it touched, and the pipeline spends nearly all of its time waiting. The tables are
// independent: the order that matters is the order within one table, which one insert keeps, and rows
// of different tables never decide anything about each other.
//
// Nothing about the pipeline is touched by the goroutines: they convert (Collapse) and insert, and hand
// back a result. The buffers, the memory accounting, the quarantine and the observer are the pipeline
// goroutine's own, applied here in the order of the buffers, so a failing table is set aside exactly as
// it was when the inserts were serial.
func (p *Pipeline) writeBuffered(ctx context.Context) error {
	n := min(len(p.order), max(p.cfg.Buffer.InsertParallelism, 1))
	batch := make([]*chsink.Table, n)
	copy(batch, p.order[:n])
	results := make([]insertResult, n)

	var wg sync.WaitGroup
	for i, t := range batch {
		b := p.buffers[t]
		res := &results[i]
		res.table, res.held, res.bytes = t, len(b.rows), b.bytes
		rows := b.rows
		wg.Add(1)
		go func() {
			defer wg.Done()
			collapsed := chsink.Collapse(rows)
			began := p.d.Now()
			res.rows = len(collapsed)
			res.err = p.withRetry(ctx, "insert into "+t.FullName(), func(actx context.Context) error {
				return p.d.Sink.Insert(actx, t, collapsed)
			})
			res.took = p.d.Now().Sub(began)
		}()
	}
	wg.Wait()

	// The buffers that were written are gone from the order, whatever happened to the others: a table
	// that failed is either set aside (and dropBuffers takes it out) or the flush ends with the error.
	var failed error
	for _, res := range results {
		if res.err != nil {
			if failed == nil {
				failed = p.applyInsertFailure(ctx, res)
			}
			continue
		}
		p.d.Observer.Inserted(res.table.Source.FullName(), res.rows, res.took)
		p.memBytes -= res.bytes
		p.memRows -= res.held
		delete(p.buffers, res.table)
		p.order = removeTable(p.order, res.table)
	}
	p.d.Observer.BufferChanged(p.memRows, p.memBytes)
	return failed
}

// applyInsertFailure decides what a failed insert means: a table that is not what it was resolved for
// is set aside and the rest of the flush goes on (tables.on_replication_error: quarantine), anything
// else ends the flush.
func (p *Pipeline) applyInsertFailure(ctx context.Context, res insertResult) error {
	wrapped := fmt.Errorf("write %d rows to %s: %w", res.rows, res.table.FullName(), res.err)
	if chsink.IsTableProblem(res.err) && ctx.Err() == nil {
		// The table is not what it was resolved for (a column or the table is gone).
		// With quarantine the table is set aside and the rest is written.
		if handled := p.tableFailed(res.table.Source.FullName(), wrapped); handled == nil {
			return nil
		}
		return &WriteError{Table: res.table.Source.FullName(), Rows: res.rows, Err: res.err}
	}
	return wrapped
}

// removeTable drops one table from the order of the buffers, keeping the order of the rest.
func removeTable(order []*chsink.Table, t *chsink.Table) []*chsink.Table {
	for i, other := range order {
		if other == t {
			return append(order[:i], order[i+1:]...)
		}
	}
	return order
}

// positionDue reports whether the position has waited for the flush interval. Writing the data more
// often than the position is safe — at-least-once asks only that the position is never ahead of the
// data — and far cheaper: the position carries the whole state of the resyncs, one entry per table of
// the source while the initial load runs, so writing it on every chunk costs more than the rows do.
func (p *Pipeline) positionDue() bool {
	return !p.dirtySince.IsZero() && p.d.Now().Sub(p.dirtySince) >= p.flushEvery()
}

// flushEvery is how long rows and the position may wait: buffer.flush_interval at the head of the
// binlog, buffer.catchup_flush_interval while there is a backlog.
//
// One interval cannot serve both. At the head it is the delay an operator sees as the lag, so it has
// to be short — a quarter of a second, not seconds. While catching up it is the size of the batches,
// so it has to be long — an insert of two rows costs the same round-trip and the same fsync as an
// insert of a thousand, and with the changes of a busy source spread over hundreds of tables, a short
// interval turns a flush into hundreds of tiny inserts (one or two rows per insert, and the pipeline
// spends most of its time waiting for them).
func (p *Pipeline) flushEvery() time.Duration {
	if p.atHead {
		return time.Duration(p.cfg.Buffer.FlushInterval)
	}
	return time.Duration(p.cfg.Buffer.CatchupFlushInterval)
}

func (p *Pipeline) shouldFlush() bool {
	b := p.cfg.Buffer
	if p.memBytes >= int(b.MaxMemory) {
		return true
	}
	for _, buf := range p.buffers {
		if len(buf.rows) >= b.MaxRows || buf.bytes >= int(b.MaxBytes) {
			return true
		}
	}
	return !p.dirtySince.IsZero() && p.d.Now().Sub(p.dirtySince) >= p.flushEvery()
}

// flush writes every buffer and then saves the last committed position, whether it is due or not.
// It is for the moments that must leave the state durable: a stop, a barrier of a schema statement,
// a resync that has just begun (its shadow table would be left behind by a restart that forgot it).
//
// All buffers are written before the position is saved, so the saved position never gets ahead of
// the data. The saved position is the last transaction that has ended: rows of a transaction still
// in progress may be written (a huge transaction cannot be held in memory), but the position stays
// behind it, so a crash replays that transaction whole with the same versions.
func (p *Pipeline) flush(ctx context.Context) error { return p.flushThen(ctx, true) }

// flushBuffers writes every buffer and saves the position only when it has waited for the flush
// interval (positionDue). This is the path every chunk and every finished table takes.
func (p *Pipeline) flushBuffers(ctx context.Context) error { return p.flushThen(ctx, false) }

func (p *Pipeline) flushThen(ctx context.Context, savePosition bool) error {
	p.syncQuarantine()
	for len(p.order) > 0 {
		if err := p.writeBuffered(ctx); err != nil {
			return err
		}
	}

	if !savePosition && !p.positionDue() {
		// The rows are in ClickHouse; the position they belong to waits its turn. dirtySince keeps
		// saying the position is behind, so the next flush past the interval writes it.
		return nil
	}
	p.materializeResyncs()
	p.materializeManaged()
	if p.committed != p.saved {
		if err := p.withRetry(ctx, "save position", func(actx context.Context) error {
			return p.d.Store.Save(actx, p.committed)
		}); err != nil {
			return fmt.Errorf("save position: %w", err)
		}
		p.saved = p.committed
		var open time.Time
		if p.tx != nil {
			open = p.tx.commit
		}
		p.d.Observer.PositionSaved(p.saved, p.committedAt, open)
	}
	p.dirtySince = time.Time{}
	return nil
}
