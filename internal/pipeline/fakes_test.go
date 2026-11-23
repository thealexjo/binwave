package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/snapshot"
	"github.com/thealexjo/binwave/internal/typemap"
)

var (
	epoch = time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	// errScriptEnded ends a fake stream once its script is played.
	errScriptEnded = errors.New("script ended")
	// errCrash is a permanent failure: the pipeline stops, like a process that died.
	errCrash = errors.New("injected crash")
)

const minimalYAML = `
source: {mysql: {host: h, user: u, server_id: 1}}
target: {clickhouse: {addresses: ["h:9000"], database: replica}}
tables: {include: ["shop.*"]}
`

func testConfig(t testing.TB) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(minimalYAML), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// itemsTable is shop.items(id INT PRIMARY KEY, name VARCHAR(20) NULL).
func itemsTable() *schema.Table {
	return &schema.Table{
		Database: "shop", Name: "items",
		Columns: []typemap.Column{
			{Name: "id", Kind: typemap.KindInt},
			{Name: "name", Kind: typemap.KindVarChar, Length: 20, Charset: "utf8mb4", Nullable: true},
		},
		PrimaryKey: []int{0},
	}
}

func insertRows(tbl *schema.Table, id int32, name any) event.Rows {
	return event.Rows{Table: tbl, Op: event.Insert, Rows: []event.Row{{After: []any{id, name}}}}
}

func updateRows(tbl *schema.Table, id int32, before, after any) event.Rows {
	return event.Rows{Table: tbl, Op: event.Update, Rows: []event.Row{{Before: []any{id, before}, After: []any{id, after}}}}
}

func updateKey(tbl *schema.Table, oldID, newID int32, name any) event.Rows {
	return event.Rows{Table: tbl, Op: event.Update, Rows: []event.Row{{Before: []any{oldID, name}, After: []any{newID, name}}}}
}

func deleteRows(tbl *schema.Table, id int32, name any) event.Rows {
	return event.Rows{Table: tbl, Op: event.Delete, Rows: []event.Row{{Before: []any{id, name}}}}
}

// scriptedTx is one transaction of a fake stream, numbered like its GTID.
type scriptedTx struct {
	n      int
	events []event.Event
}

// txAt builds transaction n committed at the given time.
func txAt(n int, at time.Time, items ...event.Event) scriptedTx {
	evs := []event.Event{event.Begin{GTID: fmt.Sprintf("u:%d", n), CommitTime: at}}
	evs = append(evs, items...)
	evs = append(evs, event.Commit{GTIDSet: fmt.Sprintf("u:1-%d", n)})
	return scriptedTx{n: n, events: evs}
}

// tx builds transaction n committed n seconds after the epoch.
func tx(n int, items ...event.Event) scriptedTx {
	return txAt(n, epoch.Add(time.Duration(n)*time.Second), items...)
}

var gtidUpTo = regexp.MustCompile(`:(?:\d+-)?(\d+)$`)

// fakeSource plays a script, honoring the resume position like a real server: transactions
// already contained in the GTID set "u:1-K" are not sent again.
type fakeSource struct {
	executed string
	script   []scriptedTx

	// beforeHandle runs before each event is delivered; it may advance a clock or cancel the run.
	beforeHandle func(ev event.Event)
	// until, when set, makes the stream send heartbeats after the script until it returns true: the
	// pipeline does its work between transactions on them, and a test waits for that work this way.
	until func() bool
	// holdOpen keeps the stream open after the script until the context ends.
	holdOpen bool
	// onEnd is called when the script is played, like a stop signal arriving: the stream then
	// ends with the context's error and the pipeline shuts down gracefully.
	onEnd func()

	mu      sync.Mutex
	streams []string // the position each Stream call started from
}

func (s *fakeSource) ExecutedGTIDSet(context.Context) (string, error) { return s.executed, nil }

func (s *fakeSource) Stream(ctx context.Context, from string, started func(), handle func(context.Context, event.Event) error) error {
	s.mu.Lock()
	s.streams = append(s.streams, from)
	s.mu.Unlock()

	skip := 0
	if m := gtidUpTo.FindStringSubmatch(from); m != nil {
		skip, _ = strconv.Atoi(m[1])
	}
	if started != nil {
		started()
	}
	for _, t := range s.script {
		if t.n <= skip {
			continue
		}
		for _, ev := range t.events {
			if s.beforeHandle != nil {
				s.beforeHandle(ev)
			}
			if err := handle(ctx, ev); err != nil {
				return err
			}
		}
	}
	if s.until != nil {
		for i := 0; !s.until(); i++ {
			if i > 5000 {
				return errors.New("the condition the stream waits for did not come true")
			}
			if err := handle(ctx, event.Heartbeat{}); err != nil {
				return err
			}
			time.Sleep(time.Millisecond)
		}
	}
	if s.onEnd != nil {
		s.onEnd()
		<-ctx.Done()
		return ctx.Err()
	}
	if s.holdOpen {
		<-ctx.Done()
		return ctx.Err()
	}
	return errScriptEnded
}

// faults numbers every write to the sink or the store and fails the chosen one, in the way a
// crash would: before the write took effect, or after it did but before the caller heard back.
//
// Once the crash has happened the fakes stay dead until revive, as they would be to a process
// that no longer exists: even the best-effort flush on the way out cannot reach them.
type faults struct {
	mu      sync.Mutex
	ops     int
	crashAt int // 1-based; 0 never
	after   bool
	dead    bool
}

// step returns the errors to raise before and after applying the current operation.
func (f *faults) step() (before, after error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead {
		return errCrash, nil
	}
	f.ops++
	if f.crashAt == 0 || f.ops != f.crashAt {
		return nil, nil
	}
	f.dead = true
	if f.after {
		return nil, errCrash
	}
	return errCrash, nil
}

// revive brings the fakes back for the restarted process and clears the scheduled crash.
func (f *faults) revive() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dead = false
	f.crashAt = 0
}

// crashIn schedules a crash at the nth operation from now.
func (f *faults) crashIn(n int, after bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.crashAt = f.ops + n
	f.after = after
}

func (f *faults) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ops
}

// journal records the order of side effects across the fakes.
type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(format string, args ...any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, fmt.Sprintf(format, args...))
}

func (j *journal) all() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

// cell is one row of the fake ClickHouse: ReplacingMergeTree keeps, per key, the row with the
// highest version, and the later insert among equal versions.
type cell struct {
	version uint64
	deleted bool
	values  []any
}

// fakeSink models the target tables closely enough to judge convergence.
type fakeSink struct {
	journal *journal
	faults  *faults

	mu       sync.Mutex
	tables   map[string]map[string]cell // table name -> key -> row
	versions map[uint64]bool            // every version ever inserted
	inserts  int
	ensures  []string
	changes  []chsink.ChangeRequest
	renames  []chsink.RenameRequest
	drops    []chsink.DropRequest
	rebuilds []chsink.RebuildRequest
	swaps    []chsink.SwapRequest
	// onSwap is called before a swap takes effect, to check invariants at that moment.
	onSwap func(chsink.SwapRequest)
	// onEnsure is called after an Ensure that succeeded, to look at that moment.
	onEnsure func(*chsink.Table)
	// marked answers SwapMarks by "database.table": the table, or the shadow table, carries the marker
	// of its resync. A marked table means the exchange of its swap was made.
	marked map[string]bool
	// beforeEnsure, if set, runs first in every Ensure, under the lock; an error is what Ensure returns.
	beforeEnsure func(t *chsink.Table, ddl chsink.DDL) error
	// foreign names, by "database.table", tables under the name of a shadow table that binwave did not
	// make: SwapMarks says a shadow table that exists (shapes) is binwave's unless it is here.
	foreign map[string]bool
	// lost names, by "database.table", the shadow tables somebody dropped: a swap that was not made
	// before finds nothing to put in place.
	lost map[string]bool
	// swapFails makes the next swap fail with err, having sent the exchange or not.
	swapFails *swapFailure
	// insertErrsByTarget fails every insert into a ClickHouse table, by "database.table": the shadow
	// table and the table have the same source table.
	insertErrsByTarget map[string]error
	// shapes answers Shape by "database.table"; a table that is not there does not exist.
	shapes map[string]chsink.TableShape
	// accessErr fails CheckAccess.
	accessErr error
	// rebuildErr fails the rebuild.
	rebuildErr error
	truncs     []chsink.TruncateRequest
	// renameErrs fails the nth RenameTable call (1-based).
	renameErrs map[int]error
	// changeErrs fails the nth Change call (1-based).
	changeErrs map[int]error
	// insertErrs and ensureErrs fail the nth call (1-based) with the given error.
	insertErrs map[int]error
	ensureErrs map[int]error
	// insertErrsByTable fails the inserts of a table, whenever they happen: a flush writes its tables
	// at the same time (buffer.insert_parallelism), so which call is the nth is not a test's business.
	// The value is how many times it fails; a negative count fails every time.
	insertErrsByTable map[string]*tableFailure
	// checkCtx makes Insert fail if its context is already cancelled.
	ctxAtInsert []error
	// beforeInsert, when set, runs at the start of Insert, before the lock, so a test can see how many
	// inserts a flush has in flight at once.
	beforeInsert func(table string)
}

// swapFailure is how a swap fails: after the exchange was sent or before.
type swapFailure struct {
	sent bool
	err  error
}

// tableFailure is how an insert of one table fails, and how many times.
type tableFailure struct {
	err  error
	left int // times left to fail; negative means always
}

// failsForever makes every insert of the table fail.
func failsForever(err error) *tableFailure { return &tableFailure{err: err, left: -1} }

func newFakeSink(j *journal, f *faults) *fakeSink {
	return &fakeSink{journal: j, faults: f, tables: map[string]map[string]cell{}, versions: map[uint64]bool{}}
}

func (s *fakeSink) Ensure(_ context.Context, t *chsink.Table, ddl chsink.DDL) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	call := len(s.ensures) + 1
	s.ensures = append(s.ensures, t.Source.Signature())
	s.journal.add("ensure %s", t.Source.FullName())
	if err := s.ensureErrs[call]; err != nil {
		return err
	}
	if s.beforeEnsure != nil {
		if err := s.beforeEnsure(t, ddl); err != nil {
			return err
		}
	}
	if name := t.Database + "." + t.Name; strings.HasPrefix(t.Comment, "binwave: ") && s.shapes[name].Exists &&
		s.foreign[name] {
		return &chsink.ConflictError{Err: errors.New("the table is not the one binwave made")}
	}
	if ddl.Existing && !s.shapes[t.Database+"."+t.Name].Exists {
		return &chsink.TableError{Table: t.Source.FullName(), Recoverable: true, Err: errors.New("the table is gone")}
	}
	if strings.HasPrefix(t.Comment, "binwave: resync shadow") {
		// A shadow table made: there for Shape and SwapMarks, as binwave's.
		if s.shapes == nil {
			s.shapes = map[string]chsink.TableShape{}
		}
		if name := t.Database + "." + t.Name; !s.shapes[name].Exists {
			s.shapes[name] = chsink.TableShape{Exists: true}
		}
	}
	if s.onEnsure != nil {
		s.onEnsure(t)
	}
	return nil
}

func keyOf(t *chsink.Table, r chsink.Row) string {
	key := ""
	for _, name := range t.OrderBy {
		for i, c := range t.Columns {
			if c.Name == name {
				key += fmt.Sprintf("%v|", r.Values[i])
			}
		}
	}
	return key
}

// Change records the schema change and fails the nth call (1-based) if changeErrs says so.
func (s *fakeSink) Change(_ context.Context, req chsink.ChangeRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes = append(s.changes, req)
	s.journal.add("change %s", req.Source)
	return s.changeErrs[len(s.changes)]
}

// RenameTable records the rename and fails the nth call (1-based) if renameErrs says so.
func (s *fakeSink) RenameTable(_ context.Context, req chsink.RenameRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renames = append(s.renames, req)
	s.journal.add("rename %s.%s to %s.%s", req.FromDatabase, req.From, req.ToDatabase, req.To)
	return s.renameErrs[len(s.renames)]
}

// DropTable records the drop.
func (s *fakeSink) DropTable(_ context.Context, req chsink.DropRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drops = append(s.drops, req)
	s.journal.add("drop %s.%s", req.Database, req.Table)
	delete(s.shapes, req.Database+"."+req.Table)
	delete(s.tables, req.Database+"."+req.Table)
	delete(s.marked, req.Database+"."+req.Table)
	return nil
}

func (s *fakeSink) Shape(_ context.Context, database, table string) (chsink.TableShape, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shapes[database+"."+table], nil
}

// Rebuild records the rebuild.
func (s *fakeSink) Rebuild(_ context.Context, req chsink.RebuildRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebuilds = append(s.rebuilds, req)
	s.journal.add("rebuild %s.%s", req.Database, req.Table)
	return s.rebuildErr
}

// CheckAccess fails with accessErr, when set.
func (s *fakeSink) CheckAccess(context.Context, string, ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accessErr
}

// SwapShadow puts the shadow table in the place of the table, like the real one does.
// SwapShadow answers the UUID of the copy as "uuid:" and the name of the shadow table.
func (s *fakeSink) SwapShadow(_ context.Context, req chsink.SwapRequest) (string, error) {
	if s.onSwap != nil {
		s.onSwap(req) // to check, at this exact moment, what the pipeline knew before it swapped
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := "uuid:" + req.Database + "." + req.Shadow
	if f := s.swapFails; f != nil {
		s.swapFails = nil
		s.journal.add("swap-failed %s.%s", req.Database, req.Table)
		if !f.sent {
			copied = ""
		}
		return copied, f.err
	}
	if s.lost[req.Database+"."+req.Shadow] && req.Copy == "" {
		s.journal.add("swap-lost %s.%s", req.Database, req.Table)
		return "", &chsink.TableError{Table: req.Source, Recoverable: true, Err: errors.New("the shadow table is gone")}
	}
	s.swaps = append(s.swaps, req)
	s.journal.add("swap %s.%s", req.Database, req.Table)
	if s.marked[req.Database+"."+req.Table] {
		// The exchange was made: what is left is the old table under the name of the shadow table.
		delete(s.tables, req.Database+"."+req.Shadow)
		delete(s.shapes, req.Database+"."+req.Shadow)
		return copied, nil
	}
	s.tables[req.Database+"."+req.Table] = s.tables[req.Database+"."+req.Shadow]
	delete(s.tables, req.Database+"."+req.Shadow)
	delete(s.shapes, req.Database+"."+req.Shadow)
	return copied, nil
}

// SwapMarks reports the tables that marked lists.
func (s *fakeSink) SwapMarks(_ context.Context, req chsink.SwapRequest) (chsink.SwapMarks, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	shadow := req.Database + "." + req.Shadow
	exists := s.shapes[shadow].Exists || s.marked[shadow] || s.foreign[shadow]
	return chsink.SwapMarks{
		Table:        s.marked[req.Database+"."+req.Table],
		Shadow:       s.marked[shadow] || (exists && !s.foreign[shadow]),
		ShadowExists: exists,
	}, nil
}

// DropShadow drops the shadow table if it is binwave's (marked, or made and not foreign), recorded
// among the drops.
func (s *fakeSink) DropShadow(_ context.Context, req chsink.SwapRequest) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	shadow := req.Database + "." + req.Shadow
	if s.foreign[shadow] || (!s.marked[shadow] && !s.shapes[shadow].Exists) {
		return false, nil
	}
	s.drops = append(s.drops, chsink.DropRequest{Source: req.Shadow, Database: req.Database, Table: req.Shadow})
	s.journal.add("drop %s", shadow)
	delete(s.shapes, shadow)
	delete(s.tables, shadow)
	delete(s.marked, shadow)
	return true, nil
}

// TruncateTable records the truncation.
func (s *fakeSink) TruncateTable(_ context.Context, req chsink.TruncateRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.truncs = append(s.truncs, req)
	s.journal.add("truncate %s.%s", req.Database, req.Table)
	return nil
}

func (s *fakeSink) Insert(ctx context.Context, t *chsink.Table, rows []chsink.Row) error {
	if s.beforeInsert != nil {
		s.beforeInsert(t.Source.FullName()) // before the lock: the fake serializes, the pipeline does not
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ctxAtInsert = append(s.ctxAtInsert, ctx.Err())
	s.inserts++
	if err := s.insertErrs[s.inserts]; err != nil {
		s.journal.add("insert-failed %s", t.Source.FullName())
		return err
	}
	if err := s.insertErrsByTarget[t.Database+"."+t.Name]; err != nil {
		s.journal.add("insert-failed %s.%s", t.Database, t.Name)
		return err
	}
	if f := s.insertErrsByTable[t.Source.FullName()]; f != nil && f.left != 0 {
		if f.left > 0 {
			f.left--
		}
		s.journal.add("insert-failed %s", t.Source.FullName())
		return f.err
	}
	before, after := s.faults.step()
	if before != nil {
		s.journal.add("insert-crashed %s", t.Source.FullName())
		return before
	}

	name := t.Database + "." + t.Name
	if s.tables[name] == nil {
		s.tables[name] = map[string]cell{}
	}
	for _, r := range rows {
		key := keyOf(t, r)
		if cur, ok := s.tables[name][key]; !ok || r.Version >= cur.version {
			s.tables[name][key] = cell{version: r.Version, deleted: r.Deleted, values: r.Values}
		}
		s.versions[r.Version] = true
	}
	s.journal.add("insert %s x%d", name, len(rows))
	return after
}

// visibleItems returns the live rows of shop.items by key, the state a FINAL query would show.
func (s *fakeSink) visibleItems() map[string][]any {
	const table = "shop.items"
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]any{}
	for k, c := range s.tables[table] {
		if !c.deleted {
			out[k] = c.values
		}
	}
	return out
}

func (s *fakeSink) hasVersion(v uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.versions[v]
}

// fakeStore keeps the position in memory, like a file or table that survives a crash.
type fakeStore struct {
	journal *journal
	faults  *faults

	mu       sync.Mutex
	pos      checkpoint.Position
	found    bool
	saves    []checkpoint.Position
	saveErrs map[int]error
	// onSave is called before a save takes effect, to check invariants at that moment.
	onSave func(checkpoint.Position)
}

func newFakeStore(j *journal, f *faults) *fakeStore { return &fakeStore{journal: j, faults: f} }

func (s *fakeStore) Load(context.Context) (checkpoint.Position, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.journal.add("load")
	return s.pos, s.found, nil
}

func (s *fakeStore) Save(_ context.Context, pos checkpoint.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	call := len(s.saves) + 1
	if err := s.saveErrs[call]; err != nil {
		return err
	}
	before, after := s.faults.step()
	if before != nil {
		s.journal.add("save-crashed %s", pos.GTIDSet)
		return before
	}
	if s.onSave != nil {
		s.onSave(pos)
	}
	s.pos, s.found = pos, true
	s.saves = append(s.saves, pos)
	s.journal.add("save %s v%d", pos.GTIDSet, pos.Version)
	return after
}

func (s *fakeStore) current() checkpoint.Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pos
}

// all returns every position that was saved, in order.
func (s *fakeStore) all() []checkpoint.Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]checkpoint.Position(nil), s.saves...)
}

// fakeClock is advanced by the tests; the pipeline only reads it.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// harness wires a pipeline to the fakes.
type harness struct {
	cfg     *config.Config
	source  *fakeSource
	sink    *fakeSink
	store   *fakeStore
	journal *journal
	faults  *faults
	clock   *fakeClock
	sleeps  []time.Duration
	guard   Guard
	obs     Observer
	// snapshot is what a resync reads from; nil where a test does not resynchronize.
	snapshot Snapshotter
	// retry and sourceTransient are Deps.Retry and Deps.SourceTransient; zero where a test does not
	// care.
	retry           RetryPolicy
	sourceTransient func(error) bool

	logger *slog.Logger

	progress atomic.Int64
	readied  atomic.Int64
}

func newHarness(t testing.TB) *harness {
	t.Helper()
	j := &journal{}
	f := &faults{}
	return &harness{
		cfg:     testConfig(t),
		source:  &fakeSource{executed: "u:1-0"},
		sink:    newFakeSink(j, f),
		store:   newFakeStore(j, f),
		journal: j,
		faults:  f,
		clock:   &fakeClock{now: epoch},
		logger:  slog.New(slog.DiscardHandler),
	}
}

func (h *harness) pipeline(t testing.TB) *Pipeline {
	t.Helper()
	p, err := New(h.cfg, Deps{
		Source: h.source, Sink: h.sink, Store: h.store,
		Logger:          h.logger,
		Guard:           h.guard,
		Observer:        h.obs,
		Snapshot:        h.snapshot,
		Retry:           h.retry,
		Now:             h.clock.Now,
		SourceTransient: h.sourceTransient,
		Sleep: func(ctx context.Context, d time.Duration) error {
			h.sleeps = append(h.sleeps, d)
			h.clock.Advance(d) // waiting takes (model) time, which is what retry budgets measure
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (h *harness) hooks() Hooks {
	return Hooks{Ready: func() { h.readied.Add(1) }, Progress: func() { h.progress.Add(1) }}
}

// run executes the pipeline until the script ends, then stops it gracefully like a stop signal
// would, and returns Run's error. A failure of the final flush is therefore visible.
func (h *harness) run(t *testing.T) error {
	t.Helper()
	return h.runPipeline(t, h.pipeline(t))
}

// runPipeline is run for a pipeline the test made itself, to reach it from other goroutines.
func (h *harness) runPipeline(t *testing.T, p *Pipeline) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.source.onEnd = cancel
	err := p.Run(ctx, h.hooks())
	if errors.Is(err, errScriptEnded) {
		return nil
	}
	return err
}

// fakeGuard records the tables it was asked about and fails for the ones in reject.
type fakeGuard struct {
	mu     sync.Mutex
	asked  []string
	reject map[string]error
	// stopReject answers CheckTable in the err slot instead of problem, the way the real Guard
	// does for tables.on_replication_error: stop (config.OnErrorStop) — a problem the caller must
	// not quarantine away, because it means the whole service stops.
	stopReject map[string]error
	forgotten  []string
}

func (g *fakeGuard) Forget(database, table string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.forgotten = append(g.forgotten, database+"."+table)
}

func (g *fakeGuard) CheckTable(_ context.Context, database, table string) (problem, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	full := database + "." + table
	g.asked = append(g.asked, full)
	if e := g.stopReject[full]; e != nil {
		return nil, e
	}
	return g.reject[full], nil
}

// fakeObserver records what the pipeline reports, in order.
type fakeObserver struct {
	mu          sync.Mutex
	calls       []string
	rows        int
	retries     []string
	sourceRetry []string
	recovered   int
	saved       []savedCall
	tables      int
	skipped     int
	quarantined int
}

type savedCall struct {
	pos           checkpoint.Position
	applied, open time.Time
}

func (o *fakeObserver) note(format string, args ...any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, fmt.Sprintf(format, args...))
}

func (o *fakeObserver) TransactionBegan(c time.Time) { o.note("began %d", c.Sub(epoch)/time.Second) }
func (o *fakeObserver) TransactionCommitted()        { o.note("committed") }
func (o *fakeObserver) TransactionSkipped()          { o.note("skipped-transaction") }
func (o *fakeObserver) RebuildStarted(t string)      { o.note("rebuild-started %s", t) }
func (o *fakeObserver) RebuildFinished(t string, ok bool) {
	o.note("rebuild-finished %s %v", t, ok)
}

func (o *fakeObserver) RowsRead(table string, n int) {
	o.mu.Lock()
	o.rows += n
	o.mu.Unlock()
	o.note("read %s %d", table, n)
}
func (o *fakeObserver) BufferChanged(rows, _ int) { o.note("buffer %d", rows) }
func (o *fakeObserver) Inserted(table string, rows int, _ time.Duration) {
	o.note("inserted %s %d", table, rows)
}

func (o *fakeObserver) PositionSaved(pos checkpoint.Position, applied, open time.Time) {
	o.mu.Lock()
	o.saved = append(o.saved, savedCall{pos, applied, open})
	o.mu.Unlock()
	o.note("saved %s", pos.GTIDSet)
}
func (o *fakeObserver) PositionLoaded(pos checkpoint.Position) { o.note("loaded %s", pos.GTIDSet) }
func (o *fakeObserver) Idle()                                  { o.note("idle") }
func (o *fakeObserver) Retry(op, reason string, _ error) {
	o.mu.Lock()
	o.retries = append(o.retries, reason+" "+op)
	o.mu.Unlock()
}

func (o *fakeObserver) SourceRetry(op string, _ error) {
	o.mu.Lock()
	o.sourceRetry = append(o.sourceRetry, op)
	o.mu.Unlock()
}

func (o *fakeObserver) sourceRetries() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.sourceRetry...)
}

func (o *fakeObserver) Recovered() {
	o.mu.Lock()
	o.recovered++
	o.mu.Unlock()
}

func (o *fakeObserver) TablesSelected(n int) {
	o.mu.Lock()
	o.tables = n
	o.mu.Unlock()
	o.note("tables %d", n)
}

func (o *fakeObserver) RowsSkipped(table string, n int) { o.note("skipped %s %d", table, n) }
func (o *fakeObserver) QuarantineChanged(entries []checkpoint.Quarantined) {
	o.mu.Lock()
	o.quarantined = len(entries)
	o.mu.Unlock()
	o.note("quarantine %d", len(entries))
}

func (o *fakeObserver) ResyncChanged(st []snapshot.Status) { o.note("resync %d", len(st)) }
func (o *fakeObserver) ResyncRowsRead(table string, n int) { o.note("resync-rows %s %d", table, n) }
func (o *fakeObserver) ResyncFinished(table string, ok bool) {
	o.note("resync-finished %s %v", table, ok)
}

func (o *fakeObserver) AutoresyncResolved(table string, ok bool, attempts int) {
	o.note("autoresync-resolved %s %v %d", table, ok, attempts)
}

func (o *fakeObserver) DestructiveDDLSkipped() {
	o.mu.Lock()
	o.skipped++
	o.mu.Unlock()
}

// sequence returns the recorded calls that start with one of the prefixes, in order.
func (o *fakeObserver) sequence(prefixes ...string) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []string
	for _, c := range o.calls {
		for _, p := range prefixes {
			if strings.HasPrefix(c, p) {
				out = append(out, c)
				break
			}
		}
	}
	return out
}

// otherTable is shop.other, shaped like shop.items.
func otherTable() *schema.Table {
	t := itemsTable()
	t.Name = "other"
	return t
}

// liveRows returns the live rows of a table by key.
func (s *fakeSink) liveRows(table string) map[string][]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]any{}
	for k, c := range s.tables[table] {
		if !c.deleted {
			out[k] = c.values
		}
	}
	return out
}
