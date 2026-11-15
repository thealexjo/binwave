package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/snapshot"
)

// The initial load (replication.initial_position: snapshot) is the exact same mechanism as
// `tables resync` (internal/pipeline/resync.go): a shadow table, a swap, giving up quarantines the
// table — everything the resync tests above already cover applies here unchanged.
// checkpoint.Resync.InitialLoad only says "don't hold `tables resync` up behind this, and don't count
// it against the same parallelism budget". These tests exercise what is actually specific to the
// initial load: deciding which tables it covers at the very first start, and that parallelism limit.

// TestInitialLoadFillsATableWhileTheStreamGoesOn: on the first run, the table does not exist under its
// real name until the load (and the live rows that arrive alongside it, redirected to the same shadow
// table) are both in and the stream has caught up — nobody ever sees it half filled.
func TestInitialLoadFillsATableWhileTheStreamGoesOn(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Replication.InitialPosition = config.StartFromSnapshot
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, executed: fence(1)}
	h.obs = &fakeObserver{}
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 3, "c"))}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	want := map[string]string{"1|": "a", "2|": "b", "3|": "c"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
	if got := live(t, h, "shop.items__resync"); len(got) != 0 {
		t.Errorf("the shadow table still has rows %v", got)
	}
	if pos := h.store.current(); pos.Resync != "" {
		t.Errorf("the state of the initial load is still saved: %q", pos.Resync)
	}
	if got := h.obs.(*fakeObserver).sequence("resync-finished"); len(got) != 1 || got[0] != "resync-finished shop.items true" {
		t.Errorf("finished = %v", got)
	}
}

// TestTheInitialLoadSavesHowManyTablesItCovers: the saved resync list only says what is left, so
// "N of M tables loaded" needs M saved with it — once, when the load is decided. It is cleared when
// the last table is loaded, or the status would claim a load that is over forever.
func TestTheInitialLoadSavesHowManyTablesItCovers(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Replication.InitialPosition = config.StartFromSnapshot
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 2, "b"))}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	saves := h.store.all()
	if len(saves) == 0 {
		t.Fatal("nothing was saved")
	}
	if got := saves[0].LoadTotal; got != 1 {
		t.Errorf("the first saved position covers %d tables, want the 1 the load decided on", got)
	}
	if last := saves[len(saves)-1]; last.LoadTotal != 0 || last.Resync != "" {
		t.Errorf("after the load finished the position still says load_total=%d resync=%q", last.LoadTotal, last.Resync)
	}
}

// TestInitialLoadQuarantinesATableItCannotReadInChunks: with the default policy (quarantine), a table
// whose key a chunk cannot resume from is set aside right at the first start, loudly, instead of
// silently replicating forward-only and never getting its history — which is exactly the silent gap
// binwave never leaves.
func TestInitialLoadQuarantinesATableItCannotReadInChunks(t *testing.T) {
	h := newHarness(t)
	h.cfg.Replication.InitialPosition = config.StartFromSnapshot
	h.snapshot = &fakeSnapshot{defErr: errors.New("no primary key")}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	q := h.store.current().Quarantine
	entries, err := checkpoint.DecodeQuarantine(q)
	if err != nil || len(entries) != 1 || entries[0].Table != "shop.items" {
		t.Fatalf("quarantine = %v, %v, want shop.items quarantined from the very first saved position", entries, err)
	}
	if entries[0].Code != string(errcode.TBL106) {
		t.Errorf("code = %q, want %s", entries[0].Code, errcode.TBL106)
	}
	if got := live(t, h, "shop.items"); len(got) != 0 {
		t.Errorf("rows = %v, want none: a quarantined table replicates nothing until it is released", got)
	}
}

// TestTablesToLoadDoesNotQuarantineTablesOverAContextCancellation: TableDefinition failing with
// "context canceled" means the start is stopping (a shutdown, a cancelled retry), not that the
// table's key is unchunkable. Attributing it to the table would quarantine it, and every table
// after it in the catalog that the loop had not even reached yet, with a permanent-sounding reason
// ("its key cannot be read in chunks") that has nothing to do with any of them.
func TestTablesToLoadDoesNotQuarantineTablesOverAContextCancellation(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{
		catalog: []schema.CatalogTable{{Database: "shop", Name: "items"}, {Database: "shop", Name: "orders"}},
		defErr:  context.Canceled,
	}
	p := h.pipeline(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	entries, err := p.tablesToLoad(ctx, fence(0))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tablesToLoad() error = %v, want it to report the cancellation, not a table problem", err)
	}
	if entries != nil {
		t.Errorf("entries = %v, want none: a cancelled read decides nothing", entries)
	}
	if q := p.Quarantined(); len(q) != 0 {
		t.Errorf("quarantined = %v, want none: a cancelled context is not a key problem of any table", q)
	}
}

// TestInitialLoadStopsTheServiceWhenPolicyIsStopAndATableCannotBeChunked: with
// tables.on_replication_error: stop, the operator asked for zero tolerance, so a table the load cannot
// cover refuses the start instead of being quietly set aside.
func TestInitialLoadStopsTheServiceWhenPolicyIsStopAndATableCannotBeChunked(t *testing.T) {
	h := newHarness(t)
	h.cfg.Replication.InitialPosition = config.StartFromSnapshot
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	h.snapshot = &fakeSnapshot{defErr: errors.New("no primary key")}

	err := h.run(t)

	if !errors.Is(err, ErrUnsupported) || err == nil {
		t.Fatalf("Run() = %v, want an error naming the table and wrapping ErrUnsupported", err)
	}
}

// TestFillLoadSlotsRespectsParallelism: only replication.snapshot.parallelism loads read at once; the
// rest wait for a slot, and freeing one lets the next begin.
func TestFillLoadSlotsRespectsParallelism(t *testing.T) {
	h := newHarness(t)
	h.cfg.Replication.Snapshot.Parallelism = 2
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}}
	p := h.pipeline(t)
	ctx := context.Background()
	for _, name := range []string{"shop.a", "shop.b", "shop.c", "shop.d"} {
		def := itemsTable()
		def.Name = name[len("shop."):]
		target, err := p.shadowTable(def, def.Name+"__resync")
		if err != nil {
			t.Fatal(err)
		}
		job := &resyncJob{state: checkpoint.Resync{Table: name, Shadow: def.Name + "__resync", InitialLoad: true}, tbl: def, target: target}
		p.resyncs[name] = job
		p.trackResync(name, job)
	}
	t.Cleanup(func() {
		for _, j := range p.resyncs {
			p.stopJob(j)
		}
	})

	running := func() (names []string) {
		for name, j := range p.resyncs {
			if j.cancel != nil {
				names = append(names, name)
			}
		}
		return names
	}

	if err := p.fillLoadSlots(ctx); err != nil {
		t.Fatal(err)
	}
	if got := running(); len(got) != 2 {
		t.Fatalf("running = %v, want 2 (the configured parallelism)", got)
	}

	// One slot frees; fillLoadSlots is what notices and starts the next one waiting.
	freed := running()[0]
	p.stopJob(p.resyncs[freed])
	delete(p.resyncs, freed)
	if err := p.fillLoadSlots(ctx); err != nil {
		t.Fatal(err)
	}
	if got := running(); len(got) != 2 {
		t.Errorf("running after a slot freed = %v, want 2 again", got)
	}
}

// TestATablesResyncIsNotHeldUpBehindTheInitialLoad: an operator's `tables resync` starts at once even
// while every parallelism slot is taken by the initial load — it is not queued behind work the operator
// did not ask for.
func TestATablesResyncIsNotHeldUpBehindTheInitialLoad(t *testing.T) {
	h := newHarness(t)
	h.cfg.Replication.Snapshot.Parallelism = 1
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}}
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	ctx := context.Background()

	loading := itemsTable()
	loading.Name = "loading"
	target, err := p.shadowTable(loading, "loading__resync")
	if err != nil {
		t.Fatal(err)
	}
	load := &resyncJob{state: checkpoint.Resync{Table: "shop.loading", Shadow: "loading__resync", InitialLoad: true}, tbl: loading, target: target}
	p.resyncs["shop.loading"] = load
	p.trackResync("shop.loading", load)
	if err := p.fillLoadSlots(ctx); err != nil {
		t.Fatal(err)
	}
	if load.cancel == nil {
		t.Fatal("the initial load did not take the only slot, so nothing is proven below")
	}
	t.Cleanup(func() {
		for _, j := range p.resyncs {
			p.stopJob(j)
		}
	})

	def := itemsTable()
	if err := p.startResync(ctx, "shop.items", def, "", 0, resyncOrigin{}); err != nil {
		t.Fatalf("startResync() = %v", err)
	}

	job, ok := p.resyncs["shop.items"]
	if !ok || job.cancel == nil {
		t.Error("the manual resync did not start reading at once")
	}
}

// TestInitialLoadContinuesFromTheSavedCursorAfterARestart: the saved position already lists the load
// (as the very first run would have saved it, InitialLoad and all); a new process resumes it exactly as
// it would a resync, and it is not launched immediately — fillLoadSlots decides that.
func TestInitialLoadContinuesFromTheSavedCursorAfterARestart(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}, executed: fence(1)}
	cursor, err := checkpoint.EncodeCursor([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{Table: "shop.items", Shadow: "items__resync", Since: fence(0), Rows: 1, Cursor: cursor, InitialLoad: true}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(0), Resync: state}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	// What the first process wrote before it stopped.
	h.sink.tables["shop.items__resync"] = map[string]cell{"1|": {version: 0, values: []any{int64(1), "a"}}}
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 4, "d"))}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	want := map[string]string{"1|": "a", "2|": "b", "3|": "c", "4|": "d"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

// TestResyncsResumedLogsOneSummaryLineNotOnePerTable: with replication.initial_position: snapshot,
// every table becomes a saved resync entry at the first start, so a source with hundreds of tables
// would print hundreds of "resync continues" lines at every single restart, forever, until
// each one finishes. resumeResyncs logs one summary line instead; the per-table detail (still useful
// when debugging one table) moves to Debug, off by default.
func TestResyncsResumedLogsOneSummaryLineNotOnePerTable(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}, executed: fence(1)}
	cursor, err := checkpoint.EncodeCursor([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{Table: "shop.items", Shadow: "items__resync", Since: fence(0), Rows: 1, Cursor: cursor, InitialLoad: true}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(0), Resync: state}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	h.sink.tables["shop.items__resync"] = map[string]cell{"1|": {version: 0, values: []any{int64(1), "a"}}}
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 4, "d"))}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	var out strings.Builder
	var mu sync.Mutex
	h.logger = slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return out.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelInfo})) // the default level; resync goroutines log concurrently

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	mu.Lock()
	log := out.String()
	mu.Unlock()
	if !strings.Contains(log, "resyncs resumed") || !strings.Contains(log, "total=1") {
		t.Errorf("log = %q, want the one-line summary with total=1", log)
	}
	if strings.Contains(log, "resync continues") {
		t.Errorf("log = %q, want the per-table detail to stay at Debug, not appear at the default Info level", log)
	}
}

// waitingLoads registers n initial-load resyncs that have no slot yet, the way the very first start
// of replication.initial_position: snapshot does for every table of the source.
func waitingLoads(t testing.TB, h *harness, n int) *Pipeline {
	t.Helper()
	entries := make([]checkpoint.Resync, n)
	for i := range entries {
		entries[i] = checkpoint.Resync{
			Table: fmt.Sprintf("shop.t%04d", i), Shadow: fmt.Sprintf("t%04d__resync", i), Since: fence(0), InitialLoad: true,
		}
	}
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	p.q.setSince(fence(0))
	t.Cleanup(func() {
		for _, j := range p.resyncs {
			p.stopJob(j)
		}
	})
	pos := checkpoint.Position{GTIDSet: fence(0), Resync: checkpoint.EncodeResyncs(entries)}
	if err := p.resumeResyncs(context.Background(), pos); err != nil {
		t.Fatalf("resumeResyncs() = %v", err)
	}
	return p
}

// TestServicingResyncsDoesNotCostWhatTheSourceHasTables: what every transaction boundary does for
// the resyncs must cost what the resyncs in flight cost, not what the source has tables. With
// replication.initial_position: snapshot every table of the source is a registered resync from the
// first start until it is loaded, so walking all of them on every commit (building and sorting their
// names, encoding the whole list into the position, publishing every status) is work that grows with
// the schema: on a source with thousands of tables it starved the pipeline of the CPU long enough for
// systemd's watchdog to abort the process (Failed with result 'watchdog').
func TestServicingResyncsDoesNotCostWhatTheSourceHasTables(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}}
	h.cfg.Replication.Snapshot.Parallelism = 1
	p := waitingLoads(t, h, 2000)

	// The one slot is taken by a table whose reader hands nothing over, so every pass below is what a
	// transaction boundary costs when there is nothing to do but thousands of tables to ignore.
	blocked := &resyncJob{
		state:   checkpoint.Resync{Table: "shop.blocked", Shadow: "blocked__resync", Since: fence(0), InitialLoad: true},
		results: make(chan chunkResult),
		cancel:  func() {},
		stopped: make(chan struct{}),
	}
	close(blocked.stopped)
	p.resyncs[blocked.state.Table] = blocked
	p.reading[blocked.state.Table] = blocked

	ctx := context.Background()
	if err := p.serviceResyncs(ctx); err != nil { // the first pass publishes the statuses once
		t.Fatal(err)
	}

	const passes = 50
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range passes {
		if err := p.serviceResyncs(ctx); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)

	// Walking every entry allocated a names slice and a sorted copy of it per pass, and rebuilt the
	// statuses of all of them: megabytes over 50 passes. Touching only what is in flight is nothing.
	const budget = 64 << 10
	if grew := after.TotalAlloc - before.TotalAlloc; grew > budget {
		t.Errorf("%d passes over %d waiting tables allocated %d bytes, want under %d: the hot path still walks every table",
			passes, len(p.resyncs), grew, budget)
	}
	if len(p.waiting) != 2000 {
		t.Errorf("waiting = %d, want the 2000 tables still waiting for a slot", len(p.waiting))
	}
}

// TestAResyncIsGivenUpWhenItsTableIsQuarantinedAgain: a table set aside while its resync reads it
// cannot have the snapshot's rows applied either, so the resync is given up. The pipeline learns
// which tables those are from the quarantine set itself (quarantineSet.fresh), not by walking every
// resync there is, so this has to keep working for a table nobody is otherwise looking at.
func TestAResyncIsGivenUpWhenItsTableIsQuarantinedAgain(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}}
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	p.q.setSince(fence(0))
	ctx := context.Background()
	t.Cleanup(func() {
		for _, j := range p.resyncs {
			p.stopJob(j)
		}
	})

	if err := p.startResync(ctx, "shop.items", itemsTable(), "", 0, resyncOrigin{}); err != nil {
		t.Fatalf("startResync() = %v", err)
	}
	if !p.Quarantine("shop.items", errcode.TBL101, "no key after all") {
		t.Fatal("Quarantine() = false")
	}

	if err := p.serviceResyncs(ctx); err != nil {
		t.Fatal(err)
	}

	if _, still := p.resyncs["shop.items"]; still {
		t.Error("the resync of a table that was quarantined again is still running")
	}
	if got := h.obs.(*fakeObserver).sequence("resync-finished"); len(got) != 1 || got[0] != "resync-finished shop.items false" {
		t.Errorf("finished = %v, want the resync given up", got)
	}
	if q := p.Quarantined(); len(q) != 1 || q[0].Table != "shop.items" {
		t.Errorf("quarantined = %v, want the table still set aside", q)
	}
}

// TestResumeResyncsReportsProgressPeriodicallyAndTouchesProgress: walking thousands of pending
// resyncs at the very first start of a large source can take minutes; total silence for that long
// would look exactly like a hang, both in the log and on /healthz (stale after 30s without
// progress, internal/observability). resumeResyncs must report periodically and keep touching
// progress the whole way, not just once at the very end.
func TestResumeResyncsReportsProgressPeriodicallyAndTouchesProgress(t *testing.T) {
	const total = 450 // more than two reporting periods (resyncResumeReportEvery = 200)
	entries := make([]checkpoint.Resync, total)
	for i := range entries {
		entries[i] = checkpoint.Resync{
			Table: fmt.Sprintf("shop.t%d", i), Shadow: fmt.Sprintf("t%d__resync", i), Since: fence(0), InitialLoad: true,
		}
	}
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{}
	p := h.pipeline(t)

	var out strings.Builder
	var mu sync.Mutex
	p.log = slog.New(slog.NewTextHandler(writerFunc(func(b []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return out.Write(b)
	}), &slog.HandlerOptions{Level: slog.LevelInfo}))

	var progress int
	p.hooks = Hooks{Progress: func() { progress++ }}

	pos := checkpoint.Position{GTIDSet: fence(0), Resync: checkpoint.EncodeResyncs(entries)}
	if err := p.resumeResyncs(context.Background(), pos); err != nil {
		t.Fatalf("resumeResyncs() = %v", err)
	}

	if progress < total {
		t.Errorf("progress touched %d times for %d tables, want at least once per table so /healthz never goes stale mid-walk", progress, total)
	}

	mu.Lock()
	log := out.String()
	mu.Unlock()
	for _, want := range []string{"done=200", "done=400"} {
		if !strings.Contains(log, want) {
			t.Errorf("log = %q, want a periodic progress line at %s", log, want)
		}
	}
	if strings.Contains(log, "done=450") {
		t.Errorf("log = %q, the last entry belongs only to the final summary, not another periodic line", log)
	}
	if !strings.Contains(log, "resyncs resumed") || !strings.Contains(log, "total=450") {
		t.Errorf("log = %q, want the final summary with total=450", log)
	}
}

// TestInitialLoadThatGivesUpIsQuarantinedLikeAResync: giving up an initial load is no different from
// giving up a `tables resync` — the table is quarantined, because nobody has ever seen it complete.
// This drives takeChunks directly, without the real retry delay a failing reader goroutine would cost.
func TestInitialLoadThatGivesUpIsQuarantinedLikeAResync(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	p.q.setSince(fence(0))

	target, err := p.shadowTable(itemsTable(), "items__resync")
	if err != nil {
		t.Fatal(err)
	}
	job := &resyncJob{
		state:   checkpoint.Resync{Table: "shop.items", Shadow: "items__resync", Since: fence(0), Started: epoch, InitialLoad: true},
		tbl:     itemsTable(),
		target:  target,
		results: make(chan chunkResult, 1),
	}
	p.resyncs["shop.items"] = job
	job.results <- chunkResult{err: errors.New("connection refused")}

	if _, err := p.takeChunks(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	if _, still := p.resyncs["shop.items"]; still {
		t.Error("the load is still tracked after giving up")
	}
	if len(p.Quarantined()) != 1 {
		t.Errorf("quarantined = %v, want shop.items quarantined", p.Quarantined())
	}
	if len(h.sink.drops) != 1 {
		t.Errorf("the shadow table was not dropped: %v", h.sink.drops)
	}
}

// TestSetLoadRateChangesTheSharedBudgetWhileRunning: it refuses before Run has set the budget up, and
// while running it changes the one budget every initial load reads through.
func TestSetLoadRateChangesTheSharedBudgetWhileRunning(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{}
	p := h.pipeline(t)

	if err := p.SetLoadRate(100); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("SetLoadRate() before Run() = %v, want ErrNotRunning", err)
	}

	h.source.until = func() bool {
		if err := p.SetLoadRate(500); err != nil {
			t.Errorf("SetLoadRate() while running = %v", err)
		}
		return true
	}
	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if got := p.loadBudget.Rate(); got != 500 {
		t.Errorf("the shared budget's rate = %d, want 500", got)
	}
}

// TestDDLDuringAnInitialLoadDiscardsItAndBeginsAgain: what was read is in the old structure, so the
// load is discarded and begins again, counted as a restart, staying an initial load throughout — not
// quarantined by the DDL statement itself, only if it cannot begin again afterward.
func TestDDLDuringAnInitialLoadDiscardsItAndBeginsAgain(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Replication.InitialPosition = config.StartFromSnapshot
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(3)}
	h.obs = &fakeObserver{}
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 2, "b")),
		uuidTx(2, event.DDL{Database: "shop", Query: "ALTER TABLE items ADD COLUMN extra INT NULL", Time: epoch}),
		uuidTx(3, insertRows(items, 4, "d")),
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	obs := h.obs.(*fakeObserver)
	got := obs.sequence("resync-finished")
	if len(got) < 2 || got[0] != "resync-finished shop.items false" || got[len(got)-1] != "resync-finished shop.items true" {
		t.Errorf("finished = %v, want the first load discarded and the second finished", got)
	}
	entries, err := checkpoint.DecodeQuarantine(h.store.current().Quarantine)
	if err != nil || len(entries) != 0 {
		t.Errorf("quarantine = %v, %v: a DDL statement during the load, on its own, must not quarantine the table", entries, err)
	}
}

// TestProgressIsReportedPerChunkNotPerEvent: one Commit can make the pipeline drain every chunk the
// readers have ready — dozens of them, each a converted batch of up to 100 000 rows with its insert
// and its saved position. Progress is what the systemd watchdog and /healthz watch, and reporting it
// once per event (at the top of handle) meant minutes of real work looked like a hang: systemd
// aborted the process with Failed with result 'watchdog' while it was converting chunks.
func TestProgressIsReportedPerChunkNotPerEvent(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	p.q.setSince(fence(0))

	target, err := p.shadowTable(itemsTable(), "items__resync")
	if err != nil {
		t.Fatal(err)
	}
	const chunks = 4
	job := &resyncJob{
		state:   checkpoint.Resync{Table: "shop.items", Shadow: "items__resync", Since: fence(0), Started: epoch, InitialLoad: true},
		tbl:     itemsTable(),
		target:  target,
		results: make(chan chunkResult, chunks),
	}
	p.resyncs["shop.items"] = job
	p.reading["shop.items"] = job
	for i := range chunks {
		// What a reading goroutine hands over: the rows already converted (see launch).
		rows, err := target.Rows(event.Rows{Table: job.tbl, Op: event.Insert, Rows: []event.Row{{After: []any{int64(i + 1), "a"}}}}, 1)
		if err != nil {
			t.Fatal(err)
		}
		job.results <- chunkResult{
			chunk: snapshot.Chunk{Last: []any{int64(i + 1)}, Watermark: fence(1)},
			rows:  rows, nrows: 1,
		}
	}

	var progress int
	p.hooks = Hooks{Progress: func() { progress++ }}

	if _, err := p.takeChunks(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	if progress < chunks {
		t.Errorf("progress reported %d times for %d chunks, want at least one per chunk: "+
			"a long run of fast work must not look like a hang to the watchdog", progress, chunks)
	}
}

// TestProgressIsReportedWhenAnAttemptBegins: withRetry reported progress only from a keepalive tick
// a second into a call, so a long sequence of calls that each finish sooner reported nothing at all.
func TestProgressIsReportedWhenAnAttemptBegins(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	var progress int
	p.hooks = Hooks{Progress: func() { progress++ }}

	if err := p.withRetry(context.Background(), "a fast call", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	if progress == 0 {
		t.Error("a call that finished before the first keepalive tick reported no progress at all")
	}
}

// doneLoads registers n tables of an initial load that are read in full and wait for the stream to
// reach their watermark, the state an initial load spends its last hours in.
func doneLoads(t testing.TB, h *harness, n int) *Pipeline {
	t.Helper()
	entries := make([]checkpoint.Resync, n)
	for i := range entries {
		entries[i] = checkpoint.Resync{
			Table: fmt.Sprintf("shop.t%04d", i), Shadow: fmt.Sprintf("t%04d__resync", i), Since: fence(0),
			// Read at a point the stream is nowhere near: the watermarks grow with the tables, as
			// gtid_executed does while the tables are read one after another.
			Done: true, Final: fence(1000 + i), Rows: 1, InitialLoad: true,
		}
	}
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	p.q.setSince(fence(0))
	t.Cleanup(func() {
		for _, j := range p.resyncs {
			p.stopJob(j)
		}
	})
	pos := checkpoint.Position{GTIDSet: fence(1), Resync: checkpoint.EncodeResyncs(entries)}
	if err := p.resumeResyncs(context.Background(), pos); err != nil {
		t.Fatalf("resumeResyncs() = %v", err)
	}
	p.committed = pos
	return p
}

// TestServicingResyncsDoesNotCostWhatTheTablesWaitingForTheStreamCost: at the end of an initial load
// nearly every table is read and waiting for the stream to reach its watermark — thousands of them.
// Asking each one on every transaction whether the stream has arrived parsed two GTID sets from text
// per table per transaction: with thousands of tables, hundreds of thousands of allocations and
// megabytes of garbage per transaction, and the service spent its time in the garbage collector while
// the lag grew. The queue is
// ordered the way the stream reaches the watermarks, so a pass looks at its head and nothing else.
func TestServicingResyncsDoesNotCostWhatTheTablesWaitingForTheStreamCost(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{}
	p := doneLoads(t, h, 2000)
	if len(p.awaiting) != 2000 {
		t.Fatalf("awaiting = %d, want the 2000 tables waiting for the stream", len(p.awaiting))
	}

	ctx := context.Background()
	if err := p.serviceResyncs(ctx); err != nil { // the first pass publishes the statuses once
		t.Fatal(err)
	}

	const passes = 50
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range passes {
		if err := p.serviceResyncs(ctx); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)

	// One parse of the committed set per pass and nothing per waiting table.
	const budget = 64 << 10
	if grew := after.TotalAlloc - before.TotalAlloc; grew > budget {
		t.Errorf("%d passes with %d tables waiting for the stream allocated %d bytes, want under %d: the hot path still asks every table",
			passes, len(p.awaiting), grew, budget)
	}
	if h.sink.swapped() != 0 {
		t.Errorf("%d tables were replaced although the stream never reached their watermark", h.sink.swapped())
	}
}

// TestTablesAreReplacedInTheOrderTheStreamReachesThem: the queue is only useful if it does not hold a
// table back. As the committed position advances, every table whose watermark it covers is replaced,
// in that order, and a saved state that comes back in name order (a restart) is put in the right order
// first.
func TestTablesAreReplacedInTheOrderTheStreamReachesThem(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{}
	const tables = 8
	p := doneLoads(t, h, tables)
	// resumeResyncs walks the saved entries in name order, which is not the order they were read in;
	// the queue must be sorted by how much of the source each watermark covers.
	for i, job := range p.awaiting {
		if want := fence(1000 + i); job.state.Final != want {
			t.Fatalf("the queue is in the wrong order: entry %d has %s, want %s", i, job.state.Final, want)
		}
	}

	ctx := context.Background()
	var swappedAt []int
	for reached := 1000; reached <= 1000+tables; reached++ {
		p.committed.GTIDSet = fence(reached)
		before := h.sink.swapped()
		if err := p.serviceResyncs(ctx); err != nil {
			t.Fatal(err)
		}
		for range h.sink.swapped() - before {
			swappedAt = append(swappedAt, reached)
		}
	}

	if len(swappedAt) != tables {
		t.Fatalf("%d tables were replaced of %d: %v", len(swappedAt), tables, swappedAt)
	}
	// The table read at 1000 is replaced when the stream reaches 1000, and so on in order.
	for i, at := range swappedAt {
		if want := 1000 + i; at != want {
			t.Errorf("the %d-th table was replaced when the stream reached %d, want %d", i, at, want)
		}
	}
	if len(p.awaiting) != 0 || len(p.resyncs) != 0 {
		t.Errorf("%d tables are still waiting and %d resyncs registered after every swap", len(p.awaiting), len(p.resyncs))
	}
}

// BenchmarkServiceResyncsWithTablesWaitingForTheStream measures a transaction boundary at the end of an
// initial load: thousands of tables read and waiting for the stream. It must not grow with them.
func BenchmarkServiceResyncsWithTablesWaitingForTheStream(b *testing.B) {
	for _, tables := range []int{100, 2000} {
		b.Run(fmt.Sprintf("waiting=%d", tables), func(b *testing.B) {
			h := newHarness(b)
			h.snapshot = &fakeSnapshot{}
			p := doneLoads(b, h, tables)
			ctx := context.Background()
			if err := p.serviceResyncs(ctx); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := p.serviceResyncs(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestTheInitialLoadReadsTheBiggestTablesFirst: the slots are few and the tables differ in size by
// orders of magnitude, so the order decides when the load ends. A table of a hundred million rows that
// starts last leaves every other table waiting for it alone; started first, it is read while hundreds
// of small ones come and go beside it.
func TestTheInitialLoadReadsTheBiggestTablesFirst(t *testing.T) {
	h := newHarness(t)
	// The catalog gives them in name order, as information_schema does, with sizes that disagree.
	h.snapshot = &fakeSnapshot{catalog: []schema.CatalogTable{
		{Database: "shop", Name: "a_small", EstimatedRows: 10},
		{Database: "shop", Name: "b_huge", EstimatedRows: 100_000_000},
		{Database: "shop", Name: "c_medium", EstimatedRows: 5_000},
	}}
	p := h.pipeline(t)

	entries, err := p.tablesToLoad(context.Background(), fence(0))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Table)
	}
	if want := []string{"shop.b_huge", "shop.c_medium", "shop.a_small"}; !reflect.DeepEqual(got, want) {
		t.Errorf("load order = %v, want %v", got, want)
	}
	for _, e := range entries {
		if e.Estimate == 0 {
			t.Errorf("%s was saved without the size that decides its place in the queue", e.Table)
		}
	}

	// Off, the order is the order of the catalog: an operator who sees the largest-first order do harm
	// on some source has a way back.
	no := false
	h.cfg.Replication.Snapshot.LargestFirst = &no
	entries, err = p.tablesToLoad(context.Background(), fence(0))
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	for _, e := range entries {
		got = append(got, e.Table)
	}
	if want := []string{"shop.a_small", "shop.b_huge", "shop.c_medium"}; !reflect.DeepEqual(got, want) {
		t.Errorf("load order with largest_first off = %v, want the order of the catalog %v", got, want)
	}
}

// TestASlotGoesToTheBiggestTableWaiting: the order has to survive a restart, where the saved entries
// come back in name order and the only thing that says how big each table is is what was saved with it.
func TestASlotGoesToTheBiggestTableWaiting(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{}
	h.cfg.Replication.Snapshot.Parallelism = 1
	entries := []checkpoint.Resync{
		{Table: "shop.a_small", Shadow: "a_small__resync", Since: fence(0), InitialLoad: true, Estimate: 10},
		{Table: "shop.b_huge", Shadow: "b_huge__resync", Since: fence(0), InitialLoad: true, Estimate: 100_000_000},
		{Table: "shop.c_medium", Shadow: "c_medium__resync", Since: fence(0), InitialLoad: true, Estimate: 5_000},
	}
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	p.q.setSince(fence(0))
	t.Cleanup(func() {
		for _, j := range p.resyncs {
			p.stopJob(j)
		}
	})
	ctx := context.Background()
	if err := p.resumeResyncs(ctx, checkpoint.Position{GTIDSet: fence(0), Resync: checkpoint.EncodeResyncs(entries)}); err != nil {
		t.Fatal(err)
	}
	if err := p.fillLoadSlots(ctx); err != nil {
		t.Fatal(err)
	}

	// The one slot went to the huge table, not to the first one by name.
	var reading []string
	for name := range p.reading {
		reading = append(reading, name)
	}
	if !reflect.DeepEqual(reading, []string{"shop.b_huge"}) {
		t.Errorf("reading = %v, want only shop.b_huge", reading)
	}
}
