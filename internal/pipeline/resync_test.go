package pipeline

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/gtid"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/snapshot"
)

// fakeSnapshot is the source as a resync or an initial load sees it: a table with rows, and the GTID
// set the source has executed when a chunk is read.
type fakeSnapshot struct {
	mu       sync.Mutex
	rows     [][]any // in key order
	executed string
	def      func() *schema.Table
	reads    int
	// catalog answers Catalog; nil means shop.items (the definition def or itemsTable() gives).
	catalog []schema.CatalogTable
	// defErr, if set, is what TableDefinition fails with instead of returning a definition: every
	// time, or only the first failDefs times when that is above 0.
	defErr   error
	failDefs int
	defs     int
	// readErr, if set, is what ReadChunk fails with: every time, or only the first failReads times
	// when that is above 0.
	readErr   error
	failReads int
	// limits records the size every chunk asked for, in order.
	limits []int
	// read, when set, is signalled after every ReadChunk, so a test can wait for one to have happened.
	read chan struct{}
	// hold, when set, keeps every ReadChunk after the first waiting until it is closed, so a test can
	// change something while the read runs without racing the reading goroutine to the end of the table.
	hold chan struct{}
	// partitions records the partition every chunk was read from, in order; "" for the whole table.
	partitions []string
	// byPartition holds the rows of each partition of a partitioned table; rows is then unused.
	byPartition map[string][][]any
}

// askedPartitions is the partition every chunk was read from, in order.
func (f *fakeSnapshot) askedPartitions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.partitions...)
}

// askedLimits is the size of every chunk read so far, in order.
func (f *fakeSnapshot) askedLimits() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.limits...)
}

func (f *fakeSnapshot) ExecutedGTIDSet(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executed, nil
}

func (f *fakeSnapshot) TableDefinition(_ context.Context, database, table string) (*schema.Table, error) {
	f.mu.Lock()
	f.defs++
	failing := f.defErr != nil && (f.failDefs == 0 || f.defs <= f.failDefs)
	f.mu.Unlock()
	if failing {
		return nil, f.defErr
	}
	if f.def != nil {
		return f.def(), nil
	}
	t := itemsTable()
	t.Database, t.Name = database, table
	return t, nil
}

func (f *fakeSnapshot) Catalog(context.Context, string, string) (*schema.Catalog, error) {
	if f.catalog != nil {
		return &schema.Catalog{Tables: f.catalog}, nil
	}
	return &schema.Catalog{Tables: []schema.CatalogTable{{Database: "shop", Name: "items"}}}, nil
}

func (f *fakeSnapshot) ReadChunk(ctx context.Context, req snapshot.ChunkRequest) ([][]any, int, error) {
	f.mu.Lock()
	hold, first := f.hold, f.reads == 0
	f.mu.Unlock()
	if hold != nil && !first {
		select {
		case <-hold:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
	after, limit := req.After, req.Limit
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	f.limits = append(f.limits, limit)
	f.partitions = append(f.partitions, req.Partition)
	if f.read != nil {
		select {
		case f.read <- struct{}{}:
		default:
		}
	}
	if f.readErr != nil && (f.failReads == 0 || f.reads <= f.failReads) {
		return nil, 0, f.readErr
	}
	rows := f.rows
	if req.Partition != "" {
		rows = f.byPartition[req.Partition]
	}
	var out [][]any
	for _, r := range rows {
		if after != nil && r[0].(int64) <= after[0].(int64) {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, r)
	}
	return out, len(out) * 32, nil
}

// asks queues a request for a resync when the stream reaches the event, the way the control channel does,
// without waiting for the answer (the stream goroutine that calls it is the one that answers).
func asks(p *Pipeline, reply chan error) func() { return asksFor(p, reply, itemsTable()) }

// asksFor is asks with the definition the resync reads with, for a table whose structure the test sets.
func asksFor(p *Pipeline, reply chan error, def *schema.Table) func() {
	return func() {
		p.rs.mu.Lock()
		p.rs.queue = append(p.rs.queue, resyncRequest{table: "shop.items", def: def, reply: reply})
		p.rs.mu.Unlock()
	}
}

func (s *fakeSink) swapped() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.swaps)
}

func live(t *testing.T, h *harness, table string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for k, v := range h.sink.liveRows(table) {
		out[k], _ = v[1].(string)
	}
	return out
}

func equal(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// TestResyncReplacesATableWhileTheStreamGoesOn: whether the snapshot was read before or after a change
// that the stream delivers after the resync began, the table ends up as the source has it, and a row that
// only the old table had is gone. The resync begins after transaction 2, so the snapshot cannot be older.
func TestResyncReplacesATableWhileTheStreamGoesOn(t *testing.T) {
	items := itemsTable()
	tests := map[string]struct {
		snapshotRows [][]any
		executed     string
	}{
		"snapshot read before the change of row 3 reaches the stream": {
			snapshotRows: [][]any{{int64(1), "a"}, {int64(3), "c"}, {int64(4), "d"}},
			executed:     fence(2),
		},
		"snapshot read after the change of row 3": {
			snapshotRows: [][]any{{int64(1), "a"}, {int64(3), "c2"}, {int64(4), "d"}},
			executed:     fence(3),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.snapshot = &fakeSnapshot{rows: tt.snapshotRows, executed: tt.executed}
			h.obs = &fakeObserver{}
			p := h.pipeline(t)
			reply := make(chan error, 1)
			h.source.script = []scriptedTx{
				// Row 2 was in the table and is not in the source any more: only a resync can remove it.
				uuidTx(1, insertRows(items, 1, "a"), insertRows(items, 2, "b"), insertRows(items, 3, "c")),
				uuidTx(2, insertRows(items, 4, "d")),
				uuidTx(3, updateRows(items, 3, "c", "c2")),
				uuidTx(4, deleteRows(items, 4, "d")),
				uuidTx(5, insertRows(items, 5, "e")),
			}
			h.source.beforeHandle = func(ev event.Event) {
				if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
					asks(p, reply)()
				}
			}
			h.source.until = func() bool { return h.sink.swapped() > 0 }
			// A row that the source does not have and no event of the stream removes: the old table
			// keeps it until the resync replaces the table.
			h.sink.tables["shop.items"] = map[string]cell{"9|": {version: 1, values: []any{int32(9), "stale"}}}

			if err := h.runPipeline(t, p); err != nil {
				t.Fatalf("Run() = %v", err)
			}

			if err := <-reply; err != nil {
				t.Fatalf("the resync was refused: %v", err)
			}
			want := map[string]string{"1|": "a", "3|": "c2", "5|": "e"}
			if got := live(t, h, "shop.items"); !equal(got, want) {
				t.Errorf("rows after the resync = %v, want %v", got, want)
			}
			if got := live(t, h, "shop.items__resync"); len(got) != 0 {
				t.Errorf("the shadow table still has rows %v", got)
			}
			if pos := h.store.current(); pos.Resync != "" {
				t.Errorf("the state of the resync is still saved: %q", pos.Resync)
			}
			if got := h.obs.(*fakeObserver).sequence("resync-finished"); len(got) != 1 || got[0] != "resync-finished shop.items true" {
				t.Errorf("finished = %v", got)
			}
		})
	}
}

// TestResyncTakesTheTableOutOfQuarantine: the resync is what brings a quarantined table back.
func TestResyncTakesTheTableOutOfQuarantine(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	// The source has all three rows; the table missed two of them while it was quarantined.
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}, executed: fence(3)}
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, insertRows(items, 3, "c")),
		uuidTx(4, insertRows(items, 4, "d")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			p.Quarantine("shop.items", errcode.TBL101, "test")
		}
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":3" {
			asks(p, reply)()
		}
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}
	if len(p.Quarantined()) != 0 {
		t.Errorf("still quarantined: %v", p.Quarantined())
	}
	// The changes that the quarantine dropped are in the table again, and the ones after the resync began replicate.
	want := map[string]string{"1|": "a", "2|": "b", "3|": "c", "4|": "d"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

// TestResyncContinuesFromTheSavedCursorAfterARestart: the state is saved with the position, and a new
// process reads on from the cursor into the same shadow table.
func TestResyncContinuesFromTheSavedCursorAfterARestart(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	snap := &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}, executed: fence(2)}
	h.snapshot = snap
	cursor, err := checkpoint.EncodeCursor([]any{int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Cursor: cursor, Rows: 2,
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	// What the first process wrote before it stopped.
	h.sink.tables["shop.items__resync"] = map[string]cell{
		"1|": {version: 1, values: []any{int64(1), "a"}}, "2|": {version: 1, values: []any{int64(2), "b"}},
	}
	h.sink.tables["shop.items"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "stale"}}}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 4, "d"))}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	snap.mu.Lock()
	defer snap.mu.Unlock()
	want := map[string]string{"1|": "a", "2|": "b", "3|": "c", "4|": "d"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

// TestASwapACrashInterruptedIsFinishedBeforeTheStream: a crash after the exchange of the swap leaves the
// resync saved, and the old table under the name of the shadow table. The swap is finished at the
// restart, before the stream: a change of the table written under that name would go with the old table.
func TestASwapACrashInterruptedIsFinishedBeforeTheStream(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(1),
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.marked = map[string]bool{"shop.items": true}
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	h.sink.tables["shop.items"] = map[string]cell{"1|": {version: 1, values: []any{int64(1), "a"}}}
	h.sink.tables["shop.items__resync"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "old"}}}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if n := h.sink.swapped(); n != 1 {
		t.Errorf("the swap was finished %d times, want once: %v", n, h.journal.all())
	}
	want := map[string]string{"1|": "a", "2|": "b"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
	if pos := h.store.current(); pos.Resync != "" {
		t.Errorf("the resync is still saved: %s", pos.Resync)
	}
}

// TestResyncFinishesOnlyWhenTheStreamReachedThePointItWasReadAt: with the last chunk read at u:1-9, the
// swap waits until the stream has committed up to it.
func TestResyncFinishesOnlyWhenTheStreamReachedThePointItWasReadAt(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(4)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, insertRows(items, 3, "c")),
		uuidTx(4, insertRows(items, 4, "d")),
		uuidTx(5, insertRows(items, 5, "e")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			asks(p, reply)()
		}
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }
	// What the stream had reached when the table was replaced. The committed position is the measure,
	// not the saved one: the position is written at most once per flush interval, so a save may still
	// be owed at this moment (see positionDue).
	reached := ""
	h.sink.onSwap = func(chsink.SwapRequest) { reached = p.committed.GTIDSet }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if reached == "" {
		t.Fatalf("the table was never replaced: %v", h.journal.all())
	}
	if ok, err := gtid.Subset(fence(4), reached); err != nil || !ok {
		t.Errorf("the table was replaced when the stream had reached %s, before %s (%v)", reached, fence(4), err)
	}
}

// TestADamagedWatermarkGivesTheResyncUp: a saved Final that does not parse cannot say when the
// shadow table is complete, so it is never swapped in; the resync is given up as on a failed read.
func TestADamagedWatermarkGivesTheResyncUp(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{executed: fence(2)}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Rows: 1,
		Done: true, Final: "not a GTID set",
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	h.sink.tables["shop.items__resync"] = map[string]cell{"1|": {version: 1, values: []any{int64(1), "a"}}}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if n := h.sink.swapped(); n != 0 {
		t.Errorf("the shadow table was swapped in %d times, want never: %v", n, h.journal.all())
	}
	if got := quarantinedNames(h.store.current()); len(got) != 1 || got[0] != "shop.items" {
		t.Errorf("quarantined = %v, want shop.items: its changes since the resync began are lost", got)
	}
	if len(h.sink.drops) != 1 || h.sink.drops[0].Table != "items__resync" {
		t.Errorf("drops = %v, want the shadow table dropped", h.sink.drops)
	}
}

// TestDDLDuringAResyncDiscardsItAndBeginsAgain: what was read is in the old structure.
func TestDDLDuringAResyncDiscardsItAndBeginsAgain(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(3)}
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, event.DDL{Database: "shop", Query: "ALTER TABLE items ADD COLUMN extra INT NULL", Time: epoch}),
		uuidTx(4, insertRows(items, 4, "d")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			asks(p, reply)()
		}
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	obs := h.obs.(*fakeObserver)
	got := obs.sequence("resync-finished")
	if len(got) < 2 || got[0] != "resync-finished shop.items false" || got[len(got)-1] != "resync-finished shop.items true" {
		t.Errorf("finished = %v, want the first resync discarded and the second finished", got)
	}
	dropped := 0
	for _, e := range h.journal.all() {
		if e == "drop shop.items__resync" {
			dropped++
		}
	}
	if dropped != 1 {
		t.Errorf("shadow tables dropped = %d, want 1", dropped)
	}
}

func TestResyncIsRefusedWhenItCannotBegin(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{}
	p := h.pipeline(t)

	if err := p.StartResync(context.Background(), "shop.items"); !errors.Is(err, ErrNotRunning) {
		t.Errorf("while not running: %v, want ErrNotRunning", err)
	}
	p.running.Store(true)
	if err := p.StartResync(context.Background(), "other.items"); err == nil {
		t.Error("a table outside tables.include was accepted")
	}
	h2 := newHarness(t)
	if err := h2.pipeline(t).StartResync(context.Background(), "shop.items"); err == nil {
		t.Error("a service without a source to read from accepted a resync")
	}
}

// TestResyncDoesNotReuseAShadowTableItDidNotMakeThisTime: a table left by an earlier resync is the
// operator's to remove; binwave does not drop or reuse it.
func TestResyncDoesNotReuseALeftoverShadowTable(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a")), uuidTx(2, insertRows(items, 2, "b"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			asks(p, reply)()
		}
	}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	err := <-reply
	if !chsink.IsConflict(err) {
		t.Fatalf("the request was answered with %v, want a conflict the operator settles", err)
	}
	if len(h.sink.drops) != 0 || len(h.sink.swaps) != 0 {
		t.Errorf("binwave dropped %v or swapped %v", h.sink.drops, h.sink.swaps)
	}
}

// TestResyncIsRefusedWithoutTheRightsItNeeds: found out at the request, before anything is created.
func TestResyncIsRefusedWithoutTheRightsItNeeds(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	h.sink.accessErr = &chsink.AccessError{Database: "shop", Privilege: "DROP TABLE"}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a")), uuidTx(2, insertRows(items, 2, "b"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			asks(p, reply)()
		}
	}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v: a refused request must not stop replication", err)
	}

	err := <-reply
	var access *chsink.AccessError
	if !errors.As(err, &access) || access.Privilege != "DROP TABLE" {
		t.Fatalf("the request was answered with %v, want the missing privilege", err)
	}
	if len(h.sink.swaps) != 0 || h.store.current().Resync != "" {
		t.Errorf("something was started: swaps %v, state %q", h.sink.swaps, h.store.current().Resync)
	}
}

// TestResyncGivesUpWhenAChunkCannotBeConverted: the chunks are converted by the reading goroutine,
// not by the pipeline (conversion is the expensive half and the pipeline serves the whole stream).
// A conversion that fails there must reach the pipeline as such: the resync gives up and the table
// goes to quarantine, instead of the rows quietly never arriving.
func TestResyncGivesUpWhenAChunkCannotBeConverted(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	// NULL in the NOT NULL id column: the shadow table cannot take this row.
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {nil, "b"}}, executed: fence(2)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, insertRows(items, 3, "c")),
		uuidTx(4, insertRows(items, 4, "d")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			asks(p, reply)()
		}
	}
	h.source.until = func() bool { return len(p.Quarantined()) > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	q := p.Quarantined()
	if len(q) != 1 || q[0].Table != "shop.items" {
		t.Fatalf("quarantined = %+v, want shop.items", q)
	}
	if h.sink.swapped() != 0 {
		t.Error("the table was replaced with a shadow table the resync could not fill")
	}
	if pos := h.store.current(); pos.Resync != "" {
		t.Errorf("the state of the resync that gave up is still saved: %q", pos.Resync)
	}
}

// TestTheConfiguredChunkSizeIsWhatTheSourceIsAskedFor: replication.snapshot.chunk_size and
// adaptive_chunks: false mean every chunk asks the source for exactly that many rows.
func TestTheConfiguredChunkSizeIsWhatTheSourceIsAskedFor(t *testing.T) {
	items := itemsTable()
	var rows [][]any
	for i := 1; i <= 7; i++ {
		rows = append(rows, []any{int64(i), "x"})
	}
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: rows, executed: fence(1)}
	no := false
	h.cfg.Replication.Snapshot.ChunkSize = 3
	h.cfg.Replication.Snapshot.AdaptiveChunks = &no
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "x"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asks(p, reply)()
		}
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	for i, limit := range h.snapshot.(*fakeSnapshot).askedLimits() {
		if limit != 3 {
			t.Errorf("chunk %d asked for %d rows, want the configured 3", i, limit)
		}
	}
}

// TestSetChunkSizeChangesWhatTheSourceIsAskedForWhileItReads: `binwave replication chunk-size` takes
// effect on the next chunk of a read that is already running, and says so when nothing is running.
func TestSetChunkSizeChangesWhatTheSourceIsAskedForWhileItReads(t *testing.T) {
	items := itemsTable()
	var rows [][]any
	for i := 1; i <= 12; i++ {
		rows = append(rows, []any{int64(i), "x"})
	}
	h := newHarness(t)
	read := make(chan struct{}, 1)
	// The rest of the table is read only once the size was changed: the reading goroutine is faster
	// than the pipeline that takes its chunks, and could otherwise read all of it at the old size.
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(release)
	h.snapshot = &fakeSnapshot{rows: rows, executed: fence(2), read: read, hold: hold}
	h.cfg.Replication.Snapshot.ChunkSize = 2
	no := false
	h.cfg.Replication.Snapshot.AdaptiveChunks = &no
	p := h.pipeline(t)

	if err := p.SetChunkSize(1000); !errors.Is(err, ErrNotRunning) {
		t.Errorf("SetChunkSize() before the pipeline runs = %v, want ErrNotRunning", err)
	}

	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "x")),
		uuidTx(2, insertRows(items, 2, "x")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		b, ok := ev.(event.Begin)
		if !ok {
			return
		}
		switch b.GTID {
		case testUUID + ":1":
			asks(p, reply)()
		case testUUID + ":2":
			// At least one chunk must have been read at the configured size before the size changes,
			// or the test proves nothing about the change taking effect on a read already running.
			select {
			case <-read:
			case <-time.After(5 * time.Second):
				t.Error("no chunk was read before the size was changed")
			}
			if err := p.SetChunkSize(6); err != nil {
				t.Errorf("SetChunkSize(6) = %v", err)
			}
			release()
		}
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	limits := h.snapshot.(*fakeSnapshot).askedLimits()
	if len(limits) < 2 || limits[0] != 2 {
		t.Fatalf("chunks asked for %v, want the configured 2 first", limits)
	}
	if limits[len(limits)-1] != 6 {
		t.Errorf("chunks asked for %v, want 6 once it was asked for while the read ran", limits)
	}
}

// TestAPartitionedTableIsResyncedPartitionByPartition: a partitioned table is read one partition at a
// time, every partition once and nothing from the table as a whole, and the rows of every partition end
// up in ClickHouse.
func TestAPartitionedTableIsResyncedPartitionByPartition(t *testing.T) {
	items := itemsTable()
	partitionedItems := itemsTable()
	partitionedItems.Partitions = []string{"p1", "p2", "p3"}
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{
		executed: fence(1),
		def:      func() *schema.Table { return partitionedItems },
		byPartition: map[string][][]any{
			"p1": {{int64(1), "a"}, {int64(2), "b"}},
			"p2": {},
			"p3": {{int64(3), "c"}},
		},
	}
	h.cfg.Replication.Snapshot.ChunkSize = 2
	no := false
	h.cfg.Replication.Snapshot.AdaptiveChunks = &no
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asksFor(p, reply, partitionedItems)()
		}
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	got := h.snapshot.(*fakeSnapshot).askedPartitions()
	seen := map[string]bool{}
	for _, part := range got {
		if part == "" {
			t.Fatalf("a chunk was read from the whole table: %q", got)
		}
		seen[part] = true
	}
	for _, want := range partitionedItems.Partitions {
		if !seen[want] {
			t.Errorf("partition %s was never read: %q", want, got)
		}
	}
	// The rows of every partition are in the table the resync replaced.
	want := map[string]string{"1|": "a", "2|": "b", "3|": "c"}
	if live := live(t, h, "shop.items"); !equal(live, want) {
		t.Errorf("rows after the resync = %v, want %v", live, want)
	}
}

// TestTheSavedStateOfAResyncCarriesItsScope: what is saved with the position must be enough to go on
// with — the strategy, and the partition the cursor belongs to. A cursor without its partition would be
// continued in the wrong key space after a restart.
func TestTheSavedStateOfAResyncCarriesItsScope(t *testing.T) {
	items := itemsTable()
	partitionedItems := itemsTable()
	partitionedItems.Partitions = []string{"p1", "p2"}
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{
		executed: fence(1),
		def:      func() *schema.Table { return partitionedItems },
		byPartition: map[string][][]any{
			"p1": {{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}},
			"p2": {{int64(4), "d"}},
		},
	}
	h.cfg.Replication.Snapshot.ChunkSize = 2
	no := false
	h.cfg.Replication.Snapshot.AdaptiveChunks = &no
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asksFor(p, reply, partitionedItems)()
		}
	}
	var saved []checkpoint.Resync
	h.store.onSave = func(pos checkpoint.Position) {
		entries, err := checkpoint.DecodeResyncs(pos.Resync)
		if err != nil {
			t.Error(err)
			return
		}
		saved = append(saved, entries...)
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	if len(saved) == 0 {
		t.Fatal("the state of the resync was never saved")
	}
	for _, st := range saved {
		if st.Strategy != string(snapshot.StrategyPartitioned) {
			t.Errorf("saved strategy %q, want %q", st.Strategy, snapshot.StrategyPartitioned)
		}
		if st.Cursor != nil && st.Partition == "" {
			t.Errorf("a cursor was saved with no partition: %+v", st)
		}
	}
}

// TestAFinishedResyncLeavesTheStatusesAtOnce: the progress of a reading table is published at most once
// a second, but a resync that appeared or ended is not progress — it is the answer to "what is being
// resynchronized". Under the throttle alone a finished table stayed in /status and in
// binwave_resyncing_tables until something else happened, and on a quiet source that is seconds.
func TestAFinishedResyncLeavesTheStatusesAtOnce(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(2)}
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	// Two transactions: the resync begins and is published during the first, which arms the throttle,
	// and the swap happens during the second, when the throttle would hold the change back.
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asks(p, reply)()
		}
	}
	// The clock stands still, so nothing is published because a second has passed. until runs on the
	// stream's own goroutine, between events, so it sees the statuses as they are right after the pass
	// that swapped the table — the pipeline has not stopped yet, and its final publish has not run.
	var left []snapshot.Status
	h.source.until = func() bool {
		if h.sink.swapped() == 0 {
			return false
		}
		left = p.Resyncs()
		return true
	}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}
	if len(left) != 0 {
		t.Errorf("the table was still reported as resynchronizing after it was replaced: %+v", left)
	}
}

// TestThePositionIsSavedAsSoonAsATableIsReplaced: the rows do not need the position saved at once (they
// were written before the swap), but the entry of a finished resync does. A restart that still finds the
// entry with the shadow table gone cannot tell a finished swap from a shadow table someone dropped, so
// it reads the whole table again — which on a big table is minutes of work for nothing.
func TestThePositionIsSavedAsSoonAsATableIsReplaced(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asks(p, reply)()
		}
	}
	// The clock stands still, so only a save that does not wait for the flush interval can happen. At
	// the moment of the swap the saved state still has the resync; until runs on the stream's own
	// goroutine, right after the pass that swapped, and by then it must be gone — and the pipeline has
	// not stopped yet, so its final save has not run.
	var atSwap, afterPass checkpoint.Position
	h.sink.onSwap = func(chsink.SwapRequest) { atSwap = h.store.current() }
	h.source.until = func() bool {
		if h.sink.swapped() == 0 {
			return false
		}
		afterPass = h.store.current()
		return true
	}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	if before, err := checkpoint.DecodeResyncs(atSwap.Resync); err != nil || len(before) == 0 {
		t.Fatalf("the state at the moment of the swap had no resync (%v): the test proves nothing", err)
	}
	entries, err := checkpoint.DecodeResyncs(afterPass.Resync)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("the finished resync was still in the saved state after the swap: %+v", entries)
	}
}

// TestTheExchangeComesAfterThePositionHasTheTableReadInFull: the last chunk is taken in the same pass
// as the swap. A crash after the exchange must not find the entry still reading: the restart would write
// the changes of the table under the name of the shadow table, which then holds the old table.
func TestTheExchangeComesAfterThePositionHasTheTableReadInFull(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asks(p, reply)()
		}
	}
	// The clock stands still: no save comes from the flush interval between the read and the swap.
	var atSwap checkpoint.Position
	h.sink.onSwap = func(chsink.SwapRequest) { atSwap = h.store.current() }
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	entries, err := checkpoint.DecodeResyncs(atSwap.Resync)
	if err != nil || len(entries) != 1 {
		t.Fatalf("the state saved at the moment of the swap = %q (%v), want the resync", atSwap.Resync, err)
	}
	if st := entries[0]; !st.Done || st.Final != fence(1) {
		t.Errorf("the saved resync at the moment of the swap = %+v, want it read in full up to %s", st, fence(1))
	}
	saved, err := gtid.Parse(atSwap.GTIDSet)
	if err != nil {
		t.Fatal(err)
	}
	final, err := gtid.Parse(fence(1))
	if err != nil {
		t.Fatal(err)
	}
	if !saved.Covers(final) {
		t.Errorf("the position saved at the moment of the swap is %s, before the watermark %s", atSwap.GTIDSet, fence(1))
	}
}

// TestEverySavedPositionOfARestartKeepsTheResyncsLeft: the resyncs of a position are resumed one by one,
// and the first finishes a swap a crash interrupted while the next begins again, which saves the position.
// A position saved on the way must not have only the entries resumed so far: a crash then would forget the
// rest, and with them the rows of the tables their initial load had still to read.
func TestEverySavedPositionOfARestartKeepsTheResyncsLeft(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	// The snapshot is taken past where the stream goes, so neither table left is swapped in this run.
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(9)}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{
		{Table: "shop.a", Shadow: "a__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(1)},
		{Table: "shop.b", Shadow: "b__resync", Version: 1, Since: fence(1), Pending: true},
		{Table: "shop.c", Shadow: "c__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(9)},
	})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.marked = map[string]bool{"shop.a": true}
	h.sink.shapes = map[string]chsink.TableShape{"shop.a__resync": {Exists: true}, "shop.c__resync": {Exists: true}}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if n := h.sink.swapped(); n != 1 {
		t.Errorf("swaps = %d, want the one of shop.a", n)
	}
	saves := h.store.all()
	if len(saves) == 0 {
		t.Fatal("no position was saved: the test proves nothing")
	}
	for i, pos := range saves {
		entries, err := checkpoint.DecodeResyncs(pos.Resync)
		if err != nil {
			t.Fatal(err)
		}
		left := map[string]bool{}
		for _, e := range entries {
			left[e.Table] = true
		}
		if !left["shop.b"] || !left["shop.c"] {
			t.Errorf("save %d of %d has the resyncs %v, want shop.b and shop.c among them", i+1, len(saves), entries)
		}
	}
	if entries, _ := checkpoint.DecodeResyncs(saves[len(saves)-1].Resync); len(entries) != 2 {
		t.Errorf("the last saved resyncs = %+v, want shop.b and shop.c", entries)
	}
}

// TestAResyncWhoseShadowTableIsLostReadsEveryPartitionAgain: what a restart finds read into a shadow
// table that is gone (dropped, or made again for a changed structure) is read again, every partition of
// it: the reader goes on from a saved partition even without a cursor, and the partitions before it would
// be missing from the table the swap puts in place of the full one. A table read in full whose shadow
// table is gone, the table without the marker, was never swapped in as far as anything can tell: it is
// read again too, not taken for swapped. And a second restart, right after the shadow table is made
// again, still reads everything: the position says so before the table is made.
func TestAResyncWhoseShadowTableIsLostReadsEveryPartitionAgain(t *testing.T) {
	cursor, err := checkpoint.EncodeCursor([]any{int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	mismatch := &chsink.SchemaMismatchError{Table: "`replica`.`items__resync`", Problems: []string{"column name is missing"}}
	reading := checkpoint.Resync{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1),
		Strategy: "partitioned", Partition: "p2", Cursor: cursor, Rows: 3,
	}
	atBoundary := reading
	atBoundary.Cursor = nil
	readInFull := reading
	readInFull.Cursor, readInFull.Partition, readInFull.Rows, readInFull.Done = nil, "", 4, true
	notReached, reached := readInFull, readInFull
	notReached.Final, reached.Final = fence(5), fence(1)
	tests := map[string]struct {
		entry  checkpoint.Resync
		shadow bool // the shadow table is there, and does not match the structure
	}{
		"gone inside a partition":                 {entry: reading},
		"gone at the start of a partition":        {entry: atBoundary},
		"made again for a new structure":          {entry: reading, shadow: true},
		"read in full, gone before its watermark": {entry: notReached},
		"read in full, gone after its watermark":  {entry: reached},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			items := itemsTable()
			partitioned := itemsTable()
			partitioned.Partitions = []string{"p1", "p2"}
			setup := func(pos checkpoint.Position) *harness {
				h := newHarness(t)
				h.snapshot = &fakeSnapshot{
					executed: fence(1),
					def:      func() *schema.Table { return partitioned },
					byPartition: map[string][][]any{
						"p1": {{int64(1), "a"}, {int64(2), "b"}},
						"p2": {{int64(3), "c"}, {int64(4), "d"}},
					},
				}
				h.store.pos, h.store.found = pos, true
				// What the table held before: a row the source does not have, and none of what it does.
				h.sink.tables["shop.items"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "stale"}}}
				h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 5, "e"))}
				h.source.until = func() bool { return h.sink.swapped() > 0 }
				return h
			}
			check := func(h *harness, when string) {
				t.Helper()
				want := map[string]string{"1|": "a", "2|": "b", "3|": "c", "4|": "d", "5|": "e"}
				if got := live(t, h, "shop.items"); !equal(got, want) {
					t.Errorf("%s: rows = %v, want %v", when, got, want)
				}
			}

			h := setup(checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: checkpoint.EncodeResyncs([]checkpoint.Resync{tt.entry})})
			if tt.shadow {
				h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
				h.sink.ensureErrs = map[int]error{1: mismatch}
			}
			var atEnsure checkpoint.Position
			h.sink.onEnsure = func(t *chsink.Table) {
				if t.Name == "items__resync" {
					atEnsure = h.store.current()
				}
			}
			if err := h.run(t); err != nil {
				t.Fatalf("Run() = %v", err)
			}
			check(h, "after the restart")

			// A crash right after the shadow table was made: it is there, empty, with the right structure.
			again := setup(atEnsure)
			again.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
			if err := again.run(t); err != nil {
				t.Fatalf("Run() after a second restart = %v", err)
			}
			check(again, "after a second restart right after the shadow table was made")
		})
	}
}

// TestAResyncBegunAgainAfterACrashDropsItsOwnShadowTable: a resync saved as pending begins again at the
// restart and makes its shadow table; a crash before a position says so leaves that table, and the next
// restart begins again from the same entry. The table is binwave's and holds nothing that is kept: found
// as a leftover it would stop every restart that follows.
func TestAResyncBegunAgainAfterACrashDropsItsOwnShadowTable(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Pending: true,
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	h.sink.marked = map[string]bool{"shop.items__resync": true}
	h.sink.tables["shop.items__resync"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "read before"}}}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	want := map[string]string{"1|": "a", "2|": "b"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

// TestWithDryRunARestartChangesNothingOfTheResyncs: ddl.dry_run runs nothing in ClickHouse, and every
// step a resync has left after a restart does: a resync that begins again drops its own shadow table, a
// swap a crash interrupted drops the old table and removes the marker, one that reads again makes its
// shadow table. Replication stops before any of it, with the saved resyncs as they were.
func TestWithDryRunARestartChangesNothingOfTheResyncs(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.DryRun = true
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{
		{Table: "shop.a", Shadow: "a__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(1)},
		{Table: "shop.b", Shadow: "b__resync", Version: 1, Since: fence(1), Pending: true},
		{Table: "shop.c", Shadow: "c__resync", Version: 1, Since: fence(1)},
	})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.marked = map[string]bool{"shop.a": true, "shop.b__resync": true}
	h.sink.shapes = map[string]chsink.TableShape{"shop.b__resync": {Exists: true}}

	resyncFailure(t, h.run(t), "shop.a")

	if n := h.sink.swapped(); n != 0 || len(h.sink.drops) != 0 || len(h.sink.ensures) != 0 {
		t.Errorf("swaps = %d, drops = %v, ensures = %v: dry_run must change nothing", n, h.sink.drops, h.sink.ensures)
	}
	if got := h.store.current().Resync; got != state {
		t.Errorf("the saved resyncs = %s, want them as they were: %s", got, state)
	}
}

// TestAShadowTableAWriteOfWhichFailedIsNotSwappedIn: the last insert into the shadow table fails in the
// pass that would swap it in, the table is quarantined and its rows dropped. The shadow table does not
// hold everything: the table is left as it was, and the resync given up.
func TestAShadowTableAWriteOfWhichFailedIsNotSwappedIn(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asks(p, reply)()
		}
	}
	h.sink.tables["shop.items"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "old"}}}
	h.sink.insertErrsByTarget = map[string]error{"shop.items__resync": &chsink.TableError{Table: "shop.items", Err: errors.New("the insert was refused")}}
	h.source.until = func() bool { return p.q.isQuarantined("shop.items") && len(p.resyncs) == 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	if n := h.sink.swapped(); n != 0 {
		t.Errorf("the shadow table was swapped in %d times: %v", n, h.journal.all())
	}
	// Row 1 was written before the resync began (it begins at the end of transaction 1).
	if got, want := live(t, h, "shop.items"), map[string]string{"1|": "a", "9|": "old"}; !equal(got, want) {
		t.Errorf("rows = %v, want the table as it was: %v", got, want)
	}
	if pos := h.store.current(); pos.Resync != "" || !slices.Contains(quarantinedNames(pos), "shop.items") {
		t.Errorf("saved resyncs %q, quarantine %v: want the resync given up and the table quarantined", pos.Resync, quarantinedNames(pos))
	}
}

// TestAShadowTableLostBeforeTheSwapIsNotTakenForASwap: the shadow table is read in full, and dropped
// while the stream catches up with its watermark. The swap finds nothing to put in place: the table is
// left as it was and quarantined, not reported as resynchronized.
func TestAShadowTableLostBeforeTheSwapIsNotTakenForASwap(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(3)}
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")), uuidTx(2, insertRows(items, 2, "b")), uuidTx(3, insertRows(items, 3, "c")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		b, ok := ev.(event.Begin)
		switch {
		case ok && b.GTID == testUUID+":1":
			asks(p, reply)()
		case ok && b.GTID == testUUID+":3":
			h.sink.mu.Lock()
			h.sink.lost = map[string]bool{"shop.items__resync": true}
			h.sink.mu.Unlock()
		}
	}
	h.sink.tables["shop.items"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "old"}}}
	h.source.until = func() bool { return p.q.isQuarantined("shop.items") && len(p.resyncs) == 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	if n := h.sink.swapped(); n != 0 {
		t.Errorf("a swap was taken for made %d times: %v", n, h.journal.all())
	}
	if got := h.obs.(*fakeObserver).sequence("resync-finished"); len(got) != 1 || got[0] != "resync-finished shop.items false" {
		t.Errorf("finished = %v, want the resync reported as given up", got)
	}
	// Row 1 was written before the resync began (it begins at the end of transaction 1).
	if got, want := live(t, h, "shop.items"), map[string]string{"1|": "a", "9|": "old"}; !equal(got, want) {
		t.Errorf("rows = %v, want the table as it was: %v", got, want)
	}
}

// TestACleanupThatFailsAfterTheExchangeDoesNotGiveTheResyncUp: the exchange was sent, and what follows
// it fails as a problem of the table. Giving the resync up would drop the old table under the name of
// the shadow table and leave the marker on the table, where the next resync of it takes the marker for
// its own swap: replication stops instead, with the resync saved, and resuming finishes the swap.
func TestACleanupThatFailsAfterTheExchangeDoesNotGiveTheResyncUp(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asks(p, reply)()
		}
	}
	h.sink.swapFails = &swapFailure{sent: true, err: &chsink.TableError{Table: "shop.items", Err: errors.New("the old table could not be dropped")}}
	h.source.until = heartbeatsFor(100)

	resyncFailure(t, h.runPipeline(t, p), "shop.items")
	if len(h.sink.drops) != 0 {
		t.Errorf("drops = %v, want none", h.sink.drops)
	}
	if _, ok := savedResync(t, h); !ok {
		t.Error("the resync is not saved: resuming could not finish the swap")
	}
}

// TestAResyncDoesNotBeginOnATableThatCarriesTheMarker: the marker on the table says a swap of it was not
// finished; the swap of a new resync would take it for its own and drop the shadow table it filled.
func TestAResyncDoesNotBeginOnATableThatCarriesTheMarker(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a"))}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":1" {
			asks(p, reply)()
		}
	}
	h.sink.marked = map[string]bool{"shop.items": true}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; !chsink.IsConflict(err) {
		t.Errorf("the resync answered %v, want a refusal for the marker", err)
	}
	if len(h.sink.ensures) != 1 { // the table itself, not a shadow table
		t.Errorf("ensures = %v, want no shadow table made", h.sink.ensures)
	}
}

// TestARestartGivesUpAResyncWhoseTableWasQuarantinedDuringIt: a write of the table failed during its
// resync, and a position was saved with the table quarantined before the resync was given up. Its shadow
// table may miss the rows dropped with the quarantine: the restart gives the resync up, not swaps it in.
func TestARestartGivesUpAResyncWhoseTableWasQuarantinedDuringIt(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(1),
	}})
	quarantine := checkpoint.EncodeQuarantine([]checkpoint.Quarantined{{Table: "shop.items", Since: fence(1), Reason: "the insert was refused", At: epoch}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state, Quarantine: quarantine}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	h.sink.tables["shop.items__resync"] = map[string]cell{"1|": {version: 1, values: []any{int64(1), "a"}}}
	h.sink.tables["shop.items"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "old"}}}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if n := h.sink.swapped(); n != 0 {
		t.Errorf("the shadow table was swapped in %d times", n)
	}
	if got, want := live(t, h, "shop.items"), map[string]string{"9|": "old"}; !equal(got, want) {
		t.Errorf("rows = %v, want the table as it was: %v", got, want)
	}
	if pos := h.store.current(); pos.Resync != "" || !slices.Contains(quarantinedNames(pos), "shop.items") {
		t.Errorf("saved resyncs %q, quarantine %v: want the resync given up and the table still quarantined", pos.Resync, quarantinedNames(pos))
	}
}

// TestARestartThatGivesAResyncUpSavesItGone: a restart finds the shadow table gone and saves that the
// table is read again, then cannot read the definition of the table and quarantines it. The position
// must not keep the resync next to the quarantine: the next restart would try it again.
func TestARestartThatGivesAResyncUpSavesItGone(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1), defErr: errors.New("the table is not there")}
	cursor, err := checkpoint.EncodeCursor([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Cursor: cursor, Rows: 1,
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if pos := h.store.current(); pos.Resync != "" || !slices.Contains(quarantinedNames(pos), "shop.items") {
		t.Errorf("saved resyncs %q, quarantine %v: want the resync gone and the table quarantined", pos.Resync, quarantinedNames(pos))
	}
}

// TestAResyncDiscardedByAStatementIsSavedUntilItBeginsAgain: a statement discards a resync, which begins
// again at the end of it. The flush it begins with saves the position: that position must have the
// resync, as one that begins again, or a crash before the next save forgets it, and with it the rows of
// the table the source had before.
func TestAResyncDiscardedByAStatementIsSavedUntilItBeginsAgain(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(3)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, event.DDL{Database: "shop", Query: "ALTER TABLE items ADD COLUMN extra INT NULL", Time: epoch}),
		uuidTx(4, insertRows(items, 4, "d")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			asks(p, reply)()
		}
	}
	var shadows int
	var atSecond checkpoint.Position
	h.sink.onEnsure = func(t *chsink.Table) {
		if t.Name == "items__resync" {
			if shadows++; shadows == 2 {
				atSecond = h.store.current()
			}
		}
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if shadows < 2 {
		t.Fatalf("the shadow table was made %d times, want again after the statement", shadows)
	}
	entries, err := checkpoint.DecodeResyncs(atSecond.Resync)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Table != "shop.items" || !entries[0].Pending {
		t.Errorf("the position saved when the resync began again has %+v, want shop.items to begin again", entries)
	}
}

// TestAShadowTableLostBeforeItsFirstRowIsNotMadeAgain: the shadow table is gone before the stream
// writes a row of the table to it. Binding the table must not make it again: an empty one would be
// swapped in later without what the lost one had. The table is quarantined, the resync given up.
func TestAShadowTableLostBeforeItsFirstRowIsNotMadeAgain(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(3)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")), uuidTx(2, insertRows(items, 2, "b")), uuidTx(3, insertRows(items, 3, "c")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		b, ok := ev.(event.Begin)
		switch {
		case ok && b.GTID == testUUID+":1":
			asks(p, reply)()
		case ok && b.GTID == testUUID+":2":
			h.sink.mu.Lock()
			// Dropped by hand: a table made again under the name would carry the marker, and be swapped in.
			delete(h.sink.shapes, "shop.items__resync")
			delete(h.sink.tables, "shop.items__resync")
			h.sink.mu.Unlock()
		}
	}
	h.sink.tables["shop.items"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "old"}}}
	h.source.until = func() bool { return p.q.isQuarantined("shop.items") && len(p.resyncs) == 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	if n := h.sink.swapped(); n != 0 {
		t.Errorf("swapped in %d times: %v", n, h.journal.all())
	}
	if _, there := h.sink.shapes["shop.items__resync"]; there {
		t.Errorf("the lost shadow table was made again: %v", h.journal.all())
	}
	// Row 1 was written before the resync began (it begins at the end of transaction 1).
	if got, want := live(t, h, "shop.items"), map[string]string{"1|": "a", "9|": "old"}; !equal(got, want) {
		t.Errorf("rows = %v, want the table as it was: %v", got, want)
	}
}

// TestARestartDoesNotTouchATableThatTookTheNameOfTheShadowTable: while the service was down, a table
// binwave did not make took the name of the shadow table. Nothing of it is written to, swapped in or
// dropped: replication stops on the resync, which stays saved.
func TestARestartDoesNotTouchATableThatTookTheNameOfTheShadowTable(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(1),
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.foreign = map[string]bool{"shop.items__resync": true}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}

	err := h.run(t)
	var failed *FailedError
	if !errors.As(err, &failed) || failed.Failure.Code != string(errcode.SYNC104) {
		t.Fatalf("Run() = %v, want replication failed with %s", err, errcode.SYNC104)
	}
	if n := h.sink.swapped(); n != 0 || len(h.sink.drops) != 0 || len(h.sink.ensures) != 0 {
		t.Errorf("swaps = %d, drops = %v, ensures = %v: the table must not be touched", n, h.sink.drops, h.sink.ensures)
	}
	if _, ok := savedResync(t, h); !ok {
		t.Error("the resync is not saved")
	}
}

// TestWithQuarantineAPendingResyncThatCannotBeginAgainQuarantinesTheTable: a resync that has to begin
// again after a statement, and cannot, quarantines its table with tables.on_replication_error:
// quarantine, whether that happens right after the statement or after a restart.
func TestWithQuarantineAPendingResyncThatCannotBeginAgainQuarantinesTheTable(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1), defErr: errors.New("the source does not answer for the table")}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Pending: true,
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v, want replication to go on with the table quarantined", err)
	}
	if pos := h.store.current(); pos.Resync != "" || !slices.Contains(quarantinedNames(pos), "shop.items") {
		t.Errorf("saved resyncs %q, quarantine %v: want the resync gone and the table quarantined", pos.Resync, quarantinedNames(pos))
	}
}

// TestTheInitialLoadDoesNotTakeWhatAnEarlierResyncLeft: at the very first start, a table under the name
// of a shadow table, or the marker of an unfinished swap on a table, is left from before. The load would
// reuse the one (its rows beating the snapshot) or take the other for its own swap (a table never read
// reported loaded): the table is quarantined instead, and nothing of it is touched.
func TestTheInitialLoadDoesNotTakeWhatAnEarlierResyncLeft(t *testing.T) {
	for name, leftover := range map[string]func(*fakeSink){
		"a shadow table":        func(s *fakeSink) { s.marked = map[string]bool{"shop.items__resync": true} },
		"the marker on a table": func(s *fakeSink) { s.marked = map[string]bool{"shop.items": true} },
		"a table of that name":  func(s *fakeSink) { s.foreign = map[string]bool{"shop.items__resync": true} },
	} {
		t.Run(name, func(t *testing.T) {
			items := itemsTable()
			h := newHarness(t)
			h.cfg.Replication.InitialPosition = config.StartFromSnapshot
			h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, executed: fence(1)}
			leftover(h.sink)
			h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 3, "c"))}

			if err := h.run(t); err != nil {
				t.Fatalf("Run() = %v", err)
			}
			if n := h.sink.swapped(); n != 0 || len(h.sink.drops) != 0 {
				t.Errorf("swaps = %d, drops = %v: nothing left from before may be taken", n, h.sink.drops)
			}
			if pos := h.store.current(); pos.Resync != "" || !slices.Contains(quarantinedNames(pos), "shop.items") {
				t.Errorf("saved resyncs %q, quarantine %v: want the table quarantined, not loaded", pos.Resync, quarantinedNames(pos))
			}
		})
	}
}

// TestEveryExchangeComesAfterASavedPositionAtItsWatermark: two tables wait for the stream, one read at
// u:1-2 and one at u:1-4. The save before the first exchange has both read in full, but the stream only
// at the first watermark: the second exchange needs a save of its own.
func TestEveryExchangeComesAfterASavedPositionAtItsWatermark(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{executed: fence(9)}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{
		{Table: "shop.a", Shadow: "a__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(2)},
		{Table: "shop.b", Shadow: "b__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(4)},
	})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.a__resync": {Exists: true}, "shop.b__resync": {Exists: true}}
	// The clock stands still: no save comes from the flush interval.
	atSwap := map[string]string{}
	h.sink.onSwap = func(req chsink.SwapRequest) { atSwap[req.Table] = h.store.current().GTIDSet }
	h.source.script = []scriptedTx{
		uuidTx(2, insertRows(items, 2, "b")), uuidTx(3, insertRows(items, 3, "c")), uuidTx(4, insertRows(items, 4, "d")),
	}
	h.source.until = func() bool { return h.sink.swapped() > 1 }

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	for table, final := range map[string]string{"a": fence(2), "b": fence(4)} {
		at, ok := atSwap[table]
		if !ok {
			t.Fatalf("shop.%s was not swapped in: %v", table, atSwap)
		}
		saved, err := gtid.Parse(at)
		if err != nil {
			t.Fatal(err)
		}
		want, err := gtid.Parse(final)
		if err != nil {
			t.Fatal(err)
		}
		if !saved.Covers(want) {
			t.Errorf("the position saved at the exchange of shop.%s is %s, before its watermark %s", table, at, final)
		}
	}
}

// TestTheShadowTableIsNamedAfterTheTargetTable: tables of one name in two source databases have
// different targets in one ClickHouse database, and a shadow named after the source would be shared.
func TestTheShadowTableIsNamedAfterTheTargetTable(t *testing.T) {
	for _, initialLoad := range []bool{false, true} {
		t.Run(map[bool]string{false: "resync", true: "initial load"}[initialLoad], func(t *testing.T) {
			items := itemsTable()
			h := newHarness(t)
			h.cfg.Tables.Overrides = map[string]config.TableOverride{"shop.items": {TargetTable: "goods"}}
			h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(2)}
			p := h.pipeline(t)
			reply := make(chan error, 1)
			h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "a")), uuidTx(2, insertRows(items, 2, "b"))}
			if initialLoad {
				h.cfg.Replication.InitialPosition = config.StartFromSnapshot
				h.snapshot.(*fakeSnapshot).executed = fence(1)
			} else {
				h.source.beforeHandle = func(ev event.Event) {
					if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
						asks(p, reply)()
					}
				}
			}
			h.source.until = func() bool { return h.sink.swapped() > 0 }

			if err := h.runPipeline(t, p); err != nil {
				t.Fatalf("Run() = %v", err)
			}
			h.sink.mu.Lock()
			defer h.sink.mu.Unlock()
			if len(h.sink.swaps) != 1 || h.sink.swaps[0].Table != "goods" || h.sink.swaps[0].Shadow != "goods__resync" {
				t.Errorf("swaps = %+v, want goods__resync in the place of goods", h.sink.swaps)
			}
		})
	}
}

// TestATableBoundDuringAResyncCannotTakeItsTables: the table being resynchronized has no row since
// the resync began, so nothing of it is bound; a table seen for the first time is still refused the
// shadow, whose rows the swap would publish, and the target, which the swap replaces.
func TestATableBoundDuringAResyncCannotTakeItsTables(t *testing.T) {
	for _, target := range []string{"items__resync", "items"} {
		t.Run(target, func(t *testing.T) {
			items, other := itemsTable(), otherTable()
			h := newHarness(t)
			h.cfg.Tables.Overrides = map[string]config.TableOverride{"shop.other": {TargetTable: target}}
			h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(10)}
			p := h.pipeline(t)
			reply := make(chan error, 1)
			h.source.script = []scriptedTx{
				uuidTx(1, insertRows(items, 1, "a")),
				uuidTx(2, insertRows(items, 2, "b")),
				uuidTx(3, insertRows(other, 3, "c")),
			}
			h.source.beforeHandle = func(ev event.Event) {
				if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
					asks(p, reply)()
				}
			}
			h.source.until = heartbeatsFor(100000)

			err := h.runPipeline(t, p)
			prepared := map[string]int{}
			for _, e := range h.journal.all() {
				if table, ok := strings.CutPrefix(e, "ensure "); ok {
					prepared[table]++
				}
			}
			if len(prepared) != 1 || prepared["shop.items"] != 2 {
				t.Errorf("tables prepared = %v, want only the target and the shadow of shop.items", prepared)
			}
			if err == nil || !strings.Contains(err.Error(), "shop.items") || !strings.Contains(err.Error(), "shop.other") {
				t.Fatalf("Run() = %v, want an error naming both source tables", err)
			}
		})
	}
}

// TestStartResyncRefusesATableAnotherSourceTableHolds: a table that appeared after the start was
// never seen by the preflight, so a resync of it must not take the target of another source table
// or the shadow table of another resync: its swap would put its rows in the place of the other's.
func TestStartResyncRefusesATableAnotherSourceTableHolds(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.cfg.Tables.TargetDatabase = "replica"
	h.snapshot = &fakeSnapshot{}
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	t.Cleanup(func() {
		for _, j := range p.resyncs {
			p.stopJob(j)
		}
	})
	if _, err := p.bind(ctx, itemsTable()); err != nil {
		t.Fatal(err)
	}
	if err := p.startResync(ctx, "shop.items", itemsTable(), "", 0, resyncOrigin{}); err != nil {
		t.Fatalf("startResync(shop.items) = %v", err)
	}

	other := itemsTable()
	other.Database = "store" // its target is replica.items, and its shadow that of shop.items
	shadowNamed := itemsTable()
	shadowNamed.Name = "items__resync" // its target is the shadow table of shop.items
	for _, def := range []*schema.Table{other, shadowNamed} {
		for _, origin := range []resyncOrigin{{}, {autoresync: true}, {initialLoad: true}} {
			ensures := len(h.sink.ensures)
			err := p.startResync(ctx, def.FullName(), def, "", 0, origin)
			if err == nil || !strings.Contains(err.Error(), "shop.items") || !strings.Contains(err.Error(), "target_table") {
				t.Errorf("startResync(%s, %+v) = %v, want a refusal naming shop.items", def.FullName(), origin, err)
			}
			if len(h.sink.ensures) != ensures {
				t.Errorf("startResync(%s, %+v) created a table before it refused", def.FullName(), origin)
			}
			if _, active := p.resyncs[def.FullName()]; active {
				t.Errorf("%s is being resynchronized", def.FullName())
			}
		}
	}
}

// TestPartitionStatementsThatRemoveRowsDiscardAResync: the rows of a dropped partition that the
// resync already read would stay in the shadow table, since the statement is not applied in
// ClickHouse; a partition statement that keeps the rows changes nothing a snapshot depends on.
func TestPartitionStatementsThatRemoveRowsDiscardAResync(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	p.resyncs["shop.items"] = &resyncJob{}
	for _, tc := range []struct {
		query   string
		discard bool
	}{
		{"ALTER TABLE items DROP PARTITION p1", true},
		{"ALTER TABLE items TRUNCATE PARTITION p1", true},
		{"ALTER TABLE items EXCHANGE PARTITION p1 WITH TABLE store.items", true},
		{"ALTER TABLE items ADD PARTITION (PARTITION p3 VALUES LESS THAN (300))", false},
		{"ALTER TABLE items REORGANIZE PARTITION p1 INTO (PARTITION p1 VALUES LESS THAN (10))", false},
		{"ALTER TABLE items ADD INDEX i (name)", false},
	} {
		got := p.resyncImpacts(event.DDL{Database: "shop", Query: tc.query})
		if discard := len(got) == 1 && got[0].table == "shop.items" && got[0].restart; discard != tc.discard {
			t.Errorf("%s: impacts = %+v, want the resync discarded: %v", tc.query, got, tc.discard)
		}
	}
}

// TestACancelledResyncRequestIsNotBegunLater: the operator was told the resync did not begin, so it
// must not begin when the pipeline comes to its queue after the request gave up.
func TestACancelledResyncRequestIsNotBegunLater(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	var asked error
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, insertRows(items, 3, "c")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); !ok || b.GTID != testUUID+":2" {
			return
		}
		// The pipeline goroutine is here, so it cannot take the request while it waits.
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- p.StartResync(ctx, "shop.items") }()
		for !p.requested() {
			time.Sleep(time.Millisecond)
		}
		cancel()
		asked = <-done
	}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if !errors.Is(asked, context.Canceled) {
		t.Fatalf("StartResync() = %v, want the cancellation", asked)
	}
	if p.requested() {
		t.Error("the cancelled request is still in the queue")
	}
	for _, e := range h.journal.all() {
		if strings.Contains(e, "__resync") {
			t.Errorf("%s: a resync began after its request gave up", e)
		}
	}
	if got := h.obs.(*fakeObserver).sequence("resync-rows"); len(got) > 0 {
		t.Errorf("%v: a resync read the table after its request gave up", got)
	}
}
