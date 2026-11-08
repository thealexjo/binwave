package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
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
	h.store.pos, h.store.found = checkpoint.Position{GTIDSet: fence(1), Version: 1, Resync: state}, true
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
