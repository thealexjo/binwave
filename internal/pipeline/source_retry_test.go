package pipeline

import (
	"errors"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// errSourceDown stands for what the source answers while it restarts: the connection is refused or
// cut, or the server says it is shutting down.
var errSourceDown = errors.New("connection refused")

// sourceDown is Deps.SourceTransient for the tests: errSourceDown passes, anything else does not.
func sourceDown(err error) bool { return errors.Is(err, errSourceDown) }

// waitsOutTheSource makes h retry the source the way a restart of it needs, quickly.
func waitsOutTheSource(h *harness) {
	h.sourceTransient = sourceDown
	h.retry = RetryPolicy{Initial: time.Millisecond, Max: 2 * time.Millisecond, AttemptTimeout: time.Minute}
}

// TestAResyncWaitsOutASourceThatIsDown: a restart of MySQL takes longer than a few retries, and the
// table being read at that moment has nothing to do with it. A read that fails because the source is
// down is tried again for as long as it takes, and the resync finishes as if nothing had happened.
func TestAResyncWaitsOutASourceThatIsDown(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	waitsOutTheSource(h)
	h.snapshot = &fakeSnapshot{
		rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, executed: fence(2),
		readErr: errSourceDown, failReads: 3 * maxReadFailures,
	}
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, insertRows(items, 3, "c")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			asks(p, reply)()
		}
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 || len(p.Quarantined()) > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("the resync was refused: %v", err)
	}

	if q := p.Quarantined(); len(q) != 0 {
		t.Fatalf("quarantined = %+v, want none: the source being down is not a problem of the table", q)
	}
	want := map[string]string{"1|": "a", "2|": "b", "3|": "c"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
	if got := h.obs.(*fakeObserver).sourceRetries(); len(got) != 3*maxReadFailures {
		t.Errorf("source retries reported = %d, want %d", len(got), 3*maxReadFailures)
	}
}

// TestAResyncGivesUpOnAReadFailureThatDoesNotPass: what is not known to pass with time (a query the
// server refuses, a value the driver cannot scan) is tried a few times, then the resync gives up and
// the table is quarantined, loudly, instead of the reader trying forever without anyone knowing.
func TestAResyncGivesUpOnAReadFailureThatDoesNotPass(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	waitsOutTheSource(h)
	snap := &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(2), readErr: errors.New("unknown column")}
	h.snapshot = snap
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, insertRows(items, 3, "c")),
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

	if q := p.Quarantined(); len(q) != 1 || q[0].Table != "shop.items" {
		t.Fatalf("quarantined = %+v, want shop.items", q)
	}
	snap.mu.Lock()
	reads := snap.reads
	snap.mu.Unlock()
	if reads != maxReadFailures+1 {
		t.Errorf("reads = %d, want %d: the first and %d retries", reads, maxReadFailures+1, maxReadFailures)
	}
}

// TestAResyncContinuedAfterARestartWaitsForTheSource: binwave restarted because the source went away,
// and the source is not quite back yet when the resync it was doing continues. That is no reason to
// throw the shadow table away and quarantine the table.
func TestAResyncContinuedAfterARestartWaitsForTheSource(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	waitsOutTheSource(h)
	h.snapshot = &fakeSnapshot{
		rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, executed: fence(2),
		defErr: errSourceDown, failDefs: 3,
	}
	cursor, err := checkpoint.EncodeCursor([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Cursor: cursor, Rows: 1,
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	h.sink.tables["shop.items__resync"] = map[string]cell{"1|": {version: 1, values: []any{int64(1), "a"}}}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 3, "c"))}
	p := h.pipeline(t)
	h.source.until = func() bool { return h.sink.swapped() > 0 || len(p.Quarantined()) > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if q := p.Quarantined(); len(q) != 0 {
		t.Fatalf("quarantined = %+v, want none", q)
	}
	want := map[string]string{"1|": "a", "2|": "b", "3|": "c"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

// TestTheInitialLoadWaitsForTheSourceToDescribeATable: deciding what the initial load covers reads the
// definition of every table, thousands of them on a big source; the source restarting meanwhile does
// not make their keys unreadable in chunks.
func TestTheInitialLoadWaitsForTheSourceToDescribeATable(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	waitsOutTheSource(h)
	h.cfg.Replication.InitialPosition = config.StartFromSnapshot
	h.snapshot = &fakeSnapshot{
		rows: [][]any{{int64(1), "a"}}, executed: fence(1),
		defErr: errSourceDown, failDefs: 3,
	}
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 2, "b"))}
	p := h.pipeline(t)
	h.source.until = func() bool { return h.sink.swapped() > 0 || len(p.Quarantined()) > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if q := p.Quarantined(); len(q) != 0 {
		t.Fatalf("quarantined = %+v, want none", q)
	}
	want := map[string]string{"1|": "a", "2|": "b"}
	if got := live(t, h, "shop.items"); !equal(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

// TestAResyncBegunAgainAfterADDLWaitsForTheSource: a statement discards a resync, which begins again
// with the new structure read from the source; the source being down at that moment does not
// quarantine the table.
func TestAResyncBegunAgainAfterADDLWaitsForTheSource(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	waitsOutTheSource(h)
	// The first definition is the one the request carries; the ones after the statement fail first.
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(3), defErr: errSourceDown, failDefs: 3}
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
	h.source.until = func() bool { return h.sink.swapped() > 0 || len(p.Quarantined()) > 0 }

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if q := p.Quarantined(); len(q) != 0 {
		t.Fatalf("quarantined = %+v, want none", q)
	}
	if got := h.obs.(*fakeObserver).sequence("resync-finished"); len(got) == 0 || got[len(got)-1] != "resync-finished shop.items true" {
		t.Errorf("finished = %v, want the resync begun again and finished", got)
	}
}
