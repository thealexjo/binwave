package pipeline

import (
	"errors"
	"slices"
	"testing"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/failure"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// heartbeatsFor keeps the stream alive after the script, for at most n heartbeats.
func heartbeatsFor(n int) func() bool {
	return func() bool { n--; return n < 0 }
}

// resyncFailure checks that Run stopped replication on the resync of table.
func resyncFailure(t *testing.T, err error, table string, code errcode.Code) {
	t.Helper()
	var failed *FailedError
	if !errors.As(err, &failed) {
		t.Fatalf("Run() = %v, want replication failed on the resync of %s", err, table)
	}
	f := failed.Failure
	if f.Kind != failure.KindResync || f.Table != table || f.Code != string(code) || f.Skippable() {
		t.Errorf("failure = %+v, want a resync failure of %s with %s that cannot be skipped", f, table, code)
	}
}

// savedResync returns the resync of shop.items in the saved position.
func savedResync(t *testing.T, h *harness) (checkpoint.Resync, bool) {
	t.Helper()
	entries, err := checkpoint.DecodeResyncs(h.store.current().Resync)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Table == "shop.items" {
			return e, true
		}
	}
	return checkpoint.Resync{}, false
}

// TestWithStopAFailedReadStopsReplicationAndKeepsTheResync: the table cannot be quarantined, so the
// resync is not given up, which would leave the table without its changes since the resync began.
func TestWithStopAFailedReadStopsReplicationAndKeepsTheResync(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	waitsOutTheSource(h)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(2), readErr: errors.New("unknown column")}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":2" {
			asks(p, reply)()
		}
	}
	h.source.until = heartbeatsFor(100000)

	resyncFailure(t, h.runPipeline(t, p), "shop.items", errcode.SYNC101)

	if got := quarantinedNames(h.store.current()); len(got) != 0 {
		t.Errorf("quarantined = %v, want none with stop", got)
	}
	if len(h.sink.drops) != 0 {
		t.Errorf("drops = %v, want the shadow table kept", h.sink.drops)
	}
	if _, ok := savedResync(t, h); !ok {
		t.Errorf("the saved position has no resync of shop.items: resuming would leave the table without its changes")
	}
}

// TestWithStopADamagedWatermarkStopsReplication: never swapped in, never dropped.
func TestWithStopADamagedWatermarkStopsReplication(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	h.snapshot = &fakeSnapshot{executed: fence(2)}
	state := checkpoint.EncodeResyncs([]checkpoint.Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Rows: 1,
		Done: true, Final: "not a GTID set",
	}})
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: state}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}

	resyncFailure(t, h.run(t), "shop.items", errcode.SYNC101)

	if n := h.sink.swapped(); n != 0 || len(h.sink.drops) != 0 {
		t.Errorf("swaps = %d, drops = %v, want neither", n, h.sink.drops)
	}
	if _, ok := savedResync(t, h); !ok {
		t.Error("the saved position has no resync of shop.items")
	}
}

// TestWithStopAResyncThatCannotBeginAgainIsKeptAndBegunOnResume: a DDL statement discards the resync,
// beginning it again fails, and the resync waits in the position for replication to be resumed.
func TestWithStopAResyncThatCannotBeginAgainIsKeptAndBegunOnResume(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	snap := &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(3)}
	h.snapshot = snap
	p := h.pipeline(t)
	reply := make(chan error, 1)
	script := []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, event.DDL{Database: "shop", Query: "ALTER TABLE items ADD COLUMN extra INT NULL", Time: epoch}),
	}
	h.source.script = script
	h.source.beforeHandle = func(ev event.Event) {
		b, ok := ev.(event.Begin)
		switch {
		case ok && b.GTID == testUUID+":2":
			asks(p, reply)()
		case ok && b.GTID == testUUID+":3":
			snap.mu.Lock()
			snap.defErr = errors.New("the source does not answer for the table")
			snap.mu.Unlock()
		}
	}
	h.source.until = heartbeatsFor(100000)

	resyncFailure(t, h.runPipeline(t, p), "shop.items", errcode.SYNC101)
	st, ok := savedResync(t, h)
	if !ok || !st.Pending {
		t.Fatalf("saved resync = %+v (found %v), want shop.items pending", st, ok)
	}

	// Resumed once the cause is gone: the resync begins from the start and finishes.
	snap.mu.Lock()
	snap.defErr = nil
	snap.mu.Unlock()
	h.source.beforeHandle = nil
	h.source.script = slices.Concat(script, []scriptedTx{uuidTx(4, insertRows(items, 4, "d"))})
	h.source.until = func() bool { return h.sink.swapped() > 0 }
	if err := h.run(t); err != nil {
		t.Fatalf("Run() after resume = %v", err)
	}
	if _, ok := savedResync(t, h); ok {
		t.Error("the resync is still in the saved position after it finished")
	}
}

// TestWithStopARenameDuringAResyncResynchronizesTheNewName: the renamed table holds the rows of before
// the resync and nothing since; quarantine is not there to say so, so it is read again.
func TestWithStopARenameDuringAResyncResynchronizesTheNewName(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(3)}
	p := h.pipeline(t)
	reply := make(chan error, 1)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a")),
		uuidTx(2, insertRows(items, 2, "b")),
		uuidTx(3, event.DDL{Database: "shop", Query: "RENAME TABLE items TO goods", Time: epoch}),
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
	h.sink.mu.Lock()
	defer h.sink.mu.Unlock()
	if len(h.sink.swaps) != 1 || h.sink.swaps[0].Source != "shop.goods" {
		t.Errorf("swaps = %+v, want shop.goods resynchronized", h.sink.swaps)
	}
}
