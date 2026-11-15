package pipeline

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
)

func TestObserverSeesTheLifeOfATransaction(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"), insertRows(items, 2, "b"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	got := obs.sequence("began", "read", "committed", "inserted", "saved")
	want := []string{"began 1", "read shop.items 1", "read shop.items 1", "committed", "inserted shop.items 2", "saved u:1-1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("observed %v, want %v", got, want)
	}
	if obs.tables != 1 {
		t.Errorf("tables = %d, want 1", obs.tables)
	}
	if len(obs.saved) != 1 || !obs.saved[0].applied.Equal(epoch.Add(time.Second)) || !obs.saved[0].open.IsZero() {
		t.Errorf("saved = %+v, want the commit time of transaction 1 and no open transaction", obs.saved)
	}
	if obs.saved[0].pos.Version == 0 {
		t.Error("the saved position carries no version")
	}
}

// TestObserverIsToldOfTheOpenTransactionWhenItsRowsAreWrittenEarly: a transaction larger than the
// buffer is written in pieces while it is still being read; the position stays behind it, and
// the lag must too.
func TestObserverIsToldOfTheOpenTransactionWhenItsRowsAreWrittenEarly(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	h.cfg.Buffer.MaxRows = 1
	items := itemsTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		tx(2, insertRows(items, 2, "b"), insertRows(items, 3, "c")),
	}
	// The position is written at most once per flush interval (positionDue), so the interval has to
	// pass before transaction 2 opens for its first row to write the position as well as the rows —
	// which is the moment this test is about.
	begins := 0
	h.source.beforeHandle = func(ev event.Event) {
		if _, ok := ev.(event.Begin); ok {
			if begins++; begins == 2 {
				h.clock.Advance(time.Duration(h.cfg.Buffer.FlushInterval))
			}
		}
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	var withOpen int
	for _, s := range obs.saved {
		if !s.open.IsZero() {
			withOpen++
			if !s.open.Equal(epoch.Add(2 * time.Second)) {
				t.Errorf("open transaction committed at %v, want transaction 2's time", s.open)
			}
			if s.pos.GTIDSet != "u:1-1" {
				t.Errorf("the position %s covers the open transaction", s.pos.GTIDSet)
			}
		}
	}
	if withOpen == 0 {
		t.Errorf("no position save happened while transaction 2 was open: %+v", obs.saved)
	}
}

func TestIdleIsReportedOnlyWhenEverythingReadIsWrittenAndSaved(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	h.cfg.Buffer.FlushInterval = config.Duration(time.Hour) // nothing is written on its own
	items := itemsTable()
	h.source.script = []scriptedTx{
		{n: 99, events: []event.Event{event.Heartbeat{}}}, // nothing yet: idle
		tx(1, insertRows(items, 1, "a")),
		{n: 100, events: []event.Event{event.Heartbeat{}}}, // data waits in the buffer: not idle
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	got := obs.sequence("idle", "committed")
	want := []string{"idle", "committed"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("observed %v, want %v: the second heartbeat must not count as idle while a transaction is unsaved", got, want)
	}
}

func TestIdleFollowsTheFlushOfTheHeartbeatThatCausedIt(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	items := itemsTable()
	// The heartbeat comes a flush interval after the transaction, so it flushes, and then nothing is pending.
	h.source.beforeHandle = func(ev event.Event) {
		if _, ok := ev.(event.Heartbeat); ok {
			h.clock.Advance(2 * time.Second)
		}
	}
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		{n: 100, events: []event.Event{event.Heartbeat{}}},
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	got := obs.sequence("saved", "idle")
	want := []string{"saved u:1-1", "idle"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("observed %v, want %v", got, want)
	}
}

func TestObserverSeesRetriesByReason(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	h.sink.insertErrs = map[int]error{1: &ch.Exception{Code: 252}, 2: syscall.ECONNRESET}
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"))}

	_ = h.run(t)

	want := []string{"overload insert into `shop`.`items`", "transient insert into `shop`.`items`"}
	if !reflect.DeepEqual(obs.retries, want) {
		t.Errorf("retries = %v, want %v", obs.retries, want)
	}
}

func TestObserverSeesSkippedDestructiveDDL(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLAdditive // full mode mirrors the statement
	obs := &fakeObserver{}
	h.obs = obs
	items := itemsTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		tx(2, event.DDL{Database: "shop", Query: "TRUNCATE TABLE items"}),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if obs.skipped != 1 {
		t.Errorf("skipped DDL = %d, want 1", obs.skipped)
	}
}

func TestBufferedRowsAreReportedUpAndDown(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"), insertRows(items, 2, "b"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	got := obs.sequence("buffer")
	want := []string{"buffer 1", "buffer 2", "buffer 0"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("buffer sizes %v, want %v", got, want)
	}
}

// TestAnySuccessEndsTheRunOfFailures: a run of retries does not always end in a success of the same
// request. It can end with a failure a retry cannot fix (the table is quarantined, replication stops)
// or with a pause; the next request that succeeds ends it all the same, or the status keeps calling
// the old failure ongoing and holds the service at degraded while everything goes through.
func TestAnySuccessEndsTheRunOfFailures(t *testing.T) {
	tests := map[string]func(p *Pipeline) error{
		"ended by a failure a retry cannot fix": func(p *Pipeline) error {
			calls := 0
			return p.withRetry(context.Background(), "insert", func(context.Context) error {
				calls++
				if calls == 1 {
					return syscall.ECONNRESET
				}
				return errors.New("a row ClickHouse refuses")
			})
		},
		"ended by a pause": func(p *Pipeline) error {
			ctx, cancel := context.WithCancel(context.Background())
			calls := 0
			return p.withRetry(ctx, "insert", func(context.Context) error {
				calls++
				if calls == 2 {
					cancel() // the pause comes while the request is retried
				}
				return syscall.ECONNRESET
			})
		},
	}
	for name, failing := range tests {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			obs := &fakeObserver{}
			h.obs = obs
			p := h.pipeline(t)
			if err := failing(p); err == nil {
				t.Fatal("the run of failures succeeded")
			}

			if err := p.withRetry(context.Background(), "insert", func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}

			obs.mu.Lock()
			defer obs.mu.Unlock()
			if obs.recovered == 0 {
				t.Error("a success after the run of failures was not reported as a recovery")
			}
		})
	}
}

// TestEveryRowAResyncReadsIsCounted: the rows a resync reads used to be counted from the statuses it
// publishes, which are throttled; a table read between two publications, which is most tables of an
// initial load, was not counted at all. They are counted chunk by chunk instead.
func TestEveryRowAResyncReadsIsCounted(t *testing.T) {
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
	obs := &fakeObserver{}
	h.obs = obs
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

	want := []string{"resync-rows shop.items 3", "resync-rows shop.items 3", "resync-rows shop.items 1"}
	if got := obs.sequence("resync-rows"); !reflect.DeepEqual(got, want) {
		t.Errorf("rows reported = %v, want %v", got, want)
	}
}

// TestTheTablesAreCountedFromTheCatalogAndTheStream: a table counts from the check after the start,
// whether its rows come or not, and the statements of the stream that create, rename and drop tables
// change the count; a table the configuration does not select never counts.
func TestTheTablesAreCountedFromTheCatalogAndTheStream(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	p := h.pipeline(t)
	p.TablesFound([]string{"shop.items", "shop.quiet", "warehouse.stock"})
	h.source.script = []scriptedTx{
		tx(1, insertRows(itemsTable(), 1, "a")),
		tx(2, event.DDL{Database: "shop", Query: "CREATE TABLE fresh (id INT PRIMARY KEY)"}),
		tx(3, event.DDL{Database: "shop", Query: "RENAME TABLE quiet TO warehouse.quiet"}),
		tx(4, event.DDL{Database: "shop", Query: "CREATE TABLE shop.stock LIKE warehouse.stock"}),
		tx(5, event.DDL{Database: "shop", Query: "DROP TABLE fresh"}),
		tx(6, event.DDL{Database: "shop", Query: "DROP DATABASE shop"}),
	}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}

	got := obs.sequence("tables")
	want := []string{"tables 2", "tables 3", "tables 2", "tables 3", "tables 2", "tables 0"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("observed %v, want %v", got, want)
	}
}
