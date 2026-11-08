package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/typemap"
)

// versionsOf returns the versions the chain assigns to transactions at the given commit times.
func versionsOf(t *testing.T, times ...time.Time) []uint64 {
	t.Helper()
	v := chsink.NewVersioner(0)
	var out []uint64
	for _, at := range times {
		n, err := v.Next(at)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

// eagerFlush makes every commit flush: the clock jumps past the flush interval before it.
func (h *harness) eagerFlush() {
	h.source.beforeHandle = func(ev event.Event) {
		if _, ok := ev.(event.Commit); ok {
			h.clock.Advance(2 * time.Second)
		}
	}
}

func entriesWithPrefix(all []string, prefix string) []string {
	var out []string
	for _, e := range all {
		if strings.HasPrefix(e, prefix) {
			out = append(out, e)
		}
	}
	return out
}

func TestDataIsWrittenBeforeThePositionThatCoversIt(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a")), tx(2, insertRows(items, 2, "b"))}
	h.eagerFlush()

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	v := versionsOf(t, epoch.Add(time.Second), epoch.Add(2*time.Second))
	want := []string{
		"load",
		"save u:1-0 v0", // the configured start is saved before anything is read
		"ensure shop.items",
		"insert shop.items x1",
		fmt.Sprintf("save u:1-1 v%d", v[0]),
		"insert shop.items x1",
		fmt.Sprintf("save u:1-2 v%d", v[1]),
	}
	if got := h.journal.all(); !reflect.DeepEqual(got, want) {
		t.Errorf("order of effects:\n got %v\nwant %v", got, want)
	}
}

func TestPositionStaysBehindAnUnfinishedTransaction(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.MaxRows = 2
	items := itemsTable()
	// One transaction of five row changes that never reaches its commit.
	evs := []event.Event{event.Begin{GTID: "u:1", CommitTime: epoch.Add(time.Second)}}
	for i := int32(1); i <= 5; i++ {
		evs = append(evs, insertRows(items, i, "x"))
	}
	h.source.script = []scriptedTx{{n: 1, events: evs}}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if got := len(h.sink.visibleItems()); got != 5 {
		t.Errorf("%d rows written, want 5: rows of a huge transaction are flushed as they come", got)
	}
	pos := h.store.current()
	if pos.GTIDSet != "u:1-0" {
		t.Errorf("saved position = %q, want the start: the transaction never committed", pos.GTIDSet)
	}
	if n := len(h.store.saves); n != 1 {
		t.Errorf("position saved %d times, want only the initial save", n)
	}
}

// TestThePositionIsWrittenOnceTheIntervalPassedNoMatterHowManyFlushes: the same seven transactions,
// with the flush interval passing in the middle. The full buffer before it wrote rows only; the
// first flush after it writes the position too.
func TestThePositionIsWrittenOnceTheIntervalPassedNoMatterHowManyFlushes(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.MaxRows = 3
	items := itemsTable()
	for n := 1; n <= 7; n++ {
		h.source.script = append(h.source.script, tx(n, insertRows(items, int32(n), "x")))
	}
	begins := 0
	h.source.beforeHandle = func(ev event.Event) {
		if _, ok := ev.(event.Begin); ok {
			if begins++; begins == 5 {
				h.clock.Advance(time.Duration(h.cfg.Buffer.FlushInterval))
			}
		}
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	var saved []string
	for _, p := range h.store.saves {
		saved = append(saved, p.GTIDSet)
	}
	want := []string{"u:1-0", "u:1-4", "u:1-7"}
	if !reflect.DeepEqual(saved, want) {
		t.Errorf("saved positions = %v, want %v", saved, want)
	}
}

// TestSizeLimitFlushesTheRowsAndLeavesThePositionForItsTurn: a full buffer writes its rows at once,
// in the middle of a transaction if need be, but does not write the position — that waits for its
// interval, because it carries the whole state of the resyncs (see positionDue). That the position,
// whenever it is written, never covers a transaction still open is what
// TestObserverIsToldOfTheOpenTransactionWhenItsRowsAreWrittenEarly checks.
func TestSizeLimitFlushesTheRowsAndLeavesThePositionForItsTurn(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.MaxRows = 3
	items := itemsTable()
	for n := 1; n <= 7; n++ {
		h.source.script = append(h.source.script, tx(n, insertRows(items, int32(n), "x")))
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	var saved []string
	for _, p := range h.store.saves {
		saved = append(saved, p.GTIDSet)
	}
	// The clock stands still in this test, so the only positions written are the first one and the
	// final one of the graceful stop; the three flushes in between wrote rows only.
	want := []string{"u:1-0", "u:1-7"}
	if !reflect.DeepEqual(saved, want) {
		t.Errorf("saved positions = %v, want %v", saved, want)
	}
	if got := entriesWithPrefix(h.journal.all(), "insert"); !reflect.DeepEqual(got, []string{"insert shop.items x3", "insert shop.items x3", "insert shop.items x1"}) {
		t.Errorf("inserts = %v", got)
	}
}

func TestVersionsStayMonotoneWhenTheSourceClockStepsBack(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	// The second transaction is committed later but stamped earlier: the clock stepped back.
	h.source.script = []scriptedTx{
		txAt(1, epoch.Add(time.Hour), insertRows(items, 1, "old")),
		txAt(2, epoch, updateRows(items, 1, "old", "new")),
	}
	h.eagerFlush()

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	rows := h.sink.visibleItems()
	if got := rows["1|"]; len(got) != 2 || got[1] != "new" {
		t.Errorf("visible row = %v, want the later update to win despite its earlier timestamp", got)
	}
	if h.store.saves[2].Version <= h.store.saves[1].Version {
		t.Errorf("saved versions did not increase: %d then %d", h.store.saves[1].Version, h.store.saves[2].Version)
	}
}

func TestTransactionsWithoutReplicatedRowsStillAdvanceThePosition(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1), tx(2), tx(3)}
	h.eagerFlush()

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	pos := h.store.current()
	if pos.GTIDSet != "u:1-3" {
		t.Errorf("position = %q, want u:1-3", pos.GTIDSet)
	}
	if h.sink.inserts != 0 {
		t.Errorf("%d inserts for transactions without rows", h.sink.inserts)
	}
	// The version chain covers every transaction, so it does not depend on the table filter.
	want := versionsOf(t, epoch.Add(time.Second), epoch.Add(2*time.Second), epoch.Add(3*time.Second))
	if pos.Version != want[2] {
		t.Errorf("version = %d, want %d", pos.Version, want[2])
	}
}

func TestIdleHeartbeatFlushesWhenTheIntervalHasPassed(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		{n: 2, events: []event.Event{event.Heartbeat{}, event.Heartbeat{}}},
	}
	heartbeats, insertsAtSecond := 0, -1
	h.source.beforeHandle = func(ev event.Event) {
		if _, ok := ev.(event.Heartbeat); !ok {
			return
		}
		heartbeats++
		if heartbeats == 2 {
			insertsAtSecond = h.sink.inserts // nothing flushed while the interval had not passed
			h.clock.Advance(2 * time.Second)
		}
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if insertsAtSecond != 0 {
		t.Errorf("%d inserts before the flush interval passed", insertsAtSecond)
	}
	if got := entriesWithPrefix(h.journal.all(), "insert"); len(got) != 1 {
		t.Errorf("inserts = %v", got)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-1" {
		t.Errorf("position = %q, want u:1-1", pos.GTIDSet)
	}
}

func TestMemoryStaysBoundedByTheLimit(t *testing.T) {
	h := newHarness(t)
	const limit = 2_000
	h.cfg.Buffer.MaxMemory = limit
	h.cfg.Buffer.MaxBytes = limit
	items := itemsTable()
	for n := 1; n <= 40; n++ {
		h.source.script = append(h.source.script, tx(n, insertRows(items, int32(n), strings.Repeat("x", 15))))
	}

	p := h.pipeline(t)
	peak := 0
	h.source.beforeHandle = func(event.Event) { peak = max(peak, p.memBytes) }
	if err := p.Run(context.Background(), h.hooks()); !errors.Is(err, errScriptEnded) {
		t.Fatal(err)
	}

	oneRow := chsink.RowBytes(chsink.Row{Values: []any{int32(1), strings.Repeat("x", 15)}})
	if peak > limit+oneRow {
		t.Errorf("buffered memory peaked at %d, limit %d", peak, limit)
	}
	if h.sink.inserts < 3 {
		t.Errorf("only %d flushes for 40 rows under a %d byte limit", h.sink.inserts, limit)
	}
	if got := len(h.sink.visibleItems()); got != 40 {
		t.Errorf("%d rows written, want 40", got)
	}
}

func TestStructureChangeFlushesThenChecksTheNewTable(t *testing.T) {
	h := newHarness(t)
	v1 := itemsTable()
	v1Again := itemsTable() // a different object with the same structure
	v2 := itemsTable()
	v2.Columns = append(v2.Columns, typemap.Column{Name: "note", Kind: typemap.KindVarChar, Length: 10, Charset: "utf8mb4", Nullable: true})
	wide := func(id int32, name string) event.Rows {
		return event.Rows{Table: v2, Op: event.Insert, Rows: []event.Row{{After: []any{id, name, "n"}}}}
	}
	h.source.script = []scriptedTx{
		tx(1, insertRows(v1, 1, "a")),
		tx(2, insertRows(v1Again, 2, "b")),
		tx(3, wide(3, "c")),
		tx(4, wide(4, "d")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if len(h.sink.ensures) != 2 {
		t.Fatalf("table checked %d times, want once per structure (2): %v", len(h.sink.ensures), h.sink.ensures)
	}
	// Everything converted under the old structure is written, and its position saved, before
	// the new structure is checked.
	v := versionsOf(t, epoch.Add(time.Second), epoch.Add(2*time.Second), epoch.Add(3*time.Second), epoch.Add(4*time.Second))
	want := []string{
		"load",
		"save u:1-0 v0",
		"ensure shop.items",
		"insert shop.items x2",
		fmt.Sprintf("save u:1-2 v%d", v[1]),
		"ensure shop.items", // the new structure, only after the old rows and position are safe
		"insert shop.items x2",
		fmt.Sprintf("save u:1-4 v%d", v[3]),
	}
	if got := h.journal.all(); !reflect.DeepEqual(got, want) {
		t.Errorf("order around the structure change:\n got %v\nwant %v", got, want)
	}
}

func TestDDLIsABarrier(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		tx(2, event.DDL{Database: "shop", Query: "ALTER TABLE items ADD COLUMN x INT"}),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	j := h.journal.all()
	insertAt, saveAt := -1, -1
	for i, e := range j {
		switch {
		case strings.HasPrefix(e, "insert"):
			insertAt = i
		case strings.HasPrefix(e, "save u:1-1"):
			saveAt = i
		}
	}
	if insertAt < 0 || saveAt < insertAt {
		t.Errorf("the DDL did not flush rows and position first: %v", j)
	}
}

func captureLogger() (*slog.Logger, *strings.Builder, *sync.Mutex) {
	var b strings.Builder
	var mu sync.Mutex
	h := slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return b.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), &b, &mu
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestNewRejectsWhatIsNotImplemented(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.Config)
	}{
		{"snapshot start", func(c *config.Config) { c.Replication.InitialPosition = config.StartFromSnapshot }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			tt.mutate(cfg)
			if _, err := New(cfg, Deps{}); !errors.Is(err, ErrUnsupported) {
				t.Fatalf("New() = %v, want ErrUnsupported", err)
			}
		})
	}
}

func TestTransientFailuresAreRetriedWithBackoff(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"))}
	tooManyParts := &ch.Exception{Code: 252, Message: "too many parts"}
	h.sink.insertErrs = map[int]error{1: tooManyParts, 2: tooManyParts, 3: tooManyParts}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
	if !reflect.DeepEqual(h.sleeps, want) {
		t.Errorf("backoff = %v, want %v", h.sleeps, want)
	}
	if got := len(h.sink.visibleItems()); got != 1 {
		t.Errorf("%d rows after the retries, want 1", got)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-1" {
		t.Errorf("position = %q", pos.GTIDSet)
	}
}

func TestBackoffIsCapped(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"))}
	overloaded := &ch.Exception{Code: 241}
	h.sink.insertErrs = map[int]error{}
	for i := 1; i <= 9; i++ {
		h.sink.insertErrs[i] = overloaded
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if last := h.sleeps[len(h.sleeps)-1]; last != 30*time.Second {
		t.Errorf("longest wait = %v, want the 30s cap; waits: %v", last, h.sleeps)
	}
}

// failCalls makes the first n inserts fail with err.
func failCalls(h *harness, n int, err error) {
	h.sink.insertErrs = map[int]error{}
	for i := 1; i <= n; i++ {
		h.sink.insertErrs[i] = err
	}
}

func TestOverloadIsRetriedWithinTheBudget(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}
	// 20 failures wait about 7.5 minutes of backoff, under the 10 minute default budget.
	failCalls(h, 20, &ch.Exception{Code: 252, Message: "too many parts"})

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v, want the overload to be ridden out", err)
	}
	if got := len(h.sink.visibleItems()); got != 1 {
		t.Errorf("%d rows written, want 1", got)
	}
}

func TestOverloadThatLastsStopsAfterTheBudget(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}
	h.eagerFlush()
	failCalls(h, 100000, &ch.Exception{Code: 252, Message: "too many parts"})

	err := h.run(t)

	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("Run() = %v, want ErrOverloaded", err)
	}
	var exc *ch.Exception
	if !errors.As(err, &exc) || exc.Code != 252 {
		t.Errorf("the ClickHouse error is missing from %v", err)
	}
	var waited time.Duration
	for _, d := range h.sleeps {
		waited += d
	}
	if waited < 9*time.Minute || waited > 12*time.Minute {
		t.Errorf("waited %v in total before giving up, want about the 10 minute budget", waited)
	}
	for _, p := range h.store.saves {
		if p.GTIDSet != "u:1-0" {
			t.Errorf("position %q was saved although its data was not written", p.GTIDSet)
		}
	}
}

func TestBudgetFollowsTheConfiguration(t *testing.T) {
	h := newHarness(t)
	h.cfg.Retry.OverloadBudget = config.Duration(time.Minute)
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}
	h.eagerFlush()
	failCalls(h, 100000, &ch.Exception{Code: 241})

	if err := h.run(t); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("Run() = %v, want ErrOverloaded", err)
	}
	var waited time.Duration
	for _, d := range h.sleeps {
		waited += d
	}
	if waited > 2*time.Minute {
		t.Errorf("waited %v with a one minute budget", waited)
	}
}

// TestNetworkOutageIsRetriedWithoutLimit: an outage ends by itself, so it is waited out however long it lasts.
func TestNetworkOutageIsRetriedWithoutLimit(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}
	failCalls(h, 300, &ch.Exception{Code: 210, Message: "network error"}) // hours of model time

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v, want a long outage to be waited out", err)
	}
	var waited time.Duration
	for _, d := range h.sleeps {
		waited += d
	}
	if waited < time.Hour {
		t.Errorf("waited only %v; the test did not exercise a long outage", waited)
	}
}

// TestOverloadBudgetCountsConsecutiveOverloadOnly: a different failure in between starts the count over.
func TestOverloadBudgetCountsConsecutiveOverloadOnly(t *testing.T) {
	h := newHarness(t)
	h.cfg.Retry.OverloadBudget = config.Duration(5 * time.Minute)
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}
	overload := &ch.Exception{Code: 252}
	h.sink.insertErrs = map[int]error{}
	for i := 1; i <= 13; i++ { // about 4.5 minutes of backoff
		h.sink.insertErrs[i] = overload
	}
	h.sink.insertErrs[14] = &ch.Exception{Code: 210}
	for i := 15; i <= 23; i++ { // another 4.5 minutes; together they exceed the budget
		h.sink.insertErrs[i] = overload
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v: two overloads separated by another failure must not add up", err)
	}
}

func TestPermanentFailureStopsWithoutSavingPastTheData(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"))}
	h.eagerFlush()
	// Every attempt fails, including the best-effort flush on the way out.
	unknownTable := &ch.Exception{Code: 60, Message: "unknown table"}
	h.sink.insertErrs = map[int]error{1: unknownTable, 2: unknownTable, 3: unknownTable}

	err := h.run(t)

	var exc *ch.Exception
	if !errors.As(err, &exc) || exc.Code != 60 {
		t.Fatalf("Run() = %v, want the ClickHouse exception", err)
	}
	if len(h.sleeps) != 0 {
		t.Errorf("a permanent error was retried: %v", h.sleeps)
	}
	for _, p := range h.store.saves {
		if p.GTIDSet != "u:1-0" {
			t.Errorf("position %q was saved although its data was not written", p.GTIDSet)
		}
	}
}

// TestShutdownDuringAnOutageStillFlushesOnceClickHouseIsBack: the flush at shutdown runs on a
// context that is not the cancelled one, and keeps trying.
func TestShutdownDuringAnOutageStillFlushesOnceClickHouseIsBack(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"))}
	h.eagerFlush()
	h.sink.insertErrs = map[int]error{1: &ch.Exception{Code: 210}, 2: &ch.Exception{Code: 210}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := New(h.cfg, Deps{
		Source: h.source, Sink: h.sink, Store: h.store, Logger: h.logger, Now: h.clock.Now,
		// SIGTERM arrives while the first retry is waiting.
		Sleep: func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() },
	})
	if err != nil {
		t.Fatal(err)
	}

	runErr := p.Run(ctx, h.hooks())

	// The stream failed because ClickHouse was down when the signal came; the final flush,
	// with ClickHouse back, wrote the data and the position.
	if runErr == nil {
		t.Fatal("Run() = nil, want the error that interrupted the stream")
	}
	if got := len(h.sink.visibleItems()); got != 1 {
		t.Errorf("%d rows written by the final flush, want 1", got)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-1" {
		t.Errorf("position = %q, want u:1-1", pos.GTIDSet)
	}
	if last := h.sink.ctxAtInsert[len(h.sink.ctxAtInsert)-1]; last != nil {
		t.Errorf("the final flush ran on a cancelled context: %v", last)
	}
}

func TestGracefulShutdownFlushesAndReturnsNil(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a")), tx(2, insertRows(items, 2, "b"))}
	h.source.holdOpen = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.source.beforeHandle = func(ev event.Event) {
		if c, ok := ev.(event.Commit); ok && c.GTIDSet == "u:1-2" {
			// The signal arrives while the last commit is being handled; nothing was flushed yet.
			cancel()
		}
	}

	err := h.pipeline(t).Run(ctx, h.hooks())
	if err != nil {
		t.Fatalf("Run() = %v, want nil after a graceful stop", err)
	}

	if got := len(h.sink.visibleItems()); got != 2 {
		t.Errorf("%d rows written at shutdown, want 2", got)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-2" {
		t.Errorf("position = %q, want u:1-2", pos.GTIDSet)
	}
	if last := h.sink.ctxAtInsert[len(h.sink.ctxAtInsert)-1]; last != nil {
		t.Errorf("the shutdown flush ran on a cancelled context: %v", last)
	}
	if h.readied.Load() != 1 {
		t.Errorf("Ready called %d times, want 1", h.readied.Load())
	}
}

func TestStartPosition(t *testing.T) {
	t.Run("first run from now saves the start before reading", func(t *testing.T) {
		h := newHarness(t)
		h.source.executed = "u:1-50"
		h.source.script = []scriptedTx{tx(51)}

		if err := h.run(t); err != nil {
			t.Fatal(err)
		}

		if got := h.journal.all()[:2]; !reflect.DeepEqual(got, []string{"load", "save u:1-50 v0"}) {
			t.Errorf("start sequence = %v", got)
		}
		if h.source.streams[0] != "u:1-50" {
			t.Errorf("streaming from %q", h.source.streams[0])
		}
	})

	t.Run("first run from a GTID", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Replication.InitialPosition = config.StartFromGTID
		h.cfg.Replication.GTID = "u:1-7"
		h.source.script = []scriptedTx{tx(8)}

		if err := h.run(t); err != nil {
			t.Fatal(err)
		}
		if h.source.streams[0] != "u:1-7" {
			t.Errorf("streaming from %q, want the configured GTID set", h.source.streams[0])
		}
	})

	t.Run("a saved position wins over the configured start", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Replication.InitialPosition = config.StartFromGTID
		h.cfg.Replication.GTID = "u:1-7"
		h.store.pos, h.store.found = checkpoint.Position{GTIDSet: "u:1-3", Version: 5}, true
		h.source.script = []scriptedTx{tx(4)}

		if err := h.run(t); err != nil {
			t.Fatal(err)
		}
		if h.source.streams[0] != "u:1-3" {
			t.Errorf("streaming from %q, want the saved position", h.source.streams[0])
		}
	})

	t.Run("the version chain continues from the saved version", func(t *testing.T) {
		h := newHarness(t)
		// A previous run wrote versions far ahead of this transaction's timestamp.
		saved := uint64(epoch.Add(24*time.Hour).UnixMicro()) << 12
		h.store.pos, h.store.found = checkpoint.Position{GTIDSet: "u:1-3", Version: saved}, true
		h.source.script = []scriptedTx{tx(4, insertRows(itemsTable(), 1, "a"))}
		h.eagerFlush()

		if err := h.run(t); err != nil {
			t.Fatal(err)
		}
		if got := h.store.saves[len(h.store.saves)-1].Version; got != saved+1 {
			t.Errorf("version = %d, want %d: it must continue above the saved one", got, saved+1)
		}
	})
}

func TestReadyIsSignalledWhenTheStreamStarts(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1)}
	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if h.readied.Load() != 1 {
		t.Errorf("Ready called %d times", h.readied.Load())
	}
	if h.progress.Load() == 0 {
		t.Error("Progress was never reported")
	}
}

func TestConversionErrorStopsButKeepsWhatIsCommitted(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	items := itemsTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		// NULL in the NOT NULL id column cannot be converted.
		tx(2, event.Rows{Table: items, Op: event.Insert, Rows: []event.Row{{After: []any{nil, "b"}}}}),
	}

	err := h.run(t)

	var convErr *typemap.ConversionError
	if !errors.As(err, &convErr) || convErr.Column != "id" {
		t.Fatalf("Run() = %v, want a ConversionError for column id", err)
	}
	// The transaction before it is fully committed, so the final flush keeps it.
	if got := len(h.sink.visibleItems()); got != 1 {
		t.Errorf("%d rows written, want the committed one", got)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-1" {
		t.Errorf("position = %q, want u:1-1", pos.GTIDSet)
	}
}

func TestSchemaMismatchStopsBeforeAnyInsert(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}
	mismatch := &chsink.SchemaMismatchError{Table: "`replica`.`items`", Problems: []string{"column name is missing"}}
	h.sink.ensureErrs = map[int]error{1: mismatch}

	err := h.run(t)

	var got *chsink.SchemaMismatchError
	if !errors.As(err, &got) {
		t.Fatalf("Run() = %v, want SchemaMismatchError", err)
	}
	if h.sink.inserts != 0 {
		t.Error("rows were inserted into a table that failed the check")
	}
}

// TestTablesOfTheSameNameInTwoDatabasesGoToSeparateTablesByDefault: by default a table keeps its
// database name in ClickHouse, so shop.items and other.items never meet.
func TestTablesOfTheSameNameInTwoDatabasesGoToSeparateTablesByDefault(t *testing.T) {
	h := newHarness(t)
	shop := itemsTable()
	other := itemsTable()
	other.Database = "other"
	h.source.script = []scriptedTx{
		tx(1, insertRows(shop, 1, "shop")),
		tx(2, insertRows(other, 1, "other")),
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if len(h.sink.tables) != 2 {
		t.Errorf("rows went into %d target tables, want one per source database", len(h.sink.tables))
	}
}

// TestTablesOfTheSameNameAreNotMergedIntoOneDatabase: with everything collected in one target
// database, shop.items and other.items would land in the same ClickHouse table and overwrite each
// other's rows. That must stop the pipeline with an error that names both, never merge silently.
func TestTablesOfTheSameNameAreNotMergedIntoOneDatabase(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.TargetDatabase = "replica"
	shop := itemsTable()
	other := itemsTable()
	other.Database = "other"
	h.source.script = []scriptedTx{
		tx(1, insertRows(shop, 1, "shop")),
		tx(2, insertRows(other, 1, "other")),
	}

	err := h.run(t)

	if err == nil || !strings.Contains(err.Error(), "shop.items") || !strings.Contains(err.Error(), "other.items") {
		t.Fatalf("Run() = %v, want an error naming both source tables", err)
	}
	if !strings.Contains(err.Error(), "target_table") {
		t.Errorf("the error does not say how to resolve it: %v", err)
	}
	// What was committed before the collision is kept.
	if got := h.sink.liveRows("replica.items"); len(got) != 1 || got["1|"][1] != "shop" {
		t.Errorf("rows = %v, want only the first table's row", got)
	}
}

func TestOverridingTheTargetTableResolvesTheCollision(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.TargetDatabase = "replica"
	h.cfg.Tables.Overrides = map[string]config.TableOverride{"other.items": {TargetTable: "other_items"}}
	shop := itemsTable()
	other := itemsTable()
	other.Database = "other"
	h.source.script = []scriptedTx{
		tx(1, insertRows(shop, 1, "shop")),
		tx(2, insertRows(other, 1, "other")),
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v, want the override to resolve the collision", err)
	}
	if len(h.sink.tables) != 2 {
		t.Errorf("rows went into %d target tables, want 2", len(h.sink.tables))
	}
}

// TestRedeliveredTransactionReplacesTheOpenOne: after a reconnect the source sends the open
// transaction again from its start. The pipeline must take the replay, and the result must be
// exactly what one clean delivery gives: the partial rows written before the reconnect carry a
// lower version, so the replay supersedes them.
func TestRedeliveredTransactionReplacesTheOpenOne(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.MaxRows = 1 // every row is flushed at once, so the partial rows really are written
	items := itemsTable()
	h.source.script = []scriptedTx{
		{n: 1, events: []event.Event{
			event.Begin{GTID: "u:1", CommitTime: epoch.Add(time.Second)},
			insertRows(items, 1, "partial"),
			// the connection dropped: the transaction starts over
			event.Begin{GTID: "u:1", CommitTime: epoch.Add(time.Second)},
			insertRows(items, 1, "final"),
			insertRows(items, 2, "second"),
			event.Commit{GTIDSet: "u:1-1"},
		}},
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v, want the replay to be accepted", err)
	}
	got := h.sink.visibleItems()
	if len(got) != 2 || got["1|"][1] != "final" || got["2|"][1] != "second" {
		t.Errorf("rows = %v, want the replay to supersede the partial write", got)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-1" {
		t.Errorf("position = %q", pos.GTIDSet)
	}
}

func TestProtocolViolationsAreReported(t *testing.T) {
	items := itemsTable()
	tests := []struct {
		name    string
		events  []event.Event
		wantMsg string
	}{
		{"rows outside a transaction", []event.Event{insertRows(items, 1, "a")}, "outside a transaction"},
		{"commit outside a transaction", []event.Event{event.Commit{GTIDSet: "u:1-1"}}, "commit outside"},
		{"a different transaction inside an open one", []event.Event{event.Begin{GTID: "u:1", CommitTime: epoch}, event.Begin{GTID: "u:2", CommitTime: epoch}}, "began inside"},
		{"begin without a commit time", []event.Event{event.Begin{GTID: "u:1"}}, "no commit timestamp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.source.script = []scriptedTx{{n: 1, events: tt.events}}
			err := h.run(t)
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Run() = %v, want error containing %q", err, tt.wantMsg)
			}
		})
	}
}

// Compile-time check that the fakes and the real implementations satisfy the same interfaces.
var (
	_ Source = (*fakeSource)(nil)
	_ Sink   = (*chsink.Client)(nil)
	_ Store  = (*checkpoint.File)(nil)
	_ Store  = (*checkpoint.ClickHouse)(nil)
	_        = schema.Table{}
)

func TestGuardVetsATableOnceBeforeItsFirstWrite(t *testing.T) {
	h := newHarness(t)
	g := &fakeGuard{}
	h.guard = g
	items := itemsTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		tx(2, insertRows(items, 2, "b")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(g.asked) != 1 || g.asked[0] != "shop.items" {
		t.Errorf("guard was asked %v, want shop.items once: a table is vetted when it is first bound", g.asked)
	}
}

func TestGuardRejectionStopsBeforeAnythingIsWrittenForTheTable(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	reason := errors.New("foreign key cascades")
	h.guard = &fakeGuard{reject: map[string]error{"shop.items": reason}}
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}

	err := h.run(t)

	if !errors.Is(err, reason) || !strings.Contains(err.Error(), "shop.items") {
		t.Fatalf("Run() = %v, want the guard's error naming the table", err)
	}
	if len(h.sink.ensures) != 0 || h.sink.inserts != 0 {
		t.Errorf("ensures = %v, inserts = %d: nothing may reach ClickHouse for a rejected table", h.sink.ensures, h.sink.inserts)
	}
}

// namedTable is itemsTable under another name, for a flush that touches several tables.
func namedTable(name string) *schema.Table {
	t := itemsTable()
	t.Name = name
	return t
}

// TestAFlushWritesItsTablesAtTheSameTime: a flush of a source whose changes are spread over many
// tables is a row or two per table, and such an insert costs a round-trip and the durability of the
// server rather than the rows (tens of milliseconds for a handful of rows, most of the pipeline's
// time spent waiting). Written one after another, a flush is as long as the number of tables it
// touched; the tables are independent, so they go at once.
func TestAFlushWritesItsTablesAtTheSameTime(t *testing.T) {
	const tables = 4
	h := newHarness(t)
	h.cfg.Buffer.InsertParallelism = tables
	var mu sync.Mutex
	inFlight, peak := 0, 0
	all := make(chan struct{})
	h.sink.beforeInsert = func(string) {
		mu.Lock()
		inFlight++
		peak = max(peak, inFlight)
		if inFlight == tables {
			close(all)
		}
		mu.Unlock()
		select { // wait for the others, so overlapping is proven and not a coincidence of timing
		case <-all:
		case <-time.After(5 * time.Second):
		}
		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	var rows []event.Event
	for i := range tables {
		rows = append(rows, insertRows(namedTable(fmt.Sprintf("t%d", i)), int32(i), "x"))
	}
	h.source.script = []scriptedTx{tx(1, rows...)}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if peak != tables {
		t.Errorf("%d inserts were in flight at once, want %d: the flush still writes its tables one after another", peak, tables)
	}
	for i := range tables {
		if got := len(h.sink.liveRows(fmt.Sprintf("shop.t%d", i))); got != 1 {
			t.Errorf("table t%d holds %d rows, want 1", i, got)
		}
	}
}

// TestThePositionWaitsForEveryInsertOfTheFlush: the inserts of a flush run at the same time, and the
// position must still be behind all of them — a save that overtook one slow insert would let a crash
// resume past rows that never got in.
func TestThePositionWaitsForEveryInsertOfTheFlush(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.InsertParallelism = 4
	// The insert of the last table is the slow one; the position must be saved after it, not before.
	h.sink.beforeInsert = func(table string) {
		if table == "shop.t3" {
			time.Sleep(50 * time.Millisecond)
		}
	}
	var rows []event.Event
	for i := range 4 {
		rows = append(rows, insertRows(namedTable(fmt.Sprintf("t%d", i)), int32(i), "x"))
	}
	h.source.script = []scriptedTx{tx(1, rows...)}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	// The save that carries the transaction must come after every insert of it, the slow one included.
	entries := h.journal.all()
	lastInsert, save := -1, -1
	for i, e := range entries {
		switch {
		case strings.HasPrefix(e, "insert "):
			lastInsert = i
		case strings.HasPrefix(e, "save u:1-1"):
			save = i
		}
	}
	if lastInsert < 0 || save < 0 {
		t.Fatalf("the journal has no inserts or no save of the transaction: %v", entries)
	}
	if save < lastInsert {
		t.Errorf("the position was saved before an insert of the same flush finished: %v", entries)
	}
}

// TestSerialInsertsKeepTheOrderOfTheBuffers: buffer.insert_parallelism 1 is the old behaviour, for a
// source or a server where writing at once turns out worse.
func TestSerialInsertsKeepTheOrderOfTheBuffers(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.InsertParallelism = 1
	var rows []event.Event
	for i := range 4 {
		rows = append(rows, insertRows(namedTable(fmt.Sprintf("t%d", i)), int32(i), "x"))
	}
	h.source.script = []scriptedTx{tx(1, rows...)}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	want := []string{"insert shop.t0 x1", "insert shop.t1 x1", "insert shop.t2 x1", "insert shop.t3 x1"}
	if got := entriesWithPrefix(h.journal.all(), "insert"); !reflect.DeepEqual(got, want) {
		t.Errorf("inserts = %v, want %v", got, want)
	}
}

// BenchmarkFlushOfManyTables measures a flush whose rows are spread over many tables, with an insert
// that costs what a real one costs (a round-trip and an fsync, not the rows).
func BenchmarkFlushOfManyTables(b *testing.B) {
	const tables = 24
	for _, parallelism := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("parallelism=%d", parallelism), func(b *testing.B) {
			h := newHarness(b)
			h.cfg.Buffer.InsertParallelism = parallelism
			h.sink.beforeInsert = func(string) { time.Sleep(2 * time.Millisecond) } // one round-trip
			p := h.pipeline(b)
			p.versions = chsink.NewVersioner(0)
			ctx := context.Background()
			targets := make([]*chsink.Table, tables)
			for i := range targets {
				t, err := chsink.Resolve(namedTable(fmt.Sprintf("t%d", i)), h.cfg, nil)
				if err != nil {
					b.Fatal(err)
				}
				targets[i] = t
			}
			row := chsink.Row{Values: []any{int32(1), "x"}, Version: 1}
			b.ResetTimer()
			for range b.N {
				for _, t := range targets {
					p.add(t, []chsink.Row{row})
				}
				if err := p.flushBuffers(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestAtTheHeadOfTheBinlogRowsWaitTheOrdinaryInterval: the delay an operator sees as the lag is how
// long rows wait in memory. At the head that has to be short, whatever the interval used while
// catching up: one number cannot serve both, and before this the lag sawtoothed up to the batching
// interval and back on a service that was fully caught up.
func TestAtTheHeadOfTheBinlogRowsWaitTheOrdinaryInterval(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.FlushInterval = config.Duration(250 * time.Millisecond)
	h.cfg.Buffer.CatchupFlushInterval = config.Duration(5 * time.Second)
	items := itemsTable()
	// Transactions committed "just now", as the clock of the pipeline sees it: the head of the binlog.
	h.source.script = []scriptedTx{
		txAt(1, epoch, insertRows(items, 1, "a")),
		txAt(2, epoch, insertRows(items, 2, "b")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if _, ok := ev.(event.Commit); ok {
			h.clock.Advance(300 * time.Millisecond) // past the ordinary interval, far from the catch-up one
		}
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	// Both transactions were written while the stream ran, not only by the flush of the graceful stop.
	if got := entriesWithPrefix(h.journal.all(), "insert"); len(got) != 2 {
		t.Errorf("inserts = %v, want one per transaction: at the head rows must not wait the catch-up interval", got)
	}
}

// TestWithABacklogRowsWaitTheCatchupInterval: while the stream works through a backlog the delay does
// not matter — the changes are hours old anyway — and batches do: an insert of two rows costs the same
// round-trip and the same fsync as an insert of a thousand.
func TestWithABacklogRowsWaitTheCatchupInterval(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.FlushInterval = config.Duration(250 * time.Millisecond)
	h.cfg.Buffer.CatchupFlushInterval = config.Duration(5 * time.Second)
	items := itemsTable()
	// Transactions committed hours ago: a backlog, as after an outage or during an initial load.
	old := epoch.Add(-3 * time.Hour)
	h.source.script = []scriptedTx{
		txAt(1, old, insertRows(items, 1, "a")),
		txAt(2, old, insertRows(items, 2, "b")),
		txAt(3, old, insertRows(items, 3, "c")),
	}
	h.source.beforeHandle = func(ev event.Event) {
		if _, ok := ev.(event.Commit); ok {
			h.clock.Advance(300 * time.Millisecond) // past the ordinary interval, under the catch-up one
		}
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	// One insert, at the graceful stop, holding all three transactions: the interval never came due.
	if got := entriesWithPrefix(h.journal.all(), "insert"); !reflect.DeepEqual(got, []string{"insert shop.items x3"}) {
		t.Errorf("inserts = %v, want one batch of three rows: a backlog must be written in batches", got)
	}
}

// TestAHeartbeatMeansTheHeadHoweverOldTheLastTransactionIs: a source that writes rarely has a last
// transaction that is hours old, which is not a backlog — it is simply quiet. The heartbeat says so,
// and a change made on such a source must not wait the catch-up interval.
func TestAHeartbeatMeansTheHeadHoweverOldTheLastTransactionIs(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.FlushInterval = config.Duration(250 * time.Millisecond)
	h.cfg.Buffer.CatchupFlushInterval = config.Duration(5 * time.Second)
	items := itemsTable()
	old := epoch.Add(-3 * time.Hour)
	h.source.script = []scriptedTx{
		{n: 99, events: []event.Event{event.Heartbeat{}}}, // the source has nothing more to send
		txAt(1, old, insertRows(items, 1, "a")),
		{n: 100, events: []event.Event{event.Heartbeat{}}},
	}
	h.source.beforeHandle = func(ev event.Event) {
		if _, ok := ev.(event.Heartbeat); ok {
			h.clock.Advance(300 * time.Millisecond)
		}
	}
	// until runs on the stream's own goroutine after the script, so it sees what was written while the
	// pipeline was still running — before the flush of the graceful stop, which writes everything anyway.
	var whileRunning []string
	h.source.until = func() bool {
		whileRunning = h.journal.all()
		return true
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	// The second heartbeat wrote it: the interval that applied was the ordinary one, not the catch-up
	// one, although the transaction itself was committed three hours ago.
	if got := entriesWithPrefix(whileRunning, "insert"); len(got) != 1 {
		t.Errorf("inserts while the stream ran = %v, want one: a quiet source is the head, not a backlog", got)
	}
	if got := entriesWithPrefix(whileRunning, "save"); len(got) < 2 {
		t.Errorf("saves while the stream ran = %v, want the initial one and the one after the change", got)
	}
}
