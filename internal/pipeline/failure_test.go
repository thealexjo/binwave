package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/gtid"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// failingRename is a statement whose change ClickHouse cannot follow: a replicated table replaced by
// one that binwave never saw.
const failingRename = "RENAME TABLE other_db.tmp TO shop.items"

func failureOf(t *testing.T, err error) failure.Failure {
	t.Helper()
	var fe *FailedError
	if !errors.As(err, &fe) {
		t.Fatalf("Run() = %v, want a *FailedError", err)
	}
	return fe.Failure
}

func TestFailureOfAStatementNamesTheTransactionAndTheStatement(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a")), tx(2, ddlEvent(failingRename))}

	f := failureOf(t, h.run(t))

	if f.Kind != failure.KindDDL || f.GTID != "u:2" || f.Statement != failingRename || f.Time.IsZero() {
		t.Errorf("failure = %+v", f)
	}
	if !f.Skippable() {
		t.Error("a failure inside a transaction can be skipped")
	}
	// The position is before the failed transaction: resuming tries it again.
	if pos := h.store.current(); pos.GTIDSet != "u:1-1" {
		t.Errorf("position = %q, want u:1-1", pos.GTIDSet)
	}
}

// TestFailureOfARowNamesTheKeyButNoValueInTheMessage: the values of a row may be personal data.
func TestFailureOfARowNamesTheKeyButNoValueInTheMessage(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	items := itemsTable()
	// The name column takes a string; the row carries a number, which cannot be converted.
	bad := event.Rows{Table: items, Op: event.Insert, Rows: []event.Row{
		{After: []any{int32(1), "fine"}},
		{After: []any{int32(7), 987654}},
	}}
	h.source.script = []scriptedTx{tx(1, bad)}

	f := failureOf(t, h.run(t))

	if f.Kind != failure.KindRows || f.Table != "shop.items" || f.Op != "insert" || f.Rows != 2 || f.GTID != "u:1" {
		t.Errorf("failure = %+v", f)
	}
	if f.Key != "PRIMARY KEY (id)" || !reflect.DeepEqual(f.KeyValues, []string{"7"}) {
		t.Errorf("key %q values %v: the failing row is the second one", f.Key, f.KeyValues)
	}
	if strings.Contains(f.Message, "987654") {
		t.Errorf("the message carries a value: %s", f.Message)
	}
	if !strings.Contains(f.Detail, "987654") {
		t.Errorf("the detail lacks the value the operator needs: %s", f.Detail)
	}
	red := f.Redacted()
	if red.KeyValues != nil || red.Detail != "" || red.Key != f.Key {
		t.Errorf("redacted = %+v", red)
	}
}

func TestFailureOfAWriteIsTiedToNoTransaction(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.OnMissingStructure = "stop"
	h.eagerFlush()
	missing := &chsink.TableError{Table: "shop.items", Missing: true, Err: errors.New("table is gone")}
	h.sink.insertErrs = map[int]error{1: missing}
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}

	f := failureOf(t, h.run(t))

	if f.Kind != failure.KindWrite || f.GTID != "" || f.Table != "shop.items" || f.Skippable() {
		t.Errorf("failure = %+v: a write of buffered rows belongs to no single transaction", f)
	}
}

func TestInfrastructureFailuresAreNotFailures(t *testing.T) {
	for _, err := range []error{errors.New("connection reset by the source"), context.Canceled, ErrOverloaded, syscall.ECONNRESET} {
		if isDataFailure(err) {
			t.Errorf("%v was taken for a failure of the data", err)
		}
	}
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"))}
	h.sink.insertErrs = map[int]error{1: syscall.ECONNRESET}
	h.eagerFlush()
	// A transient failure is retried, never turned into a failed state.
	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}
}

// TestSkipTransactionsDropsTheFailedOneAndGoesOn: after the failure the operator skips one transaction;
// the position moves past it, its changes are not applied, and the rest replicates.
func TestSkipTransactionsDropsTheFailedOneAndGoesOn(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{
		tx(1, insertRows(itemsTable(), 1, "a")),
		tx(2, ddlEvent(failingRename)),
		tx(3, insertRows(itemsTable(), 3, "c")),
	}
	p := h.pipeline(t)
	if f := failureOf(t, h.runPipeline(t, p)); f.GTID != "u:2" {
		t.Fatalf("failure = %+v", f)
	}

	p.SetSkip(Skip{Count: 1})
	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("after the skip: %v", err)
	}

	got := h.sink.visibleItems()
	if len(got) != 2 {
		t.Errorf("rows %v: transaction 1 and 3 must be applied", got)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-3" {
		t.Errorf("position = %q, want u:1-3", pos.GTIDSet)
	}
}

func TestSkipByGTIDDropsThatTransactionOnly(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(itemsTable(), 1, "a")),
		uuidTx(2, insertRows(itemsTable(), 2, "b")),
		uuidTx(3, insertRows(itemsTable(), 3, "c")),
	}
	set, err := gtid.Parse(testUUID + ":2")
	if err != nil {
		t.Fatal(err)
	}
	p := h.pipeline(t)
	p.SetSkip(Skip{GTIDs: set})

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	if got := h.sink.visibleItems(); len(got) != 2 {
		t.Errorf("rows %v, want transactions 1 and 3", got)
	}
}

func TestSkippedTransactionsAreCountedAndLogged(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	logger, logs, mu := captureLogger()
	h.logger = logger
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a")), tx(2, insertRows(itemsTable(), 2, "b"))}
	p := h.pipeline(t)
	p.SetSkip(Skip{Count: 2})

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Count(logs.String(), "transaction skipped by operator") != 2 {
		t.Errorf("the log:\n%s", logs.String())
	}
	if n := strings.Count(strings.Join(obs.sequence("skipped-transaction"), ","), "skipped-transaction"); n != 2 {
		t.Errorf("observer saw %d skipped transactions, want 2", n)
	}
	if len(h.sink.visibleItems()) != 0 {
		t.Error("a skipped transaction was applied")
	}
}

// TestRowsOfAFailedTransactionAreNotWritten: a transaction is applied whole or, when it fails, not
// at all, so that skipping it leaves nothing of it in ClickHouse.
func TestRowsOfAFailedTransactionAreNotWritten(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	items := itemsTable()
	bad := event.Rows{Table: items, Op: event.Insert, Rows: []event.Row{{After: []any{int32(9), 12345}}}}
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		tx(2, insertRows(items, 2, "half of it"), bad),
	}
	p := h.pipeline(t)

	f := failureOf(t, h.runPipeline(t, p))

	if f.GTID != "u:2" {
		t.Fatalf("failure = %+v", f)
	}
	if got := h.sink.visibleItems(); len(got) != 1 {
		t.Errorf("rows %v: only transaction 1 was committed", got)
	}
}

// TestSkipOfARedeliveredTransactionDropsTheRowsOfEveryAttempt: the rows of the attempt before a
// reconnect are still buffered when the replay fails; skipping the transaction must leave none of it.
func TestSkipOfARedeliveredTransactionDropsTheRowsOfEveryAttempt(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	items := itemsTable()
	bad := event.Rows{Table: items, Op: event.Insert, Rows: []event.Row{{After: []any{int32(2), 987654}}}}
	h.source.script = []scriptedTx{
		{n: 1, events: []event.Event{
			event.Begin{GTID: "u:1", CommitTime: epoch.Add(time.Second)},
			insertRows(items, 1, "first attempt"),
			event.Begin{GTID: "u:1", CommitTime: epoch.Add(time.Second)},
			insertRows(items, 1, "replay"),
			bad,
			event.Commit{GTIDSet: "u:1-1"},
		}},
		tx(2, insertRows(items, 3, "c")),
	}
	p := h.pipeline(t)
	if f := failureOf(t, h.runPipeline(t, p)); f.GTID != "u:1" {
		t.Fatalf("failure = %+v", f)
	}

	p.SetSkip(Skip{Count: 1})
	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("after the skip: %v", err)
	}

	if got := h.sink.visibleItems(); len(got) != 1 || got["3|"] == nil {
		t.Errorf("rows %v, want only transaction 2: the skipped one is half applied", got)
	}
}
