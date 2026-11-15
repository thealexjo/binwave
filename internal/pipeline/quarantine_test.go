package pipeline

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/preflight"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// nullIDInsert builds a change of shop.items that cannot be converted: NULL in the NOT NULL id.
func nullIDInsert() event.Rows {
	return event.Rows{Table: itemsTable(), Op: event.Insert, Rows: []event.Row{{After: []any{nil, "x"}}}}
}

// testUUID makes GTIDs the fence can parse; the other fakes use the shorthand "u:N".
const testUUID = "6d4d8f60-1111-2222-3333-444455556666"

// uuidTx is tx with a real GTID: transaction n of testUUID.
func uuidTx(n int, items ...event.Event) scriptedTx {
	gtid := fmt.Sprintf("%s:%d", testUUID, n)
	evs := []event.Event{event.Begin{GTID: gtid, CommitTime: epoch.Add(time.Duration(n) * time.Second)}}
	evs = append(evs, items...)
	evs = append(evs, event.Commit{GTIDSet: fmt.Sprintf("%s:1-%d", testUUID, n)})
	return scriptedTx{n: n, events: evs}
}

func fence(n int) string { return fmt.Sprintf("%s:1-%d", testUUID, n) }

func quarantinedNames(pos checkpoint.Position) []string {
	entries, _ := checkpoint.DecodeQuarantine(pos.Quarantine)
	var names []string
	for _, e := range entries {
		names = append(names, e.Table)
	}
	return names
}

func TestConversionErrorQuarantinesTheTableAndTheOthersGoOn(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	items, other := itemsTable(), otherTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a"), insertRows(other, 1, "x")),
		// In one transaction: a change that cannot be converted, and a good change of another table.
		tx(2, nullIDInsert(), insertRows(other, 2, "y")),
		tx(3, insertRows(items, 3, "c"), insertRows(other, 3, "z")),
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v, want the service to go on", err)
	}

	if got := len(h.sink.liveRows("shop.other")); got != 3 {
		t.Errorf("the healthy table holds %d rows, want all 3: one table must not stop another", got)
	}
	// The quarantined table keeps what it had before, and nothing after.
	if got := len(h.sink.liveRows("shop.items")); got > 1 {
		t.Errorf("the quarantined table holds %d rows: its changes after the quarantine must be dropped", got)
	}
	pos := h.store.current()
	if pos.GTIDSet != "u:1-3" {
		t.Errorf("position = %q, want u:1-3: the position moves on past the quarantined table's changes", pos.GTIDSet)
	}
	if got := quarantinedNames(pos); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("the saved state quarantines %v, want shop.items", got)
	}
	entries, _ := checkpoint.DecodeQuarantine(pos.Quarantine)
	if !strings.Contains(entries[0].Reason, "id") || entries[0].Since != "u:1-1" {
		t.Errorf("entry = %+v, want the reason to name the column and the position at that moment", entries[0])
	}
	if obs.quarantined != 1 || len(obs.sequence("skipped shop.items")) != 1 {
		t.Errorf("observer: quarantined = %d, skipped = %v", obs.quarantined, obs.sequence("skipped"))
	}
}

func TestConversionErrorStopsTheServiceWithThePolicyStop(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	h.source.script = []scriptedTx{tx(1, insertRows(otherTable(), 1, "x")), tx(2, nullIDInsert())}

	err := h.run(t)

	if err == nil || strings.Contains(err.Error(), "script ended") {
		t.Fatalf("Run() = %v, want the conversion error", err)
	}
	if got := quarantinedNames(h.store.current()); len(got) != 0 {
		t.Errorf("quarantined %v under the stop policy", got)
	}
}

// TestQuarantineSurvivesARestart: a restarted process reads the list with the position and goes
// on dropping the table's changes; the table is not quietly taken back into replication.
func TestQuarantineSurvivesARestart(t *testing.T) {
	h := newHarness(t)
	items, other := itemsTable(), otherTable()
	h.source.script = []scriptedTx{
		tx(1, nullIDInsert(), insertRows(other, 1, "x")),
	}
	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	// Restart on the same store; the source now has more changes, including a good one of the quarantined table.
	h2 := newHarness(t)
	h2.store = h.store
	obs := &fakeObserver{}
	h2.obs = obs
	h2.source.script = []scriptedTx{tx(1), tx(2, insertRows(items, 5, "fine"), insertRows(other, 2, "y"))}
	if err := h2.run(t); err != nil {
		t.Fatal(err)
	}

	if got := len(h2.sink.liveRows("shop.items")); got != 0 {
		t.Errorf("the table written after a restart although it is quarantined: %v", h2.sink.liveRows("shop.items"))
	}
	if got := len(h2.sink.liveRows("shop.other")); got != 1 {
		t.Errorf("the healthy table holds %d rows after the restart, want 1", got)
	}
	if got := quarantinedNames(h2.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("the state after the restart quarantines %v", got)
	}
	if obs.quarantined != 1 {
		t.Errorf("the observer was told of %d quarantined tables at start", obs.quarantined)
	}
}

func TestSchemaMismatchQuarantinesTheTable(t *testing.T) {
	h := newHarness(t)
	h.sink.ensureErrs = map[int]error{1: &chsink.SchemaMismatchError{Table: "`replica`.`items`", Problems: []string{"column name is missing"}}}
	h.source.script = []scriptedTx{
		tx(1, insertRows(itemsTable(), 1, "a")),
		tx(2, insertRows(otherTable(), 1, "x")),
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v, want the service to go on", err)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("quarantined %v", got)
	}
	if len(h.sink.liveRows("shop.items")) != 0 || len(h.sink.liveRows("shop.other")) != 1 {
		t.Errorf("items %v, other %v", h.sink.liveRows("shop.items"), h.sink.liveRows("shop.other"))
	}
}

func TestGuardProblemQuarantinesTheTableBeforeAnythingIsWritten(t *testing.T) {
	h := newHarness(t)
	h.guard = &fakeGuard{reject: map[string]error{"shop.items": errors.New("no key")}}
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"), insertRows(otherTable(), 1, "x"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("quarantined %v", got)
	}
	for _, e := range h.sink.ensures {
		if strings.Contains(e, "items") {
			t.Errorf("the quarantined table was prepared in ClickHouse: %v", h.sink.ensures)
		}
	}
	if len(h.sink.liveRows("shop.other")) != 1 {
		t.Error("the healthy table was not written")
	}
}

// TestInsertThatFindsTheTableGoneQuarantinesIt: a column or the table vanished from ClickHouse
// after binding. The rows of that table are dropped, the rest of the flush goes through.
func TestInsertThatFindsTheTableGoneQuarantinesIt(t *testing.T) {
	h := newHarness(t)
	h.sink.insertErrsByTable = map[string]*tableFailure{
		"shop.items": failsForever(&chsink.TableError{Table: "shop.items", Err: errors.New("code: 60, message: Table replica.items does not exist")}),
	}
	items, other := itemsTable(), otherTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a"), insertRows(other, 1, "x")), // the insert of items fails
		tx(2, insertRows(items, 2, "b"), insertRows(other, 2, "y")),
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if got := len(h.sink.liveRows("shop.other")); got != 2 {
		t.Errorf("the healthy table holds %d rows, want 2", got)
	}
	if got := len(h.sink.liveRows("shop.items")); got != 0 {
		t.Errorf("the broken table holds %d rows", got)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("quarantined %v", got)
	}
	if h.store.current().GTIDSet != "u:1-2" {
		t.Errorf("position = %q: the position must move past the dropped rows", h.store.current().GTIDSet)
	}
}

func TestInsertProblemOfATableStopsTheServiceWithThePolicyStop(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	h.sink.insertErrs = map[int]error{1: &chsink.TableError{Table: "shop.items", Err: errors.New("table gone")}}
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}

	err := h.run(t)

	var te *chsink.TableError
	if !errors.As(err, &te) {
		t.Fatalf("Run() = %v, want the table error", err)
	}
}

// TestNetworkFailureNeverQuarantines: an outage is the service's problem, it is retried and not
// blamed on a table.
func TestNetworkFailureNeverQuarantines(t *testing.T) {
	h := newHarness(t)
	h.sink.insertErrs = map[int]error{1: errors.New("connection reset by peer")}
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}

	err := h.run(t)

	if err == nil {
		t.Fatal("Run() succeeded although the insert failed with an unclassified error")
	}
	if got := quarantinedNames(h.store.current()); len(got) != 0 {
		t.Errorf("quarantined %v for an error that is not a table's", got)
	}
}

// TestQuarantineFromAnotherGoroutineIsSavedWithThePosition is what the checks after start do:
// they find a problem of a table while the stream is being read.
func TestQuarantineFromAnotherGoroutineIsSavedWithThePosition(t *testing.T) {
	h := newHarness(t)
	items, other := itemsTable(), otherTable()
	p := h.pipeline(t)
	h.source.beforeHandle = func(ev event.Event) {
		if b, ok := ev.(event.Begin); ok && b.GTID == "u:2" {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { defer wg.Done(); p.Quarantine("shop.items", errcode.TBL101, "no key found after start") }()
			wg.Wait()
		}
	}
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a"), insertRows(other, 1, "x")),
		tx(2, insertRows(items, 2, "b"), insertRows(other, 2, "y")),
	}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}

	if got := len(h.sink.liveRows("shop.items")); got != 1 {
		t.Errorf("shop.items holds %d rows, want only the one from before the quarantine", got)
	}
	if got := len(h.sink.liveRows("shop.other")); got != 2 {
		t.Errorf("shop.other holds %d rows, want 2", got)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("the saved state quarantines %v", got)
	}
}

func TestQuarantineIsIgnoredWithThePolicyStop(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	p := h.pipeline(t)
	if p.Quarantine("shop.items", errcode.TBL101, "x") {
		t.Error("Quarantine() reported success under the stop policy: the caller must stop the service itself")
	}
	if skip, _, _ := p.q.skip("shop.items", "u:1"); skip {
		t.Error("the table was quarantined under the stop policy")
	}
}

func TestTheSameTableIsQuarantinedOnce(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "first")
	p.Quarantine("shop.items", errcode.TBL101, "second")

	entries := p.q.list()
	if len(entries) != 1 || entries[0].Reason != "first" {
		t.Errorf("entries = %+v, want the first reason kept", entries)
	}
	if got := len(obs.sequence("quarantine")); got != 1 {
		t.Errorf("the observer was told %d times", got)
	}
}

// TestBuffersOfAQuarantinedTableAreDropped: rows buffered for the table are not written and do
// not count against the memory limit any more.
func TestBuffersOfAQuarantinedTableAreDropped(t *testing.T) {
	h := newHarness(t)
	h.cfg.Buffer.FlushInterval = config.Duration(time.Hour)
	h.source.script = []scriptedTx{
		tx(1, insertRows(itemsTable(), 1, "a"), insertRows(otherTable(), 1, "x")),
		tx(2, nullIDInsert()),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if got := len(h.sink.liveRows("shop.items")); got != 0 {
		t.Errorf("rows buffered for the table were written: %v", h.sink.liveRows("shop.items"))
	}
}

func TestReleaseFencesATableAndItsLaterChangesAreReplicated(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	items, other := itemsTable(), otherTable()
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "no key")
	p.Quarantine("shop.other", errcode.TBL101, "no key")

	// Released with a fence at u:1-2: the changes of transactions 1 and 2 stay dropped.
	released, unknown := p.Release([]string{"shop.items", "shop.nope"}, fence(2))

	if !reflect.DeepEqual(released, []string{"shop.items"}) || !reflect.DeepEqual(unknown, []string{"shop.nope"}) {
		t.Errorf("released %v, unknown %v", released, unknown)
	}
	if obs.quarantined != 1 {
		t.Errorf("the observer was told of %d quarantined tables, want 1: a released table is not quarantined", obs.quarantined)
	}
	if again, unknown := p.Release([]string{"shop.items"}, fence(9)); len(again) != 0 || len(unknown) != 1 {
		t.Errorf("a table that is already released was released again: %v %v", again, unknown)
	}

	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "before the fix")),
		uuidTx(2, insertRows(items, 2, "also before")),
		uuidTx(3, insertRows(items, 3, "after the release"), insertRows(other, 3, "quarantined still")),
		uuidTx(4, insertRows(items, 4, "and later")),
	}
	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}

	got := h.sink.liveRows("shop.items")
	if len(got) != 2 {
		t.Errorf("the released table holds %d rows, want the two after the fence: %v", len(got), got)
	}
	if len(h.sink.liveRows("shop.other")) != 0 {
		t.Error("a table that was not released was replicated")
	}
	// The fence was passed, so the entry is gone from the saved state; the other table stays.
	if names := quarantinedNames(h.store.current()); !reflect.DeepEqual(names, []string{"shop.other"}) {
		t.Errorf("the saved state holds %v, want only shop.other: the fence of shop.items was passed", names)
	}
}

// TestFenceIsSavedAndSurvivesARestart: a released table that is stopped before its fence is
// passed keeps dropping the changes up to it after the restart.
func TestFenceIsSavedAndSurvivesARestart(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "no key")
	p.Release([]string{"shop.items"}, fence(5))
	h.source.script = []scriptedTx{uuidTx(1, insertRows(items, 1, "before"))}
	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	entries, _ := checkpoint.DecodeQuarantine(h.store.current().Quarantine)
	if len(entries) != 1 || entries[0].Until != fence(5) {
		t.Fatalf("the saved state holds %+v, want the fence kept until the stream passes it", entries)
	}

	// After the restart the source has more: transaction 4 is still inside the fence, 6 is not.
	h2 := newHarness(t)
	h2.store = h.store
	h2.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "before")),
		uuidTx(4, insertRows(items, 4, "inside the fence")),
		uuidTx(6, insertRows(items, 6, "beyond it")),
	}
	if err := h2.run(t); err != nil {
		t.Fatal(err)
	}
	if got := h2.sink.liveRows("shop.items"); len(got) != 1 {
		t.Errorf("the table holds %v after the restart, want only the row beyond the fence", got)
	}
}

func TestReleasedTableThatFailsAgainIsQuarantinedAgain(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "first")
	p.Release([]string{"shop.items"}, fence(5))

	if !p.Quarantine("shop.items", errcode.TBL101, "second failure") {
		t.Fatal("Quarantine() reported failure")
	}

	entries := p.q.active()
	if len(entries) != 1 || entries[0].Reason != "second failure" || entries[0].Released() {
		t.Errorf("active entries = %+v, want the table quarantined again with the new reason", entries)
	}
}

func TestDamagedFenceStopsTheServiceInsteadOfGuessing(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "x")
	p.Release([]string{"shop.items"}, "not a gtid set")
	h.source.script = []scriptedTx{uuidTx(1, insertRows(itemsTable(), 1, "a"))}

	err := h.runPipeline(t, p)

	if err == nil || !strings.Contains(err.Error(), "fence") {
		t.Errorf("Run() = %v, want an error about the damaged fence: skipping or replicating on a guess could lose or duplicate data", err)
	}
}

// TestPipelineCanRunAgainAfterAPause: the same pipeline runs twice on one store, as a paused and
// resumed replication does, and nothing is lost or repeated.
func TestPipelineCanRunAgainAfterAPause(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	p := h.pipeline(t)
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a"))}
	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	if h.store.current().GTIDSet != "u:1-1" {
		t.Fatalf("position after the first run = %q", h.store.current().GTIDSet)
	}

	// Resumed: the source has one more change; the first is not sent again.
	h.source.script = []scriptedTx{tx(1, insertRows(items, 1, "a")), tx(2, insertRows(items, 2, "b"))}
	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}

	if got := len(h.sink.visibleItems()); got != 2 {
		t.Errorf("%d rows after the resume, want 2", got)
	}
	if h.store.current().GTIDSet != "u:1-2" {
		t.Errorf("position = %q", h.store.current().GTIDSet)
	}
	if got := len(h.source.streams); got != 2 || h.source.streams[1] != "u:1-1" {
		t.Errorf("streams started from %v, want the second from the saved position u:1-1", h.source.streams)
	}
}

// TestTablesSetAsideBeforeTheFirstRunAreKeptAndSaved: the checks at start may quarantine a table
// before the stream starts, for example while the service waits paused.
func TestTablesSetAsideBeforeTheFirstRunAreKeptAndSaved(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "found by the checks")
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"), insertRows(otherTable(), 1, "x"))}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}

	if len(h.sink.liveRows("shop.items")) != 0 || len(h.sink.liveRows("shop.other")) != 1 {
		t.Errorf("items %v, other %v", h.sink.liveRows("shop.items"), h.sink.liveRows("shop.other"))
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("the saved state quarantines %v", got)
	}
}

// TestPrepareFixesTheStartPointBeforeThePauseEnds: a service that starts paused decides "now" at
// its start, so what the source does meanwhile is replicated after the resume.
func TestPrepareFixesTheStartPointBeforeThePauseEnds(t *testing.T) {
	h := newHarness(t)
	h.source.executed = "u:1-3"
	p := h.pipeline(t)

	if err := p.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := h.store.current().GTIDSet; got != "u:1-3" {
		t.Fatalf("saved start = %q, want the position of the source when the service started", got)
	}

	// The source moved on while nothing was read; the run starts from the saved point.
	h.source.executed = "u:1-9"
	h.source.script = []scriptedTx{tx(4, insertRows(itemsTable(), 1, "written while paused"))}
	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	if h.source.streams[0] != "u:1-3" {
		t.Errorf("the stream started from %q, want u:1-3", h.source.streams[0])
	}
	if got := len(h.sink.visibleItems()); got != 1 {
		t.Errorf("%d rows, want the one written while paused", got)
	}
}

func TestIdleQuarantineListCanBeLoadedChangedAndSaved(t *testing.T) {
	h := newHarness(t)
	h.store.pos = checkpoint.Position{GTIDSet: "u:1-7", Version: 70, Quarantine: checkpoint.EncodeQuarantine([]checkpoint.Quarantined{{Table: "shop.items", Reason: "r"}})}
	h.store.found = true
	p := h.pipeline(t)
	ctx := t.Context()

	if err := p.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if got := p.Quarantined(); len(got) != 1 || got[0].Table != "shop.items" {
		t.Fatalf("Quarantined() = %+v, want the saved table before the first run", got)
	}
	p.Release([]string{"shop.items"}, "u:1-7")
	if err := p.SaveQuarantine(ctx); err != nil {
		t.Fatal(err)
	}

	pos := h.store.current()
	entries, _ := checkpoint.DecodeQuarantine(pos.Quarantine)
	if pos.GTIDSet != "u:1-7" || pos.Version != 70 || len(entries) != 1 || entries[0].Until != "u:1-7" {
		t.Errorf("saved state = %+v, want the position kept and the table released with its fence", pos)
	}
}

// TestMissingTableStopsTheServiceUnlessQuarantineIsAskedFor: the cost of setting a missing table
// aside is the changes made until it exists, so the service stops by default and loses nothing.
func TestMissingTableStopsTheServiceUnlessQuarantineIsAskedFor(t *testing.T) {
	missing := &chsink.TableError{Table: "shop.items", Missing: true, Err: errors.New("clickhouse table `replica`.`items` does not exist and ddl.dry_run is set")}
	tests := []struct {
		name        string
		onMissing   config.MissingStructurePolicy
		onError     config.ReplicationErrorPolicy
		wantStopped bool
	}{
		{"default", "", "", true},
		{"stop", config.OnMissingStop, config.OnErrorQuarantine, true},
		{"quarantine asked for", config.OnMissingQuarantine, config.OnErrorQuarantine, false},
		{"quarantine asked for but the strict policy rules", config.OnMissingQuarantine, config.OnErrorStop, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			if tt.onMissing != "" {
				h.cfg.DDL.OnMissingStructure = tt.onMissing
			}
			if tt.onError != "" {
				h.cfg.Tables.OnReplicationError = tt.onError
			}
			h.sink.ensureErrs = map[int]error{1: missing}
			h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"), insertRows(otherTable(), 1, "x"))}

			err := h.run(t)

			var te *chsink.TableError
			if stopped := errors.As(err, &te); stopped != tt.wantStopped {
				t.Fatalf("Run() = %v, want stopped = %v", err, tt.wantStopped)
			}
			if tt.wantStopped {
				// Nothing was skipped: the position stays where the change can be read again.
				if got := quarantinedNames(h.store.current()); len(got) != 0 {
					t.Errorf("quarantined %v although the service stopped", got)
				}
				return
			}
			if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
				t.Errorf("quarantined %v, want shop.items", got)
			}
			if len(h.sink.liveRows("shop.other")) != 1 {
				t.Error("the other table was not replicated")
			}
		})
	}
}

// TestTableThatVanishedAfterTheStartFollowsTheSameRule: a table dropped in ClickHouse while
// replication runs is a missing table too.
func TestTableThatVanishedAfterTheStartFollowsTheSameRule(t *testing.T) {
	vanished := &chsink.TableError{Table: "shop.items", Missing: true, Err: errors.New("code: 60, message: Table replica.items does not exist")}

	h := newHarness(t)
	h.sink.insertErrs = map[int]error{1: vanished}
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}
	if err := h.run(t); !chsink.IsMissingTable(err) {
		t.Errorf("Run() = %v, want the service to stop with the missing table by default", err)
	}

	h2 := newHarness(t)
	h2.cfg.DDL.OnMissingStructure = config.OnMissingQuarantine
	h2.sink.insertErrs = map[int]error{1: vanished}
	h2.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}
	if err := h2.run(t); err != nil {
		t.Errorf("Run() = %v, want the table quarantined when that is asked for", err)
	}
}

// TestAGuardProblemUnderStopEndsTheServiceRatherThanFailingReplication: a table's problem found by
// the Guard for a table that appeared after start (as opposed to the checks that ran at start) is
// carried in the err slot, not problem, exactly when tables.on_replication_error is stop (see the
// real Guard, internal/engine/guard.go). It is a property of the table itself, not of one
// transaction, so it must reach the caller as the *preflight.Error it is, never softened into the
// failed state (whose remedy, skipping a transaction, would not help: every future row of the same
// table hits the identical problem).
func TestAGuardProblemUnderStopEndsTheServiceRatherThanFailingReplication(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
	stopErr := &preflight.Error{Problems: []preflight.Problem{
		{Severity: preflight.SevError, Scope: preflight.ScopeTable, Table: "shop.items", Message: "a cascading foreign key"},
	}}
	h.guard = &fakeGuard{stopReject: map[string]error{"shop.items": stopErr}}
	h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"))}

	err := h.run(t)

	var pe *preflight.Error
	if !errors.As(err, &pe) {
		t.Fatalf("Run() = %v, want it to unwrap to a *preflight.Error", err)
	}
	var fe *FailedError
	if errors.As(err, &fe) {
		t.Errorf("Run() = %v, must not be a FailedError: that state's remedy (skip a transaction) cannot fix a property of the table itself", err)
	}
}

// TestATableQuarantinedDuringTheSaveOfTheListIsSavedLater: the list saved is the one encoded before
// the save, so a table set aside meanwhile is left to the next save, which the flag asks for.
func TestATableQuarantinedDuringTheSaveOfTheListIsSavedLater(t *testing.T) {
	for _, saveFails := range []bool{false, true} {
		h := newHarness(t)
		h.store.pos, h.store.found = checkpoint.Position{GTIDSet: "u:1-7", Version: 70}, true
		p := h.pipeline(t)
		ctx := t.Context()
		if err := p.Prepare(ctx); err != nil {
			t.Fatal(err)
		}
		p.quarantine("shop.items", errcode.TBL101, "no primary key", false)
		h.store.onSave = func(checkpoint.Position) {
			h.store.onSave = nil
			p.quarantine("shop.orders", errcode.TBL101, "no primary key", false) // the preflight, meanwhile
		}
		if saveFails {
			h.store.saveErrs = map[int]error{1: errors.New("the store is down")}
			h.store.onSave = nil
		}

		err := p.SaveQuarantine(ctx)

		if saveFails != (err != nil) {
			t.Fatalf("save fails %v: SaveQuarantine() = %v", saveFails, err)
		}
		if !p.qChanged.Load() {
			t.Errorf("save fails %v: the list is marked saved, though it changed during the save or was not saved", saveFails)
		}
	}
}
