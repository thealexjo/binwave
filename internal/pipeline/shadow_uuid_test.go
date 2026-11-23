package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

const savedUUID = "00000000-0000-4000-8000-0000000000aa"

// madeShadow records, at every Ensure of the shadow table, the UUID it is made or taken with and the one
// the saved position has for it at that moment.
type madeShadow struct{ uuid, saved string }

func recordShadows(t *testing.T, h *harness) *[]madeShadow {
	t.Helper()
	var made []madeShadow
	h.sink.onEnsure = func(tb *chsink.Table) {
		if tb.Name != "items__resync" {
			return
		}
		entries, err := checkpoint.DecodeResyncs(h.store.current().Resync)
		if err != nil {
			t.Error(err)
		}
		saved := ""
		for _, e := range entries {
			if e.Table == "shop.items" {
				saved = e.ShadowUUID
			}
		}
		made = append(made, madeShadow{uuid: tb.UUID, saved: saved})
	}
	return &made
}

// keepSavedUUIDs fails the test when a position is saved with another UUID for the shadow table of
// shop.items than that of the table under its name: a crash right after the save would find a table the
// resync does not know for its own.
func keepSavedUUIDs(t *testing.T, h *harness) {
	t.Helper()
	h.store.onSave = func(pos checkpoint.Position) {
		entries, err := checkpoint.DecodeResyncs(pos.Resync)
		if err != nil {
			t.Error(err)
		}
		h.sink.mu.Lock()
		there, uuid := h.sink.shapes["shop.items__resync"].Exists, h.sink.uuids["shop.items__resync"]
		h.sink.mu.Unlock()
		for _, e := range entries {
			if e.Table == "shop.items" && there && uuid != "" && e.ShadowUUID != uuid {
				t.Errorf("a position was saved with the UUID %s for the shadow table, which is there with %s", e.ShadowUUID, uuid)
			}
		}
	}
}

// TestAShadowTableIsMadeOnlyWithAUUIDSavedBeforeIt: wherever a restart makes a shadow table, the position
// saved before has the UUID it is made with, so a crash right after finds the table and knows it for the
// shadow table. A shadow table made again gets a new UUID: ClickHouse keeps that of a dropped table for a
// while, and refuses a table made with it.
func TestAShadowTableIsMadeOnlyWithAUUIDSavedBeforeIt(t *testing.T) {
	cursor, err := checkpoint.EncodeCursor([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	reading := checkpoint.Resync{Table: "shop.items", Shadow: "items__resync", ShadowUUID: savedUUID, Version: 1, Since: fence(1), Cursor: cursor, Rows: 1}
	notRead := reading
	notRead.Cursor, notRead.Rows = nil, 0
	pending := checkpoint.Resync{Table: "shop.items", Shadow: "items__resync", ShadowUUID: savedUUID, Version: 1, Since: fence(1), Pending: true}
	mismatch := &chsink.SchemaMismatchError{Table: "`replica`.`items__resync`", Problems: []string{"column name is missing"}}
	tests := map[string]struct {
		entry    *checkpoint.Resync // nil: the initial load
		shadow   bool               // the shadow table is there, made with the saved UUID
		mismatch bool               // and does not match the structure
		held     bool               // ClickHouse holds the saved UUID: the table made with it was dropped
		renewed  bool               // the shadow table is made with another UUID than the saved one
		refused  bool               // ClickHouse refuses the UUID first: nothing says the table was made
	}{
		"the initial load": {},
		"a resync whose shadow table was not made yet":    {entry: &notRead},
		"a resync whose shadow table was dropped at once": {entry: &notRead, held: true, renewed: true, refused: true},
		"a resync whose shadow table is gone":             {entry: &reading, held: true, renewed: true},
		"a resync whose table changed while stopped":      {entry: &reading, shadow: true, mismatch: true, renewed: true},
		"a resync that begins again":                      {entry: &pending, held: true, renewed: true},
		"a resync that begins again, its shadow made":     {entry: &pending, shadow: true, renewed: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			items := itemsTable()
			setup := func(pos *checkpoint.Position) *harness {
				h := newHarness(t)
				h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, executed: fence(1)}
				if pos == nil {
					h.cfg.Replication.InitialPosition = config.StartFromSnapshot
				} else {
					h.store.pos, h.store.found = *pos, true
				}
				h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 3, "c"))}
				h.source.until = func() bool { return h.sink.swapped() > 0 }
				keepSavedUUIDs(t, h)
				return h
			}
			check := func(h *harness, when string) {
				t.Helper()
				want := map[string]string{"1|": "a", "2|": "b", "3|": "c"}
				if got := live(t, h, "shop.items"); !equal(got, want) {
					t.Errorf("%s: rows = %v, want %v", when, got, want)
				}
			}

			var pos *checkpoint.Position
			if tt.entry != nil {
				pos = &checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: checkpoint.EncodeResyncs([]checkpoint.Resync{*tt.entry})}
			}
			h := setup(pos)
			if tt.shadow {
				h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
				h.sink.uuids = map[string]string{"shop.items__resync": savedUUID}
			}
			if tt.mismatch {
				h.sink.ensureErrs = map[int]error{1: mismatch}
			}
			if tt.held {
				h.sink.held = map[string]bool{savedUUID: true}
			}
			made := recordShadows(t, h)
			var atMade *checkpoint.Position
			onEnsure := h.sink.onEnsure
			h.sink.onEnsure = func(tb *chsink.Table) {
				onEnsure(tb)
				if tb.Name == "items__resync" && atMade == nil {
					p := h.store.current()
					atMade = &p
				}
			}
			if err := h.run(t); err != nil {
				t.Fatalf("Run() = %v", err)
			}
			check(h, "after the restart")
			if len(*made) == 0 {
				t.Fatal("no shadow table was made")
			}
			for _, m := range *made {
				if m.uuid == "" || m.uuid != m.saved {
					t.Errorf("the shadow table was made or taken with the UUID %q, and the saved position has %q", m.uuid, m.saved)
				}
			}
			if first := (*made)[0].uuid; tt.entry != nil && tt.renewed == (first == savedUUID) {
				t.Errorf("the shadow table was made with %s (the saved UUID %s): want a new one %v", first, savedUUID, tt.renewed)
			}
			if refused := h.sink.taken > 0; refused != tt.refused {
				// A shadow table known to be made again is made with a new UUID at once.
				t.Errorf("ClickHouse refused the UUID %d times, want refused %v", h.sink.taken, tt.refused)
			}

			// A crash right after the shadow table was made: it is there, with the UUID saved.
			again := setup(atMade)
			again.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
			again.sink.uuids = map[string]string{"shop.items__resync": (*made)[0].uuid}
			if err := again.run(t); err != nil {
				t.Fatalf("Run() after a crash right after the shadow table was made = %v", err)
			}
			check(again, "after a crash right after the shadow table was made")
		})
	}
}

// TestAResyncBegunAgainByAStatementMakesItsShadowTableWithTheUUIDSavedBeforeIt: a statement discards a
// resync and it begins again. The new shadow table is made with the UUID the entry saved before it has,
// a new one: the dropped shadow table had the first.
func TestAResyncBegunAgainByAStatementMakesItsShadowTableWithTheUUIDSavedBeforeIt(t *testing.T) {
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
	h.source.until = func() bool { return h.sink.swapped() > 0 }
	made := recordShadows(t, h)

	if err := h.runPipeline(t, p); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	var created []string
	for _, m := range *made {
		if len(created) == 0 || created[len(created)-1] != m.uuid {
			created = append(created, m.uuid)
		}
	}
	if len(created) != 2 || created[0] == created[1] {
		t.Fatalf("shadow tables made with %v, want two of two UUIDs", created)
	}
	// The first is made before the resync is saved at all (a tables resync); the one it begins again with
	// is saved, pending, before.
	for _, m := range (*made)[1:] {
		if m.uuid == created[1] && m.saved != m.uuid {
			t.Errorf("the shadow table begun again was made or taken with %s, and the saved position has %q", m.uuid, m.saved)
		}
	}
}

// TestARestartAfterASwapWhoseEndWasNotSavedFinishesTheResync: the swap was finished, the shadow table in
// the place of the table without the marker, and a crash came before the position said so. The UUID the
// resync saved tells the table is its shadow table: the resync ends, not reads the table again. Saved
// without the UUID (an earlier release), nothing tells it, and the table is read again.
func TestARestartAfterASwapWhoseEndWasNotSavedFinishesTheResync(t *testing.T) {
	for _, known := range []bool{true, false} {
		items := itemsTable()
		h := newHarness(t)
		snap := &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
		h.snapshot = snap
		entry := checkpoint.Resync{Table: "shop.items", Shadow: "items__resync", Version: 1, Since: fence(1), Rows: 1, Done: true, Final: fence(1)}
		if known {
			entry.ShadowUUID = savedUUID
		}
		h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: checkpoint.EncodeResyncs([]checkpoint.Resync{entry})}, true
		h.sink.uuids = map[string]string{"shop.items": savedUUID}
		h.sink.tables["shop.items"] = map[string]cell{"1|": {version: 1, values: []any{int64(1), "a"}}}
		h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 2, "b"))}
		h.source.until = func() bool { return h.store.current().Resync == "" }

		if err := h.run(t); err != nil {
			t.Fatalf("UUID saved %v: Run() = %v", known, err)
		}

		if reads := snap.reads; known != (reads == 0) {
			t.Errorf("UUID saved %v: the table was read %d times", known, reads)
		}
		want := map[string]string{"1|": "a", "2|": "b"}
		if got := live(t, h, "shop.items"); !equal(got, want) {
			t.Errorf("UUID saved %v: rows = %v, want %v", known, got, want)
		}
		if pos := h.store.current(); pos.Resync != "" {
			t.Errorf("UUID saved %v: the resync is still saved: %s", known, pos.Resync)
		}
	}
}

// TestAShadowTableOfAnotherUUIDIsNeitherTakenNorDropped: a table carries the marker of the resync under
// the name of its shadow table, with another UUID than the one the resync made its shadow table with.
// It is not the shadow table: a restart stops and does not write to it, and dropping the shadow table
// leaves it.
func TestAShadowTableOfAnotherUUIDIsNeitherTakenNorDropped(t *testing.T) {
	const other = "00000000-0000-4000-8000-0000000000bb"
	cursor, err := checkpoint.EncodeCursor([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	entry := checkpoint.Resync{Table: "shop.items", Shadow: "items__resync", ShadowUUID: savedUUID, Version: 1, Since: fence(1), Cursor: cursor, Rows: 1}
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, executed: fence(1)}
	h.store.pos, h.store.found = checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: checkpoint.EncodeResyncs([]checkpoint.Resync{entry})}, true
	h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
	h.sink.marked = map[string]bool{"shop.items__resync": true}
	h.sink.uuids = map[string]string{"shop.items__resync": other}
	h.sink.tables["shop.items__resync"] = map[string]cell{"9|": {version: 1, values: []any{int64(9), "not binwave's"}}}

	err = h.run(t)

	if got, want := live(t, h, "shop.items__resync"), map[string]string{"9|": "not binwave's"}; !equal(got, want) {
		t.Errorf("the table under the name of the shadow table holds %v, want %v (Run() = %v)", got, want, err)
	}
	if len(h.sink.drops) != 0 || h.sink.swapped() != 0 {
		t.Errorf("drops %v, swaps %d: want the table left (Run() = %v)", h.sink.drops, h.sink.swapped(), err)
	}

	p := h.pipeline(t)
	if dropped, err := p.dropShadow(context.Background(), entry); err != nil || dropped || len(h.sink.drops) != 0 {
		t.Errorf("dropping the shadow table of UUID %s = %v, %v: dropped the table of UUID %s", savedUUID, dropped, err, other)
	}
	h.sink.uuids["shop.items__resync"] = savedUUID
	if dropped, err := p.dropShadow(context.Background(), entry); err != nil || !dropped || len(h.sink.drops) != 1 {
		t.Errorf("dropping the shadow table of UUID %s = %v, %v, drops %v: want it dropped", savedUUID, dropped, err, h.sink.drops)
	}
}

// TestAShadowTableThatCouldNotBeDroppedKeepsItsUUIDSaved: a restart is to drop the shadow table of a resync
// and make it again (the table changed while stopped, the resync begins again), and ClickHouse refuses the
// drop. The entry stays saved as it was, with the UUID of the table that is still there, and a restart
// once the drop passes continues the resync.
func TestAShadowTableThatCouldNotBeDroppedKeepsItsUUIDSaved(t *testing.T) {
	cursor, err := checkpoint.EncodeCursor([]any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	reading := checkpoint.Resync{Table: "shop.items", Shadow: "items__resync", ShadowUUID: savedUUID, Version: 1, Since: fence(1), Cursor: cursor, Rows: 1}
	pending := checkpoint.Resync{Table: "shop.items", Shadow: "items__resync", ShadowUUID: savedUUID, Version: 1, Since: fence(1), Pending: true}
	mismatch := &chsink.SchemaMismatchError{Table: "`replica`.`items__resync`", Problems: []string{"column name is missing"}}
	refused := errors.New("the rename was refused")
	for name, tt := range map[string]struct {
		entry    checkpoint.Resync
		mismatch bool
	}{
		"the table changed while stopped": {reading, true},
		"a resync that begins again":      {pending, false},
	} {
		t.Run(name, func(t *testing.T) {
			items := itemsTable()
			saved := checkpoint.Position{Tables: "{}", GTIDSet: fence(1), Version: 1, Resync: checkpoint.EncodeResyncs([]checkpoint.Resync{tt.entry})}
			setup := func(dropErr error) *harness {
				h := newHarness(t)
				h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, executed: fence(1)}
				h.store.pos, h.store.found = saved, true
				h.sink.shapes = map[string]chsink.TableShape{"shop.items__resync": {Exists: true}}
				h.sink.uuids = map[string]string{"shop.items__resync": savedUUID}
				h.sink.dropErr = dropErr
				if tt.mismatch {
					h.sink.ensureErrs = map[int]error{1: mismatch}
				}
				h.source.script = []scriptedTx{uuidTx(2, insertRows(items, 3, "c"))}
				h.source.until = func() bool { return h.sink.swapped() > 0 }
				keepSavedUUIDs(t, h)
				return h
			}

			h := setup(refused)
			if err := h.run(t); !errors.Is(err, refused) {
				t.Fatalf("Run() = %v, want the drop that was refused", err)
			}
			if got := h.store.current().Resync; got != saved.Resync {
				t.Errorf("saved resyncs %s, want them as they were: %s", got, saved.Resync)
			}
			if !h.sink.shapes["shop.items__resync"].Exists || h.sink.uuids["shop.items__resync"] != savedUUID {
				t.Errorf("the shadow table is not there as it was: %v, %v", h.sink.shapes, h.sink.uuids)
			}

			again := setup(nil)
			if err := again.run(t); err != nil {
				t.Fatalf("Run() once the drop passes = %v", err)
			}
			if got, want := live(t, again, "shop.items"), map[string]string{"1|": "a", "2|": "b", "3|": "c"}; !equal(got, want) {
				t.Errorf("rows = %v, want %v", got, want)
			}
		})
	}
}

// TestAResyncDiscardedByAStatementKeepsTheUUIDOfAShadowTableThatCouldNotBeDropped: a statement discards a
// resync and ClickHouse refuses the drop of its shadow table. The resync, saved as one to begin again,
// keeps the UUID of that table: it cannot begin while the table is there, and begun again after a
// restart, it drops the table as its own.
func TestAResyncDiscardedByAStatementKeepsTheUUIDOfAShadowTableThatCouldNotBeDropped(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Tables.OnReplicationError = config.OnErrorStop
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
		if b, ok := ev.(event.Begin); ok && b.GTID == testUUID+":3" {
			h.sink.mu.Lock()
			h.sink.dropErr = errors.New("the rename was refused")
			h.sink.mu.Unlock()
		}
	}
	keepSavedUUIDs(t, h)

	resyncFailure(t, h.runPipeline(t, p), "shop.items")

	h.sink.mu.Lock()
	there, uuid := h.sink.shapes["shop.items__resync"].Exists, h.sink.uuids["shop.items__resync"]
	h.sink.mu.Unlock()
	st, ok := savedResync(t, h)
	if !there || !ok || !st.Pending || st.ShadowUUID != uuid {
		t.Errorf("saved %+v (%v), the shadow table there %v with %s: want the resync to begin again, with the UUID of that table", st, ok, there, uuid)
	}
}
