package pipeline

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/ddl"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/typemap"
)

var shopItems = ddl.Name{Database: "shop", Table: "items"}

func shapeOf(key []string, cols map[string]string) chsink.TableShape {
	return chsink.TableShape{Exists: true, Key: key, Columns: cols}
}

// unknownKinds says that nothing recorded what the columns were: they are guessed from their types.
func unknownKinds(string) (typemap.Column, bool) { return typemap.Column{}, false }

func retype(name, chType string, kind typemap.Kind) chsink.ColumnPlan {
	return chsink.ColumnPlan{Name: name, Type: chType, Kind: typemap.Column{Name: name, Kind: kind}}
}

func TestPlanKey(t *testing.T) {
	cols := map[string]string{"id": "Int32", "code": "String", "name": "String", "seq": "Int32"}
	tests := []struct {
		name    string
		key     []string
		req     chsink.ChangeRequest
		pk      pkSpec
		marked  bool     // the table carries the marker of a rebuild, its swap not finished
		wantKey []string // nil: no rebuild
		wantErr string
		// wantRecoverable is checked only when wantErr is set: whether tables.autoresync may fix
		// this on its own by resynchronizing the table.
		wantRecoverable bool
		// left is what stays in the change request for the ordinary path.
		leftRename []chsink.ColumnRename
	}{
		{name: "the key the table has needs nothing", key: []string{"id"}, pk: pkSpec{set: true, add: []string{"id"}}},
		{
			name: "the key the table has, the swap of its rebuild not finished, is a rebuild under it that finishes the swap",
			key:  []string{"id"}, pk: pkSpec{set: true, add: []string{"id"}}, marked: true, wantKey: []string{"id"},
		},
		{
			name: "a type of a key column the table has, the swap of its rebuild not finished, finishes the swap",
			key:  []string{"id"}, marked: true, wantKey: []string{"id"},
			req: chsink.ChangeRequest{Modify: []chsink.ColumnPlan{retype("id", "Int32", typemap.KindInt)}},
		},
		{
			name: "another key, the swap of a rebuild not finished, is a rebuild under it",
			key:  []string{"id"}, pk: pkSpec{set: true, add: []string{"code"}}, marked: true, wantKey: []string{"code"},
		},
		{name: "shorter new key is a rebuild: the sort key is the primary key", key: []string{"id", "code"}, pk: pkSpec{set: true, add: []string{"id"}}, wantKey: []string{"id"}},
		{name: "same columns in another order are a rebuild", key: []string{"id", "code"}, pk: pkSpec{set: true, add: []string{"code", "id"}}, wantKey: []string{"code", "id"}},
		{name: "new key the sort key lacks is a rebuild", key: []string{"id"}, pk: pkSpec{set: true, add: []string{"code"}}, wantKey: []string{"code"}},
		{name: "wider new key is a rebuild", key: []string{"id"}, pk: pkSpec{set: true, add: []string{"id", "code"}}, wantKey: []string{"id", "code"}},
		{name: "dropping the key leaves nothing to tell rows apart", key: []string{"id"}, pk: pkSpec{set: true}, wantErr: "no primary key"}, // not Recoverable: an override is the only fix, not a resync
		{
			name: "rename of a key column", key: []string{"id"},
			req:     chsink.ChangeRequest{Rename: []chsink.ColumnRename{{Old: "id", New: "ident"}}},
			wantKey: []string{"ident"},
		},
		{
			name: "rename of another column stays an ordinary change", key: []string{"id"},
			req:        chsink.ChangeRequest{Rename: []chsink.ColumnRename{{Old: "name", New: "title"}}},
			leftRename: []chsink.ColumnRename{{Old: "name", New: "title"}},
		},
		{
			name: "drop of a key column shrinks the key", key: []string{"id", "code"},
			req:     chsink.ChangeRequest{Drop: []string{"code"}},
			wantKey: []string{"id"},
		},
		{
			name: "drop of the only key column", key: []string{"id"},
			req:     chsink.ChangeRequest{Drop: []string{"id"}},
			wantErr: "no primary key",
		},
		{
			name: "wider type of a key column", key: []string{"id"},
			req:     chsink.ChangeRequest{Modify: []chsink.ColumnPlan{retype("id", "Int64", typemap.KindBigInt)}},
			wantKey: []string{"id"},
		},
		{
			name: "same type of a key column is nothing", key: []string{"id"},
			req: chsink.ChangeRequest{Modify: []chsink.ColumnPlan{retype("id", "Int32", typemap.KindInt)}},
		},
		{
			name: "narrower type of a key column is a rebuild that stops at a value that does not fit", key: []string{"id"},
			req:     chsink.ChangeRequest{Modify: []chsink.ColumnPlan{retype("id", "Int16", typemap.KindSmallInt)}},
			wantKey: []string{"id"},
		},
		{
			name: "rename with a wider type: the type is compared with the old column", key: []string{"id"},
			req:     chsink.ChangeRequest{Rename: []chsink.ColumnRename{{Old: "id", New: "ident"}}, Modify: []chsink.ColumnPlan{retype("ident", "Int64", typemap.KindBigInt)}},
			wantKey: []string{"ident"},
		},
		{
			name: "a type that MySQL converts by its own rules is refused", key: []string{"id"},
			req:             chsink.ChangeRequest{Modify: []chsink.ColumnPlan{retype("name", "DateTime64(0, 'UTC')", typemap.KindTimestamp)}},
			wantErr:         "resynchronized",
			wantRecoverable: true, // a resync converts against the type the column has now, not the ALTER's own arithmetic
		},
		{
			name: "a type that needs a copy is a rebuild under the same key, and other columns are not touched", key: []string{"id"},
			req:     chsink.ChangeRequest{Modify: []chsink.ColumnPlan{retype("seq", "Int16", typemap.KindSmallInt)}},
			wantKey: []string{"id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			req := tt.req
			req.Source, req.Database, req.Table = "shop.items", "shop", "items"

			shape := shapeOf(tt.key, cols)
			shape.Marked = tt.marked
			got, err := planKey(cfg, shopItems, shape, &req, tt.pk, unknownKinds, true)

			if tt.wantErr != "" {
				var te *chsink.TableError
				if !errors.As(err, &te) || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("planKey() = %v, want a table problem containing %q", err, tt.wantErr)
				}
				if got := chsink.Recoverable(err); got != tt.wantRecoverable {
					t.Errorf("Recoverable(%v) = %v, want %v", err, got, tt.wantRecoverable)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tt.wantKey == nil {
				if got != nil {
					t.Errorf("planKey() = %+v, want nothing to rebuild", got)
				}
			} else if got == nil || !reflect.DeepEqual(got.Key, tt.wantKey) {
				t.Fatalf("planKey() = %+v, want a rebuild under %v", got, tt.wantKey)
			}
			if got != nil && len(got.Drop) > 0 && len(req.Drop) != 0 {
				t.Errorf("the dropped key column stayed in the ordinary change: %+v", req.Drop)
			}
			if !reflect.DeepEqual(req.Rename, tt.leftRename) {
				t.Errorf("the ordinary change keeps the renames %v, want %v", req.Rename, tt.leftRename)
			}
		})
	}
}

func TestPlanKeyLeavesATableAloneWhenTheOverrideCoversTheNewKey(t *testing.T) {
	cfg := &config.Config{Tables: config.Tables{Overrides: map[string]config.TableOverride{"shop.items": {OrderBy: []string{"id", "code"}}}}}
	req := chsink.ChangeRequest{Source: "shop.items"}

	got, err := planKey(cfg, shopItems, shapeOf([]string{"id", "code"}, map[string]string{"id": "Int32", "code": "String"}), &req, pkSpec{set: true, add: []string{"code"}}, unknownKinds, true)

	if err != nil || got != nil {
		t.Errorf("planKey() = %+v, %v: the sort key is the override, which contains the new key", got, err)
	}
}

func TestPlanKeyRefusesAnOverrideThatDoesNotCoverTheNewKey(t *testing.T) {
	cfg := &config.Config{Tables: config.Tables{Overrides: map[string]config.TableOverride{"shop.items": {OrderBy: []string{"id"}}}}}
	req := chsink.ChangeRequest{Source: "shop.items"}

	_, err := planKey(cfg, shopItems, shapeOf([]string{"id"}, map[string]string{"id": "Int32", "code": "String"}), &req, pkSpec{set: true, add: []string{"code"}}, unknownKinds, true)

	if err == nil || !strings.Contains(err.Error(), "order_by") {
		t.Errorf("planKey() = %v, want the override named", err)
	}
}

func rebuildHarness(t *testing.T, key []string, cols map[string]string) *harness {
	t.Helper()
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLFull
	h.sink.shapes = map[string]chsink.TableShape{"shop.items": shapeOf(key, cols)}
	return h
}

var itemColumns = map[string]string{"id": "Int32", "name": "Nullable(String)"}

func TestChangeOfThePrimaryKeyRebuildsTheTableAtTheBarrier(t *testing.T) {
	h := rebuildHarness(t, []string{"id"}, itemColumns)
	obs := &fakeObserver{}
	h.obs = obs
	h.source.script = []scriptedTx{
		tx(1, insertRows(itemsTable(), 1, "a")),
		tx(2, alterEvent("ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (name, id)")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if len(h.sink.rebuilds) != 1 || !reflect.DeepEqual(h.sink.rebuilds[0].Key, []string{"name", "id"}) {
		t.Fatalf("rebuilds %+v", h.sink.rebuilds)
	}
	// The rows before the statement are written first: they must be in the copy.
	seq := strings.Join(h.journal.all(), "\n")
	if strings.Index(seq, "insert shop.items") > strings.Index(seq, "rebuild shop.items") {
		t.Errorf("the rebuild came before the rows that precede it were written:\n%s", seq)
	}
	if got := obs.sequence("rebuild-"); !reflect.DeepEqual(got, []string{"rebuild-started shop.items", "rebuild-finished shop.items true"}) {
		t.Errorf("observer saw %v", got)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-2" {
		t.Errorf("position = %q", pos.GTIDSet)
	}
}

func TestKeyThatTheTableAlreadyHasIsNotRebuilt(t *testing.T) {
	h := rebuildHarness(t, []string{"id"}, itemColumns)
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (id)"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.rebuilds) != 0 || len(h.sink.changes) != 0 {
		t.Errorf("the table was touched: %+v %+v", h.sink.rebuilds, h.sink.changes)
	}
}

// TestAStatementReplayedAfterTheSwapOfItsRebuildFinishesTheSwap: a crash came after the exchange of the
// rebuild a statement made, before the marker was removed, and the statement is replayed. The table has
// the key it asks for, and carries the marker: the rebuild under that key finishes the swap.
func TestAStatementReplayedAfterTheSwapOfItsRebuildFinishesTheSwap(t *testing.T) {
	h := rebuildHarness(t, []string{"name", "id"}, itemColumns)
	shape := h.sink.shapes["shop.items"]
	shape.Marked = true
	h.sink.shapes["shop.items"] = shape
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (name, id)"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.rebuilds) != 1 || !reflect.DeepEqual(h.sink.rebuilds[0].Key, []string{"name", "id"}) {
		t.Errorf("rebuilds %+v, want the rebuild under the key the table has", h.sink.rebuilds)
	}
}

// TestRenameAndDropOfAKeyColumnAreARebuildNotAQuarantine: what was set aside as "sort key" before.
func TestRenameAndDropOfAKeyColumnAreARebuildNotAQuarantine(t *testing.T) {
	h := rebuildHarness(t, []string{"id"}, itemColumns)
	h.source.script = []scriptedTx{
		tx(1, alterEvent("ALTER TABLE items RENAME COLUMN id TO ident, ADD COLUMN extra INT NULL")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if len(h.sink.rebuilds) != 1 || !reflect.DeepEqual(h.sink.rebuilds[0].Rename, []chsink.ColumnRename{{Old: "id", New: "ident"}}) || !reflect.DeepEqual(h.sink.rebuilds[0].Key, []string{"ident"}) {
		t.Fatalf("rebuilds %+v", h.sink.rebuilds)
	}
	// The column that is not part of the key is added by the ordinary path, before the rebuild.
	if len(h.sink.changes) != 1 || len(h.sink.changes[0].Add) != 1 || len(h.sink.changes[0].Rename) != 0 {
		t.Errorf("changes %+v", h.sink.changes)
	}
	seq := strings.Join(h.journal.all(), "\n")
	if strings.Index(seq, "change shop.items") > strings.Index(seq, "rebuild shop.items") {
		t.Errorf("the rebuild came before the ordinary change:\n%s", seq)
	}
	if len(quarantinedNames(h.store.current())) != 0 {
		t.Error("the table was set aside")
	}
}

func TestPrimaryKeyChangeOfATableNotInClickHouseYetChangesNothing(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLFull
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (name)"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.rebuilds) != 0 {
		t.Errorf("a table that does not exist was rebuilt: %+v", h.sink.rebuilds)
	}
}

func TestAdditiveModeSetsAsideATableWhoseKeyChanges(t *testing.T) {
	h := rebuildHarness(t, []string{"id"}, itemColumns)
	h.cfg.DDL.Mode = config.DDLAdditive
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (name)"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) || len(h.sink.rebuilds) != 0 {
		t.Errorf("quarantined %v, rebuilds %v", got, h.sink.rebuilds)
	}
	if !strings.Contains(h.store.current().Quarantine, "primary key") {
		t.Errorf("the reason does not name the primary key: %s", h.store.current().Quarantine)
	}
}

func TestATableThatCannotBeRebuiltIsSetAsideAndTheOthersGoOn(t *testing.T) {
	h := rebuildHarness(t, []string{"id"}, itemColumns)
	h.sink.rebuildErr = &chsink.TableError{Table: "shop.items", Err: errors.New("it has a partition key")}
	h.source.script = []scriptedTx{
		tx(1, alterEvent("ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (name)")),
		tx(2, insertRows(otherTable(), 1, "x")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("quarantined %v", got)
	}
	if len(h.sink.liveRows("shop.other")) != 1 {
		t.Error("the other table stopped replicating")
	}
}

func TestACopyThatLostRowsFailsReplication(t *testing.T) {
	h := rebuildHarness(t, []string{"id"}, itemColumns)
	h.sink.rebuildErr = &chsink.ConflictError{Err: errors.New("rows collapsed")}
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (name)"))}

	err := h.run(t)

	if !chsink.IsConflict(err) {
		t.Fatalf("Run() = %v, want the conflict", err)
	}
	if pos := h.store.current(); pos.GTIDSet == "u:1-1" {
		t.Error("the position moved past a rebuild that failed")
	}
}

// TestARebuildDoesNotFillTheTableOfAnotherSourceTable: shop.items__rebuild, created after the start,
// is replicated into the table a rebuild of shop.items would fill and swap in.
func TestARebuildDoesNotFillTheTableOfAnotherSourceTable(t *testing.T) {
	h := rebuildHarness(t, []string{"id"}, itemColumns)
	other := itemsTable()
	other.Name = "items__rebuild"
	h.source.script = []scriptedTx{
		tx(1, insertRows(other, 1, "a")),
		tx(2, alterEvent("ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (name, id)")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.rebuilds) != 0 {
		t.Errorf("rebuilt %+v into the table of shop.items__rebuild", h.sink.rebuilds)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("quarantined %v, want shop.items", got)
	}
}
