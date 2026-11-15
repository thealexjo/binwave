package pipeline

import (
	"reflect"
	"testing"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/schema"
)

func savedTables(t *testing.T, h *harness) map[string]checkpoint.Target {
	t.Helper()
	tables, known, err := checkpoint.DecodeTables(h.store.current().Tables)
	if err != nil || !known {
		t.Fatalf("saved list of tables %q: known %v, %v", h.store.current().Tables, known, err)
	}
	return tables
}

// TestAPositionWithoutAListAdoptsTheTablesBinwaveWrote: a position saved by a version that kept no
// list means replication has been running, so the targets of replicated source tables that have
// binwave's shape are binwave's. Nothing else is: not a table of the shape without a source table,
// not a target that does not have the shape.
func TestAPositionWithoutAListAdoptsTheTablesBinwaveWrote(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Overrides = map[string]config.TableOverride{"shop.renamed": {TargetTable: "goods"}}
	h.store.pos, h.store.found = checkpoint.Position{GTIDSet: "u:1-0"}, true
	h.snapshot = &fakeSnapshot{catalog: []schema.CatalogTable{
		{Database: "shop", Name: "items"},
		{Database: "shop", Name: "renamed"},
		{Database: "shop", Name: "plain"},
		{Database: "warehouse", Name: "stock"}, // not replicated
	}}
	h.sink.tablesOf = map[string][]string{"shop": {"items", "goods", "stray"}, "warehouse": {"stock"}}
	h.source.script = []scriptedTx{tx(1, ddlEvent("DROP DATABASE shop"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got, want := droppedTables(h), []string{"shop.items", "shop.goods"}; !reflect.DeepEqual(got, want) {
		t.Errorf("dropped %v, want %v", got, want)
	}
	if got := savedTables(t, h); len(got) != 0 {
		t.Errorf("saved list %v, want the dropped tables gone from it", got)
	}
}

func TestTheListAdoptedIsSaved(t *testing.T) {
	h := newHarness(t)
	h.store.pos, h.store.found = checkpoint.Position{GTIDSet: "u:1-0"}, true
	h.snapshot = &fakeSnapshot{}
	h.sink.tablesOf = map[string][]string{"shop": {"items"}}
	h.source.script = []scriptedTx{tx(1)}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got, want := savedTables(t, h), map[string]checkpoint.Target{"shop.items": {Database: "shop", Table: "items"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("saved list %v, want %v", got, want)
	}
}

// TestANewInstallationAdoptsNothing: without a saved position nothing has been replicated, so a table
// of binwave's shape that is there already is not binwave's.
func TestANewInstallationAdoptsNothing(t *testing.T) {
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{}
	h.sink.tablesOf = map[string][]string{"shop": {"items"}}
	h.source.script = []scriptedTx{tx(1)}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got := h.store.all()[0].Tables; got != "{}" {
		t.Errorf("initial position saved with list %q, want an empty one", got)
	}
	if got := savedTables(t, h); len(got) != 0 {
		t.Errorf("saved list %v, want none", got)
	}
}

func TestTheListFollowsTheTablesOfTheSource(t *testing.T) {
	items := itemsTable()
	orders := itemsTable()
	orders.Name = "orders"
	h := newHarness(t)
	h.cfg.Tables.Include = []string{"shop.items", "shop.goods", "shop.orders"}
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a"), insertRows(orders, 1, "a")),
		tx(2, ddlEvent("RENAME TABLE items TO goods")),
		tx(3, ddlEvent("RENAME TABLE orders TO archive")), // leaves the replication, keeps its data
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got, want := savedTables(t, h), map[string]checkpoint.Target{"shop.goods": {Database: "shop", Table: "goods"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("saved list %v, want %v", got, want)
	}
}

func TestADroppedTableLeavesTheList(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{
		tx(1, insertRows(itemsTable(), 1, "a")),
		tx(2, ddlEvent("DROP TABLE items")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got := savedTables(t, h); len(got) != 0 {
		t.Errorf("saved list %v, want none", got)
	}
}

// TestADryRunChangesNothingInTheList: nothing is dropped or renamed in ClickHouse, so the list stays
// as ClickHouse is.
func TestADryRunChangesNothingInTheList(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Include = []string{"shop.items", "shop.goods"}
	want := map[string]checkpoint.Target{"shop.items": {Database: "shop", Table: "items"}}
	h.store.pos, h.store.found = checkpoint.Position{GTIDSet: "u:1-0", Tables: checkpoint.EncodeTables(want)}, true
	h.cfg.DDL.DryRun = true
	h.source.script = []scriptedTx{
		tx(1, ddlEvent("RENAME TABLE items TO goods")),
		tx(2, ddlEvent("DROP TABLE goods")),
		tx(3, ddlEvent("DROP DATABASE shop")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got := savedTables(t, h); !reflect.DeepEqual(got, want) {
		t.Errorf("saved list %v, want %v", got, want)
	}
}

func TestAnInitialLoadAddsTheTableItSwapsIn(t *testing.T) {
	h := newHarness(t)
	h.cfg.Replication.InitialPosition = config.StartFromSnapshot
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}}, executed: fence(1)}
	h.source.script = []scriptedTx{uuidTx(1)}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got, want := savedTables(t, h), map[string]checkpoint.Target{"shop.items": {Database: "shop", Table: "items"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("saved list %v, want %v: the shadow table is not in it, the table swapped in is", got, want)
	}
}
