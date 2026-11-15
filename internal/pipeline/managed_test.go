package pipeline

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
)

func savedTables(t *testing.T, h *harness) map[string]checkpoint.Target {
	t.Helper()
	tables, known, err := checkpoint.DecodeTables(h.store.current().Tables)
	if err != nil || !known {
		t.Fatalf("saved list of tables %q: known %v, %v", h.store.current().Tables, known, err)
	}
	return tables
}

// TestAPositionWithoutAListIsRefused: a position saved by a pre-release that kept no list of tables
// stops the start, before anything is read or written: which tables it wrote cannot be told.
func TestAPositionWithoutAListIsRefused(t *testing.T) {
	h := newHarness(t)
	h.store.pos, h.store.found = checkpoint.Position{GTIDSet: "u:1-0"}, true
	h.source.script = []scriptedTx{tx(1, ddlEvent("DROP DATABASE shop"))}

	err := h.run(t)

	if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "pre-release") {
		t.Fatalf("run() = %v, want ErrUnsupported about a pre-release position", err)
	}
	if got := droppedTables(h); len(got) != 0 {
		t.Errorf("dropped %v, want nothing", got)
	}
	if got := h.store.all(); len(got) != 0 {
		t.Errorf("saved %v, want nothing", got)
	}
}

// TestANewInstallationStartsWithAnEmptyList: without a saved position nothing has been replicated, so
// a table of binwave's shape that is there already is not binwave's.
func TestANewInstallationStartsWithAnEmptyList(t *testing.T) {
	h := newHarness(t)
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
