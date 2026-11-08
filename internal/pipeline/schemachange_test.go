package pipeline

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/event"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

func alterEvent(query string) event.DDL {
	return event.DDL{Database: "shop", Query: query, Time: epoch.Add(30 * time.Second)}
}

func names(plans []chsink.ColumnPlan) []string {
	var out []string
	for _, p := range plans {
		out = append(out, p.Name+" "+p.Type+" "+p.Default)
	}
	return out
}

func TestAddColumnIsAppliedAtTheDDLEventBeforeTheNextRows(t *testing.T) {
	h := newHarness(t)
	items := itemsTable()
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "before")),
		tx(2, alterEvent("ALTER TABLE items ADD COLUMN qty INT NOT NULL DEFAULT 7, ADD COLUMN note VARCHAR(20) NULL")),
		tx(3, insertRows(items, 2, "after")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if len(h.sink.changes) != 1 {
		t.Fatalf("Change was called %d times, want once", len(h.sink.changes))
	}
	req := h.sink.changes[0]
	if req.Source != "shop.items" || req.Database != "shop" || req.Table != "items" {
		t.Errorf("request = %+v", req)
	}
	want := []string{"qty Int32 DEFAULT 7", "note Nullable(String) "}
	if got := names(req.Add); !reflect.DeepEqual(got, want) {
		t.Errorf("added columns %q, want %q", got, want)
	}
	// The rows before the statement are written before the change: they belong to the old structure.
	seq := strings.Join(h.journal.all(), "\n")
	if strings.Index(seq, "insert shop.items") >= strings.Index(seq, "change shop.items") {
		t.Errorf("the change was made before the rows that precede it were written:\n%s", seq)
	}
}

func TestSchemaChangeUsesTheTargetNamesOfTheConfiguration(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.TargetDatabase = "replica"
	h.cfg.Tables.Overrides = map[string]config.TableOverride{"shop.items": {TargetTable: "goods", ExcludeColumns: []string{"secret"}}}
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items ADD COLUMN secret INT NULL, ADD COLUMN qty INT NULL"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	req := h.sink.changes[0]
	if req.Database != "replica" || req.Table != "goods" {
		t.Errorf("target = %s.%s, want replica.goods", req.Database, req.Table)
	}
	if got := names(req.Add); len(got) != 1 || !strings.HasPrefix(got[0], "qty ") {
		t.Errorf("added %q: an excluded column must not reach ClickHouse", got)
	}
}

func TestTypeChangeOfAColumnIsPlannedAsAModify(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items MODIFY COLUMN id BIGINT NOT NULL"), alterEvent("ALTER TABLE items CHANGE COLUMN name name VARCHAR(100) NULL"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	if len(h.sink.changes) != 2 {
		t.Fatalf("%d changes", len(h.sink.changes))
	}
	if got := names(h.sink.changes[0].Modify); !reflect.DeepEqual(got, []string{"id Int64 "}) {
		t.Errorf("modify %q", got)
	}
	// A CHANGE that keeps the name is a MODIFY.
	if got := names(h.sink.changes[1].Modify); !reflect.DeepEqual(got, []string{"name Nullable(String) "}) {
		t.Errorf("modify %q", got)
	}
}

func TestStatementsThatDoNotChangeColumnsAreIgnored(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1,
		alterEvent("ALTER TABLE items ADD INDEX i (name)"),
		alterEvent("ALTER TABLE items ENGINE=InnoDB"),
		alterEvent("ALTER TABLE other_db.things ADD COLUMN c INT"), // not replicated
		alterEvent("CREATE INDEX j ON items (name)"),
		alterEvent("ALTER TABLE items ADD PARTITION (PARTITION p9 VALUES LESS THAN (100))"),
	)}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.changes) != 0 {
		t.Errorf("Change was called for %+v", h.sink.changes)
	}
}

// TestChangesBinwaveCannotApplySetTheTableAside: what ddl.mode additive leaves alone and the
// primary key, which is not applied yet in any mode, are not guessed: the table is set aside.
func TestChangesBinwaveCannotApplySetTheTableAside(t *testing.T) {
	for _, tt := range []struct {
		mode  config.DDLMode
		query string
		// wantSet is what is quarantined: a rename sets aside the new name as well, because its
		// rows would otherwise start a table without the history.
		wantSet []string
	}{
		{config.DDLAdditive, "ALTER TABLE items DROP COLUMN name", []string{"shop.items"}},
		{config.DDLAdditive, "ALTER TABLE items RENAME COLUMN name TO title", []string{"shop.items"}},
		{config.DDLAdditive, "ALTER TABLE items CHANGE COLUMN name title VARCHAR(20)", []string{"shop.items"}},
		{config.DDLAdditive, "ALTER TABLE items RENAME TO goods", []string{"shop.goods", "shop.items"}},
		{config.DDLAdditive, "RENAME TABLE items TO goods", []string{"shop.goods", "shop.items"}},
		{config.DDLAdditive, "ALTER TABLE items DROP PRIMARY KEY, ADD PRIMARY KEY (name)", []string{"shop.items"}},
	} {
		t.Run(string(tt.mode)+"/"+tt.query, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.DDL.Mode = tt.mode
			h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a"), insertRows(otherTable(), 1, "x")), tx(2, alterEvent(tt.query)), tx(3, insertRows(itemsTable(), 2, "b"), insertRows(otherTable(), 2, "y"))}

			if err := h.run(t); err != nil {
				t.Fatalf("Run() = %v, want the table set aside and the service on", err)
			}

			got := quarantinedNames(h.store.current())
			sort.Strings(got)
			if !reflect.DeepEqual(got, tt.wantSet) {
				t.Errorf("quarantined %v, want %v", got, tt.wantSet)
			}
			if entries := h.store.current().Quarantine; !strings.Contains(entries, "not applied to ClickHouse") {
				t.Errorf("the reason does not say what is missing: %s", entries)
			}
			if len(h.sink.liveRows("shop.other")) != 2 {
				t.Error("the other table stopped replicating")
			}
			if len(h.sink.changes)+len(h.sink.renames) != 0 {
				t.Errorf("a change was applied: %+v %+v", h.sink.changes, h.sink.renames)
			}
		})
	}
}

func TestFullModePlansDropAndRenameOfColumns(t *testing.T) {
	for _, tt := range []struct {
		query  string
		rename []chsink.ColumnRename
		drop   []string
		modify []string
	}{
		{"ALTER TABLE items DROP COLUMN name", nil, []string{"name"}, nil},
		{"ALTER TABLE items RENAME COLUMN name TO title", []chsink.ColumnRename{{Old: "name", New: "title"}}, nil, nil},
		// CHANGE with another name keeps the data and may redefine the type under the new name.
		{"ALTER TABLE items CHANGE COLUMN name title VARCHAR(50) NULL", []chsink.ColumnRename{{Old: "name", New: "title"}}, nil, []string{"title Nullable(String) "}},
		{"ALTER TABLE items DROP COLUMN name, RENAME COLUMN id TO ident", []chsink.ColumnRename{{Old: "id", New: "ident"}}, []string{"name"}, nil},
	} {
		t.Run(tt.query, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.DDL.Mode = config.DDLFull
			h.source.script = []scriptedTx{tx(1, alterEvent(tt.query))}

			if err := h.run(t); err != nil {
				t.Fatal(err)
			}
			if len(h.sink.changes) != 1 {
				t.Fatalf("Change was called %d times", len(h.sink.changes))
			}
			req := h.sink.changes[0]
			if !reflect.DeepEqual(req.Rename, tt.rename) || !reflect.DeepEqual(req.Drop, tt.drop) {
				t.Errorf("rename %v drop %v, want %v %v", req.Rename, req.Drop, tt.rename, tt.drop)
			}
			if got := names(req.Modify); len(tt.modify)+len(got) > 0 && !reflect.DeepEqual(got, tt.modify) {
				t.Errorf("modify %q, want %q", got, tt.modify)
			}
			if len(quarantinedNames(h.store.current())) != 0 {
				t.Error("a change that full mode applies quarantined the table")
			}
		})
	}
}

// TestRenameTableFollowsTheSource covers RENAME TABLE and the ALTER form of it: the ClickHouse
// table is renamed, a swap is two renames in order, and a table that leaves the replication
// keeps its data under the new name.
func TestRenameTableFollowsTheSource(t *testing.T) {
	type pair struct{ from, to string }
	for _, tt := range []struct {
		name  string
		query string
		want  []pair
	}{
		{"rename", "RENAME TABLE items TO goods", []pair{{"items", "goods"}}},
		{"alter form", "ALTER TABLE items RENAME TO goods", []pair{{"items", "goods"}}},
		{"swap", "RENAME TABLE items TO items_old, items_new TO items", []pair{{"items", "items_old"}, {"items_new", "items"}}},
		{"leaves the replication", "RENAME TABLE items TO other_db.items", []pair{{"items", "items"}}},
		{"neither is replicated", "RENAME TABLE other_db.a TO other_db.b", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.DDL.Mode = config.DDLFull
			h.source.script = []scriptedTx{tx(1, alterEvent(tt.query))}

			if err := h.run(t); err != nil {
				t.Fatal(err)
			}
			var got []pair
			for _, r := range h.sink.renames {
				got = append(got, pair{r.From, r.To})
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("renamed %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRenameIsMadeAtTheBarrierAndRebindsTheTable(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLFull
	items := itemsTable()
	goods := itemsTable()
	goods.Name = "goods"
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		tx(2, alterEvent("RENAME TABLE items TO goods")),
		tx(3, insertRows(goods, 2, "b")),
	}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}

	seq := strings.Join(h.journal.all(), "\n")
	if strings.Index(seq, "insert shop.items") > strings.Index(seq, "rename shop.items") ||
		strings.Index(seq, "rename shop.items") > strings.Index(seq, "ensure shop.goods") {
		t.Errorf("wrong order, the rows before the rename must be written first, and the new name is prepared after it:\n%s", seq)
	}
	if len(h.sink.liveRows("shop.goods")) != 1 {
		t.Errorf("rows of the new name: %v", h.sink.liveRows("shop.goods"))
	}
}

// TestRenameOfAnUnknownTableIntoAReplicatedOneStopsTheService: ClickHouse cannot follow a table
// whose rows binwave never saw. Restarting would hit the statement again, so it is a
// configuration-class error, not a quarantine.
func TestRenameOfAnUnknownTableIntoAReplicatedOneStopsTheService(t *testing.T) {
	for _, query := range []string{
		"RENAME TABLE other_db.items_new TO shop.items",
		"ALTER TABLE other_db.items_new RENAME TO shop.items",
	} {
		t.Run(query, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.DDL.Mode = config.DDLFull
			h.source.script = []scriptedTx{tx(1, alterEvent(query))}

			err := h.run(t)

			if !chsink.IsConflict(err) {
				t.Fatalf("Run() = %v, want a *ConflictError", err)
			}
			if len(h.sink.renames) != 0 {
				t.Error("ClickHouse was renamed")
			}
			if len(quarantinedNames(h.store.current())) != 0 {
				t.Error("the table was set aside: that would hide the disagreement")
			}
		})
	}
}

func TestRenameOntoAnExistingTableStopsTheService(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLFull
	h.sink.renameErrs = map[int]error{1: &chsink.ConflictError{Err: errors.New("goods exists")}}
	h.source.script = []scriptedTx{tx(1, alterEvent("RENAME TABLE items TO goods"))}

	if err := h.run(t); !chsink.IsConflict(err) {
		t.Fatalf("Run() = %v, want a *ConflictError", err)
	}
}

func TestQuarantineFollowsTheRenamedTable(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLFull
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "set aside earlier")
	h.source.script = []scriptedTx{tx(1, alterEvent("RENAME TABLE items TO goods"))}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.goods"}) {
		t.Errorf("quarantined %v, want the entry to move to shop.goods", got)
	}
	if len(h.sink.renames) != 0 {
		t.Error("a quarantined table was renamed in ClickHouse")
	}
}

func TestRenameFailureOfATableSetsItAside(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLFull
	h.sink.renameErrs = map[int]error{1: &chsink.TableError{Table: "shop.items", Err: errors.New("ClickHouse refuses")}}
	h.source.script = []scriptedTx{tx(1, alterEvent("RENAME TABLE items TO goods")), tx(2, insertRows(otherTable(), 1, "x"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("quarantined %v", got)
	}
}

func TestChangeThatCannotBeReproducedSetsTheTableAside(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items ADD COLUMN c DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")), tx(2, insertRows(otherTable(), 1, "x"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if got := quarantinedNames(h.store.current()); !reflect.DeepEqual(got, []string{"shop.items"}) {
		t.Errorf("quarantined %v: the values of the existing rows cannot be reproduced", got)
	}
	if len(h.sink.changes) != 0 {
		t.Error("the column was added anyway")
	}
}

func TestSchemaChangeOfAQuarantinedTableIsNotFollowed(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "set aside earlier")
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items ADD COLUMN c INT NULL"))}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.changes) != 0 {
		t.Errorf("a quarantined table was changed in ClickHouse: %+v", h.sink.changes)
	}
}

func TestDryRunChangeStopsTheServiceByDefaultAndQuarantinesWhenAskedFor(t *testing.T) {
	missing := &chsink.TableError{Table: "shop.items", Missing: true, Err: errors.New("lacks the change; ddl.dry_run is set")}
	for _, tt := range []struct {
		name        string
		onMissing   config.MissingStructurePolicy
		wantStopped bool
	}{
		{"default", "", true},
		{"stop", config.OnMissingStop, true},
		{"quarantine", config.OnMissingQuarantine, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.DDL.DryRun = true
			if tt.onMissing != "" {
				h.cfg.DDL.OnMissingStructure = tt.onMissing
			}
			h.sink.changeErrs = map[int]error{1: missing}
			h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items ADD COLUMN c INT NULL")), tx(2, insertRows(otherTable(), 1, "x"))}

			err := h.run(t)

			if stopped := chsink.IsMissingTable(err); stopped != tt.wantStopped {
				t.Fatalf("Run() = %v, want stopped = %v", err, tt.wantStopped)
			}
			if !h.sink.changes[0].DryRun {
				t.Error("the request did not carry ddl.dry_run")
			}
			if !tt.wantStopped && len(h.sink.liveRows("shop.other")) != 1 {
				t.Error("the other table was not replicated")
			}
		})
	}
}

func TestTransientFailureOfAChangeIsRetried(t *testing.T) {
	h := newHarness(t)
	h.sink.changeErrs = map[int]error{1: syscall.ECONNRESET}
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items ADD COLUMN c INT NULL"))}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v, want the outage retried", err)
	}
	if len(h.sink.changes) != 2 {
		t.Errorf("Change was called %d times, want a retry after the outage", len(h.sink.changes))
	}
}

// swapScript is what pt-online-schema-change writes at the end of a migration of shop.items.
func swapScript() []scriptedTx {
	return []scriptedTx{
		tx(1, alterEvent("CREATE TABLE _items_new (id INT PRIMARY KEY)")),
		tx(2, alterEvent("RENAME TABLE items TO _items_old, _items_new TO items")),
		tx(3, alterEvent("DROP TABLE IF EXISTS _items_old /* generated by server */")),
	}
}

// TestSwapOfAShadowTableIsFollowedAndItsLeftoverDropped: with only shop.items in include, the
// shadow and the old table are replicated as such, the swap is two renames, and the drop of the
// old table is mirrored.
func TestSwapOfAShadowTableIsFollowedAndItsLeftoverDropped(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Include = []string{"shop.items"}
	h.source.script = swapScript()

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	var renames []string
	for _, r := range h.sink.renames {
		renames = append(renames, r.From+">"+r.To)
	}
	if want := []string{"items>_items_old", "_items_new>items"}; !reflect.DeepEqual(renames, want) {
		t.Errorf("renames %v, want %v", renames, want)
	}
	if len(h.sink.drops) != 1 || h.sink.drops[0].Table != "_items_old" {
		t.Errorf("drops %+v, want the old table", h.sink.drops)
	}
	if len(quarantinedNames(h.store.current())) != 0 {
		t.Error("something was set aside")
	}
}

// TestSwapOfAShadowWithAnotherNameStopsTheService: a shadow that has a name the tools do not use by
// default (--new-table-name) is not replicated unless include names it, and the swap cannot be followed.
func TestSwapOfAShadowWithAnotherNameStopsTheService(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Include = []string{"shop.items"}
	h.source.script = []scriptedTx{tx(1, alterEvent("RENAME TABLE items TO _items_old, items_copy TO items"))}

	err := h.run(t)

	if !chsink.IsConflict(err) || !strings.Contains(err.Error(), "tables.include") {
		t.Fatalf("Run() = %v, want a conflict that says to list the shadow in tables.include", err)
	}
	if len(h.sink.renames) != 0 || len(h.sink.drops) != 0 {
		t.Errorf("ClickHouse was changed: %+v %+v", h.sink.renames, h.sink.drops)
	}
}

func TestDropOfAQuarantinedShadowRemovesItsEntry(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Include = []string{"shop.items"}
	p := h.pipeline(t)
	p.Quarantine("shop._items_new", errcode.TBL101, "set aside earlier")
	h.source.script = []scriptedTx{tx(1, alterEvent("DROP TABLE _items_new"))}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	if got := quarantinedNames(h.store.current()); len(got) != 0 {
		t.Errorf("quarantined %v: the shadow is gone", got)
	}
}

// TestSwapMarksTheNameThatALaterPairTakesAgain: a replay after a crash finds the swap applied
// whole, with both names of the first pair present; the request says why that is not a conflict.
func TestSwapMarksTheNameThatALaterPairTakesAgain(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Include = []string{"shop.items"}
	h.source.script = swapScript()

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.renames) != 2 || !h.sink.renames[0].SourceReused || h.sink.renames[1].SourceReused {
		t.Errorf("renames %+v: only the first pair gives its name away to a later pair", h.sink.renames)
	}
}

func TestSwapOfColumnsMarksTheNameThatALaterRenameTakesAgain(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLFull
	h.source.script = []scriptedTx{tx(1, alterEvent("ALTER TABLE items RENAME COLUMN id TO tmp, RENAME COLUMN name TO id, RENAME COLUMN tmp TO name"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	got := h.sink.changes[0].Rename
	want := []chsink.ColumnRename{{Old: "id", New: "tmp", SourceReused: true}, {Old: "name", New: "id", SourceReused: true}, {Old: "tmp", New: "name"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("renames %+v, want %+v", got, want)
	}
}
