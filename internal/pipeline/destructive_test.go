package pipeline

import (
	"reflect"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/event"
)

func ddlEvent(query string) event.DDL { return event.DDL{Database: "shop", Query: query} }

func TestFullModeMirrorsDropAndTruncate(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.DDL.Mode = config.DDLFull
	h.source.script = []scriptedTx{
		tx(1, insertRows(items, 1, "a")),
		tx(2, ddlEvent("TRUNCATE TABLE items")),
		tx(3, insertRows(items, 2, "b")),
		tx(4, ddlEvent("DROP TABLE items, other")),
		tx(5, ddlEvent("DROP TABLE warehouse.invoices")), // not replicated
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if len(h.sink.truncs) != 1 || h.sink.truncs[0].Table != "items" {
		t.Errorf("truncations %+v", h.sink.truncs)
	}
	var dropped []string
	for _, d := range h.sink.drops {
		dropped = append(dropped, d.Table)
	}
	if want := []string{"items", "other"}; !reflect.DeepEqual(dropped, want) {
		t.Errorf("drops %v, want %v", dropped, want)
	}
	// The rows before the statement are written first, so none survives a TRUNCATE.
	seq := strings.Join(h.journal.all(), "\n")
	if strings.Index(seq, "insert shop.items") > strings.Index(seq, "truncate shop.items") {
		t.Errorf("rows were written after the truncation that should have removed them:\n%s", seq)
	}
	if pos := h.store.current(); pos.GTIDSet != "u:1-5" {
		t.Errorf("position = %q, want u:1-5", pos.GTIDSet)
	}
}

func TestAdditiveModeRemovesNothing(t *testing.T) {
	for _, q := range []string{"DROP TABLE items", "TRUNCATE TABLE items", "DROP DATABASE shop"} {
		t.Run(q, func(t *testing.T) {
			h := newHarness(t)
			h.cfg.DDL.Mode = config.DDLAdditive
			obs := &fakeObserver{}
			h.obs = obs
			logger, logs, mu := captureLogger()
			h.logger = logger
			h.source.script = []scriptedTx{tx(1, insertRows(itemsTable(), 1, "a")), tx(2, ddlEvent(q)), tx(3, insertRows(itemsTable(), 2, "b"))}

			if err := h.run(t); err != nil {
				t.Fatalf("Run() = %v", err)
			}

			if len(h.sink.drops)+len(h.sink.truncs) != 0 {
				t.Errorf("ClickHouse was changed: %+v %+v", h.sink.drops, h.sink.truncs)
			}
			mu.Lock()
			defer mu.Unlock()
			if !strings.Contains(logs.String(), "additive") {
				t.Errorf("no warning that additive does not apply it:\n%s", logs.String())
			}
			if obs.skipped != 1 {
				t.Errorf("skipped statements counted %d, want 1", obs.skipped)
			}
			if len(quarantinedNames(h.store.current())) != 0 {
				t.Error("a table was set aside: there is nothing left to set aside")
			}
			if pos := h.store.current(); pos.GTIDSet != "u:1-3" {
				t.Errorf("position = %q", pos.GTIDSet)
			}
		})
	}
}

// TestPartitionOperationsAreSkippedWithAWarningInEveryMode: ClickHouse has no partitions that
// mirror MySQL's, so the rows stay; this is logged and counted, and replication goes on.
func TestPartitionOperationsAreSkippedWithAWarningInEveryMode(t *testing.T) {
	for _, mode := range []config.DDLMode{config.DDLAdditive, config.DDLFull} {
		for _, q := range []string{"ALTER TABLE items DROP PARTITION p1", "ALTER TABLE items TRUNCATE PARTITION p1", "ALTER TABLE items EXCHANGE PARTITION p1 WITH TABLE other"} {
			t.Run(string(mode)+"/"+q, func(t *testing.T) {
				h := newHarness(t)
				h.cfg.DDL.Mode = mode
				obs := &fakeObserver{}
				h.obs = obs
				logger, logs, mu := captureLogger()
				h.logger = logger
				h.source.script = []scriptedTx{tx(1, ddlEvent(q)), tx(2, insertRows(itemsTable(), 1, "a"))}

				if err := h.run(t); err != nil {
					t.Fatalf("Run() = %v", err)
				}

				mu.Lock()
				defer mu.Unlock()
				warned := strings.Contains(logs.String(), "partition DDL skipped")
				if !warned || obs.skipped != 1 {
					t.Errorf("warned %v, counted %d\n%s", warned, obs.skipped, logs.String())
				}
				if len(h.sink.drops)+len(h.sink.truncs) != 0 || len(h.sink.visibleItems()) != 1 {
					t.Error("the statement was applied, or replication stopped")
				}
			})
		}
	}
}

func TestPartitionOperationOnATableThatIsNotReplicatedIsNotReported(t *testing.T) {
	h := newHarness(t)
	obs := &fakeObserver{}
	h.obs = obs
	h.source.script = []scriptedTx{tx(1, ddlEvent("ALTER TABLE warehouse.invoices DROP PARTITION p1"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if obs.skipped != 0 {
		t.Error("a statement about a table that is not replicated was reported")
	}
}

func TestDropOfAQuarantinedTableRemovesItsEntry(t *testing.T) {
	h := newHarness(t)
	p := h.pipeline(t)
	p.Quarantine("shop.items", errcode.TBL101, "set aside earlier")
	h.source.script = []scriptedTx{tx(1, ddlEvent("DROP TABLE items"))}

	if err := h.runPipeline(t, p); err != nil {
		t.Fatal(err)
	}
	if got := quarantinedNames(h.store.current()); len(got) != 0 {
		t.Errorf("quarantined %v: the table is gone", got)
	}
}

// TestPtOSCDropSwapIsFollowed is what pt-online-schema-change --alter-foreign-keys-method=drop_swap
// writes: the original is dropped, then the new table is renamed to its name.
func TestPtOSCDropSwapIsFollowed(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Include = []string{"shop.items"}
	h.source.script = []scriptedTx{
		tx(1, ddlEvent("CREATE TABLE `shop`.`_items_new` (id INT PRIMARY KEY)")),
		tx(2, ddlEvent("DROP TABLE IF EXISTS `items` /* generated by server */")),
		tx(3, ddlEvent("RENAME TABLE `shop`.`_items_new` TO `shop`.`items`")),
	}

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if len(h.sink.drops) != 1 || h.sink.drops[0].Table != "items" {
		t.Errorf("drops %+v", h.sink.drops)
	}
	if len(h.sink.renames) != 1 || h.sink.renames[0].From != "_items_new" || h.sink.renames[0].To != "items" {
		t.Errorf("renames %+v", h.sink.renames)
	}
}

func TestDropDatabaseDropsTheTablesThatWereReplicated(t *testing.T) {
	t.Run("database of its own in ClickHouse", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Tables.Include = []string{"shop.items", "shop.renamed"}
		h.cfg.Tables.Overrides = map[string]config.TableOverride{"shop.renamed": {TargetTable: "goods"}}
		renamed := itemsTable()
		renamed.Name = "renamed"
		h.source.script = []scriptedTx{
			tx(1, insertRows(itemsTable(), 1, "a"), insertRows(renamed, 1, "a")),
			tx(2, ddlEvent("DROP DATABASE shop")),
		}

		if err := h.run(t); err != nil {
			t.Fatal(err)
		}
		if got, want := droppedTables(h), []string{"shop.items", "shop.goods"}; !reflect.DeepEqual(got, want) {
			t.Errorf("dropped %v, want %v: the tables written to, target_table followed", got, want)
		}
		if pos := h.store.current(); pos.Tables != "{}" {
			t.Errorf("saved list of tables = %s, want none left", pos.Tables)
		}
	})

	t.Run("one ClickHouse database for all: only the tables of the dropped one", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Tables.TargetDatabase = "replica"
		h.cfg.Tables.Include = []string{"shop.*", "blog.*"}
		posts := itemsTable()
		posts.Database, posts.Name = "blog", "posts"
		h.source.script = []scriptedTx{
			tx(1, insertRows(itemsTable(), 1, "a"), insertRows(posts, 1, "a")),
			tx(2, ddlEvent("DROP DATABASE shop")),
		}

		if err := h.run(t); err != nil {
			t.Fatal(err)
		}
		if got, want := droppedTables(h), []string{"replica.items"}; !reflect.DeepEqual(got, want) {
			t.Errorf("dropped %v, want %v: posts comes from blog", got, want)
		}
	})
}

// TestDropDatabaseLeavesATableBinwaveNeverWroteTo: a table under the name a replicated table of the
// dropped database would have is not binwave's unless binwave wrote to it.
func TestDropDatabaseLeavesATableBinwaveNeverWroteTo(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1, ddlEvent("DROP DATABASE shop"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.drops) != 0 {
		t.Errorf("dropped %+v", h.sink.drops)
	}
}

func droppedTables(h *harness) []string {
	var out []string
	for _, d := range h.sink.drops {
		out = append(out, d.Database+"."+d.Table)
	}
	return out
}

func TestDropDatabaseThatIsNotReplicatedIsIgnored(t *testing.T) {
	h := newHarness(t)
	h.source.script = []scriptedTx{tx(1, event.DDL{Database: "warehouse", Query: "DROP DATABASE warehouse"})}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.drops) != 0 {
		t.Errorf("dropped %+v", h.sink.drops)
	}
}

func TestDryRunOnlyPassesTheDropOnToTheSink(t *testing.T) {
	h := newHarness(t)
	h.cfg.DDL.DryRun = true
	h.source.script = []scriptedTx{tx(1, ddlEvent("DROP TABLE items"))}

	if err := h.run(t); err != nil {
		t.Fatal(err)
	}
	if len(h.sink.drops) != 1 || !h.sink.drops[0].DryRun {
		t.Errorf("the request did not carry ddl.dry_run: %+v", h.sink.drops)
	}
}
