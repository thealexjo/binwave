//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/test/migrationsim"
	"github.com/thealexjo/binwave/test/testenv"
)

// TestDroppedAndRenamedColumnsKeepTheDataMySQLKept: a renamed column keeps its values, including
// for rows that existed before the statement, a dropped column disappears, and rows written and
// updated afterwards match MySQL.
func TestDroppedAndRenamedColumnsKeepTheDataMySQLKept(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, note VARCHAR(20) NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'ann', 'n1', 10), (2, 'bob', 'n2', 20)")
	waitSame(t, my, ch, pair{"before", "SELECT id, owner, qty FROM binwave.acct ORDER BY id", "SELECT id, owner, qty FROM replica.acct FINAL ORDER BY id"})

	mustExec(t, my,
		"ALTER TABLE binwave.acct RENAME COLUMN owner TO holder, DROP COLUMN note, CHANGE COLUMN qty amount BIGINT NOT NULL",
		"INSERT INTO binwave.acct VALUES (3, 'cy', 30)",
		"UPDATE binwave.acct SET amount = 99 WHERE id = 1")

	waitSame(t, my, ch, pair{"after", "SELECT id, holder, amount FROM binwave.acct ORDER BY id", "SELECT id, holder, amount FROM replica.acct FINAL ORDER BY id"})
	if chHasColumn(t, ch, "acct", "note") || chHasColumn(t, ch, "acct", "owner") || chHasColumn(t, ch, "acct", "qty") {
		t.Error("ClickHouse still has a column that the source dropped or renamed")
	}
	e.stillRunning(t)
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("quarantined %v", got)
	}
}

// TestRenamedTableKeepsItsDataAndItsHistory: RENAME TABLE, and the ALTER form, rename the
// ClickHouse table with everything in it.
func TestRenamedTableKeepsItsDataAndItsHistory(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.a (id INT PRIMARY KEY, v VARCHAR(10) NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.a VALUES (1, 'x'), (2, 'y')")
	waitSame(t, my, ch, pair{"before", "SELECT id, v FROM binwave.a ORDER BY id", "SELECT id, v FROM replica.a FINAL ORDER BY id"})

	mustExec(t, my, "RENAME TABLE binwave.a TO binwave.b", "INSERT INTO binwave.b VALUES (3, 'z')")
	waitSame(t, my, ch, pair{"renamed", "SELECT id, v FROM binwave.b ORDER BY id", "SELECT id, v FROM replica.b FINAL ORDER BY id"})

	mustExec(t, my, "ALTER TABLE binwave.b RENAME TO binwave.c", "UPDATE binwave.c SET v = 'w' WHERE id = 1")
	waitSame(t, my, ch, pair{"renamed again", "SELECT id, v FROM binwave.c ORDER BY id", "SELECT id, v FROM replica.c FINAL ORDER BY id"})

	for _, gone := range []string{"a", "b"} {
		if chRowCount(t, ch, gone) != -1 {
			t.Errorf("table %s is still in ClickHouse", gone)
		}
	}
	e.stillRunning(t)
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("quarantined %v", got)
	}
}

// TestSwapOfATableForItsCopyLikeAnOnlineSchemaChange is what pt-online-schema-change and gh-ost
// do to the binlog, by hand: a copy with another structure is created and filled, then swapped in
// with two renames at once, and the old table is dropped. Only the original is in tables.include;
// the copy is replicated as its shadow, so its rows arrive as they are written, the swap is a
// rename in ClickHouse, and the drop of the old table is mirrored.
func TestSwapOfATableForItsCopyLikeAnOnlineSchemaChange(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.orders"]}`)
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	t.Cleanup(func() {
		if t.Failed() {
			tables, _ := textRows(ch.DB, "SELECT name FROM system.tables WHERE database = 'replica' ORDER BY name")
			t.Logf("tables in ClickHouse: %v, quarantined: %+v", tables, e.hub.Status().Quarantined)
		}
	})
	mustExec(t, my, "INSERT INTO binwave.orders VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")
	waitSame(t, my, ch, pair{"before", "SELECT id, v, qty FROM binwave.orders ORDER BY id", "SELECT id, v, qty FROM replica.orders FINAL ORDER BY id"})

	mustExec(t, my,
		"CREATE TABLE binwave._orders_new LIKE binwave.orders",
		"ALTER TABLE binwave._orders_new ADD COLUMN c INT NOT NULL DEFAULT 5, CHANGE COLUMN v title VARCHAR(30) NOT NULL, MODIFY qty BIGINT NOT NULL",
		"INSERT INTO binwave._orders_new (id, title, qty) SELECT id, v, qty FROM binwave.orders",
		// The source keeps changing while the copy is being made; the tools carry each change over.
		"INSERT INTO binwave.orders VALUES (4, 'd', 4)",
		"INSERT INTO binwave._orders_new (id, title, qty) VALUES (4, 'd', 4)",
		"UPDATE binwave.orders SET qty = 20 WHERE id = 2",
		"UPDATE binwave._orders_new SET qty = 20 WHERE id = 2",
		"RENAME TABLE binwave.orders TO binwave._orders_old, binwave._orders_new TO binwave.orders",
		"DROP TABLE binwave._orders_old",
		"INSERT INTO binwave.orders (id, title, qty) VALUES (5, 'e', 5)")

	waitSame(t, my, ch, pair{"swapped", "SELECT id, title, qty, c FROM binwave.orders ORDER BY id", "SELECT id, title, qty, c FROM replica.orders FINAL ORDER BY id"})
	for _, gone := range []string{"_orders_new", "_orders_old"} {
		if chRowCount(t, ch, gone) != -1 {
			t.Errorf("table %s is still in ClickHouse", gone)
		}
	}
	e.stillRunning(t)
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("quarantined %v", got)
	}
}

// TestRenameOntoALeftoverTableStopsTheService: ClickHouse holds a table under the new name that the
// source does not have. binwave overwrites nothing, and does not set the table aside either: the
// disagreement must be seen and settled by the operator, so replication fails (the service stays
// up, see failure_test.go) with a message that names the leftover.
func TestRenameOntoALeftoverTableStopsTheService(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.a (id INT PRIMARY KEY)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	first := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.a VALUES (1)")
	waitUntil(t, "the table", 60*time.Second, func() bool { return chRowCount(t, ch, "a") == 1 })
	if _, err := ch.DB.Exec("CREATE TABLE replica.b (id Int32) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if err := first.stop(); err != nil {
		t.Fatal(err)
	}

	cfg2 := engineConfig(t, my, ch, "file")
	cfg2.Checkpoint.FilePath = cfg.Checkpoint.FilePath
	cfg2.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg2)
	mustExec(t, my, "RENAME TABLE binwave.a TO binwave.b")

	f := waitFailedHub(t, e)
	if !strings.Contains(f.Message, "replica`.`b") {
		t.Fatalf("the failure does not name the leftover table: %s", f.Message)
	}
	if chRowCount(t, ch, "a") != 1 {
		t.Error("the table was changed although the rename was refused")
	}
}

// TestSinkChangesAreIdempotent: the DDL of a transaction that was applied but not committed is
// replayed after a crash. Each change finds its work done and changes nothing.
func TestSinkChangesAreIdempotent(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	if _, err := ch.DB.Exec("CREATE DATABASE IF NOT EXISTS replica"); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.DB.Exec("CREATE TABLE replica.t (id Int32, a String, b String, _version UInt64, _is_deleted UInt8) ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	client := openSink(t, ch)
	ctx := context.Background()
	req := chsink.ChangeRequest{
		Source: "s.t", Database: "replica", Table: "t",
		Rename: []chsink.ColumnRename{{Old: "a", New: "a2"}},
		Drop:   []string{"b"},
	}
	for i := 0; i < 3; i++ {
		if err := client.Change(ctx, req); err != nil {
			t.Fatalf("change %d: %v", i, err)
		}
	}
	if chHasColumn(t, ch, "t", "a") || chHasColumn(t, ch, "t", "b") || !chHasColumn(t, ch, "t", "a2") {
		t.Error("the columns are not what the change says")
	}

	rename := chsink.RenameRequest{Source: "s.t", FromDatabase: "replica", From: "t", ToDatabase: "replica", To: "t2"}
	for i := 0; i < 3; i++ {
		if err := client.RenameTable(ctx, rename); err != nil {
			t.Fatalf("rename %d: %v", i, err)
		}
	}
	if chRowCount(t, ch, "t2") != 0 || chRowCount(t, ch, "t") != -1 {
		t.Error("the table was not renamed")
	}

	// Both exist because the whole swap was applied before a crash: the first pair is done.
	if _, err := ch.DB.Exec("CREATE TABLE replica.t (id Int32) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	swapped := rename
	swapped.SourceReused = true
	if err := client.RenameTable(ctx, swapped); err != nil {
		t.Fatalf("the first pair of a swap that was applied whole = %v, want it done", err)
	}
	if _, err := ch.DB.Exec("DROP TABLE replica.t"); err != nil {
		t.Fatal(err)
	}

	// Both exist: a conflict, and nothing is overwritten.
	if _, err := ch.DB.Exec("CREATE TABLE replica.t (id Int32) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if err := client.RenameTable(ctx, rename); !chsink.IsConflict(err) {
		t.Fatalf("rename onto an existing table = %v, want a conflict", err)
	}

	// A column that both names hold is a conflict as well.
	if _, err := ch.DB.Exec("ALTER TABLE replica.t2 ADD COLUMN a String"); err != nil {
		t.Fatal(err)
	}
	if err := client.Change(ctx, chsink.ChangeRequest{Source: "s.t", Database: "replica", Table: "t2", Rename: []chsink.ColumnRename{{Old: "a", New: "a2"}}}); !chsink.IsConflict(err) {
		t.Fatalf("rename onto an existing column = %v, want a conflict", err)
	}
}

// TestSwapWithAShadowOfAnotherNameStopsTheServiceAndChangesNothing: a shadow that has a name the
// tools do not use by default (--new-table-name) is not replicated unless tables.include names it,
// so its rows never reach ClickHouse and the swap cannot be followed. Replication fails, with
// ClickHouse as it was before the statement; listing the shadow in include makes it work.
func TestSwapWithAShadowOfAnotherNameStopsTheServiceAndChangesNothing(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(20) NOT NULL)")
	cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.orders"]}`)
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.orders VALUES (1, 'a')")
	waitUntil(t, "the table", 60*time.Second, func() bool { return chRowCount(t, ch, "orders") == 1 })

	mustExec(t, my,
		"CREATE TABLE binwave.orders_copy LIKE binwave.orders",
		"INSERT INTO binwave.orders_copy SELECT * FROM binwave.orders",
		"RENAME TABLE binwave.orders TO binwave._orders_old, binwave.orders_copy TO binwave.orders")

	f := waitFailedHub(t, e)
	if !strings.Contains(f.Message, "tables.include") {
		t.Fatalf("the failure does not name tables.include: %s", f.Message)
	}
	if chRowCount(t, ch, "orders") != 1 || chRowCount(t, ch, "_orders_old") != -1 {
		t.Error("ClickHouse was changed by a statement that was refused")
	}
}

// TestPtOSCDropSwapIsFollowed: with --alter-foreign-keys-method=drop_swap the tool drops the
// original table and renames the new one into its place. The drop is mirrored, so the rename finds
// the name free, and ClickHouse ends with the migrated table and every row.
func TestPtOSCDropSwapIsFollowed(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(20) NOT NULL)")
	cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.orders"]}`)
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.orders VALUES (1, 'a'), (2, 'b')")
	waitUntil(t, "the table", 60*time.Second, func() bool { return chRowCount(t, ch, "orders") == 2 })

	err := migrationsim.New(rootDB(t, my), "binwave", "orders").PtOSC(context.Background(), "ADD COLUMN c INT NOT NULL DEFAULT 5", migrationsim.PtOSCOptions{DropSwap: true})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, my, "INSERT INTO binwave.orders (id, v, c) VALUES (3, 'c', 7)")

	waitSame(t, my, ch, pair{"swapped", "SELECT id, v, c FROM binwave.orders ORDER BY id", "SELECT id, v, c FROM replica.orders FINAL ORDER BY id"})
	e.stillRunning(t)
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("quarantined %v", got)
	}
}
