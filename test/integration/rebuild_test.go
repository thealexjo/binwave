//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/test/migrationsim"
	"github.com/thealexjo/binwave/test/testenv"
)

func chSortingKey(t *testing.T, ch *testenv.ClickHouse, table string) string {
	t.Helper()
	var key string
	if err := ch.DB.QueryRow("SELECT sorting_key FROM system.tables WHERE database = 'replica' AND name = ?", table).Scan(&key); err != nil {
		t.Fatalf("sorting key of %s: %v", table, err)
	}
	key = strings.NewReplacer(" ", "", "`", "").Replace(key)
	return strings.TrimSuffix(strings.TrimPrefix(key, "("), ")")
}

// chTableUUID is the identity of replica.k: it changes when the table is rebuilt.
func chTableUUID(t *testing.T, ch *testenv.ClickHouse, table ...string) string {
	t.Helper()
	name := "k"
	if len(table) > 0 {
		name = table[0]
	}
	var id string
	if err := ch.DB.QueryRow("SELECT toString(uuid) FROM system.tables WHERE database = 'replica' AND name = ?", name).Scan(&id); err != nil {
		t.Fatalf("uuid of %s: %v", name, err)
	}
	return id
}

func chTables(t *testing.T, ch *testenv.ClickHouse) []string {
	t.Helper()
	names, err := textRows(ch.DB, "SELECT name FROM system.tables WHERE database = 'replica' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// TestPrimaryKeyChangeRebuildsTheTableAndKeepsEveryRow: the primary key moves to another column.
// Rows that were updated before the statement have two versions in ClickHouse until a merge, and the
// copy must take the latest only; rows are deleted, updated (including the new key) and inserted
// after it. Everything must equal MySQL.
func TestPrimaryKeyChangeRebuildsTheTableAndKeepsEveryRow(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.k (id INT PRIMARY KEY, code VARCHAR(10) NOT NULL, v INT NOT NULL, UNIQUE KEY (code))")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my,
		"INSERT INTO binwave.k VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)",
		"UPDATE binwave.k SET code = 'a2', v = 11 WHERE id = 1", // two versions of one row before the statement
		"DELETE FROM binwave.k WHERE id = 4")
	waitSame(t, my, ch, pair{"before", "SELECT id, code, v FROM binwave.k ORDER BY id", "SELECT id, code, v FROM replica.k FINAL ORDER BY id"})
	before := chTableUUID(t, ch)

	mustExec(t, my, "ALTER TABLE binwave.k DROP PRIMARY KEY, ADD PRIMARY KEY (code)",
		"UPDATE binwave.k SET code = 'b2' WHERE id = 2", // the new key changes
		"DELETE FROM binwave.k WHERE code = 'c'",
		"INSERT INTO binwave.k VALUES (5, 'e', 50), (6, 'f', 60)")

	waitSame(t, my, ch, pair{"after", "SELECT id, code, v FROM binwave.k ORDER BY id", "SELECT id, code, v FROM replica.k FINAL ORDER BY id"})
	if key := chSortingKey(t, ch, "k"); key != "code" {
		t.Errorf("sort key %q, want code", key)
	}
	if chTableUUID(t, ch) == before {
		t.Error("the table was not rebuilt")
	}
	for _, name := range chTables(t, ch) {
		if strings.HasSuffix(name, "__rebuild") {
			t.Errorf("the temporary table %s is left behind", name)
		}
	}
	e.stillRunning(t)
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("quarantined %v", got)
	}
}

// TestShorterPrimaryKeyRebuildsTheTableToo: the sort key is the primary key, so a key that shrinks is a
// rebuild as well (a finer sort key would identify the rows, but binwave expects the key it derives
// from the source). A key that stays what it was is not.
func TestShorterPrimaryKeyRebuildsTheTableToo(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.k (a INT NOT NULL, b INT NOT NULL, v INT NOT NULL, PRIMARY KEY (a, b))")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.k VALUES (1, 1, 1), (2, 1, 2)")
	waitSame(t, my, ch, pair{"before", "SELECT a, b, v FROM binwave.k ORDER BY a, b", "SELECT a, b, v FROM replica.k FINAL ORDER BY a, b"})
	before := chTableUUID(t, ch)

	// The same key again: nothing to do.
	mustExec(t, my, "ALTER TABLE binwave.k DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)")
	mustExec(t, my, "INSERT INTO binwave.k VALUES (4, 4, 4)")
	waitUntil(t, "the row after the statement", 60*time.Second, func() bool { return chRowCount(t, ch, "k") == 3 })
	if chTableUUID(t, ch) != before {
		t.Error("the table was rebuilt although its key did not change")
	}

	mustExec(t, my, "ALTER TABLE binwave.k DROP PRIMARY KEY, ADD PRIMARY KEY (a)",
		"UPDATE binwave.k SET b = 5 WHERE a = 1", "INSERT INTO binwave.k VALUES (3, 3, 3)")

	waitSame(t, my, ch, pair{"after", "SELECT a, b, v FROM binwave.k ORDER BY a", "SELECT a, b, v FROM replica.k FINAL ORDER BY a"})
	if key := chSortingKey(t, ch, "k"); key != "a" {
		t.Errorf("sort key %q, want a", key)
	}
	e.stillRunning(t)
}

// TestRenamedRetypedAndDroppedKeyColumnsRebuildTheTable: what was set aside for its sort key before.
func TestRenamedRetypedAndDroppedKeyColumnsRebuildTheTable(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.k (id INT NOT NULL, part INT NOT NULL, v VARCHAR(10) NOT NULL, PRIMARY KEY (id, part))")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.k VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c')")
	waitUntil(t, "the rows", 60*time.Second, func() bool { return chRowCount(t, ch, "k") == 3 })

	mustExec(t, my, "ALTER TABLE binwave.k RENAME COLUMN id TO ident", "UPDATE binwave.k SET v = 'b2' WHERE ident = 2")
	waitSame(t, my, ch, pair{"renamed", "SELECT ident, part, v FROM binwave.k ORDER BY ident, part", "SELECT ident, part, v FROM replica.k FINAL ORDER BY ident, part"})
	if key := chSortingKey(t, ch, "k"); key != "ident,part" {
		t.Errorf("sort key %q", key)
	}

	mustExec(t, my, "ALTER TABLE binwave.k MODIFY COLUMN ident BIGINT NOT NULL", "INSERT INTO binwave.k VALUES (9000000000, 1, 'big')")
	waitSame(t, my, ch, pair{"widened", "SELECT ident, part, v FROM binwave.k ORDER BY ident, part", "SELECT ident, part, v FROM replica.k FINAL ORDER BY ident, part"})
	types, err := textRows(ch.DB, "SELECT type FROM system.columns WHERE database = 'replica' AND table = 'k' AND name = 'ident'")
	if err != nil || len(types) != 1 || types[0] != "Int64" {
		t.Errorf("type of ident %v (%v), want Int64", types, err)
	}

	mustExec(t, my, "ALTER TABLE binwave.k DROP COLUMN part", "UPDATE binwave.k SET v = 'c2' WHERE ident = 3")
	waitSame(t, my, ch, pair{"dropped", "SELECT ident, v FROM binwave.k ORDER BY ident", "SELECT ident, v FROM replica.k FINAL ORDER BY ident"})
	if key := chSortingKey(t, ch, "k"); key != "ident" {
		t.Errorf("sort key %q, want ident", key)
	}
	e.stillRunning(t)
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("quarantined %v", got)
	}
}

// TestATableWithATTLIsSetAsideNotRebuilt: binwave rebuilds only the tables it made; one with a TTL that
// somebody added would lose it, so it is left for the operator, with the reason.
func TestATableWithATTLIsSetAsideNotRebuilt(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.k (id INT PRIMARY KEY, code VARCHAR(10) NOT NULL, at DATETIME NOT NULL)", "CREATE TABLE binwave.other (id INT PRIMARY KEY)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.k VALUES (1, 'a', NOW())", "INSERT INTO binwave.other VALUES (1)")
	waitUntil(t, "the rows", 60*time.Second, func() bool { return chRowCount(t, ch, "k") == 1 && chRowCount(t, ch, "other") == 1 })
	if _, err := ch.DB.Exec("ALTER TABLE replica.k MODIFY TTL toDateTime(at) + INTERVAL 100 YEAR"); err != nil {
		t.Fatal(err)
	}

	mustExec(t, my, "ALTER TABLE binwave.k DROP PRIMARY KEY, ADD PRIMARY KEY (code)", "INSERT INTO binwave.other VALUES (2)")

	waitUntil(t, "the table to be set aside", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })
	waitUntil(t, "the other table", 60*time.Second, func() bool { return chRowCount(t, ch, "other") == 2 })
	if q := e.hub.Status().Quarantined; len(q) != 1 || q[0].Table != "binwave.k" || !strings.Contains(q[0].Reason, "TTL") {
		t.Errorf("quarantined %+v", q)
	}
	if key := chSortingKey(t, ch, "k"); key != "id" {
		t.Errorf("sort key %q: a table that is not rebuilt must stay as it was", key)
	}
}

func TestAdditiveModeDoesNotRebuild(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.k (id INT PRIMARY KEY, code VARCHAR(10) NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLAdditive
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.k VALUES (1, 'a')")
	waitUntil(t, "the row", 60*time.Second, func() bool { return chRowCount(t, ch, "k") == 1 })

	mustExec(t, my, "ALTER TABLE binwave.k DROP PRIMARY KEY, ADD PRIMARY KEY (code)")

	waitUntil(t, "the table to be set aside", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })
	if chSortingKey(t, ch, "k") != "id" {
		t.Error("the table was rebuilt in additive mode")
	}
}

// binwaveShaped makes a table of the shape binwave creates, with rows given as (id, code, version).
func binwaveShaped(t *testing.T, ch *testenv.ClickHouse, table, key string, rows string) {
	t.Helper()
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS replica",
		"CREATE TABLE replica." + table + " (id Int32, code String, _version UInt64, _is_deleted UInt8, _synced_at DateTime64(3, 'UTC')) " +
			"ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY (" + key + ")",
	} {
		if _, err := ch.DB.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	if rows != "" {
		if _, err := ch.DB.Exec("INSERT INTO replica." + table + " (id, code, _version, _is_deleted, _synced_at) VALUES " + rows); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRebuildCanBeRepeatedAtEveryStep: after a crash the same rebuild runs again. A leftover copy with
// the right key is reused, a table that is already rebuilt is done, and a leftover copy with another
// key is never touched.
func TestRebuildCanBeRepeatedAtEveryStep(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	client := openSink(t, ch)
	ctx := context.Background()
	binwaveShaped(t, ch, "t", "id", "(1, 'a', 1, 0, now()), (1, 'a2', 2, 0, now()), (2, 'b', 1, 0, now()), (3, 'c', 1, 0, now()), (3, 'c', 2, 1, now())")
	req := chsink.RebuildRequest{Source: "s.t", Database: "replica", Table: "t", Key: []string{"code"}}
	var reported uint64
	req.Progress = func(rows uint64) { reported = rows }

	// A leftover copy with the right key and a partial fill, as a crash in the middle of the copy leaves it.
	binwaveShaped(t, ch, "t__rebuild", "code", "(2, 'b', 1, 0, now())")
	markRebuild(t, ch, "t__rebuild")

	if err := client.Rebuild(ctx, req); err != nil {
		t.Fatal(err)
	}
	if reported != 2 {
		t.Errorf("progress reported %d rows, want the 2 live rows (1 in its latest version, 2)", reported)
	}
	if got, err := textRows(ch.DB, "SELECT id, code FROM replica.t FINAL WHERE _is_deleted = 0 ORDER BY id"); err != nil || strings.Join(got, ";") != "1\ta2;2\tb" {
		t.Errorf("rows %v (%v): the latest version of row 1 only, row 3 was deleted", got, err)
	}
	if chSortingKey(t, ch, "t") != "code" {
		t.Error("the table has not the new sort key")
	}
	for _, name := range chTables(t, ch) {
		if strings.HasSuffix(name, "__rebuild") {
			t.Errorf("%s is left behind", name)
		}
	}

	// Again, as a replay of the same statement: done already.
	if err := client.Rebuild(ctx, req); err != nil {
		t.Fatalf("the second rebuild: %v", err)
	}
	// And with the swap done but the old table not yet dropped: the copy, now t, has the marker.
	binwaveShaped(t, ch, "t__rebuild", "id", "(9, 'old', 1, 0, now())")
	markRebuild(t, ch, "t")
	if err := client.Rebuild(ctx, req); err != nil {
		t.Fatalf("the rebuild after a swap that was not cleaned up: %v", err)
	}
	if got := chTables(t, ch); strings.Join(got, ",") != "t" {
		t.Errorf("tables %v, want only t", got)
	}
	if got := chTableComment(t, ch, "t"); got != "" {
		t.Errorf("the comment of t = %q, want the marker removed", got)
	}
}

// markRebuild gives a table the marker of the copy a rebuild makes.
func markRebuild(t *testing.T, ch *testenv.ClickHouse, table string) {
	t.Helper()
	if _, err := ch.DB.Exec("ALTER TABLE replica." + table + " MODIFY COMMENT '" + chsink.RebuildMarker + "'"); err != nil {
		t.Fatal(err)
	}
}

func chTableComment(t *testing.T, ch *testenv.ClickHouse, table string) string {
	t.Helper()
	var comment string
	if err := ch.DB.QueryRow("SELECT comment FROM system.tables WHERE database = 'replica' AND name = ?", table).Scan(&comment); err != nil {
		t.Fatalf("comment of %s: %v", table, err)
	}
	return comment
}

// TestRebuildNeverUsesOrDropsATableItDidNotMake: a t__rebuild without the marker is another table,
// a replicated one or one made by hand, even with the right key: it is not filled, swapped in or
// dropped, before the rebuild or after it.
func TestRebuildNeverUsesOrDropsATableItDidNotMake(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	client := openSink(t, ch)
	ctx := context.Background()
	binwaveShaped(t, ch, "t", "id", "(1, 'a', 1, 0, now())")
	binwaveShaped(t, ch, "t__rebuild", "code", "(7, 'other', 1, 0, now())")
	req := chsink.RebuildRequest{Source: "s.t", Database: "replica", Table: "t", Key: []string{"code"}}

	err := client.Rebuild(ctx, req)
	if !chsink.IsConflict(err) || !strings.Contains(err.Error(), "t__rebuild") {
		t.Fatalf("Rebuild() = %v, want a conflict that names the table", err)
	}
	if chSortingKey(t, ch, "t") != "id" || chRowCount(t, ch, "t") != 1 || chRowCount(t, ch, "t__rebuild") != 1 {
		t.Error("a table was changed")
	}

	// Rebuilt meanwhile, by hand: the other table stays.
	if _, err := ch.DB.Exec("DROP TABLE replica.t"); err != nil {
		t.Fatal(err)
	}
	binwaveShaped(t, ch, "t", "code", "(1, 'a', 1, 0, now())")
	if err := client.Rebuild(ctx, req); err != nil {
		t.Fatalf("Rebuild() of a table with the new key = %v", err)
	}
	if chRowCount(t, ch, "t__rebuild") != 1 {
		t.Error("the other table was dropped")
	}
}

func TestRebuildLeavesALeftoverOfAnotherKeyAlone(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	client := openSink(t, ch)
	binwaveShaped(t, ch, "t", "id", "(1, 'a', 1, 0, now())")
	binwaveShaped(t, ch, "t__rebuild", "id, code", "")

	err := client.Rebuild(context.Background(), chsink.RebuildRequest{Source: "s.t", Database: "replica", Table: "t", Key: []string{"code"}})

	if !chsink.IsConflict(err) || !strings.Contains(err.Error(), "t__rebuild") {
		t.Fatalf("Rebuild() = %v, want a conflict that names the leftover", err)
	}
	if chSortingKey(t, ch, "t") != "id" || chRowCount(t, ch, "t") != 1 {
		t.Error("the original was changed")
	}
}

// TestRebuildThatWouldLoseRowsIsNotSwapped: two rows with the same new key would become one. The
// source cannot allow it, so if it happens something is wrong, and nothing is replaced.
func TestRebuildThatWouldLoseRowsIsNotSwapped(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	client := openSink(t, ch)
	binwaveShaped(t, ch, "t", "id", "(1, 'same', 1, 0, now()), (2, 'same', 1, 0, now())")

	err := client.Rebuild(context.Background(), chsink.RebuildRequest{Source: "s.t", Database: "replica", Table: "t", Key: []string{"code"}})

	if !chsink.IsConflict(err) || !strings.Contains(err.Error(), "collapsed") {
		t.Fatalf("Rebuild() = %v, want a conflict about collapsed rows", err)
	}
	if chSortingKey(t, ch, "t") != "id" || chRowCount(t, ch, "t") != 2 {
		t.Error("the original was replaced")
	}
}

func TestRebuildRefusesWhatItDidNotMake(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	client := openSink(t, ch)
	if _, err := ch.DB.Exec("CREATE DATABASE IF NOT EXISTS replica"); err != nil {
		t.Fatal(err)
	}
	for name, ddl := range map[string]string{
		"plain":       "CREATE TABLE replica.plain (id Int32, code String) ENGINE = MergeTree ORDER BY id",
		"partitioned": "CREATE TABLE replica.partitioned (id Int32, code String, _version UInt64, _is_deleted UInt8) ENGINE = ReplacingMergeTree(_version, _is_deleted) PARTITION BY id % 2 ORDER BY id",
		"computed":    "CREATE TABLE replica.computed (id Int32, code String, twice Int32 MATERIALIZED id * 2, _version UInt64, _is_deleted UInt8) ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY id",
	} {
		if _, err := ch.DB.Exec(ddl); err != nil {
			t.Fatal(err)
		}
		err := client.Rebuild(context.Background(), chsink.RebuildRequest{Source: "s." + name, Database: "replica", Table: name, Key: []string{"code"}})
		if !chsink.IsTableProblem(err) || !strings.Contains(err.Error(), "by hand") {
			t.Errorf("%s: Rebuild() = %v, want a table problem that says to do it by hand", name, err)
		}
	}
}

// TestOnlineSchemaChangeThatChangesThePrimaryKeyNeedsNoRebuild: the shadow table of the tool has the new
// key from the start, replicated as any table, and the swap is a rename: the copy inside ClickHouse
// that an in-place ALTER needs does not happen.
func TestOnlineSchemaChangeThatChangesThePrimaryKeyNeedsNoRebuild(t *testing.T) {
	t.Parallel()
	for _, tool := range []string{"pt-osc", "gh-ost"} {
		t.Run(tool, func(t *testing.T) {
			t.Parallel()
			my := testenv.StartMySQL(t)
			ch := testenv.StartClickHouse(t)
			mustExec(t, my, "CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, qty INT NOT NULL)")
			cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.orders"]}`)
			cfg.DDL.Mode = config.DDLFull
			e := startHubEngine(t, cfg)
			mustExec(t, my, "INSERT INTO binwave.orders VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")
			waitSame(t, my, ch, pair{"before", "SELECT id, v, qty FROM binwave.orders ORDER BY id", "SELECT id, v, qty FROM replica.orders FINAL ORDER BY id"})

			r := migrationsim.New(rootDB(t, my), "binwave", "orders")
			alter := "DROP PRIMARY KEY, ADD PRIMARY KEY (id, v)"
			var err error
			if tool == "pt-osc" {
				err = r.PtOSC(context.Background(), alter, migrationsim.PtOSCOptions{})
			} else {
				err = r.Ghost(context.Background(), alter, migrationsim.GhostOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			mustExec(t, my, "UPDATE binwave.orders SET v = 'b2' WHERE id = 2", "INSERT INTO binwave.orders VALUES (2, 'b', 22)")

			waitSame(t, my, ch, pair{"after", "SELECT id, v, qty FROM binwave.orders ORDER BY id, v", "SELECT id, v, qty FROM replica.orders FINAL ORDER BY id, v"})
			if key := chSortingKey(t, ch, "orders"); key != "id,v" {
				t.Errorf("sort key %q, want id,v", key)
			}
			if got := scrapeCounter(t, e, "binwave_table_rebuilds_total"); got != "0" {
				t.Errorf("binwave_table_rebuilds_total = %s: the shadow table has the new key already, no rebuild is needed", got)
			}
			e.stillRunning(t)
		})
	}
}

func scrapeCounter(t *testing.T, e *hubEngine, series string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	e.hub.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatalf("series %s not exposed", series)
	return ""
}
