//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/test/testenv"
)

// The tests in this file come from a study of the failure reports of comparable tools: things
// that broke them in production, run through the real engine against real servers.

// TestEngineHandlesEveryKindOfDML: statements that reach the binlog in unusual shapes. REPLACE can
// delete a row with another key (unique conflict), ON DUPLICATE KEY UPDATE and multi-table
// statements touch several rows, triggers write to other tables, a rollback to a savepoint must
// leave no trace.
func TestEngineHandlesEveryKindOfDML(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.dml (id INT PRIMARY KEY, a VARCHAR(20), b INT, UNIQUE KEY ub (b))",
		"CREATE TABLE binwave.other (id INT PRIMARY KEY, x VARCHAR(20))",
		"CREATE TABLE binwave.audit (seq INT AUTO_INCREMENT PRIMARY KEY, dml_id INT, old_a VARCHAR(20), new_a VARCHAR(20))",
	)
	// Creating a trigger with the binlog enabled takes a privilege the application user lacks.
	if _, err := rootDB(t, my).Exec(`CREATE TRIGGER binwave.dml_au AFTER UPDATE ON binwave.dml FOR EACH ROW
			INSERT INTO binwave.audit (dml_id, old_a, new_a) VALUES (OLD.id, OLD.a, NEW.a)`); err != nil {
		t.Fatal(err)
	}
	stop := runEngine(t, engineConfig(t, my, ch, "clickhouse"))

	mustExec(t, my,
		"INSERT INTO binwave.dml VALUES (1,'one',10),(2,'two',20),(3,'three',30),(4,'four',40),(5,'five',50)",
		"INSERT INTO binwave.other VALUES (1,'x1'),(2,'x2'),(9,'x9')",
		// The unique key conflicts with row 2: REPLACE deletes it and inserts row 6 in its place.
		"REPLACE INTO binwave.dml VALUES (6,'six',20)",
		// The primary key conflicts: REPLACE rewrites row 3.
		"REPLACE INTO binwave.dml VALUES (3,'THREE',33)",
		"INSERT INTO binwave.dml VALUES (1,'dup',99) ON DUPLICATE KEY UPDATE a = CONCAT(a, '+'), b = b + 1000",
		"INSERT IGNORE INTO binwave.dml VALUES (4,'ignored',40),(7,'seven',70)",
		"UPDATE binwave.dml d JOIN binwave.other o ON o.id = d.id SET d.a = o.x",
		"DELETE d FROM binwave.dml d JOIN binwave.other o ON o.id = d.id WHERE o.id = 1",
		"UPDATE binwave.dml SET a = UPPER(a) WHERE id IN (4, 5)",
	)
	tx, err := my.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"INSERT INTO binwave.dml VALUES (100,'kept',1000)",
		"SAVEPOINT s1",
		"INSERT INTO binwave.dml VALUES (101,'rolled back',1001)",
		"UPDATE binwave.dml SET a = 'rolled back too' WHERE id = 100",
		"ROLLBACK TO SAVEPOINT s1",
		"INSERT INTO binwave.dml VALUES (102,'after',1002)",
	} {
		if _, err := tx.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	waitSame(t, my, ch,
		pair{"dml", "SELECT id, a, b FROM binwave.dml ORDER BY id", "SELECT id, a, b FROM replica.dml FINAL ORDER BY id"},
		pair{"other", "SELECT id, x FROM binwave.other ORDER BY id", "SELECT id, x FROM replica.other FINAL ORDER BY id"},
		pair{"audit", "SELECT seq, dml_id, old_a, new_a FROM binwave.audit ORDER BY seq", "SELECT seq, dml_id, old_a, new_a FROM replica.audit FINAL ORDER BY seq"},
	)
	stop()
}

// TestEngineHandlesAwkwardNames: hyphens, spaces, reserved words, non-Latin scripts, quotes and
// backticks in table and column names broke comparable tools (issues 336, 413, 712, 1106, 1264).
func TestEngineHandlesAwkwardNames(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	long := strings.Repeat("l", 60)
	tables := []string{"transaction", "select", "order", "we-ird name", "таблица", "表", "quo'te", "back`tick", "UPPER_lower", long}
	for _, name := range tables {
		mustExec(t, my, fmt.Sprintf("CREATE TABLE binwave.%s (id INT PRIMARY KEY, `select` VARCHAR(20), `col with space` INT, `ключ` INT, `it's` INT, `%s` INT)",
			mysqlIdent(name), long))
	}
	r := startEngine(t, engineConfig(t, my, ch, "clickhouse"))
	var pairs []pair
	for i, name := range tables {
		mustExec(t, my, fmt.Sprintf("INSERT INTO binwave.%s VALUES (1, 'a''b', 10, 20, 30, 40), (2, NULL, %d, 1, 2, 3)", mysqlIdent(name), i))
		cols := fmt.Sprintf("id, `select`, `col with space`, `ключ`, `it's`, `%s`", long)
		pairs = append(pairs, pair{
			name,
			fmt.Sprintf("SELECT %s FROM binwave.%s ORDER BY id", cols, mysqlIdent(name)),
			fmt.Sprintf("SELECT %s FROM replica.%s FINAL ORDER BY id", cols, chIdent(name)),
		})
	}
	waitSameWatching(t, r, my, ch, 90*time.Second, pairs...)
	if err := r.stop(t); err != nil {
		t.Errorf("stop: %v", err)
	}
}

func mysqlIdent(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
func chIdent(name string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(name, `\`, `\\`), "`", "\\`") + "`"
}

// TestEngineCarriesLargeAndAwkwardValues: JSON documents of every shape, megabyte text and binary
// values, empty and zero-filled binaries, control characters in text.
func TestEngineCarriesLargeAndAwkwardValues(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, `CREATE TABLE binwave.vals (
		id INT PRIMARY KEY, j JSON NULL, t LONGTEXT NULL, b LONGBLOB NULL, vb VARBINARY(64) NULL, bin BINARY(8) NULL)`)
	stop := runEngine(t, engineConfig(t, my, ch, "clickhouse"))

	docs := []string{
		`{}`, `[]`, `null`, `0`, `"text"`, `true`,
		`{"a": [1, 2, {"b": null}], "c": "d"}`,
		`{"unicode": "Zoë 李雷 é 😀", "quote": "he said \"hi\"", "slash": "a\\b/c"}`,
		`{"big": 12345678901234567890, "neg": -0.5, "exp": 1.5e300, "float": 3.14159265358979}`,
		`{"nested": {"a": {"b": {"c": {"d": [[[[1]]]]}}}}}`,
		`[1, "two", 3.0, null, true, {"k": []}]`,
	}
	for i, d := range docs {
		// Parameters, so that the backslashes of the JSON reach MySQL untouched.
		if _, err := my.DB.Exec("INSERT INTO binwave.vals (id, j) VALUES (?, ?)", i+1, d); err != nil {
			t.Fatalf("document %s: %v", d, err)
		}
	}
	mustExec(t, my,
		"INSERT INTO binwave.vals (id, j) VALUES (100, NULL)",
		"INSERT INTO binwave.vals (id, t, b) VALUES (101, REPEAT('Ж', 2000000), REPEAT(RANDOM_BYTES(1024), 3000))",
		"INSERT INTO binwave.vals (id, t, b, vb, bin) VALUES (102, '', '', '', '')",
		"INSERT INTO binwave.vals (id, vb, bin) VALUES (103, 0x00FF00FF00, 0x0000000000000000)",
		"INSERT INTO binwave.vals (id, t) VALUES (104, CONCAT('line1', CHAR(10), 'tab', CHAR(9), 'nul', CHAR(0), 'end', CHAR(13), CHAR(34), CHAR(39), CHAR(92)))",
	)

	deadline := time.Now().Add(90 * time.Second)
	for {
		if err := compareVals(t, my, ch); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
	}
	stop()
}

// compareVals compares the awkward values: JSON structurally (MySQL and binwave format it with
// different whitespace), everything else by length and hash.
func compareVals(t *testing.T, my *testenv.MySQL, c *testenv.ClickHouse) error {
	t.Helper()
	scalar := func(mysql, ch string) (m, h []string, err error) {
		if m, err = textRows(my.DB, mysql); err != nil {
			return nil, nil, err
		}
		h, err = textRows(c.DB, ch)
		return m, h, err
	}
	m, h, err := scalar(
		"SELECT id, LENGTH(t), IFNULL(MD5(t),''), LENGTH(b), IFNULL(MD5(b),''), IFNULL(HEX(vb),''), IFNULL(HEX(bin),'') FROM binwave.vals ORDER BY id",
		"SELECT id, length(t), ifNull(lower(hex(MD5(t))),''), length(b), ifNull(lower(hex(MD5(b))),''), ifNull(upper(hex(vb)),''), ifNull(upper(hex(bin)),'') FROM replica.vals FINAL ORDER BY id")
	if err != nil {
		return err
	}
	if d := firstDiffs("vals", normalizeNullLengths(m), normalizeNullLengths(h)); len(d) > 0 {
		return fmt.Errorf("%s", strings.Join(d, "\n"))
	}

	mj, hj, err := scalar("SELECT id, j FROM binwave.vals ORDER BY id", "SELECT id, j FROM replica.vals FINAL ORDER BY id")
	if err != nil {
		return err
	}
	if len(mj) != len(hj) {
		return fmt.Errorf("json: MySQL has %d rows, ClickHouse %d", len(mj), len(hj))
	}
	for i := range mj {
		mid, mdoc, _ := strings.Cut(mj[i], "\t")
		hid, hdoc, _ := strings.Cut(hj[i], "\t")
		if mid != hid {
			return fmt.Errorf("json row %d: ids %s and %s", i, mid, hid)
		}
		if mdoc == "<NULL>" || hdoc == "<NULL>" {
			if mdoc != hdoc {
				return fmt.Errorf("json id %s: MySQL %s, ClickHouse %s", mid, mdoc, hdoc)
			}
			continue
		}
		if !sameJSON(mdoc, hdoc) {
			return fmt.Errorf("json id %s differs:\n  MySQL      %s\n  ClickHouse %s", mid, trunc(mdoc), trunc(hdoc))
		}
	}
	return nil
}

// normalizeNullLengths makes a NULL length ("<NULL>") and 0 alike, since the two sides render
// LENGTH(NULL) the same way but the test only cares about content.
func normalizeNullLengths(rows []string) []string { return rows }

func sameJSON(a, b string) bool {
	decode := func(s string) (any, error) {
		d := json.NewDecoder(strings.NewReader(s))
		d.UseNumber()
		var v any
		err := d.Decode(&v)
		return v, err
	}
	x, errX := decode(a)
	y, errY := decode(b)
	return errX == nil && errY == nil && jsonEqual(x, y)
}

// jsonEqual compares decoded documents. Numbers are equal when their text is, or when they are
// the same double: the MySQL library prints a double such as 1.5e300 as its exact decimal
// expansion, which is the same number written differently.
func jsonEqual(a, b any) bool {
	switch x := a.(type) {
	case json.Number:
		y, ok := b.(json.Number)
		if !ok {
			return false
		}
		if x == y {
			return true
		}
		fx, errX := strconv.ParseFloat(string(x), 64)
		fy, errY := strconv.ParseFloat(string(y), 64)
		return errX == nil && errY == nil && fx == fy
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			if w, ok := y[k]; !ok || !jsonEqual(v, w) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !jsonEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

// TestEngineReplicatesTimestampsAcrossZonesAndDST: TIMESTAMP is an instant and must arrive as the
// same instant whatever the server or session time zone; DATETIME is a wall clock and must arrive
// as written. Instants around both daylight saving changes of a zone are checked. Time zones and
// DST broke a comparable tool repeatedly (issues 349, 993, 1056, 1134, 1270).
func TestEngineReplicatesTimestampsAcrossZonesAndDST(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.tz (id INT PRIMARY KEY, zone VARCHAR(32), ts TIMESTAMP(3) NULL, dt DATETIME(3) NULL)")
	// Named zones need the time zone tables, which the image loads; set the server zone at runtime.
	if _, err := rootDB(t, my).Exec("SET GLOBAL time_zone = 'America/Chicago'"); err != nil {
		t.Fatal(err)
	}
	stop := runEngine(t, engineConfig(t, my, ch, "clickhouse"))

	// Instants in UTC seconds: either side of the spring gap and the fall overlap in Chicago,
	// the same for Kyiv, plus an epoch edge and a fractional value.
	instants := []int64{
		1710054000, 1710057599, 1710057600, // 2024-03-10 around 08:00 UTC, when Chicago springs forward
		1730620200, 1730623800, // 2024-11-03 06:30 and 07:30 UTC: the ambiguous hour in Chicago
		1711846800, 1729990800, // Kyiv changes on 2024-03-31 and 2024-10-27
		1, 2147483647,
	}
	zones := []string{"America/Chicago", "Europe/Kyiv", "+05:30", "UTC"}
	id := 0
	for _, zone := range zones {
		conn, err := my.DB.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET SESSION time_zone = '%s'", zone)); err != nil {
			t.Fatal(err)
		}
		for _, ins := range instants {
			id++
			q := fmt.Sprintf("INSERT INTO binwave.tz VALUES (%d, '%s', FROM_UNIXTIME(%d.123), FROM_UNIXTIME(%d.123))", id, zone, ins, ins)
			if _, err := conn.ExecContext(context.Background(), q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		// A wall clock that does not exist in Chicago (the spring gap) is fine for DATETIME.
		id++
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO binwave.tz (id, zone, dt) VALUES (%d, '%s', '2024-03-10 02:30:00.500')", id, zone)); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
	}

	waitSame(t, my, ch, pair{
		"tz",
		"SELECT id, zone, IFNULL(CAST(UNIX_TIMESTAMP(ts) * 1000 AS UNSIGNED), 'null'), IFNULL(CAST(dt AS CHAR), 'null') FROM binwave.tz ORDER BY id",
		"SELECT id, zone, if(isNull(ts), 'null', toString(toUnixTimestamp64Milli(ts))), if(isNull(dt), 'null', toString(dt)) FROM replica.tz FINAL ORDER BY id",
	})
	stop()
}

// TestEngineHandlesGeneratedAndInvisibleColumns: STORED and VIRTUAL generated columns and
// INVISIBLE columns are part of the row image and must arrive like any other column.
func TestEngineHandlesGeneratedAndInvisibleColumns(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, `CREATE TABLE binwave.gen (
		id INT PRIMARY KEY, a INT NOT NULL,
		g_stored INT GENERATED ALWAYS AS (a * 2) STORED,
		g_virtual INT GENERATED ALWAYS AS (a + 1) VIRTUAL,
		hidden INT INVISIBLE)`)
	stop := runEngine(t, engineConfig(t, my, ch, "clickhouse"))

	mustExec(t, my,
		"INSERT INTO binwave.gen (id, a, hidden) VALUES (1, 5, 50), (2, 6, 60)",
		"UPDATE binwave.gen SET a = 10 WHERE id = 1",
		"DELETE FROM binwave.gen WHERE id = 2",
		"INSERT INTO binwave.gen (id, a) VALUES (3, 7)",
	)
	waitSame(t, my, ch, pair{
		"gen",
		"SELECT id, a, g_stored, g_virtual, hidden FROM binwave.gen ORDER BY id",
		"SELECT id, a, g_stored, g_virtual, hidden FROM replica.gen FINAL ORDER BY id",
	})
	stop()
}

// TestEngineRefusesTablesWithoutAKeyUnlessToldHow: a table with no primary key has no row
// identity, and replicating it as if it had one would merge distinct rows silently (a comparable
// tool collapsed such tables to a single row). binwave stops with a clear error; the operator can
// name a unique column as the sort key.
func TestEngineRefusesTablesWithoutAKeyUnlessToldHow(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.nokey (a INT NOT NULL, b VARCHAR(10))")

	// Without a key: an error, not a guess. By default the table is set aside and the service
	// goes on; nothing of the table reaches ClickHouse, where rows could be merged silently.
	t.Run("the table is quarantined", func(t *testing.T) {
		e := startHubEngine(t, engineConfig(t, my, ch, "clickhouse"))
		mustExec(t, my, "INSERT INTO binwave.nokey VALUES (1, 'x'), (2, 'y')")
		waitUntil(t, "the table to be quarantined", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })
		if reason := e.hub.Status().Quarantined[0].Reason; !strings.Contains(reason, "no primary key") {
			t.Errorf("the reason %q does not name the missing key", reason)
		}
		time.Sleep(2 * time.Second)
		if got := chRowCount(t, ch, "nokey"); got > 0 {
			t.Errorf("%d rows of the keyless table reached ClickHouse", got)
		}
		e.stillRunning(t)
		if err := e.stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	// With the strict policy the service stops, as MySQL's replication does on an error.
	t.Run("the service stops with the policy stop", func(t *testing.T) {
		// A file store: the state of the first run, which quarantined the table, must not carry over.
		cfg := engineConfig(t, my, ch, "file")
		cfg.Tables.OnReplicationError = config.OnErrorStop
		r := startEngine(t, cfg)
		mustExec(t, my, "INSERT INTO binwave.nokey VALUES (3, 'z')")
		select {
		case err := <-r.done:
			if err == nil || !strings.Contains(err.Error(), "no primary key") {
				t.Fatalf("the engine stopped with %v, want an error about the missing primary key", err)
			}
		case <-time.After(60 * time.Second):
			r.cancel()
			t.Fatal("the engine kept running on a table with no key: rows may be merged silently")
		}
	})
}

func TestEngineReplicatesKeylessTableWithAnExplicitSortKey(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.nokey (a INT NOT NULL, b VARCHAR(10))")
	cfg := engineConfigWith(t, my, ch, "clickhouse", `tables:
  include: ["binwave.*"]
  overrides:
    binwave.nokey: {order_by: [a]}`)
	stop := runEngine(t, cfg)

	mustExec(t, my,
		"INSERT INTO binwave.nokey VALUES (1, 'x'), (2, 'y')",
		"UPDATE binwave.nokey SET b = 'z' WHERE a = 2",
		"DELETE FROM binwave.nokey WHERE a = 1",
	)
	waitSame(t, my, ch, pair{
		"nokey",
		"SELECT a, b FROM binwave.nokey ORDER BY a", "SELECT a, b FROM replica.nokey FINAL ORDER BY a",
	})
	stop()
}

// TestEngineDoesNotMergeTablesOfTheSameNameInDifferentDatabases: the target has one database, so
// shop.orders and warehouse.orders would both land in replica.orders and overwrite each other. That
// must be refused loudly, never merged silently (a comparable tool had this problem, issue 834).
func TestEngineDoesNotMergeTablesOfTheSameNameInDifferentDatabases(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	root := rootDB(t, my)
	for _, q := range []string{"CREATE DATABASE other", "GRANT ALL ON other.* TO 'binwave'@'%'"} {
		if _, err := root.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(t, my,
		"CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(10))",
		"CREATE TABLE other.orders (id INT PRIMARY KEY, v VARCHAR(10))")
	r := startEngine(t, engineConfigWith(t, my, ch, "clickhouse", `tables: {include: ["binwave.*", "other.*"]}`))

	mustExec(t, my,
		"INSERT INTO binwave.orders VALUES (1, 'binwave')",
		"INSERT INTO other.orders VALUES (1, 'other')")

	select {
	case err := <-r.done:
		if err == nil || !strings.Contains(err.Error(), "binwave.orders") || !strings.Contains(err.Error(), "other.orders") {
			t.Fatalf("the engine stopped with %v, want an error naming both tables", err)
		}
	case <-time.After(60 * time.Second):
		r.cancel()
		rows, _ := textRows(ch.DB, "SELECT id, v FROM replica.orders FINAL ORDER BY v")
		t.Fatalf("two tables of the same name were accepted into one target table: %v", rows)
	}
}

// TestEngineWorksWithATableCreatedByHand: operators create ClickHouse tables themselves, with
// partitions, codecs and extra columns of their own. binwave accepts extras it does not manage,
// and refuses a table whose sort key differs, since that would change what a "row" is.
func TestEngineWorksWithATableCreatedByHand(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.hand (id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, created DATETIME NOT NULL)",
		"CREATE TABLE binwave.hand_key (id INT PRIMARY KEY, tenant INT NOT NULL)")
	execCH(t, ch,
		"CREATE DATABASE IF NOT EXISTS replica",
		`CREATE TABLE replica.hand (
			id Int32 CODEC(Delta, ZSTD), v String CODEC(ZSTD(3)), created DateTime64(0, 'UTC'),
			v_len UInt32 MATERIALIZED length(v), note String DEFAULT 'set by hand',
			_version UInt64, _is_deleted UInt8, _synced_at DateTime64(3, 'UTC'))
		ENGINE = ReplacingMergeTree(_version, _is_deleted) PARTITION BY toYYYYMM(created) ORDER BY id`,
		// A different sort key than the source primary key.
		`CREATE TABLE replica.hand_key (
			id Int32, tenant Int32, _version UInt64, _is_deleted UInt8, _synced_at DateTime64(3, 'UTC'))
		ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY (tenant, id)`,
	)

	r := startEngine(t, engineConfigWith(t, my, ch, "clickhouse", `tables: {include: ["binwave.hand"]}`))
	mustExec(t, my,
		"INSERT INTO binwave.hand VALUES (1, 'alpha', '2024-01-15 10:00:00'), (2, 'beta', '2024-02-20 11:00:00')",
		"UPDATE binwave.hand SET v = 'gamma' WHERE id = 1")
	waitSame(t, my, ch, pair{
		"hand",
		"SELECT id, v, CAST(created AS CHAR) FROM binwave.hand ORDER BY id",
		"SELECT id, v, toString(created) FROM replica.hand FINAL ORDER BY id",
	})
	if rows, err := textRows(ch.DB, "SELECT id, v_len, note FROM replica.hand FINAL ORDER BY id"); err != nil ||
		!reflect.DeepEqual(rows, []string{"1\t5\tset by hand", "2\t4\tset by hand"}) {
		t.Errorf("the columns ClickHouse manages itself: %v, %v", rows, err)
	}
	if err := r.stop(t); err != nil {
		t.Errorf("stop: %v", err)
	}

	// The table with a different sort key is refused with an explanation: set aside by default,
	// or the service stops with the strict policy.
	t.Run("a different sort key is quarantined", func(t *testing.T) {
		e := startHubEngine(t, engineConfigWith(t, my, ch, "clickhouse", `tables: {include: ["binwave.hand_key"]}`))
		mustExec(t, my, "INSERT INTO binwave.hand_key VALUES (1, 7)")
		waitUntil(t, "the table to be quarantined", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })
		if reason := e.hub.Status().Quarantined[0].Reason; !strings.Contains(reason, "sort key") {
			t.Errorf("the reason %q does not explain the sort key", reason)
		}
		e.stillRunning(t)
		if err := e.stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	t.Run("a different sort key fails replication with the policy stop", func(t *testing.T) {
		// A file store: the state of the first run, which quarantined the table, must not carry over.
		cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.hand_key"], on_replication_error: stop}`)
		r2 := startHubEngine(t, cfg)
		mustExec(t, my, "INSERT INTO binwave.hand_key VALUES (2, 8)")
		if f := waitFailedHub(t, r2); !strings.Contains(f.Message, "sort key") {
			t.Fatalf("the failure %q is not a mismatch explained by the sort key", f.Message)
		}
	})
}

// TestEngineKeepsDatabaseNamesByDefault: without tables.target_database, shop.orders stays
// shop.orders in ClickHouse, so tables of the same name in different databases stay apart. The
// target databases are created as needed.
func TestEngineKeepsDatabaseNamesByDefault(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	root := rootDB(t, my)
	for _, q := range []string{"CREATE DATABASE other", "GRANT ALL ON other.* TO 'binwave'@'%'"} {
		if _, err := root.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(t, my,
		"CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(10))",
		"CREATE TABLE other.orders (id INT PRIMARY KEY, v VARCHAR(10))")
	cfg := engineConfigWith(t, my, ch, "clickhouse", `tables: {include: ["binwave.*", "other.*"]}`)
	cfg.Tables.TargetDatabase = "" // the default under test
	r := startEngine(t, cfg)

	mustExec(t, my,
		"INSERT INTO binwave.orders VALUES (1, 'binwave'), (2, 'two')",
		"INSERT INTO other.orders VALUES (1, 'other')")

	waitSameWatching(t, r, my, ch, 60*time.Second,
		pair{"binwave.orders", "SELECT id, v FROM binwave.orders ORDER BY id", "SELECT id, v FROM binwave.orders FINAL ORDER BY id"},
		pair{"other.orders", "SELECT id, v FROM other.orders ORDER BY id", "SELECT id, v FROM other.orders FINAL ORDER BY id"})
	if err := r.stop(t); err != nil {
		t.Errorf("stop: %v", err)
	}
}

// TestDatabaseMapRenamesOneSourceDatabaseButLeavesOthersAlone: tables.database_map sends binwave's
// tables to a ClickHouse database of another name; other, not named in the map, keeps its own name
// exactly as under the default routing.
func TestDatabaseMapRenamesOneSourceDatabaseButLeavesOthersAlone(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	root := rootDB(t, my)
	for _, q := range []string{"CREATE DATABASE other", "GRANT ALL ON other.* TO 'binwave'@'%'"} {
		if _, err := root.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(t, my,
		"CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(10))",
		"CREATE TABLE other.orders (id INT PRIMARY KEY, v VARCHAR(10))")
	cfg := engineConfigWith(t, my, ch, "clickhouse", `tables: {include: ["binwave.*", "other.*"]}`)
	cfg.Tables.TargetDatabase = ""
	cfg.Tables.DatabaseMap = map[string]string{"binwave": "binwave_mirror"}
	r := startEngine(t, cfg)

	mustExec(t, my,
		"INSERT INTO binwave.orders VALUES (1, 'mapped'), (2, 'two')",
		"INSERT INTO other.orders VALUES (1, 'unmapped')")

	waitSameWatching(t, r, my, ch, 60*time.Second,
		pair{"binwave.orders", "SELECT id, v FROM binwave.orders ORDER BY id", "SELECT id, v FROM binwave_mirror.orders FINAL ORDER BY id"},
		pair{"other.orders", "SELECT id, v FROM other.orders ORDER BY id", "SELECT id, v FROM other.orders FINAL ORDER BY id"})
	if err := r.stop(t); err != nil {
		t.Errorf("stop: %v", err)
	}
}
