//go:build integration

package integration

import (
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/test/testenv"
)

// TestAddedColumnsGiveExistingRowsTheValuesMySQLGave is the consistency criterion of the slice:
// after an ALTER TABLE ADD COLUMN the rows that existed before it read, in ClickHouse, exactly
// what they read in MySQL, and so do the rows written afterwards and the rows updated afterwards.
func TestAddedColumnsGiveExistingRowsTheValuesMySQLGave(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL)")
	e := startHubEngine(t, engineConfig(t, my, ch, "file"))

	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'ann'), (2, 'bob'), (3, 'cy')")
	waitSame(t, my, ch, pair{"before", "SELECT id, owner FROM binwave.acct ORDER BY id", "SELECT id, owner FROM replica.acct FINAL ORDER BY id"})

	mustExec(t, my, `ALTER TABLE binwave.acct
		ADD COLUMN a_int INT NOT NULL DEFAULT 7,
		ADD COLUMN a_neg INT NOT NULL DEFAULT -3,
		ADD COLUMN a_null INT NULL,
		ADD COLUMN a_zero BIGINT UNSIGNED NOT NULL,
		ADD COLUMN a_dec DECIMAL(6,2) NOT NULL DEFAULT 1.50,
		ADD COLUMN a_flt DOUBLE NOT NULL,
		ADD COLUMN a_str VARCHAR(10) NOT NULL DEFAULT 'x y',
		ADD COLUMN a_empty VARCHAR(10) NOT NULL,
		ADD COLUMN a_enum ENUM('p','q') NOT NULL,
		ADD COLUMN a_set SET('m','n') NOT NULL DEFAULT 'm,n',
		ADD COLUMN a_txt TEXT NULL,
		ADD COLUMN a_year YEAR NOT NULL,
		ADD COLUMN a_dt DATETIME(3) NOT NULL DEFAULT '2020-01-02 03:04:05.678',
		ADD COLUMN a_date DATE NULL DEFAULT '2021-12-31'`)

	// Rows written after the statement, and a row that existed before it and is updated after it.
	mustExec(t, my,
		`INSERT INTO binwave.acct (id, owner, a_int, a_null, a_zero, a_dec, a_flt, a_str, a_empty, a_enum, a_set, a_txt, a_year, a_dt, a_date)
		 VALUES (4, 'di', 40, 41, 42, 43.21, 2.5, 'new', 'e', 'q', 'n', 'text', 2024, '2024-05-06 07:08:09.123', NULL)`,
		"UPDATE binwave.acct SET a_int = 100, a_txt = 'updated' WHERE id = 2")

	columns := "id, owner, a_int, a_neg, a_null, a_zero, "
	waitSame(t, my, ch, pair{
		"after",
		"SELECT " + columns + "a_dec, a_flt, a_str, a_empty, a_enum, a_set, a_txt, a_year, CAST(a_dt AS CHAR), CAST(a_date AS CHAR) FROM binwave.acct ORDER BY id",
		"SELECT " + columns + "toDecimalString(a_dec, 2), a_flt, a_str, a_empty, a_enum, arrayStringConcat(a_set, ','), a_txt, a_year, toString(a_dt), if(a_date IS NULL, NULL, toString(a_date)) FROM replica.acct FINAL ORDER BY id",
	})
	e.stillRunning(t)
	if got := len(e.hub.Status().Quarantined); got != 0 {
		t.Errorf("%d tables quarantined", got)
	}
}

// TestTimestampDefaultOfAnAddedColumnIsTheTimeOfTheStatement: MySQL gives the existing rows the time
// the ALTER ran; binwave uses the statement's time from the binlog, to the second.
func TestTimestampDefaultOfAnAddedColumnIsTheTimeOfTheStatement(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.ev (id INT PRIMARY KEY)")
	startHubEngine(t, engineConfig(t, my, ch, "file"))
	mustExec(t, my, "INSERT INTO binwave.ev VALUES (1), (2)")
	waitSame(t, my, ch, pair{"before", "SELECT id FROM binwave.ev ORDER BY id", "SELECT id FROM replica.ev FINAL ORDER BY id"})

	mustExec(t, my, "ALTER TABLE binwave.ev ADD COLUMN at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP")

	var mysqlUnix int64
	if err := my.DB.QueryRow("SELECT UNIX_TIMESTAMP(at) FROM binwave.ev WHERE id = 1").Scan(&mysqlUnix); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the column to exist in ClickHouse", 60*time.Second, func() bool {
		var n uint64
		return ch.DB.QueryRow("SELECT count() FROM system.columns WHERE database = 'replica' AND table = 'ev' AND name = 'at'").Scan(&n) == nil && n == 1
	})
	var chUnix int64
	if err := ch.DB.QueryRow("SELECT toUnixTimestamp(at) FROM replica.ev FINAL WHERE id = 1").Scan(&chUnix); err != nil {
		t.Fatal(err)
	}
	if diff := chUnix - mysqlUnix; diff < -2 || diff > 2 {
		t.Errorf("ClickHouse says the existing row got %d, MySQL says %d: the statement's time must be within a couple of seconds", chUnix, mysqlUnix)
	}
}

// TestWidenedColumnsKeepTheirValues: a wider numeric column, a longer VARCHAR, one more ENUM member
// and a NOT NULL that becomes NULL change nothing about the rows, and ClickHouse follows without
// stopping and without quarantine, in ddl.mode additive too.
func TestWidenedColumnsKeepTheirValues(t *testing.T) {
	t.Parallel()
	for _, mode := range []config.DDLMode{config.DDLAdditive, config.DDLFull} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			my := testenv.StartMySQL(t)
			ch := testenv.StartClickHouse(t)
			mustExec(t, my, `CREATE TABLE binwave.w (id INT PRIMARY KEY, i INT NOT NULL, u SMALLINT UNSIGNED NOT NULL, d DECIMAL(10,2) NOT NULL,
				f FLOAT NOT NULL, t DATETIME NOT NULL, n INT NOT NULL, v VARCHAR(5) NOT NULL, e ENUM('a','b') NOT NULL)`)
			cfg := engineConfig(t, my, ch, "file")
			cfg.DDL.Mode = mode
			e := startHubEngine(t, cfg)

			mustExec(t, my, "INSERT INTO binwave.w VALUES (1, 2147483647, 65535, 12345678.90, 1.5, '2024-01-02 03:04:05', 5, 'abcde', 'b')")
			waitSame(t, my, ch, pair{"before", "SELECT id, i FROM binwave.w", "SELECT id, i FROM replica.w FINAL"})

			mustExec(t, my, `ALTER TABLE binwave.w
				MODIFY COLUMN i BIGINT NOT NULL,
				MODIFY COLUMN u INT UNSIGNED NOT NULL,
				MODIFY COLUMN d DECIMAL(14,4) NOT NULL,
				MODIFY COLUMN f DOUBLE NOT NULL,
				MODIFY COLUMN t DATETIME(3) NOT NULL,
				MODIFY COLUMN n INT NULL,
				MODIFY COLUMN v VARCHAR(50) NOT NULL,
				MODIFY COLUMN e ENUM('a','b','c') NOT NULL`)
			mustExec(t, my, "INSERT INTO binwave.w VALUES (2, 9000000000, 4000000000, 9999999999.1234, 2.25, '2024-05-06 07:08:09.123', NULL, 'a much longer text than five characters', 'c')")

			waitSame(t, my, ch, pair{
				"after",
				"SELECT id, i, u, d, f, CAST(t AS CHAR), n, v, e FROM binwave.w ORDER BY id",
				"SELECT id, i, u, toDecimalString(d, 4), f, toString(t), n, v, e FROM replica.w FINAL ORDER BY id",
			})
			e.stillRunning(t)
			if got := e.hub.Status().Quarantined; len(got) != 0 {
				t.Errorf("quarantined %+v", got)
			}
			types, err := textRows(ch.DB, "SELECT name, type FROM system.columns WHERE database = 'replica' AND table = 'w' AND name IN ('i', 'u', 'd', 'n') ORDER BY name")
			if err != nil || strings.Join(types, ";") != "d\tDecimal(14, 4);i\tInt64;n\tNullable(Int32);u\tUInt32" {
				t.Errorf("ClickHouse column types %v (%v)", types, err)
			}
		})
	}
}

// TestChangeThatCannotBeReproducedSetsOnlyThatTableAside: the values of a new column that MySQL
// computes cannot be reproduced for the rows that exist, so the table is set aside with the reason and
// the others go on. (The changes of types have their own tests in typechange_test.go.)
func TestChangeThatCannotBeReproducedSetsOnlyThatTableAside(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	for _, q := range []string{
		"CREATE TABLE binwave.expression (id INT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE binwave.fine (id INT PRIMARY KEY, v INT NOT NULL)",
	} {
		mustExec(t, my, q)
	}
	e := startHubEngine(t, engineConfig(t, my, ch, "file"))
	mustExec(t, my, "INSERT INTO binwave.expression VALUES (1, 1)", "INSERT INTO binwave.fine VALUES (1, 1)")
	waitUntil(t, "the first rows", 60*time.Second, func() bool { return chRowCount(t, ch, "fine") == 1 })

	mustExec(t, my,
		"ALTER TABLE binwave.expression ADD COLUMN c VARCHAR(10) NOT NULL DEFAULT (UPPER('x'))",
		"ALTER TABLE binwave.fine MODIFY COLUMN v BIGINT NOT NULL",
		"INSERT INTO binwave.fine VALUES (2, 5000000000)")

	waitUntil(t, "the table to be set aside", 60*time.Second, func() bool { return len(e.hub.Status().Quarantined) == 1 })
	if q := e.hub.Status().Quarantined; q[0].Table != "binwave.expression" || !strings.Contains(q[0].Reason, "expression") {
		t.Errorf("quarantined %+v", q)
	}
	waitUntil(t, "the healthy table to follow", 60*time.Second, func() bool { return chRowCount(t, ch, "fine") == 2 })
	e.stillRunning(t)
}

// TestSchemaChangeWhileTheServiceIsDownIsAppliedAfterTheStart: the statement is in the binlog after
// the saved position, so the restarted service reads it, applies it, and the table converges.
func TestSchemaChangeWhileTheServiceIsDownIsAppliedAfterTheStart(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")

	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'ann')")
	waitSame(t, my, ch, pair{"first", "SELECT id, owner FROM binwave.acct", "SELECT id, owner FROM replica.acct FINAL"})
	if err := e.stop(); err != nil {
		t.Fatal(err)
	}

	mustExec(t, my, "ALTER TABLE binwave.acct ADD COLUMN qty INT NOT NULL DEFAULT 9", "INSERT INTO binwave.acct VALUES (2, 'bob', 20)")
	startHubEngine(t, cfg)

	waitSame(t, my, ch, pair{"after", "SELECT id, owner, qty FROM binwave.acct ORDER BY id", "SELECT id, owner, qty FROM replica.acct FINAL ORDER BY id"})
}

// TestColumnAddedBeforeTheTableWasEverWritten: nothing exists in ClickHouse yet when the statement
// comes, so there is nothing to change; the table is created with the structure the rows show.
func TestColumnAddedBeforeTheTableWasEverWritten(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.fresh (id INT PRIMARY KEY)")
	e := startHubEngine(t, engineConfig(t, my, ch, "file"))

	mustExec(t, my, "ALTER TABLE binwave.fresh ADD COLUMN c INT NOT NULL DEFAULT 5", "INSERT INTO binwave.fresh VALUES (1, 6)")

	waitSame(t, my, ch, pair{"fresh", "SELECT id, c FROM binwave.fresh", "SELECT id, c FROM replica.fresh FINAL"})
	e.stillRunning(t)
}

// TestDryRunLogsTheChangeAndStopsByDefault: with ddl.dry_run binwave does not change ClickHouse; the
// table cannot go on without the change, so the service stops, or sets the table aside when asked.
func TestDryRunLogsTheChangeAndStopsByDefault(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL)", "CREATE TABLE binwave.other (id INT PRIMARY KEY)")
	cfg := engineConfig(t, my, ch, "file")
	first := startHubEngine(t, cfg) // creates the tables in ClickHouse
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'ann')", "INSERT INTO binwave.other VALUES (1)")
	waitUntil(t, "the tables", 60*time.Second, func() bool { return chRowCount(t, ch, "acct") == 1 && chRowCount(t, ch, "other") == 1 })
	if err := first.stop(); err != nil {
		t.Fatal(err)
	}

	t.Run("stops by default", func(t *testing.T) {
		cfg := engineConfig(t, my, ch, "file")
		cfg.Checkpoint.FilePath += ".stop"
		cfg.Replication.InitialPosition = config.StartFromNow
		cfg.DDL.DryRun = true
		e := startHubEngine(t, cfg)
		mustExec(t, my, "ALTER TABLE binwave.acct ADD COLUMN qty INT NOT NULL DEFAULT 9")
		f := waitFailedHub(t, e)
		if !strings.Contains(f.Message, "ddl.dry_run") || !strings.Contains(f.Message, "the statements are in the log") {
			t.Fatalf("the failure does not name the dry run: %s", f.Message)
		}
		if chHasColumn(t, ch, "acct", "qty") {
			t.Errorf("the column reached ClickHouse in a dry run")
		}
	})
}

func chHasColumn(t *testing.T, ch *testenv.ClickHouse, table, column string) bool {
	t.Helper()
	var n uint64
	if err := ch.DB.QueryRow("SELECT count() FROM system.columns WHERE database = 'replica' AND table = ? AND name = ?", table, column).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}
