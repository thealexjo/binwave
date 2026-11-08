//go:build integration

package integration

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/test/testenv"
)

// typeChange is a change of the type of one column, tried on a real MySQL and a real ClickHouse: the
// values before the ALTER, the ALTER, a value written after it. Where binwave follows, the column
// must read the same in both; where MySQL converts by rules binwave cannot reproduce, the table is
// set aside; where a value does not fit, replication fails and shows it.
type typeChange struct {
	name, oldDef, newDef string
	// values are the SQL literals written before the ALTER, one row each; after is written after it.
	values []string
	after  string
	// mysql and clickhouse render the column for the comparison ("c" is the column); the defaults are the text of both.
	mysql, clickhouse string
}

func (c typeChange) selects() (string, string) {
	m, h := "CAST(c AS CHAR)", "toString(c)"
	if c.mysql != "" {
		m = c.mysql
	}
	if c.clickhouse != "" {
		h = c.clickhouse
	}
	return m, h
}

// The stored value of a FLOAT or DOUBLE column as decimal text: MySQL converts a FLOAT to decimal with the
// precision of a FLOAT unless it is made a double first, and ClickHouse truncates where MySQL rounds, so both
// look at the exact double at 12 digits.
const decimalText = "CAST(c + 0 AS DECIMAL(40,12))"

const chFloatText = "toDecimalString(toDecimal128(toFloat64(c), 12), 12)"

// bigText renders a double with values up to 2^31 and 6 digits, where ClickHouse's conversion to a decimal is exact.
const (
	mysqlBigText = "CAST(c + 0 AS DECIMAL(40,6))"
	chBigText    = "toDecimalString(toDecimal128(round(toFloat64(c), 6), 6), 6)"
)

// reColumn finds the column placeholder in a rendering.
var reColumn = regexp.MustCompile(`\bc\b`)

func chDecimalText(scale int) string { return fmt.Sprintf("toDecimalString(c, %d)", scale) }

// convertibleChanges are the changes that binwave follows: every one is an ALTER of a MySQL column whose
// values, after it, must be exactly what MySQL holds.
var convertibleChanges = []typeChange{
	{name: "int_to_bigint", oldDef: "INT NOT NULL", newDef: "BIGINT NOT NULL", values: []string{"-2147483648", "0", "2147483647"}, after: "9000000000"},
	{name: "bigint_to_int", oldDef: "BIGINT NOT NULL", newDef: "INT NOT NULL", values: []string{"-5", "0", "2147483647"}, after: "-2147483648"},
	{name: "int_to_smallint", oldDef: "INT NOT NULL", newDef: "SMALLINT NOT NULL", values: []string{"32767", "-32768", "5"}, after: "-1"},
	{name: "int_to_tinyint_unsigned", oldDef: "INT NOT NULL", newDef: "TINYINT UNSIGNED NOT NULL", values: []string{"0", "255", "7"}, after: "200"},
	{name: "uint_to_int", oldDef: "INT UNSIGNED NOT NULL", newDef: "INT NOT NULL", values: []string{"0", "2147483647", "5"}, after: "-3"},
	{name: "int_to_uint", oldDef: "INT NOT NULL", newDef: "INT UNSIGNED NOT NULL", values: []string{"0", "2147483647", "5"}, after: "4294967295"},
	{name: "ubigint_to_bigint", oldDef: "BIGINT UNSIGNED NOT NULL", newDef: "BIGINT NOT NULL", values: []string{"0", "9223372036854775807", "5"}, after: "-9223372036854775808"},
	{name: "int_not_null_from_null", oldDef: "INT NULL", newDef: "INT NOT NULL", values: []string{"1", "2", "3"}, after: "4"},
	{name: "int_null_from_not_null", oldDef: "INT NOT NULL", newDef: "INT NULL", values: []string{"1", "2", "3"}, after: "NULL"},
	{name: "int_to_decimal", oldDef: "INT NOT NULL", newDef: "DECIMAL(12,2) NOT NULL", values: []string{"5", "-7", "0"}, after: "123.45", mysql: "CAST(c AS CHAR)", clickhouse: chDecimalText(2)},
	{name: "int_to_double", oldDef: "INT NOT NULL", newDef: "DOUBLE NOT NULL", values: []string{"5", "2147483647", "-1"}, after: "0.5", mysql: mysqlBigText, clickhouse: chBigText},
	{name: "bigint_to_double", oldDef: "BIGINT NOT NULL", newDef: "DOUBLE NOT NULL", values: []string{"9007199254740993", "-9007199254740993", "1"}, after: "3.0", mysql: "CAST(c AS DECIMAL(40,0))", clickhouse: "toDecimalString(toDecimal128(c, 0), 0)"},
	{name: "int_to_varchar", oldDef: "INT NOT NULL", newDef: "VARCHAR(20) NOT NULL", values: []string{"-5", "100", "0"}, after: "'text'"},
	{name: "bit_to_int", oldDef: "BIT(8) NOT NULL", newDef: "INT NOT NULL", values: []string{"b'101'", "b'11111111'", "b'0'"}, after: "77", mysql: "CAST(c AS UNSIGNED)"},
	{name: "int_to_bit", oldDef: "INT NOT NULL", newDef: "BIT(16) NOT NULL", values: []string{"5", "65535", "0"}, after: "b'1'", mysql: "CAST(c AS UNSIGNED)"},
	{name: "year_to_int", oldDef: "YEAR NOT NULL", newDef: "INT NOT NULL", values: []string{"2024", "1901", "2155"}, after: "1"},
	{name: "decimal_wider", oldDef: "DECIMAL(10,2) NOT NULL", newDef: "DECIMAL(14,4) NOT NULL", values: []string{"1.50", "-99999999.99", "0.01"}, after: "12345.6789", mysql: "CAST(c AS CHAR)", clickhouse: chDecimalText(4)},
	{name: "decimal_scale_down", oldDef: "DECIMAL(10,4) NOT NULL", newDef: "DECIMAL(10,2) NOT NULL", values: []string{"1.2345", "1.2350", "-1.2350"}, after: "2.49", mysql: "CAST(c AS CHAR)", clickhouse: chDecimalText(2)},
	{name: "decimal_precision_down", oldDef: "DECIMAL(14,4) NOT NULL", newDef: "DECIMAL(10,4) NOT NULL", values: []string{"123456.1234", "-123456.1234", "0.0001"}, after: "1.5", mysql: "CAST(c AS CHAR)", clickhouse: chDecimalText(4)},
	{name: "decimal_to_int", oldDef: "DECIMAL(10,2) NOT NULL", newDef: "INT NOT NULL", values: []string{"2.50", "-2.50", "2.49"}, after: "7"},
	{name: "decimal_to_double", oldDef: "DECIMAL(10,2) NOT NULL", newDef: "DOUBLE NOT NULL", values: []string{"0.10", "123456.78", "-0.01"}, after: "0.25", mysql: mysqlBigText, clickhouse: chBigText},
	{name: "decimal_to_varchar", oldDef: "DECIMAL(10,2) NOT NULL", newDef: "VARCHAR(20) NOT NULL", values: []string{"1.50", "-0.10", "0.00"}, after: "'text'"},
	{name: "float_to_double", oldDef: "FLOAT NOT NULL", newDef: "DOUBLE NOT NULL", values: []string{"0.1", "1.5", "-3.25"}, after: "0.123456789012", mysql: decimalText, clickhouse: chFloatText},
	{name: "double_to_float", oldDef: "DOUBLE NOT NULL", newDef: "FLOAT NOT NULL", values: []string{"0.1", "1.5", "-3.25"}, after: "2.5", mysql: decimalText, clickhouse: chFloatText},
	{name: "date_to_datetime", oldDef: "DATE NOT NULL", newDef: "DATETIME NOT NULL", values: []string{"'2024-02-29'", "'1970-01-01'", "'2299-12-31'"}, after: "'2024-05-06 07:08:09'"},
	{name: "datetime_finer", oldDef: "DATETIME NOT NULL", newDef: "DATETIME(6) NOT NULL", values: []string{"'2024-02-29 01:02:03'", "'1970-01-01 00:00:00'", "'2099-12-31 23:59:59'"}, after: "'2024-05-06 07:08:09.123456'"},
	{name: "varchar_longer", oldDef: "VARCHAR(10) NOT NULL", newDef: "VARCHAR(50) NOT NULL", values: []string{"'a'", "''", "'0123456789'"}, after: "'a much longer text than before'"},
	{name: "varchar_to_text", oldDef: "VARCHAR(10) NOT NULL", newDef: "TEXT NOT NULL", values: []string{"'a'", "''", "'0123456789'"}, after: "'long'"},
	{name: "varchar_not_null_from_null", oldDef: "VARCHAR(10) NULL", newDef: "VARCHAR(10) NOT NULL", values: []string{"'a'", "'b'", "''"}, after: "'c'"},
	{name: "enum_to_varchar", oldDef: "ENUM('a','b') NOT NULL", newDef: "VARCHAR(10) NOT NULL", values: []string{"'a'", "'b'", "'a'"}, after: "'anything'"},
	{name: "varchar_to_enum", oldDef: "VARCHAR(10) NOT NULL", newDef: "ENUM('a','b','c') NOT NULL", values: []string{"'a'", "'b'", "'a'"}, after: "'c'"},
	{name: "enum_more_members", oldDef: "ENUM('a','b') NOT NULL", newDef: "ENUM('a','b','c') NOT NULL", values: []string{"'a'", "'b'", "'a'"}, after: "'c'"},
	{name: "bit_wider", oldDef: "BIT(8) NOT NULL", newDef: "BIT(16) NOT NULL", values: []string{"b'101'", "b'11111111'", "b'0'"}, after: "b'1111111111111111'", mysql: "CAST(c AS UNSIGNED)"},
	{name: "varbinary_to_blob", oldDef: "VARBINARY(10) NOT NULL", newDef: "BLOB NOT NULL", values: []string{"'ab'", "''", "'0123456789'"}, after: "'bytes'", mysql: "HEX(c)", clickhouse: "upper(hex(c))"},
}

// TestConvertibleTypeChangesReadTheSameInMySQLAndClickHouse is the consistency criterion of the type
// changes: every change binwave follows leaves every value, the ones written before the ALTER
// and the one written after it, the same in ClickHouse as in MySQL.
func TestConvertibleTypeChangesReadTheSameInMySQLAndClickHouse(t *testing.T) {
	t.Parallel()
	// Groups of changes share a table, so that a run has few servers; one column each.
	const perTable = 8
	for start := 0; start < len(convertibleChanges); start += perTable {
		group := convertibleChanges[start:min(start+perTable, len(convertibleChanges))]
		t.Run(group[0].name, func(t *testing.T) {
			t.Parallel()
			runTypeChanges(t, group)
		})
	}
}

func runTypeChanges(t *testing.T, group []typeChange) {
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	var defs []string
	for _, c := range group {
		defs = append(defs, "c_"+c.name+" "+c.oldDef)
	}
	mustExec(t, my, "CREATE TABLE binwave.tc (id INT PRIMARY KEY, "+strings.Join(defs, ", ")+")")
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	t.Cleanup(func() {
		if t.Failed() {
			st := e.hub.Status()
			t.Logf("state %s, quarantined %+v", st.State, st.Quarantined)
			if st.Failure != nil {
				t.Logf("failure detail: %s", st.Failure.Detail)
			}
		}
	})

	for i := 0; i < 3; i++ {
		var cols, vals []string
		for _, c := range group {
			cols = append(cols, "c_"+c.name)
			vals = append(vals, c.values[i])
		}
		mustExec(t, my, fmt.Sprintf("INSERT INTO binwave.tc (id, %s) VALUES (%d, %s)", strings.Join(cols, ", "), i+1, strings.Join(vals, ", ")))
	}
	waitUntil(t, "the rows", 60*time.Second, func() bool { return chRowCount(t, ch, "tc") == 3 })

	var mods []string
	for _, c := range group {
		mods = append(mods, "MODIFY COLUMN c_"+c.name+" "+c.newDef)
	}
	mustExec(t, my, "ALTER TABLE binwave.tc "+strings.Join(mods, ", "))
	var cols, vals []string
	for _, c := range group {
		cols = append(cols, "c_"+c.name)
		vals = append(vals, c.after)
	}
	if _, err := my.DB.Exec(fmt.Sprintf("INSERT INTO binwave.tc (id, %s) VALUES (4, %s)", strings.Join(cols, ", "), strings.Join(vals, ", "))); err != nil {
		t.Fatalf("the row after the ALTER: %v", err)
	}

	var pairs []pair
	for _, c := range group {
		m, h := c.selects()
		col := "c_" + c.name
		m, h = reColumn.ReplaceAllString(m, col), reColumn.ReplaceAllString(h, col)
		pairs = append(pairs, pair{
			c.name,
			"SELECT id, " + m + " FROM binwave.tc ORDER BY id",
			"SELECT id, " + h + " FROM replica.tc FINAL ORDER BY id",
		})
	}
	waitSame(t, my, ch, pairs...)
	e.stillRunning(t)
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("quarantined %v: %+v", got, e.hub.Status().Quarantined)
	}
}

// refusedChanges are the changes where MySQL converts the values by rules binwave cannot reproduce from
// the binlog and ClickHouse alone: the table is set aside, with the reason.
var refusedChanges = []typeChange{
	{name: "datetime_to_timestamp", oldDef: "DATETIME NOT NULL", newDef: "TIMESTAMP NOT NULL", values: []string{"'2024-02-29 01:02:03'"}},
	{name: "timestamp_to_datetime", oldDef: "TIMESTAMP NOT NULL DEFAULT '2024-01-01 00:00:00'", newDef: "DATETIME NOT NULL", values: []string{"'2024-02-29 01:02:03'"}},
	{name: "datetime_fewer_digits", oldDef: "DATETIME(6) NOT NULL", newDef: "DATETIME NOT NULL", values: []string{"'2024-02-29 01:02:03.987654'"}},
	{name: "datetime_to_date", oldDef: "DATETIME NOT NULL", newDef: "DATE NOT NULL", values: []string{"'2024-02-29 23:59:59'"}},
	{name: "date_to_timestamp", oldDef: "DATE NOT NULL", newDef: "TIMESTAMP NOT NULL", values: []string{"'2024-02-29'"}},
	{name: "float_to_int", oldDef: "DOUBLE NOT NULL", newDef: "INT NOT NULL", values: []string{"2.5"}},
	{name: "double_to_decimal", oldDef: "DOUBLE NOT NULL", newDef: "DECIMAL(10,2) NOT NULL", values: []string{"2.555"}},
	{name: "varchar_to_int", oldDef: "VARCHAR(10) NOT NULL", newDef: "INT NOT NULL", values: []string{"'12'"}},
	{name: "int_to_time", oldDef: "INT NOT NULL", newDef: "TIME NOT NULL", values: []string{"123456"}},
	{name: "int_to_year", oldDef: "INT NOT NULL", newDef: "YEAR NOT NULL", values: []string{"2024"}},
	{name: "int_to_enum", oldDef: "INT NOT NULL", newDef: "ENUM('a','b','c') NOT NULL", values: []string{"1"}},
	{name: "time_fewer_digits", oldDef: "TIME(6) NOT NULL", newDef: "TIME NOT NULL", values: []string{"'12:34:56.789'"}},
	{name: "varchar_to_json", oldDef: "VARCHAR(30) NOT NULL", newDef: "JSON NOT NULL", values: []string{"'{\"a\": 1}'"}},
	{name: "binary_longer", oldDef: "BINARY(4) NOT NULL", newDef: "BINARY(8) NOT NULL", values: []string{"'abcd'"}},
}

func TestChangesWhoseValuesMySQLConvertsByItsOwnRulesSetTheTableAside(t *testing.T) {
	t.Parallel()
	for _, c := range refusedChanges {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			my := testenv.StartMySQL(t)
			ch := testenv.StartClickHouse(t)
			mustExec(t, my, "CREATE TABLE binwave.tc (id INT PRIMARY KEY, c "+c.oldDef+")", "CREATE TABLE binwave.other (id INT PRIMARY KEY)")
			cfg := engineConfig(t, my, ch, "file")
			cfg.DDL.Mode = config.DDLFull
			e := startHubEngine(t, cfg)
			mustExec(t, my, "INSERT INTO binwave.tc VALUES (1, "+c.values[0]+")", "INSERT INTO binwave.other VALUES (1)")
			waitUntil(t, "the rows", 60*time.Second, func() bool { return chRowCount(t, ch, "tc") == 1 && chRowCount(t, ch, "other") == 1 })

			mustExec(t, my, "ALTER TABLE binwave.tc MODIFY COLUMN c "+c.newDef, "INSERT INTO binwave.other VALUES (2)")

			waitUntil(t, "the table to be set aside", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })
			waitUntil(t, "the other table", 60*time.Second, func() bool { return chRowCount(t, ch, "other") == 2 })
			if q := e.hub.Status().Quarantined; len(q) != 1 || q[0].Table != "binwave.tc" || !strings.Contains(q[0].Reason, "resynchronized") {
				t.Errorf("quarantined %+v", q)
			}
			e.stillRunning(t)
		})
	}
}

// failingChanges are followed by binwave, but a value of the table does not convert exactly: MySQL was in
// a sql_mode that is not strict and changed it (cut it, made a NULL a zero), and ClickHouse must not
// do the same silently. Replication fails and says so; nothing was swapped.
var failingChanges = []struct {
	name, oldDef, newDef, value string
}{
	{"value_out_of_range", "BIGINT NOT NULL", "SMALLINT NOT NULL", "100000"},
	{"null_in_a_column_that_stops_allowing_it", "INT NULL", "INT NOT NULL", "NULL"},
	{"text_that_is_not_a_member", "VARCHAR(10) NOT NULL", "ENUM('a','b') NOT NULL", "'zzz'"},
	{"decimal_too_big_for_the_new_precision", "DECIMAL(14,2) NOT NULL", "DECIMAL(8,2) NOT NULL", "9999999999.99"},
}

func TestAValueThatDoesNotConvertExactlyFailsReplicationAndSwapsNothing(t *testing.T) {
	t.Parallel()
	for _, c := range failingChanges {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			my := testenv.StartMySQL(t)
			ch := testenv.StartClickHouse(t)
			mustExec(t, my, "CREATE TABLE binwave.tc (id INT PRIMARY KEY, c "+c.oldDef+")")
			cfg := engineConfig(t, my, ch, "file")
			cfg.DDL.Mode = config.DDLFull
			e := startHubEngine(t, cfg)
			mustExec(t, my, "INSERT INTO binwave.tc VALUES (1, 1), (2, "+c.value+")")
			waitUntil(t, "the rows", 60*time.Second, func() bool { return chRowCount(t, ch, "tc") == 2 })
			before := chTableUUID(t, ch, "tc")

			conn, err := my.DB.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			for _, q := range []string{"SET SESSION sql_mode = ''", "ALTER TABLE binwave.tc MODIFY COLUMN c " + c.newDef} {
				if _, err := conn.ExecContext(context.Background(), q); err != nil {
					t.Fatal(q, err)
				}
			}

			f := waitFailedHub(t, e)
			if !strings.Contains(f.Message, "does not convert exactly") {
				t.Errorf("failure %q", f.Message)
			}
			if e.hub.Status().State != observability.StateFailed {
				t.Errorf("state %s", e.hub.Status().State)
			}
			if chTableUUID(t, ch, "tc") != before {
				t.Error("the table was replaced although a value did not convert")
			}
		})
	}
}

// TestSourceTypesOfExistingColumnsAreRecordedWhenTheTableIsFirstWritten: a table made before binwave
// wrote the comments (or by hand) gets them when its rows first arrive, so that a later change of a type
// is judged by what the column was: DATETIME to TIMESTAMP looks like no change to ClickHouse.
func TestSourceTypesOfExistingColumnsAreRecordedWhenTheTableIsFirstWritten(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.old (id INT PRIMARY KEY, at DATETIME NOT NULL, note VARCHAR(10) NOT NULL)")
	first := startHubEngine(t, engineConfig(t, my, ch, "file"))
	mustExec(t, my, "INSERT INTO binwave.old VALUES (1, '2024-02-29 01:02:03', 'a')")
	waitUntil(t, "the row", 60*time.Second, func() bool { return chRowCount(t, ch, "old") == 1 })
	if err := first.stop(); err != nil {
		t.Fatal(err)
	}
	// As if binwave had made the table before it wrote the comments.
	for _, q := range []string{"ALTER TABLE replica.old COMMENT COLUMN at ''", "ALTER TABLE replica.old COMMENT COLUMN note 'somebody wrote this'"} {
		if _, err := ch.DB.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.Mode = config.DDLFull
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.old VALUES (2, '2024-02-29 01:02:04', 'b')")
	waitUntil(t, "the second row", 60*time.Second, func() bool { return chRowCount(t, ch, "old") == 2 })

	comments, err := textRows(ch.DB, "SELECT name, comment FROM system.columns WHERE database = 'replica' AND table = 'old' AND name IN ('at', 'note') ORDER BY name")
	if err != nil || strings.Join(comments, ";") != "at\tmysql: DATETIME 0;note\tsomebody wrote this" {
		t.Errorf("comments %v (%v): the column without one gets the source type, the one with a comment of somebody keeps it", comments, err)
	}

	mustExec(t, my, "ALTER TABLE binwave.old MODIFY COLUMN at TIMESTAMP NOT NULL")
	waitUntil(t, "the table to be set aside", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })
	if q := e.hub.Status().Quarantined; !strings.Contains(q[0].Reason, "time zone") {
		t.Errorf("reason %q", q[0].Reason)
	}
}
