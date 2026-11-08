//go:build integration

package integration

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/thealexjo/binwave/internal/event"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/internal/typemap"
	"github.com/thealexjo/binwave/test/testenv"
)

const typesDDL = `CREATE TABLE binwave.types (
	id            BIGINT UNSIGNED NOT NULL,
	c_tinyint     TINYINT NULL,
	c_utinyint    TINYINT UNSIGNED NULL,
	c_smallint    SMALLINT NULL,
	c_usmallint   SMALLINT UNSIGNED NULL,
	c_mediumint   MEDIUMINT NULL,
	c_umediumint  MEDIUMINT UNSIGNED NULL,
	c_int         INT NULL,
	c_uint        INT UNSIGNED NULL,
	c_bigint      BIGINT NULL,
	c_ubigint     BIGINT UNSIGNED NULL,
	c_float       FLOAT NULL,
	c_double      DOUBLE NULL,
	c_decimal     DECIMAL(65,30) NULL,
	c_money       DECIMAL(10,2) NULL,
	c_year        YEAR NULL,
	c_date        DATE NULL,
	c_datetime    DATETIME(6) NULL,
	c_timestamp   TIMESTAMP(6) NULL,
	c_time        TIME(3) NULL,
	c_char        CHAR(3) NULL,
	c_varchar     VARCHAR(50) NULL,
	c_latin1      VARCHAR(20) CHARACTER SET latin1 NULL,
	c_text        TEXT NULL,
	c_binary      BINARY(4) NULL,
	c_varbinary   VARBINARY(8) NULL,
	c_blob        BLOB NULL,
	c_enum        ENUM('red','green','blue') NULL,
	c_set         SET('x','y','z') NULL,
	c_bit         BIT(12) NULL,
	c_json        JSON NULL,
	PRIMARY KEY (id)
)`

func utc(y int, mo time.Month, d, h, mi, s, ns int) time.Time {
	return time.Date(y, mo, d, h, mi, s, ns, time.UTC)
}

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// typeColumns lists the table's columns in order; every value not overridden by a case is NULL.
var typeColumns = []string{
	"id", "c_tinyint", "c_utinyint", "c_smallint", "c_usmallint", "c_mediumint", "c_umediumint",
	"c_int", "c_uint", "c_bigint", "c_ubigint", "c_float", "c_double", "c_decimal", "c_money",
	"c_year", "c_date", "c_datetime", "c_timestamp", "c_time", "c_char", "c_varchar", "c_latin1",
	"c_text", "c_binary", "c_varbinary", "c_blob", "c_enum", "c_set", "c_bit", "c_json",
}

// insertTypeRows writes four rows of edge values into binwave.types on a dedicated session: zero
// dates need a permissive sql_mode, and TIMESTAMP conversion is proven under a non-UTC time zone.
func insertTypeRows(t *testing.T, my *testenv.MySQL) {
	t.Helper()
	ctx := context.Background()
	conn, err := my.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range []string{
		"SET SESSION sql_mode = ''",
		"SET SESSION time_zone = '+00:00'",
		`INSERT INTO binwave.types VALUES (
			1, -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295,
			-9223372036854775808, 18446744073709551615, 1.5, -2.25,
			12345678901234567890123456789012345.123456789012345678901234567890, -99999999.99,
			2024, '2024-02-29', '2024-02-29 23:59:59.123456', '2038-01-19 03:14:07.999999', '-838:59:59.000',
			'ab', 'héllo wörld 日本語', _latin1 0x636166E9, 'multi\nline',
			UNHEX('01020300'), UNHEX('00FF10'), UNHEX('DEADBEEF'),
			'green', 'x,z', b'101010101010', '{"a": [1, 2, {"b": null}], "c": "d"}')`,
		"INSERT INTO binwave.types (id) VALUES (2)",
		`INSERT INTO binwave.types VALUES (
			3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3.4028235e38, 1.7976931348623157e308,
			-0.000000000000000000000000000001, 0.00,
			0, '2024-00-15', '0000-00-00 00:00:00', '0000-00-00 00:00:00', '00:00:00.000',
			'', '', '', '', UNHEX('00000000'), '', '',
			NULL, '', b'0', 'null')`,
		"SET SESSION time_zone = '+03:00'",
		"INSERT INTO binwave.types (id, c_datetime, c_timestamp) VALUES (4, '2024-06-01 12:00:00.5', '2024-06-01 12:00:00.5')",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s\n%v", stmt, err)
		}
	}
}

// expectedTypeRows lists the values typemap must produce for the rows of insertTypeRows, keyed by
// column name. The same expectations hold after a round trip through ClickHouse.
func expectedTypeRows() []map[string]any {
	// Values a NULL column converts to; a NULL SET becomes an empty array by design.
	nulls := func() map[string]any {
		m := map[string]any{}
		for _, c := range typeColumns {
			m[c] = nil
		}
		m["c_set"] = []string{}
		return m
	}
	overlay := func(base map[string]any, over map[string]any) map[string]any {
		for k, v := range over {
			base[k] = v
		}
		return base
	}

	want := []map[string]any{
		// Row 1: extremes of every type.
		{
			"id":        uint64(1),
			"c_tinyint": int8(-128), "c_utinyint": uint8(255),
			"c_smallint": int16(-32768), "c_usmallint": uint16(65535),
			"c_mediumint": int32(-8388608), "c_umediumint": uint32(16777215),
			"c_int": int32(-2147483648), "c_uint": uint32(4294967295),
			"c_bigint": int64(-9223372036854775808), "c_ubigint": uint64(18446744073709551615),
			"c_float": float32(1.5), "c_double": float64(-2.25),
			"c_decimal":   dec("12345678901234567890123456789012345.123456789012345678901234567890"),
			"c_money":     dec("-99999999.99"),
			"c_year":      uint16(2024),
			"c_date":      utc(2024, 2, 29, 0, 0, 0, 0),
			"c_datetime":  utc(2024, 2, 29, 23, 59, 59, 123456000),
			"c_timestamp": utc(2038, 1, 19, 3, 14, 7, 999999000),
			"c_time":      int64(-3020399000000),
			"c_char":      "ab",
			"c_varchar":   "héllo wörld 日本語",
			"c_latin1":    "café",
			"c_text":      "multi\nline",
			"c_binary":    "\x01\x02\x03\x00", // trailing zero restored after the binlog strips it
			"c_varbinary": "\x00\xff\x10",
			"c_blob":      "\xde\xad\xbe\xef",
			"c_enum":      "green",
			"c_set":       []string{"x", "z"},
			"c_bit":       uint64(2730),
			// Compact and type-faithful; it differs from MySQL's SELECT output only in whitespace.
			"c_json": `{"a":[1,2,{"b":null}],"c":"d"}`,
		},
		// Row 2: NULLs everywhere.
		overlay(nulls(), map[string]any{"id": uint64(2)}),
		// Row 3: zeros, empty values and zero dates, which are replaced by the documented rule.
		overlay(nulls(), map[string]any{
			"id":        uint64(3),
			"c_tinyint": int8(0), "c_utinyint": uint8(0), "c_smallint": int16(0), "c_usmallint": uint16(0),
			"c_mediumint": int32(0), "c_umediumint": uint32(0), "c_int": int32(0), "c_uint": uint32(0),
			"c_bigint": int64(0), "c_ubigint": uint64(0),
			"c_float": float32(3.4028235e38), "c_double": float64(1.7976931348623157e308),
			"c_decimal": dec("-0.000000000000000000000000000001"), "c_money": dec("0"),
			"c_year": uint16(0),
			"c_date": utc(1900, 1, 1, 0, 0, 0, 0), "c_datetime": utc(1900, 1, 1, 0, 0, 0, 0), "c_timestamp": utc(1900, 1, 1, 0, 0, 0, 0),
			"c_time": int64(0),
			"c_char": "", "c_varchar": "", "c_latin1": "", "c_text": "",
			"c_binary": "\x00\x00\x00\x00", "c_varbinary": "", "c_blob": "",
			"c_set": []string{}, "c_bit": uint64(0), "c_json": "null",
		}),
		// Row 4: TIMESTAMP is converted to UTC, DATETIME stays as written.
		overlay(nulls(), map[string]any{
			"id":          uint64(4),
			"c_datetime":  utc(2024, 6, 1, 12, 0, 0, 500000000),
			"c_timestamp": utc(2024, 6, 1, 9, 0, 0, 500000000),
		}),
	}
	return want
}

// TestEveryTypeSurvivesTheBinlogAndConverts writes edge values into MySQL, reads them back from
// the real binlog through the source, and converts them with typemap. It is the check that the
// value forms the source produces are the ones typemap expects.
func TestEveryTypeSurvivesTheBinlogAndConverts(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()

	if _, err := my.DB.ExecContext(ctx, typesDDL); err != nil {
		t.Fatal(err)
	}
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(ctx)
	if err != nil {
		t.Fatal(err)
	}

	insertTypeRows(t, my)

	events := streamCommits(t, src, start, 4)
	batches := rowsOf(events, "binwave.types")
	if len(batches) != 4 {
		t.Fatalf("got %d row batches, want 4", len(batches))
	}

	want := expectedTypeRows()

	for i, batch := range batches {
		if batch.Op != event.Insert || len(batch.Rows) != 1 {
			t.Fatalf("batch %d = %s with %d rows", i, batch.Op, len(batch.Rows))
		}
		got := convertRow(t, batch, batch.Rows[0].After)
		for _, col := range typeColumns {
			if !sameValue(got[col], want[i][col]) {
				t.Errorf("row %d, %s: got %#v, want %#v", i+1, col, got[col], want[i][col])
			}
		}
	}
}

// convertRow resolves every column with typemap and converts the values.
func convertRow(t *testing.T, batch event.Rows, values []any) map[string]any {
	t.Helper()
	out := make(map[string]any, len(values))
	for i, col := range batch.Table.Columns {
		m, err := typemap.Resolve(batch.Table.FullName(), col, typemap.Options{})
		if err != nil {
			t.Fatalf("resolve %s: %v", col.Name, err)
		}
		v, err := m.Convert(values[i])
		if err != nil {
			t.Fatalf("convert %s (%#v): %v", col.Name, values[i], err)
		}
		out[col.Name] = v
	}
	return out
}

func sameValue(got, want any) bool {
	switch w := want.(type) {
	case time.Time:
		g, ok := got.(time.Time)
		return ok && g.Equal(w)
	case decimal.Decimal:
		g, ok := got.(decimal.Decimal)
		return ok && g.Equal(w)
	default:
		return reflect.DeepEqual(got, want)
	}
}
