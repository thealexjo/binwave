package mysql

import (
	"reflect"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/thealexjo/binwave/internal/typemap"
)

var testCollations = collations{
	8:   {charset: "latin1", maxLen: 1},
	45:  {charset: "utf8mb4", maxLen: 4},
	33:  {charset: "utf8mb3", maxLen: 3},
	63:  {charset: "binary", maxLen: 1},
	255: {charset: "utf8mb4", maxLen: 4},
}

// mcol is one column of a hand-built TABLE_MAP event.
type mcol struct {
	name      string
	typ       byte
	meta      uint16
	nullable  bool
	unsigned  bool
	collation uint64   // character columns only
	enum, set []string // ENUM/SET members
}

// buildMap assembles a TABLE_MAP event the way MySQL 8 writes it with binlog_row_metadata=FULL.
func buildMap(pk []uint64, cols ...mcol) *replication.TableMapEvent {
	ev := &replication.TableMapEvent{
		Schema:      []byte("shop"),
		Table:       []byte("t"),
		ColumnCount: uint64(len(cols)),
		NullBitmap:  make([]byte, (len(cols)+7)/8),
		PrimaryKey:  pk,
	}
	numeric := 0
	for i, c := range cols {
		ev.ColumnType = append(ev.ColumnType, c.typ)
		ev.ColumnMeta = append(ev.ColumnMeta, c.meta)
		ev.ColumnName = append(ev.ColumnName, []byte(c.name))
		if c.nullable {
			ev.NullBitmap[i/8] |= 1 << (i % 8)
		}
		if isNumeric(c.typ) {
			if numeric%8 == 0 {
				ev.SignednessBitmap = append(ev.SignednessBitmap, 0)
			}
			if c.unsigned {
				ev.SignednessBitmap[numeric/8] |= 0x80 >> (numeric % 8)
			}
			numeric++
		}
		if c.collation != 0 {
			ev.ColumnCharset = append(ev.ColumnCharset, c.collation)
		}
		if c.enum != nil {
			ev.EnumStrValue = append(ev.EnumStrValue, toBytes(c.enum))
		}
		if c.set != nil {
			ev.SetStrValue = append(ev.SetStrValue, toBytes(c.set))
		}
	}
	return ev
}

func isNumeric(typ byte) bool {
	switch typ {
	case gomysql.MYSQL_TYPE_TINY, gomysql.MYSQL_TYPE_SHORT, gomysql.MYSQL_TYPE_INT24, gomysql.MYSQL_TYPE_LONG,
		gomysql.MYSQL_TYPE_LONGLONG, gomysql.MYSQL_TYPE_NEWDECIMAL, gomysql.MYSQL_TYPE_FLOAT,
		gomysql.MYSQL_TYPE_DOUBLE, gomysql.MYSQL_TYPE_YEAR:
		return true
	}
	return false
}

func toBytes(ss []string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// stringTypeMeta encodes CHAR/BINARY/ENUM/SET metadata the way MySQL does: real type and length in two bytes.
func stringTypeMeta(realType byte, length int) uint16 {
	b0 := uint16(realType)
	if length >= 256 {
		b0 = uint16(realType&0xcf) | uint16((^(length >> 4))&0x30)
	}
	return b0<<8 | uint16(length&0xff)
}

func TestColumnMapping(t *testing.T) {
	str := func(length int) uint16 { return stringTypeMeta(gomysql.MYSQL_TYPE_STRING, length) }

	tests := []struct {
		name string
		col  mcol
		want typemap.Column
	}{
		{"tinyint", mcol{typ: gomysql.MYSQL_TYPE_TINY}, typemap.Column{Kind: typemap.KindTinyInt}},
		{"smallint unsigned", mcol{typ: gomysql.MYSQL_TYPE_SHORT, unsigned: true}, typemap.Column{Kind: typemap.KindSmallInt, Unsigned: true}},
		{"mediumint", mcol{typ: gomysql.MYSQL_TYPE_INT24}, typemap.Column{Kind: typemap.KindMediumInt}},
		{"int nullable", mcol{typ: gomysql.MYSQL_TYPE_LONG, nullable: true}, typemap.Column{Kind: typemap.KindInt, Nullable: true}},
		{"bigint unsigned", mcol{typ: gomysql.MYSQL_TYPE_LONGLONG, unsigned: true}, typemap.Column{Kind: typemap.KindBigInt, Unsigned: true}},
		{"float", mcol{typ: gomysql.MYSQL_TYPE_FLOAT}, typemap.Column{Kind: typemap.KindFloat}},
		{"double", mcol{typ: gomysql.MYSQL_TYPE_DOUBLE}, typemap.Column{Kind: typemap.KindDouble}},
		{"decimal", mcol{typ: gomysql.MYSQL_TYPE_NEWDECIMAL, meta: 65<<8 | 30}, typemap.Column{Kind: typemap.KindDecimal, Precision: 65, Scale: 30}},
		{"year", mcol{typ: gomysql.MYSQL_TYPE_YEAR}, typemap.Column{Kind: typemap.KindYear}},
		{"date", mcol{typ: gomysql.MYSQL_TYPE_DATE}, typemap.Column{Kind: typemap.KindDate}},
		{"newdate", mcol{typ: gomysql.MYSQL_TYPE_NEWDATE}, typemap.Column{Kind: typemap.KindDate}},
		{"datetime(6)", mcol{typ: gomysql.MYSQL_TYPE_DATETIME2, meta: 6}, typemap.Column{Kind: typemap.KindDateTime, FSP: 6}},
		{"timestamp(3)", mcol{typ: gomysql.MYSQL_TYPE_TIMESTAMP2, meta: 3}, typemap.Column{Kind: typemap.KindTimestamp, FSP: 3}},
		{"time(2)", mcol{typ: gomysql.MYSQL_TYPE_TIME2, meta: 2}, typemap.Column{Kind: typemap.KindTime, FSP: 2}},
		{"bit(1)", mcol{typ: gomysql.MYSQL_TYPE_BIT, meta: 0<<8 | 1}, typemap.Column{Kind: typemap.KindBit, Length: 1}},
		{"bit(12)", mcol{typ: gomysql.MYSQL_TYPE_BIT, meta: 1<<8 | 4}, typemap.Column{Kind: typemap.KindBit, Length: 12}},
		{"bit(64)", mcol{typ: gomysql.MYSQL_TYPE_BIT, meta: 8 << 8}, typemap.Column{Kind: typemap.KindBit, Length: 64}},
		{"json", mcol{typ: gomysql.MYSQL_TYPE_JSON, meta: 4}, typemap.Column{Kind: typemap.KindJSON}},
		{"geometry", mcol{typ: gomysql.MYSQL_TYPE_GEOMETRY, meta: 4}, typemap.Column{Kind: typemap.KindGeometry}},

		{"varchar(50) utf8mb4", mcol{typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 200, collation: 45}, typemap.Column{Kind: typemap.KindVarChar, Length: 50, Charset: "utf8mb4"}},
		{"varchar latin1", mcol{typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 30, collation: 8}, typemap.Column{Kind: typemap.KindVarChar, Length: 30, Charset: "latin1"}},
		{"varbinary(16)", mcol{typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 16, collation: 63}, typemap.Column{Kind: typemap.KindVarBinary, Length: 16}},
		{"char(3) utf8mb4", mcol{typ: gomysql.MYSQL_TYPE_STRING, meta: str(12), collation: 255}, typemap.Column{Kind: typemap.KindChar, Length: 3, Charset: "utf8mb4"}},
		{"binary(16)", mcol{typ: gomysql.MYSQL_TYPE_STRING, meta: str(16), collation: 63}, typemap.Column{Kind: typemap.KindBinary, Length: 16}},
		{"binary(255)", mcol{typ: gomysql.MYSQL_TYPE_STRING, meta: str(255), collation: 63}, typemap.Column{Kind: typemap.KindBinary, Length: 255}},
		{"char(255) latin1", mcol{typ: gomysql.MYSQL_TYPE_STRING, meta: str(255), collation: 8}, typemap.Column{Kind: typemap.KindChar, Length: 255, Charset: "latin1"}},
		{"text", mcol{typ: gomysql.MYSQL_TYPE_BLOB, meta: 2, collation: 45}, typemap.Column{Kind: typemap.KindText, Charset: "utf8mb4"}},
		{"longtext utf8mb3", mcol{typ: gomysql.MYSQL_TYPE_BLOB, meta: 4, collation: 33}, typemap.Column{Kind: typemap.KindText, Charset: "utf8mb3"}},
		{"blob", mcol{typ: gomysql.MYSQL_TYPE_BLOB, meta: 2, collation: 63}, typemap.Column{Kind: typemap.KindBlob}},
		{"tinyblob", mcol{typ: gomysql.MYSQL_TYPE_BLOB, meta: 1, collation: 63}, typemap.Column{Kind: typemap.KindBlob}},

		{
			"enum",
			mcol{typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_ENUM)<<8 | 1, enum: []string{"a", "b c"}},
			typemap.Column{Kind: typemap.KindEnum, Values: []string{"a", "b c"}},
		},
		{
			"set",
			mcol{typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_SET)<<8 | 1, set: []string{"x", "y"}},
			typemap.Column{Kind: typemap.KindSet, Values: []string{"x", "y"}},
		},
		{
			"enum with utf8 members",
			mcol{typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_ENUM)<<8 | 1, enum: []string{"да", "нет"}},
			typemap.Column{Kind: typemap.KindEnum, Values: []string{"да", "нет"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.col.name = "c"
			tt.want.Name = "c"

			tbl, err := tableFromMap(buildMap(nil, tt.col), testCollations)
			if err != nil {
				t.Fatalf("tableFromMap() = %v", err)
			}
			if got := tbl.Columns[0]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("column = %+v\nwant     %+v", got, tt.want)
			}
		})
	}
}

// TestMixedColumnsKeepSignednessAndMembersAligned guards the parallel lists of the binlog: the
// signedness bitmap covers numeric columns only, and ENUM/SET members are ordered per kind.
func TestMixedColumnsKeepSignednessAndMembersAligned(t *testing.T) {
	ev := buildMap([]uint64{0},
		mcol{name: "id", typ: gomysql.MYSQL_TYPE_LONGLONG, unsigned: true},
		mcol{name: "name", typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 40, collation: 45},
		mcol{name: "age", typ: gomysql.MYSQL_TYPE_TINY},
		mcol{name: "kind", typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_ENUM)<<8 | 1, enum: []string{"one", "two"}},
		mcol{name: "score", typ: gomysql.MYSQL_TYPE_SHORT, unsigned: true},
		mcol{name: "tags", typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_SET)<<8 | 1, set: []string{"a", "b"}},
		mcol{name: "level", typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_ENUM)<<8 | 1, enum: []string{"low", "high"}},
	)

	tbl, err := tableFromMap(ev, testCollations)
	if err != nil {
		t.Fatal(err)
	}

	unsigned := map[string]bool{}
	for _, c := range tbl.Columns {
		unsigned[c.Name] = c.Unsigned
	}
	want := map[string]bool{"id": true, "name": false, "age": false, "kind": false, "score": true, "tags": false, "level": false}
	if !reflect.DeepEqual(unsigned, want) {
		t.Errorf("unsigned = %v, want %v", unsigned, want)
	}
	if got := tbl.Columns[3].Values; !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Errorf("first enum members = %v", got)
	}
	if got := tbl.Columns[6].Values; !reflect.DeepEqual(got, []string{"low", "high"}) {
		t.Errorf("second enum members = %v", got)
	}
	if got := tbl.Columns[5].Values; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("set members = %v", got)
	}
}

func TestPrimaryKeyKeepsKeyOrder(t *testing.T) {
	ev := buildMap([]uint64{1, 0},
		mcol{name: "a", typ: gomysql.MYSQL_TYPE_LONG},
		mcol{name: "b", typ: gomysql.MYSQL_TYPE_LONG},
	)
	tbl, err := tableFromMap(ev, testCollations)
	if err != nil {
		t.Fatal(err)
	}
	if got := tbl.PrimaryKeyColumns(); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Errorf("PrimaryKeyColumns() = %v", got)
	}
	if tbl.FullName() != "shop.t" {
		t.Errorf("FullName() = %q", tbl.FullName())
	}
}

func TestTableWithoutPrimaryKey(t *testing.T) {
	tbl, err := tableFromMap(buildMap(nil, mcol{name: "a", typ: gomysql.MYSQL_TYPE_LONG}), testCollations)
	if err != nil {
		t.Fatal(err)
	}
	if len(tbl.PrimaryKey) != 0 {
		t.Errorf("PrimaryKey = %v, want none", tbl.PrimaryKey)
	}
}

func TestTableFromMapErrors(t *testing.T) {
	good := mcol{name: "a", typ: gomysql.MYSQL_TYPE_LONG}

	tests := []struct {
		name    string
		ev      func() *replication.TableMapEvent
		wantMsg string
	}{
		{"no column names", func() *replication.TableMapEvent {
			ev := buildMap(nil, good)
			ev.ColumnName = nil
			return ev
		}, "binlog_row_metadata must be FULL"},
		{"no columns", func() *replication.TableMapEvent { return buildMap(nil) }, "no columns"},
		{"short null bitmap", func() *replication.TableMapEvent {
			ev := buildMap(nil, good)
			ev.NullBitmap = nil
			return ev
		}, "null bitmap"},
		{"inconsistent character sets", func() *replication.TableMapEvent {
			ev := buildMap(nil, mcol{name: "s", typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 10, collation: 45})
			ev.ColumnCharset = nil
			ev.DefaultCharset = []uint64{45, 0} // a dangling (ordinal, collation) pair
			return ev
		}, "malformed default character set"},
		{"character sets cover too few columns", func() *replication.TableMapEvent {
			return buildMap(nil,
				mcol{name: "a", typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 10, collation: 45},
				mcol{name: "b", typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 10},
			)
		}, "covers 1 of the character columns"},
		{"primary key out of range", func() *replication.TableMapEvent { return buildMap([]uint64{5}, good) }, "primary key refers to column 5"},
		{"unknown type", func() *replication.TableMapEvent {
			return buildMap(nil, mcol{name: "v", typ: gomysql.MYSQL_TYPE_VECTOR})
		}, "unsupported"},
		{"unknown collation", func() *replication.TableMapEvent {
			return buildMap(nil, mcol{name: "s", typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 10, collation: 999})
		}, "unknown collation id 999"},
		{"no charset info", func() *replication.TableMapEvent {
			ev := buildMap(nil, mcol{name: "s", typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 10, collation: 45})
			ev.ColumnCharset = nil
			return ev
		}, "no character set"},
		{"missing enum members", func() *replication.TableMapEvent {
			ev := buildMap(nil, mcol{name: "e", typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_ENUM)<<8 | 1, enum: []string{"a"}})
			ev.EnumStrValue = nil
			return ev
		}, "no ENUM members"},
		{"non-utf8 enum member", func() *replication.TableMapEvent {
			return buildMap(nil, mcol{name: "e", typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_ENUM)<<8 | 1, enum: []string{"caf\xe9"}})
		}, "not valid UTF-8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tableFromMap(tt.ev(), testCollations)
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("tableFromMap() = %v, want error containing %q", err, tt.wantMsg)
			}
			if !strings.Contains(err.Error(), "shop.t") {
				t.Errorf("error %q does not name the table", err)
			}
		})
	}
}

func TestStringMeta(t *testing.T) {
	tests := []struct {
		realType byte
		length   int
	}{
		{gomysql.MYSQL_TYPE_STRING, 1},
		{gomysql.MYSQL_TYPE_STRING, 16},
		{gomysql.MYSQL_TYPE_STRING, 255},
		{gomysql.MYSQL_TYPE_STRING, 256},
		{gomysql.MYSQL_TYPE_STRING, 1020}, // CHAR(255) utf8mb4
		{gomysql.MYSQL_TYPE_ENUM, 1},
		{gomysql.MYSQL_TYPE_SET, 8},
	}
	for _, tt := range tests {
		gotType, gotLen := stringMeta(stringTypeMeta(tt.realType, tt.length))
		if gotType != tt.realType || gotLen != tt.length {
			t.Errorf("stringMeta(encode(%d, %d)) = (%d, %d)", tt.realType, tt.length, gotType, gotLen)
		}
	}
}
