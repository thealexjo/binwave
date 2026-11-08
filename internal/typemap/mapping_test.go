package typemap

import (
	"errors"
	"strings"
	"testing"
)

func TestResolveDefaultTypes(t *testing.T) {
	tests := []struct {
		name string
		col  Column
		want string
	}{
		{"tinyint", Column{Kind: KindTinyInt}, "Int8"},
		{"tinyint unsigned", Column{Kind: KindTinyInt, Unsigned: true}, "UInt8"},
		{"smallint", Column{Kind: KindSmallInt}, "Int16"},
		{"smallint unsigned", Column{Kind: KindSmallInt, Unsigned: true}, "UInt16"},
		{"mediumint", Column{Kind: KindMediumInt}, "Int32"},
		{"mediumint unsigned", Column{Kind: KindMediumInt, Unsigned: true}, "UInt32"},
		{"int", Column{Kind: KindInt}, "Int32"},
		{"int unsigned", Column{Kind: KindInt, Unsigned: true}, "UInt32"},
		{"bigint", Column{Kind: KindBigInt}, "Int64"},
		{"bigint unsigned", Column{Kind: KindBigInt, Unsigned: true}, "UInt64"},
		{"float", Column{Kind: KindFloat}, "Float32"},
		{"double", Column{Kind: KindDouble}, "Float64"},
		{"decimal", Column{Kind: KindDecimal, Precision: 10, Scale: 2}, "Decimal(10, 2)"},
		{"decimal max", Column{Kind: KindDecimal, Precision: 65, Scale: 30}, "Decimal(65, 30)"},
		{"year", Column{Kind: KindYear}, "UInt16"},
		{"date", Column{Kind: KindDate}, "Date32"},
		{"datetime", Column{Kind: KindDateTime}, "DateTime64(0, 'UTC')"},
		{"datetime(6)", Column{Kind: KindDateTime, FSP: 6}, "DateTime64(6, 'UTC')"},
		{"timestamp(3)", Column{Kind: KindTimestamp, FSP: 3}, "DateTime64(3, 'UTC')"},
		{"time", Column{Kind: KindTime, FSP: 3}, "Int64"},
		{"char", Column{Kind: KindChar, Length: 10}, "String"},
		{"varchar latin1", Column{Kind: KindVarChar, Length: 255, Charset: "latin1"}, "String"},
		{"text", Column{Kind: KindText}, "String"},
		{"binary(16)", Column{Kind: KindBinary, Length: 16}, "FixedString(16)"},
		{"binary(0)", Column{Kind: KindBinary, Length: 0}, "String"},
		{"varbinary", Column{Kind: KindVarBinary, Length: 64}, "String"},
		{"blob", Column{Kind: KindBlob}, "String"},
		{"geometry", Column{Kind: KindGeometry}, "String"},
		{"json", Column{Kind: KindJSON}, "String"},
		{"enum", Column{Kind: KindEnum, Values: []string{"a", "b"}}, "LowCardinality(String)"},
		{"set", Column{Kind: KindSet, Values: []string{"a", "b"}}, "Array(LowCardinality(String))"},
		{"bit(1)", Column{Kind: KindBit, Length: 1}, "UInt64"},
		{"bit(64)", Column{Kind: KindBit, Length: 64}, "UInt64"},

		{"nullable int", Column{Kind: KindInt, Nullable: true}, "Nullable(Int32)"},
		{"nullable datetime", Column{Kind: KindDateTime, FSP: 3, Nullable: true}, "Nullable(DateTime64(3, 'UTC'))"},
		{"nullable binary", Column{Kind: KindBinary, Length: 4, Nullable: true}, "Nullable(FixedString(4))"},
		{"nullable enum", Column{Kind: KindEnum, Values: []string{"a"}, Nullable: true}, "LowCardinality(Nullable(String))"},
		{"nullable set", Column{Kind: KindSet, Values: []string{"a"}, Nullable: true}, "Array(LowCardinality(String))"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.col.Name = "c"
			m, err := Resolve("db.t", tt.col, Options{})
			if err != nil {
				t.Fatalf("Resolve() error: %v", err)
			}
			if m.Type != tt.want {
				t.Errorf("Type = %q, want %q", m.Type, tt.want)
			}
		})
	}
}

func TestResolveOverrides(t *testing.T) {
	enumCol := Column{Kind: KindEnum, Values: []string{"active", "it's"}}

	tests := []struct {
		name     string
		col      Column
		override string
		want     string
	}{
		{"tinyint as bool", Column{Kind: KindTinyInt}, "Bool", "Bool"},
		{"nullable bool", Column{Kind: KindTinyInt, Unsigned: true, Nullable: true}, "Bool", "Nullable(Bool)"},
		{"varchar as low cardinality", Column{Kind: KindVarChar, Length: 32}, "LowCardinality(String)", "LowCardinality(String)"},
		{"nullable low cardinality", Column{Kind: KindChar, Nullable: true}, "LowCardinality(String)", "LowCardinality(Nullable(String))"},
		{"enum as string", enumCol, "String", "String"},
		{"enum as enum8", enumCol, "Enum8('active' = 1, 'it\\'s' = 2)", "Enum8('active' = 1, 'it\\'s' = 2)"},
		{"enum as enum16 with extra member", enumCol, "Enum16('active'=1,'it''s'=2,'new'=3)", "Enum16('active'=1,'it''s'=2,'new'=3)"},
		{"set as string", Column{Kind: KindSet, Values: []string{"a"}}, "String", "String"},
		{"json as json", Column{Kind: KindJSON}, "JSON", "JSON"},
		{"same as default", Column{Kind: KindInt}, "Int32", "Int32"},
		{"override is trimmed", Column{Kind: KindInt}, "  Bool ", "Bool"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Resolve("db.t", tt.col, Options{Override: tt.override})
			if err != nil {
				t.Fatalf("Resolve() error: %v", err)
			}
			if m.Type != tt.want {
				t.Errorf("Type = %q, want %q", m.Type, tt.want)
			}
		})
	}
}

func TestResolveErrors(t *testing.T) {
	enumCol := Column{Kind: KindEnum, Values: []string{"a", "b"}}

	tests := []struct {
		name     string
		col      Column
		override string
		want     string
	}{
		{"unknown kind", Column{Kind: KindInvalid}, "", "unsupported mapping"},
		{"decimal precision", Column{Kind: KindDecimal, Precision: 66}, "", "invalid DECIMAL precision/scale (66,0)"},
		{"decimal scale above precision", Column{Kind: KindDecimal, Precision: 5, Scale: 6}, "", "invalid DECIMAL"},
		{"fsp", Column{Kind: KindDateTime, FSP: 7}, "", "invalid fractional seconds precision 7"},
		{"binary length", Column{Kind: KindBinary, Length: 256}, "", "invalid BINARY length 256"},
		{"bit length", Column{Kind: KindBit, Length: 0}, "", "invalid BIT length 0"},
		{"set too large", Column{Kind: KindSet, Values: make([]string, 65)}, "", "at most 64"},
		{"unknown charset", Column{Kind: KindVarChar, Charset: "klingon"}, "", `character set "klingon"`},
		{"bool on string", Column{Kind: KindVarChar}, "Bool", `override "Bool" is not compatible with VARCHAR`},
		{"string on int", Column{Kind: KindInt}, "String", "not compatible with INT"},
		{"low cardinality on set", Column{Kind: KindSet}, "LowCardinality(String)", "not compatible with SET"},
		{"json on text", Column{Kind: KindText}, "JSON", "not compatible with TEXT"},
		{"nullable json", Column{Kind: KindJSON, Nullable: true}, "JSON", "cannot be Nullable"},
		{"enum8 on varchar", Column{Kind: KindVarChar}, "Enum8('a' = 1)", "not compatible with VARCHAR"},
		{"enum missing member", enumCol, "Enum8('a' = 1)", `missing ENUM member "b"`},
		{"enum syntax", enumCol, "Enum8(a = 1)", "want Enum8"},
		{"arbitrary type", Column{Kind: KindInt}, "Int64", "not compatible with INT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.col.Name = "c"
			_, err := Resolve("db.t", tt.col, Options{Override: tt.override})
			if err == nil {
				t.Fatalf("Resolve() succeeded, want error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
			if !strings.Contains(err.Error(), "map db.t.c") {
				t.Errorf("error %q lacks table/column context", err)
			}
		})
	}
}

func TestResolveUnsupportedIsDetectable(t *testing.T) {
	_, err := Resolve("db.t", Column{Name: "c", Kind: KindVarChar}, Options{Override: "Bool"})
	if !errors.Is(err, ErrUnsupported) {
		t.Errorf("errors.Is(err, ErrUnsupported) = false for %v", err)
	}
}

func TestParseEnumType(t *testing.T) {
	got, err := parseEnumType(`Enum16('a' = 1, 'b,c' = -2, 'it\'s' = 3, 'x''y'=4, '' = 5)`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"a": 1, "b,c": -2, "it's": 3, "x'y": 4, "": 5}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("member %q = %d, want %d", k, got[k], v)
		}
	}

	for _, bad := range []string{"Enum8", "Enum8()", "Enum8('a')", "Enum8('a' = )", "Enum8('a' = 1,)", "Enum8('a = 1)", "Enum8('a' = 1 'b' = 2)", "Enum8('a' = 1, 'a' = 2)", "Enum8('a' = -)"} {
		if _, err := parseEnumType(bad); err == nil {
			t.Errorf("parseEnumType(%q) succeeded, want error", bad)
		}
	}
}

func TestColumnSQLType(t *testing.T) {
	tests := map[string]Column{
		"DECIMAL(10,2) UNSIGNED NOT NULL":  {Kind: KindDecimal, Precision: 10, Scale: 2, Unsigned: true},
		"VARCHAR(64) CHARACTER SET latin1": {Kind: KindVarChar, Length: 64, Charset: "latin1", Nullable: true},
		"DATETIME(3) NOT NULL":             {Kind: KindDateTime, FSP: 3},
		"DATETIME NOT NULL":                {Kind: KindDateTime},
		"ENUM('a','it''s') NOT NULL":       {Kind: KindEnum, Values: []string{"a", "it's"}},
		"BINARY(16) NOT NULL":              {Kind: KindBinary, Length: 16},
		"Kind(99) NOT NULL":                {Kind: Kind(99)},
	}
	for want, col := range tests {
		if got := col.SQLType(); got != want {
			t.Errorf("SQLType() = %q, want %q", got, want)
		}
	}
}
