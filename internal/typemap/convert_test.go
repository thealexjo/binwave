package typemap

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// recorder collects adjustments for assertions.
type recorder struct {
	mu   sync.Mutex
	list []Adjustment
}

func (r *recorder) Report(a Adjustment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, a)
}

type convertCase struct {
	name string
	in   any
	want any
	// wantErr is a substring of the expected error; empty means success.
	wantErr string
	// wantAdjust is the expected adjustment kind; empty means none.
	wantAdjust AdjustmentKind
}

func runConvert(t *testing.T, col Column, override string, cases []convertCase) {
	t.Helper()
	col.Name = "c"
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			m, err := Resolve("db.t", col, Options{Override: override, Reporter: rec})
			if err != nil {
				t.Fatalf("Resolve() error: %v", err)
			}

			got, err := m.Convert(tt.in)

			if tt.wantErr != "" {
				var cerr *ConversionError
				if !errors.As(err, &cerr) {
					t.Fatalf("Convert(%#v) error = %v, want *ConversionError containing %q", tt.in, err, tt.wantErr)
				}
				if !strings.Contains(cerr.Detail(), tt.wantErr) {
					t.Errorf("detail = %q, want it to contain %q", cerr.Detail(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Convert(%#v) error: %v", tt.in, err)
			}
			assertValue(t, got, tt.want)

			switch {
			case tt.wantAdjust == "" && len(rec.list) > 0:
				t.Errorf("unexpected adjustments: %+v", rec.list)
			case tt.wantAdjust != "":
				if len(rec.list) != 1 || rec.list[0].Kind != tt.wantAdjust {
					t.Fatalf("adjustments = %+v, want one %q", rec.list, tt.wantAdjust)
				}
				if a := rec.list[0]; a.Table != "db.t" || a.Column != "c" || a.Original == "" || a.Replacement == "" {
					t.Errorf("adjustment lacks context: %+v", a)
				}
			}
		})
	}
}

func assertValue(t *testing.T, got, want any) {
	t.Helper()
	switch w := want.(type) {
	case decimal.Decimal:
		g, ok := got.(decimal.Decimal)
		if !ok || !g.Equal(w) {
			t.Errorf("got %#v, want decimal %s", got, w)
		}
	case time.Time:
		g, ok := got.(time.Time)
		if !ok || !g.Equal(w) || g.Location() != time.UTC {
			t.Errorf("got %#v, want %s (UTC)", got, w)
		}
	default:
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v (%T), want %#v (%T)", got, got, want, want)
		}
	}
}

func TestConvertIntegers(t *testing.T) {
	runConvert(t, Column{Kind: KindTinyInt}, "", []convertCase{
		{name: "min", in: int8(-128), want: int8(-128)},
		{name: "max from int64", in: int64(127), want: int8(127)},
		{name: "overflow", in: int64(128), wantErr: "128 out of Int8 range"},
		{name: "underflow", in: int64(-129), wantErr: "-129 out of Int8 range"},
		{name: "not integer", in: "1", wantErr: "want an integer, got string"},
		{name: "float rejected", in: 1.0, wantErr: "want an integer"},
	})
	runConvert(t, Column{Kind: KindTinyInt, Unsigned: true}, "", []convertCase{
		{name: "max", in: uint8(255), want: uint8(255)},
		{name: "from int", in: 200, want: uint8(200)},
		{name: "negative", in: int8(-1), wantErr: "-1 out of UInt8 range"},
	})
	runConvert(t, Column{Kind: KindSmallInt}, "", []convertCase{
		{name: "min", in: int16(math.MinInt16), want: int16(math.MinInt16)},
		{name: "overflow", in: uint16(math.MaxUint16), wantErr: "out of Int16 range"},
	})
	runConvert(t, Column{Kind: KindSmallInt, Unsigned: true}, "", []convertCase{
		{name: "max", in: int32(math.MaxUint16), want: uint16(math.MaxUint16)},
	})
	runConvert(t, Column{Kind: KindMediumInt}, "", []convertCase{
		{name: "mediumint min", in: int32(-8388608), want: int32(-8388608)},
	})
	runConvert(t, Column{Kind: KindMediumInt, Unsigned: true}, "", []convertCase{
		{name: "mediumint max", in: uint32(16777215), want: uint32(16777215)},
	})
	runConvert(t, Column{Kind: KindInt}, "", []convertCase{
		{name: "min", in: int64(math.MinInt32), want: int32(math.MinInt32)},
		{name: "overflow", in: int64(math.MaxInt32) + 1, wantErr: "out of Int32 range"},
	})
	runConvert(t, Column{Kind: KindInt, Unsigned: true}, "", []convertCase{
		{name: "max", in: uint32(math.MaxUint32), want: uint32(math.MaxUint32)},
		{name: "overflow", in: uint64(math.MaxUint32) + 1, wantErr: "out of UInt32 range"},
	})
	runConvert(t, Column{Kind: KindBigInt}, "", []convertCase{
		{name: "min", in: int64(math.MinInt64), want: int64(math.MinInt64)},
		{name: "max", in: int64(math.MaxInt64), want: int64(math.MaxInt64)},
		{name: "uint64 overflow", in: uint64(math.MaxInt64) + 1, wantErr: "9223372036854775808 out of Int64 range"},
	})
	runConvert(t, Column{Kind: KindBigInt, Unsigned: true}, "", []convertCase{
		{name: "max", in: uint64(math.MaxUint64), want: uint64(math.MaxUint64)},
		{name: "from uint", in: uint(7), want: uint64(7)},
		{name: "min int64 is negative", in: int64(math.MinInt64), wantErr: "-9223372036854775808 out of UInt64 range"},
	})
}

func TestConvertBoolOverride(t *testing.T) {
	runConvert(t, Column{Kind: KindTinyInt}, "Bool", []convertCase{
		{name: "zero", in: int8(0), want: false},
		{name: "one", in: int8(1), want: true},
		{name: "any non-zero", in: int8(-5), want: true},
		{name: "not integer", in: true, wantErr: "want an integer"},
	})
}

func TestConvertFloats(t *testing.T) {
	runConvert(t, Column{Kind: KindFloat}, "", []convertCase{
		{name: "float32", in: float32(1.5), want: float32(1.5)},
		{name: "float64", in: 2.25, want: float32(2.25)},
		{name: "string", in: "1.5", wantErr: "want float32 or float64"},
	})
	runConvert(t, Column{Kind: KindDouble}, "", []convertCase{
		{name: "float64", in: math.MaxFloat64, want: math.MaxFloat64},
		{name: "float32", in: float32(0.5), want: 0.5},
		{name: "int", in: 1, wantErr: "want float32 or float64"},
	})
}

func TestConvertDecimal(t *testing.T) {
	runConvert(t, Column{Kind: KindDecimal, Precision: 10, Scale: 2}, "", []convertCase{
		{name: "string", in: "12345678.90", want: decimal.RequireFromString("12345678.90")},
		{name: "bytes", in: []byte("-0.01"), want: decimal.RequireFromString("-0.01")},
		{name: "decimal", in: decimal.New(5, 0), want: decimal.New(5, 0)},
		{name: "fewer fraction digits", in: "1.5", want: decimal.RequireFromString("1.50")},
		{name: "zero", in: "0.00", want: decimal.Zero},
		{name: "too many integer digits", in: "123456789.00", wantErr: "does not fit DECIMAL(10,2)"},
		{name: "too many fraction digits", in: "1.001", wantErr: "more than 2 fractional digits"},
		{name: "trailing zeros are fine", in: "1.1000", want: decimal.RequireFromString("1.1")},
		{name: "garbage", in: "12,5", wantErr: `invalid decimal "12,5"`},
		{name: "float rejected", in: 1.5, wantErr: "want decimal text"},
	})

	const wide = "12345678901234567890123456789012345.123456789012345678901234567891"
	runConvert(t, Column{Kind: KindDecimal, Precision: 65, Scale: 30}, "", []convertCase{
		{name: "decimal(65,30) keeps every digit", in: wide[:len(wide)-1], want: decimal.RequireFromString(wide[:len(wide)-1])},
	})
	runConvert(t, Column{Kind: KindDecimal, Precision: 3, Scale: 3}, "", []convertCase{
		{name: "only fraction", in: "0.999", want: decimal.RequireFromString("0.999")},
		{name: "integer part not allowed", in: "1.000", wantErr: "does not fit DECIMAL(3,3)"},
	})

	t.Run("exact string survives", func(t *testing.T) {
		m, err := Resolve("db.t", Column{Name: "c", Kind: KindDecimal, Precision: 65, Scale: 30}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		in := "99999999999999999999999999999999999.999999999999999999999999999999"
		got, err := m.Convert(in)
		if err != nil {
			t.Fatal(err)
		}
		if s := got.(decimal.Decimal).StringFixed(30); s != in {
			t.Errorf("round trip = %s, want %s", s, in)
		}
	})
}

func TestConvertYear(t *testing.T) {
	runConvert(t, Column{Kind: KindYear}, "", []convertCase{
		{name: "min", in: 1901, want: uint16(1901)},
		{name: "max", in: int64(2155), want: uint16(2155)},
		{name: "zero year", in: 0, want: uint16(0)},
		{name: "below", in: 1900, wantErr: "out of MySQL YEAR range"},
		{name: "negative", in: -1, wantErr: "out of MySQL YEAR range"},
	})
}

func TestConvertBit(t *testing.T) {
	runConvert(t, Column{Kind: KindBit, Length: 1}, "", []convertCase{
		{name: "one", in: int64(1), want: uint64(1)},
		{name: "too wide", in: int64(2), wantErr: "does not fit BIT(1)"},
		{name: "bytes", in: []byte{0x01}, want: uint64(1)},
	})
	runConvert(t, Column{Kind: KindBit, Length: 64}, "", []convertCase{
		{name: "all bits", in: uint64(math.MaxUint64), want: uint64(math.MaxUint64)},
		{name: "big endian bytes", in: []byte{0x01, 0x02}, want: uint64(0x0102)},
		{name: "nine bytes", in: make([]byte, 9), wantErr: "want at most 8"},
		{name: "negative", in: int64(-1), wantErr: "negative BIT value"},
		{name: "string", in: "1", wantErr: "want an integer or []byte"},
	})
}

func date(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

func TestConvertDate(t *testing.T) {
	runConvert(t, Column{Kind: KindDate}, "", []convertCase{
		{name: "regular", in: "2024-02-29", want: date(2024, 2, 29)},
		{name: "bytes", in: []byte("1999-12-31"), want: date(1999, 12, 31)},
		{name: "min", in: "1900-01-01", want: MinDate},
		{name: "max", in: "2299-12-31", want: MaxDate},
		{name: "time.Time drops clock", in: time.Date(2024, 5, 6, 23, 59, 0, 0, time.UTC), want: date(2024, 5, 6)},
		{name: "zero date", in: "0000-00-00", want: MinDate, wantAdjust: AdjustZeroDate},
		{name: "zero month", in: "2024-00-15", want: MinDate, wantAdjust: AdjustInvalidDate},
		{name: "zero day", in: "2024-03-00", want: MinDate, wantAdjust: AdjustInvalidDate},
		{name: "non-existent day", in: "2023-02-29", want: MinDate, wantAdjust: AdjustInvalidDate},
		{name: "below range", in: "1000-01-01", want: MinDate, wantAdjust: AdjustOutOfRange},
		{name: "above range", in: "9999-12-31", want: MaxDate, wantAdjust: AdjustOutOfRange},
		{name: "garbage", in: "yesterday", wantErr: "invalid date"},
		{name: "month 13", in: "2024-13-01", wantErr: "invalid month"},
		{name: "wrong type", in: 20240101, wantErr: "want MySQL date text or time.Time"},
	})
}

func TestConvertDateTime(t *testing.T) {
	msk := time.FixedZone("MSK", 3*3600)

	runConvert(t, Column{Kind: KindDateTime, FSP: 6}, "", []convertCase{
		{name: "as is", in: "2024-06-01 12:34:56", want: time.Date(2024, 6, 1, 12, 34, 56, 0, time.UTC)},
		{name: "microseconds", in: "2024-06-01 12:34:56.123456", want: time.Date(2024, 6, 1, 12, 34, 56, 123456000, time.UTC)},
		{name: "short fraction", in: "2024-06-01 12:34:56.5", want: time.Date(2024, 6, 1, 12, 34, 56, 500000000, time.UTC)},
		{name: "ISO T separator", in: "2024-06-01T12:34:56", want: time.Date(2024, 6, 1, 12, 34, 56, 0, time.UTC)},
		{name: "date only", in: "2024-06-01", want: date(2024, 6, 1)},
		{name: "wall clock kept from time.Time", in: time.Date(2024, 6, 1, 12, 0, 0, 0, msk), want: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)},
		{name: "zero", in: "0000-00-00 00:00:00", want: MinDate, wantAdjust: AdjustZeroDate},
		{name: "zero with fraction", in: "0000-00-00 00:00:00.000000", want: MinDate, wantAdjust: AdjustZeroDate},
		{name: "zero date part with time", in: "0000-00-00 10:00:00", want: MinDate, wantAdjust: AdjustInvalidDate},
		{name: "max", in: "2299-12-31 23:59:59.999999", want: MaxDateTime},
		{name: "above range", in: "9999-12-31 23:59:59", want: MaxDateTime, wantAdjust: AdjustOutOfRange},
		{name: "below range", in: "1899-12-31 23:59:59", want: MinDate, wantAdjust: AdjustOutOfRange},
		{name: "zero time.Time", in: time.Time{}, want: MinDate, wantAdjust: AdjustOutOfRange},
		{name: "bad hour", in: "2024-06-01 24:00:00", wantErr: "invalid hour"},
		{name: "bad minute", in: "2024-06-01 10:60:00", wantErr: "invalid minute"},
		{name: "bad second", in: "2024-06-01 10:00:60", wantErr: "invalid second"},
		{name: "bad fraction", in: "2024-06-01 10:00:00.", wantErr: "invalid fractional seconds"},
		{name: "bad clock", in: "2024-06-01 10:00", wantErr: "invalid time"},
	})

	runConvert(t, Column{Kind: KindTimestamp, FSP: 3}, "", []convertCase{
		{name: "text is UTC", in: "2024-06-01 09:00:00.250", want: time.Date(2024, 6, 1, 9, 0, 0, 250000000, time.UTC)},
		{name: "instant converted to UTC", in: time.Date(2024, 6, 1, 12, 0, 0, 0, msk), want: time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)},
		{name: "zero timestamp", in: "0000-00-00 00:00:00", want: MinDate, wantAdjust: AdjustZeroDate},
	})
}

func TestConvertTime(t *testing.T) {
	runConvert(t, Column{Kind: KindTime, FSP: 6}, "", []convertCase{
		{name: "zero", in: "00:00:00", want: int64(0)},
		{name: "regular", in: "12:34:56.000001", want: int64(45296000001)},
		{name: "max", in: "838:59:59.000000", want: int64(3020399000000)},
		{name: "min", in: "-838:59:59", want: int64(-3020399000000)},
		{name: "bytes", in: []byte("01:00:00"), want: int64(3600000000)},
		{name: "duration", in: 90 * time.Second, want: int64(90000000)},
		{name: "above max", in: "838:59:59.000001", wantErr: "out of MySQL TIME range"},
		{name: "hours too large", in: "839:00:00", wantErr: "invalid time"},
		{name: "bad minutes", in: "10:61:00", wantErr: "invalid time"},
		{name: "bad syntax", in: "10:00", wantErr: "invalid time"},
		{name: "wrong type", in: 5, wantErr: "want MySQL time text"},
	})
}

func TestConvertText(t *testing.T) {
	runConvert(t, Column{Kind: KindVarChar, Charset: "utf8mb4"}, "", []convertCase{
		{name: "string", in: "привет 👋", want: "привет 👋"},
		{name: "bytes", in: []byte("abc"), want: "abc"},
		{name: "empty", in: "", want: ""},
		{name: "wrong type", in: 5, wantErr: "want string or []byte"},
	})
	runConvert(t, Column{Kind: KindVarChar, Charset: "latin1"}, "", []convertCase{
		// MySQL latin1 is cp1252: 0x80 is the euro sign, 0xE9 is é.
		{name: "cp1252", in: []byte{'c', 'a', 'f', 0xE9, ' ', 0x80}, want: "café €"},
	})
	runConvert(t, Column{Kind: KindChar, Charset: "cp1251"}, "", []convertCase{
		{name: "cp1251", in: []byte{0xEF, 0xF0, 0xE8, 0xE2, 0xE5, 0xF2}, want: "привет"},
	})
	runConvert(t, Column{Kind: KindText, Charset: "KOI8R"}, "LowCardinality(String)", []convertCase{
		{name: "charset name is case-insensitive", in: []byte{0xD0, 0xD2, 0xC9}, want: "при"},
	})
	runConvert(t, Column{Kind: KindText, Charset: "utf16le"}, "", []convertCase{
		{name: "utf16le", in: []byte{'h', 0, 'i', 0}, want: "hi"},
	})
}

func TestConvertBinary(t *testing.T) {
	runConvert(t, Column{Kind: KindBinary, Length: 16}, "", []convertCase{
		{
			name: "uuid with trailing zero bytes is padded back",
			in:   []byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17},
			want: string([]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0, 0}),
		},
		{name: "full length", in: strings.Repeat("x", 16), want: strings.Repeat("x", 16)},
		{name: "empty", in: []byte{}, want: string(make([]byte, 16))},
		{name: "too long", in: strings.Repeat("x", 17), wantErr: "17 bytes do not fit BINARY(16)"},
	})
	runConvert(t, Column{Kind: KindBlob}, "", []convertCase{
		{name: "raw bytes untouched", in: []byte{0xff, 0x00, 0xfe}, want: string([]byte{0xff, 0x00, 0xfe})},
	})
	runConvert(t, Column{Kind: KindJSON}, "", []convertCase{
		{name: "json text", in: []byte(`{"a":[1,2]}`), want: `{"a":[1,2]}`},
	})
	runConvert(t, Column{Kind: KindJSON}, "JSON", []convertCase{
		{name: "json override", in: `{"a":1}`, want: `{"a":1}`},
	})
}

func TestConvertEnum(t *testing.T) {
	col := Column{Kind: KindEnum, Values: []string{"active", "blocked"}}

	runConvert(t, col, "", []convertCase{
		{name: "index", in: int64(2), want: "blocked"},
		{name: "invalid index 0", in: int64(0), want: ""},
		{name: "string", in: "active", want: "active"},
		{name: "bytes", in: []byte("active"), want: "active"},
		{name: "index out of range", in: int64(3), wantErr: "ENUM index 3 out of range 0..2"},
		{name: "negative", in: -1, wantErr: "out of range"},
		{name: "wrong type", in: 1.0, wantErr: "want an ENUM index or string"},
	})
	runConvert(t, col, "Enum8('active' = 1, 'blocked' = 2)", []convertCase{
		{name: "member", in: int64(1), want: "active"},
		{name: "not a member", in: int64(0), wantErr: `ENUM value "" is not a member`},
	})
}

func TestConvertSet(t *testing.T) {
	col := Column{Kind: KindSet, Values: []string{"a", "b", "c"}}

	runConvert(t, col, "", []convertCase{
		{name: "bitmask", in: int64(0b101), want: []string{"a", "c"}},
		{name: "empty", in: int64(0), want: []string{}},
		{name: "text", in: "a,b", want: []string{"a", "b"}},
		{name: "empty text", in: "", want: []string{}},
		{name: "bits beyond members", in: int64(0b1000), wantErr: "bits beyond 3 members"},
		{name: "negative", in: int64(-1), wantErr: "negative SET bitmask"},
		{name: "wrong type", in: 1.5, wantErr: "want a SET bitmask or string"},
	})
	runConvert(t, col, "String", []convertCase{
		{name: "joined", in: int64(0b011), want: "a,b"},
	})

	full := Column{Kind: KindSet, Values: make([]string, 64)}
	for i := range full.Values {
		full.Values[i] = string(rune('A' + i%26))
	}
	runConvert(t, full, "", []convertCase{
		{name: "64th member", in: uint64(1 << 63), want: []string{full.Values[63]}},
	})
}

func TestConvertNull(t *testing.T) {
	runConvert(t, Column{Kind: KindInt, Nullable: true}, "", []convertCase{
		{name: "nullable", in: nil, want: nil},
	})
	runConvert(t, Column{Kind: KindEnum, Values: []string{"a"}, Nullable: true}, "", []convertCase{
		{name: "nullable low cardinality", in: nil, want: nil},
	})
	runConvert(t, Column{Kind: KindInt}, "", []convertCase{
		{name: "not null", in: nil, wantErr: "NULL in a NOT NULL column"},
	})
	runConvert(t, Column{Kind: KindSet, Values: []string{"a"}, Nullable: true}, "", []convertCase{
		{name: "nullable set becomes empty array", in: nil, want: []string{}, wantAdjust: AdjustNullSet},
	})
	runConvert(t, Column{Kind: KindSet, Values: []string{"a"}, Nullable: true}, "String", []convertCase{
		{name: "nullable set as string stays NULL", in: nil, want: nil},
	})
}

func TestConversionErrorContext(t *testing.T) {
	m, err := Resolve("shop.users", Column{Name: "age", Kind: KindTinyInt, Unsigned: true}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Convert(int64(300))
	var cerr *ConversionError
	if !errors.As(err, &cerr) {
		t.Fatalf("error = %v", err)
	}

	// The message goes to logs and to the status of the metrics port: no value in it.
	if strings.Contains(err.Error(), "300") || !strings.Contains(err.Error(), "shop.users.age") {
		t.Errorf("the message must name the column and hide the value: %s", err)
	}
	want := "convert shop.users.age (TINYINT UNSIGNED NOT NULL → UInt8): 300 out of UInt8 range (value: 300)"
	if cerr.Detail() != want {
		t.Errorf("detail = %s\nwant    %s", cerr.Detail(), want)
	}

	long := strings.Repeat("x", 500)
	m, err = Resolve("t", Column{Name: "c", Kind: KindBinary, Length: 1}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Convert(long)
	if !errors.As(err, &cerr) || len(cerr.Detail()) > 300 || !strings.Contains(cerr.Detail(), "…") {
		t.Errorf("long values must be truncated in the detail, got %d chars", len(cerr.Detail()))
	}
}
