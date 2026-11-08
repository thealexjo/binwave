package typemap

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/shopspring/decimal"
)

// integer is a source integer with its sign preserved.
type integer struct {
	neg bool
	abs uint64
}

func toInteger(v any) (integer, error) {
	signed := func(i int64) integer {
		if i < 0 {
			// Negation of MinInt64 overflows int64 but not uint64.
			return integer{neg: true, abs: uint64(-(i + 1)) + 1}
		}
		return integer{abs: uint64(i)}
	}
	switch x := v.(type) {
	case int:
		return signed(int64(x)), nil
	case int8:
		return signed(int64(x)), nil
	case int16:
		return signed(int64(x)), nil
	case int32:
		return signed(int64(x)), nil
	case int64:
		return signed(x), nil
	case uint:
		return integer{abs: uint64(x)}, nil
	case uint8:
		return integer{abs: uint64(x)}, nil
	case uint16:
		return integer{abs: uint64(x)}, nil
	case uint32:
		return integer{abs: uint64(x)}, nil
	case uint64:
		return integer{abs: x}, nil
	}
	return integer{}, fmt.Errorf("want an integer, got %T", v)
}

func (i integer) String() string {
	if i.neg {
		return fmt.Sprintf("-%d", i.abs)
	}
	return fmt.Sprintf("%d", i.abs)
}

type intRange struct {
	min int64
	max uint64
}

var intRanges = map[string]intRange{
	"Int8": {math.MinInt8, math.MaxInt8}, "Int16": {math.MinInt16, math.MaxInt16},
	"Int32": {math.MinInt32, math.MaxInt32}, "Int64": {math.MinInt64, math.MaxInt64},
	"UInt8": {0, math.MaxUint8}, "UInt16": {0, math.MaxUint16},
	"UInt32": {0, math.MaxUint32}, "UInt64": {0, math.MaxUint64},
}

func (r intRange) contains(i integer) bool {
	if i.neg {
		if r.min >= 0 {
			return false
		}
		return i.abs <= uint64(-(r.min+1))+1
	}
	return i.abs <= r.max
}

// integerConverter returns the exact Go type the ClickHouse driver expects for the column.
func integerConverter(chType string) converter {
	r := intRanges[chType]
	return func(_ *Mapping, v any) (any, error) {
		i, err := toInteger(v)
		if err != nil {
			return nil, err
		}
		if !r.contains(i) {
			return nil, fmt.Errorf("%s out of %s range", i, chType)
		}
		signedValue := int64(i.abs)
		if i.neg {
			signedValue = -int64(i.abs-1) - 1
		}
		switch chType {
		case "Int8":
			return int8(signedValue), nil
		case "Int16":
			return int16(signedValue), nil
		case "Int32":
			return int32(signedValue), nil
		case "Int64":
			return signedValue, nil
		case "UInt8":
			return uint8(i.abs), nil
		case "UInt16":
			return uint16(i.abs), nil
		case "UInt32":
			return uint32(i.abs), nil
		default:
			return i.abs, nil
		}
	}
}

func convertBool(_ *Mapping, v any) (any, error) {
	i, err := toInteger(v)
	if err != nil {
		return nil, err
	}
	return i.abs != 0, nil
}

func convertFloat32(_ *Mapping, v any) (any, error) {
	switch x := v.(type) {
	case float32:
		return x, nil
	case float64:
		return float32(x), nil
	}
	return nil, fmt.Errorf("want float32 or float64, got %T", v)
}

func convertFloat64(_ *Mapping, v any) (any, error) {
	switch x := v.(type) {
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	}
	return nil, fmt.Errorf("want float32 or float64, got %T", v)
}

// convertDecimal never goes through floating point: MySQL DECIMAL is exact and so is the result.
func convertDecimal(m *Mapping, v any) (any, error) {
	var d decimal.Decimal
	switch x := v.(type) {
	case decimal.Decimal:
		d = x
	case string:
		parsed, err := decimal.NewFromString(x)
		if err != nil {
			return nil, fmt.Errorf("invalid decimal %q", x)
		}
		d = parsed
	case []byte:
		parsed, err := decimal.NewFromString(string(x))
		if err != nil {
			return nil, fmt.Errorf("invalid decimal %q", x)
		}
		d = parsed
	default:
		return nil, fmt.Errorf("want decimal text or decimal.Decimal, got %T", v)
	}

	col := m.Column
	if !d.Equal(d.Truncate(int32(col.Scale))) {
		return nil, fmt.Errorf("decimal %s has more than %d fractional digits", d, col.Scale)
	}
	intDigits := 0
	if whole := d.Abs().Truncate(0).String(); whole != "0" {
		intDigits = len(whole)
	}
	if intDigits > col.Precision-col.Scale {
		return nil, fmt.Errorf("decimal %s does not fit DECIMAL(%d,%d)", d, col.Precision, col.Scale)
	}
	return d, nil
}

func convertYear(_ *Mapping, v any) (any, error) {
	i, err := toInteger(v)
	if err != nil {
		return nil, err
	}
	// MySQL YEAR is 1901-2155, or 0 for the zero year.
	if i.neg || (i.abs != 0 && (i.abs < 1901 || i.abs > 2155)) {
		return nil, fmt.Errorf("year %s out of MySQL YEAR range", i)
	}
	return uint16(i.abs), nil
}

func convertBit(m *Mapping, v any) (any, error) {
	var value uint64
	switch x := v.(type) {
	case []byte:
		if len(x) > 8 {
			return nil, fmt.Errorf("BIT value has %d bytes, want at most 8", len(x))
		}
		var buf [8]byte
		copy(buf[8-len(x):], x)
		value = binary.BigEndian.Uint64(buf[:])
	default:
		i, err := toInteger(v)
		if err != nil {
			return nil, fmt.Errorf("want an integer or []byte, got %T", v)
		}
		if i.neg {
			return nil, fmt.Errorf("negative BIT value %s", i)
		}
		value = i.abs
	}
	if m.Column.Length < 64 && value>>uint(m.Column.Length) != 0 {
		return nil, fmt.Errorf("value %d does not fit BIT(%d)", value, m.Column.Length)
	}
	return value, nil
}
