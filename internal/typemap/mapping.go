package typemap

import (
	"errors"
	"fmt"
	"strings"
)

// Mapping is the resolved ClickHouse representation of one source column.
type Mapping struct {
	Table  string
	Column Column
	// Type is the full ClickHouse column type, including Nullable/LowCardinality wrappers.
	Type string

	convert  converter
	reporter Reporter
}

// converter turns a non-nil source value into a ClickHouse driver value.
type converter func(m *Mapping, v any) (any, error)

// Options tune how a column is resolved.
type Options struct {
	// Override is a ClickHouse base type (without Nullable) from the table configuration.
	Override string
	// Reporter receives value adjustments; nil discards them.
	Reporter Reporter
}

// Resolve picks the ClickHouse type for a column and prepares its value converter.
// It fails for unsupported types, invalid column metadata and incompatible overrides.
func Resolve(table string, col Column, opts Options) (*Mapping, error) {
	m := &Mapping{Table: table, Column: col, reporter: opts.Reporter}
	if m.reporter == nil {
		m.reporter = nopReporter{}
	}

	if err := resolve(m, strings.TrimSpace(opts.Override)); err != nil {
		return nil, fmt.Errorf("map %s.%s (%s): %w", table, col.Name, col.SQLType(), err)
	}
	return m, nil
}

// Convert turns a source value into a value accepted by the ClickHouse driver for m.Type.
//
// Accepted inputs (the source normalizes binlog values to these):
//   - integers: any Go integer type, with the correct sign (never two's complement of unsigned);
//   - FLOAT/DOUBLE: float32, float64;
//   - DECIMAL: string, []byte or decimal.Decimal (never floats);
//   - DATE, DATETIME, TIMESTAMP: MySQL text ("2006-01-02", "2006-01-02 15:04:05.999999") or time.Time;
//     TIMESTAMP text is UTC, DATETIME is written as is;
//   - TIME: MySQL text ("-838:59:59.000000") or time.Duration;
//   - strings and binary: string or []byte with raw bytes in the column character set;
//   - ENUM: 1-based index (0 is MySQL's invalid empty value) or member string; SET: bitmask or "a,b";
//   - BIT: integer or big-endian []byte;
//   - nil for NULL.
func (m *Mapping) Convert(v any) (any, error) {
	if v == nil {
		return m.convertNull()
	}
	out, err := m.convert(m, v)
	if err != nil {
		return nil, m.fail(v, err)
	}
	return out, nil
}

func (m *Mapping) convertNull() (any, error) {
	if !m.Column.Nullable {
		return nil, m.fail(nil, errors.New("NULL in a NOT NULL column"))
	}
	if m.Column.Kind == KindSet && strings.HasPrefix(m.Type, "Array(") {
		m.report(AdjustNullSet, "NULL", "[]")
		return []string{}, nil
	}
	return nil, nil
}

func (m *Mapping) fail(v any, err error) error {
	return &ConversionError{
		Table:      m.Table,
		Column:     m.Column.Name,
		SourceType: m.Column.SQLType(),
		TargetType: m.Type,
		Value:      v,
		Err:        err,
	}
}

func (m *Mapping) report(kind AdjustmentKind, original, replacement string) {
	m.reporter.Report(Adjustment{
		Table:       m.Table,
		Column:      m.Column.Name,
		Kind:        kind,
		Original:    original,
		Replacement: replacement,
	})
}

func resolve(m *Mapping, override string) error {
	col := m.Column

	base, conv, err := defaultMapping(col)
	if err != nil {
		return err
	}

	if override != "" && override != base {
		base, conv, err = overrideMapping(col, override)
		if err != nil {
			return err
		}
	}

	m.Type = wrapNullable(col, base)
	m.convert = conv
	return nil
}

var integerTypes = map[Kind][2]string{
	KindTinyInt:   {"Int8", "UInt8"},
	KindSmallInt:  {"Int16", "UInt16"},
	KindMediumInt: {"Int32", "UInt32"},
	KindInt:       {"Int32", "UInt32"},
	KindBigInt:    {"Int64", "UInt64"},
}

func defaultMapping(col Column) (string, converter, error) {
	switch col.Kind {
	case KindTinyInt, KindSmallInt, KindMediumInt, KindInt, KindBigInt:
		name := integerTypes[col.Kind][0]
		if col.Unsigned {
			name = integerTypes[col.Kind][1]
		}
		return name, integerConverter(name), nil

	case KindFloat:
		return "Float32", convertFloat32, nil
	case KindDouble:
		return "Float64", convertFloat64, nil

	case KindDecimal:
		if col.Precision < 1 || col.Precision > 65 || col.Scale < 0 || col.Scale > 30 || col.Scale > col.Precision {
			return "", nil, fmt.Errorf("invalid DECIMAL precision/scale (%d,%d)", col.Precision, col.Scale)
		}
		return fmt.Sprintf("Decimal(%d, %d)", col.Precision, col.Scale), convertDecimal, nil

	case KindYear:
		return "UInt16", convertYear, nil

	case KindDate:
		return "Date32", convertDate, nil

	case KindDateTime, KindTimestamp:
		if col.FSP < 0 || col.FSP > 6 {
			return "", nil, fmt.Errorf("invalid fractional seconds precision %d", col.FSP)
		}
		return fmt.Sprintf("DateTime64(%d, 'UTC')", col.FSP), convertDateTime, nil

	case KindTime:
		return "Int64", convertTime, nil

	case KindChar, KindVarChar, KindText:
		tc, err := newTranscoder(col.Charset)
		if err != nil {
			return "", nil, err
		}
		return "String", textConverter(tc), nil

	case KindBinary:
		if col.Length == 0 {
			return "String", convertBytes, nil
		}
		if col.Length < 0 || col.Length > 255 {
			return "", nil, fmt.Errorf("invalid BINARY length %d", col.Length)
		}
		return fmt.Sprintf("FixedString(%d)", col.Length), convertFixedBinary, nil

	case KindVarBinary, KindBlob, KindGeometry:
		return "String", convertBytes, nil

	case KindJSON:
		return "String", convertBytes, nil

	case KindEnum:
		return "LowCardinality(String)", convertEnum, nil

	case KindSet:
		if len(col.Values) > 64 {
			return "", nil, fmt.Errorf("SET has %d members, MySQL allows at most 64", len(col.Values))
		}
		return "Array(LowCardinality(String))", convertSetArray, nil

	case KindBit:
		if col.Length < 1 || col.Length > 64 {
			return "", nil, fmt.Errorf("invalid BIT length %d", col.Length)
		}
		return "UInt64", convertBit, nil
	}

	return "", nil, fmt.Errorf("%w: column kind %s", ErrUnsupported, col.Kind)
}

// overrideMapping validates a configured type against the source column. Only conversions
// that cannot lose or corrupt data are allowed.
func overrideMapping(col Column, override string) (string, converter, error) {
	incompatible := fmt.Errorf("%w: override %q is not compatible with %s", ErrUnsupported, override, col.Kind)

	switch {
	case override == "Bool":
		if !col.Kind.isInteger() {
			return "", nil, incompatible
		}
		return override, convertBool, nil

	case override == "String" || override == "LowCardinality(String)":
		switch {
		case col.Kind.isText():
			tc, err := newTranscoder(col.Charset)
			if err != nil {
				return "", nil, err
			}
			return override, textConverter(tc), nil
		case col.Kind == KindEnum:
			return override, convertEnum, nil
		case col.Kind == KindSet && override == "String":
			return override, convertSetString, nil
		case col.Kind == KindJSON && override == "String":
			return override, convertBytes, nil
		}
		return "", nil, incompatible

	case override == "JSON":
		if col.Kind != KindJSON {
			return "", nil, incompatible
		}
		if col.Nullable {
			return "", nil, fmt.Errorf("%w: ClickHouse JSON cannot be Nullable; keep String for nullable JSON columns", ErrUnsupported)
		}
		return override, convertBytes, nil

	case strings.HasPrefix(override, "Enum8(") || strings.HasPrefix(override, "Enum16("):
		if col.Kind != KindEnum {
			return "", nil, incompatible
		}
		members, err := parseEnumType(override)
		if err != nil {
			return "", nil, fmt.Errorf("override %q: %w", override, err)
		}
		for _, v := range col.Values {
			if _, ok := members[v]; !ok {
				return "", nil, fmt.Errorf("override %q is missing ENUM member %q", override, v)
			}
		}
		return override, enumOverrideConverter(members), nil
	}

	return "", nil, incompatible
}

// wrapNullable applies Nullable in the position ClickHouse accepts.
func wrapNullable(col Column, base string) string {
	if !col.Nullable {
		return base
	}
	switch {
	case strings.HasPrefix(base, "Array("):
		// Arrays cannot be Nullable; NULL is written as an empty array and reported.
		return base
	case strings.HasPrefix(base, "LowCardinality("):
		inner := strings.TrimSuffix(strings.TrimPrefix(base, "LowCardinality("), ")")
		return "LowCardinality(Nullable(" + inner + "))"
	}
	return "Nullable(" + base + ")"
}
