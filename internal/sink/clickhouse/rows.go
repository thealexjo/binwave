package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/thealexjo/binwave/internal/event"
)

// Row is one ClickHouse row: the converted replicated column values plus the service columns
// that are filled in when the row is inserted.
type Row struct {
	// Values follow Table.Columns.
	Values  []any
	Version uint64
	Deleted bool

	// key identifies the row within the sort key, to collapse repeated changes of one transaction.
	key string
}

// Rows converts a batch of changes of one transaction into ClickHouse rows.
//
//   - Insert and Update write the new row image.
//   - Delete writes the old image as a tombstone (_is_deleted = 1).
//   - An Update that changes the sort key also writes a tombstone for the old key, because
//     ReplacingMergeTree would otherwise keep the old row forever.
//
// version is the transaction's version (see Version). Conversion failures are typemap.ConversionError.
func (t *Table) Rows(batch event.Rows, version uint64) ([]Row, error) {
	out := make([]Row, 0, len(batch.Rows))
	for i, r := range batch.Rows {
		switch batch.Op {
		case event.Insert:
			row, err := t.convert(r.After, version, false)
			if err != nil {
				return nil, &RowError{Index: i, After: true, Err: err}
			}
			out = append(out, row)

		case event.Delete:
			row, err := t.convert(r.Before, version, true)
			if err != nil {
				return nil, &RowError{Index: i, Err: err}
			}
			out = append(out, row)

		case event.Update:
			if t.rawKey(r.Before) != t.rawKey(r.After) {
				old, err := t.convert(r.Before, version, true)
				if err != nil {
					return nil, &RowError{Index: i, Err: err}
				}
				out = append(out, old)
			}
			row, err := t.convert(r.After, version, false)
			if err != nil {
				return nil, &RowError{Index: i, After: true, Err: err}
			}
			out = append(out, row)

		default:
			return nil, fmt.Errorf("table %s: unsupported operation %s", t.Source.FullName(), batch.Op)
		}
	}
	return out, nil
}

func (t *Table) convert(image []any, version uint64, deleted bool) (Row, error) {
	if len(image) != len(t.Source.Columns) {
		return Row{}, fmt.Errorf("table %s: row has %d values for %d columns", t.Source.FullName(), len(image), len(t.Source.Columns))
	}
	values := make([]any, len(t.Columns))
	for i, c := range t.Columns {
		v, err := c.Mapping.Convert(image[c.SourceIndex])
		if err != nil {
			return Row{}, fmt.Errorf("table %s: %w", t.Source.FullName(), err)
		}
		values[i] = v
	}
	return Row{Values: values, Version: version, Deleted: deleted, key: t.rawKey(image)}, nil
}

// rawKey renders the sort key columns of a source row image as a comparable string.
func (t *Table) rawKey(image []any) string {
	var b strings.Builder
	for _, idx := range t.orderIdx {
		writeKeyPart(&b, image[idx])
		b.WriteByte(0)
	}
	return b.String()
}

func writeKeyPart(b *strings.Builder, v any) {
	switch x := v.(type) {
	case decimal.Decimal:
		b.WriteString(x.String()) // equal values render alike whatever their internal exponent
	case time.Time:
		fmt.Fprintf(b, "%d", x.UnixNano())
	case []byte:
		fmt.Fprintf(b, "b%x", x)
	default:
		fmt.Fprintf(b, "%T:%v", v, v)
	}
}

// Collapse keeps one row per key and version, the last one: several changes of one row inside a
// transaction only matter by their final state. Rows of different versions are all kept, since
// ClickHouse resolves them by version. Order is preserved.
func Collapse(rows []Row) []Row {
	type id struct {
		key     string
		version uint64
	}
	last := make(map[id]int, len(rows))
	for i, r := range rows {
		last[id{r.key, r.Version}] = i
	}
	if len(last) == len(rows) {
		return rows
	}
	out := make([]Row, 0, len(last))
	for i, r := range rows {
		if last[id{r.key, r.Version}] == i {
			out = append(out, r)
		}
	}
	return out
}

// RowError says which row of a batch could not be converted, and whether it was the row image
// after the change or the one before it; the pipeline uses it to name the key of the row.
type RowError struct {
	Index int
	After bool
	Err   error
}

func (e *RowError) Error() string { return e.Err.Error() }
func (e *RowError) Unwrap() error { return e.Err }
