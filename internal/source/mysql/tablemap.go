package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"unicode/utf8"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/typemap"
)

// binaryCollation is the collation id MySQL assigns to binary strings (BINARY, VARBINARY, BLOB).
const binaryCollation = 63

// collation describes a character set behind a collation id.
type collation struct {
	charset string
	maxLen  int // maximum bytes per character
}

// collations resolves the collation ids found in binlog table metadata.
type collations map[uint64]collation

// loadCollations reads the id to character set mapping from the server itself, so it is exact
// for every MySQL flavor and version instead of a table that could go stale.
func loadCollations(ctx context.Context, db *sql.DB) (collations, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.ID, c.CHARACTER_SET_NAME, s.MAXLEN
		FROM information_schema.COLLATIONS c
		JOIN information_schema.CHARACTER_SETS s ON s.CHARACTER_SET_NAME = c.CHARACTER_SET_NAME`)
	if err != nil {
		return nil, fmt.Errorf("load collations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(collations)
	for rows.Next() {
		var (
			id     uint64
			cs     string
			maxLen int
		)
		if err := rows.Scan(&id, &cs, &maxLen); err != nil {
			return nil, fmt.Errorf("load collations: %w", err)
		}
		out[id] = collation{charset: cs, maxLen: maxLen}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load collations: %w", err)
	}
	return out, nil
}

// tableFromMap builds the table description from a TABLE_MAP event. It relies on
// binlog_row_metadata=FULL for names, signedness, character sets and ENUM/SET members.
func tableFromMap(ev *replication.TableMapEvent, cs collations) (*schema.Table, error) {
	name := string(ev.Schema) + "." + string(ev.Table)
	n := int(ev.ColumnCount)
	if n == 0 {
		return nil, fmt.Errorf("table %s: the binlog describes no columns", name)
	}
	if len(ev.ColumnName) != n || len(ev.ColumnType) != n || len(ev.ColumnMeta) != n {
		return nil, fmt.Errorf("table %s: the binlog has no column metadata; binlog_row_metadata must be FULL", name)
	}

	if len(ev.NullBitmap) < (n+7)/8 {
		return nil, fmt.Errorf("table %s: null bitmap has %d bytes for %d columns", name, len(ev.NullBitmap), n)
	}
	collationIDs, err := columnCollations(ev)
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", name, err)
	}

	b := &columnBuilder{
		ev:         ev,
		cs:         cs,
		names:      ev.ColumnNameString(),
		unsigned:   ev.UnsignedMap(),
		collations: collationIDs,
		enums:      ev.EnumStrValueString(),
		sets:       ev.SetStrValueString(),
	}

	tbl := &schema.Table{Database: string(ev.Schema), Name: string(ev.Table), Columns: make([]typemap.Column, n)}
	for i := range n {
		col, err := b.column(i)
		if err != nil {
			return nil, fmt.Errorf("table %s: column %q: %w", name, b.names[i], err)
		}
		tbl.Columns[i] = col
	}

	for _, idx := range ev.PrimaryKey {
		if idx >= uint64(n) {
			return nil, fmt.Errorf("table %s: primary key refers to column %d of %d", name, idx, n)
		}
		tbl.PrimaryKey = append(tbl.PrimaryKey, int(idx))
	}
	return tbl, nil
}

// columnCollations maps character column indexes to collation ids. go-mysql offers the same
// through TableMapEvent.CollationMap, but that panics on inconsistent metadata, and a corrupt
// event must produce an error instead.
//
// With a default collation, DefaultCharset is [default, (character column ordinal, collation)...];
// otherwise ColumnCharset lists one collation per character column.
func columnCollations(ev *replication.TableMapEvent) (map[int]uint64, error) {
	var (
		defaultID uint64
		overrides map[int]uint64
	)
	switch {
	case len(ev.DefaultCharset) > 0:
		defaultID = ev.DefaultCharset[0]
		pairs := ev.DefaultCharset[1:]
		if len(pairs)%2 != 0 {
			return nil, errors.New("malformed default character set metadata")
		}
		overrides = make(map[int]uint64, len(pairs)/2)
		for i := 0; i < len(pairs); i += 2 {
			overrides[int(pairs[i])] = pairs[i+1]
		}
	case len(ev.ColumnCharset) > 0:
	default:
		return nil, nil
	}

	out := make(map[int]uint64)
	ordinal := 0
	for i := range int(ev.ColumnCount) {
		if !ev.IsCharacterColumn(i) {
			continue
		}
		switch {
		case len(ev.DefaultCharset) > 0:
			id, ok := overrides[ordinal]
			if !ok {
				id = defaultID
			}
			out[i] = id
		case ordinal < len(ev.ColumnCharset):
			out[i] = ev.ColumnCharset[ordinal]
		default:
			return nil, fmt.Errorf("character set metadata covers %d of the character columns", len(ev.ColumnCharset))
		}
		ordinal++
	}
	return out, nil
}

type columnBuilder struct {
	ev         *replication.TableMapEvent
	cs         collations
	names      []string
	unsigned   map[int]bool
	collations map[int]uint64
	enums      [][]string
	sets       [][]string

	nextEnum, nextSet int
}

// column describes column i. ENUM and SET columns must be visited in order, because their
// members are stored as parallel lists.
func (b *columnBuilder) column(i int) (typemap.Column, error) {
	ev := b.ev
	meta := ev.ColumnMeta[i]
	col := typemap.Column{Name: b.names[i]}
	if avail, nullable := ev.Nullable(i); avail {
		col.Nullable = nullable
	}

	typ := ev.ColumnType[i]
	length := 0
	if typ == gomysql.MYSQL_TYPE_STRING {
		typ, length = stringMeta(meta)
	}

	switch typ {
	case gomysql.MYSQL_TYPE_TINY:
		col.Kind = typemap.KindTinyInt
	case gomysql.MYSQL_TYPE_SHORT:
		col.Kind = typemap.KindSmallInt
	case gomysql.MYSQL_TYPE_INT24:
		col.Kind = typemap.KindMediumInt
	case gomysql.MYSQL_TYPE_LONG:
		col.Kind = typemap.KindInt
	case gomysql.MYSQL_TYPE_LONGLONG:
		col.Kind = typemap.KindBigInt
	case gomysql.MYSQL_TYPE_FLOAT:
		col.Kind = typemap.KindFloat
	case gomysql.MYSQL_TYPE_DOUBLE:
		col.Kind = typemap.KindDouble
	case gomysql.MYSQL_TYPE_NEWDECIMAL:
		col.Kind, col.Precision, col.Scale = typemap.KindDecimal, int(meta>>8), int(meta&0xff)
	case gomysql.MYSQL_TYPE_YEAR:
		col.Kind = typemap.KindYear
	case gomysql.MYSQL_TYPE_DATE, gomysql.MYSQL_TYPE_NEWDATE:
		col.Kind = typemap.KindDate
	case gomysql.MYSQL_TYPE_DATETIME2:
		col.Kind, col.FSP = typemap.KindDateTime, int(meta)
	case gomysql.MYSQL_TYPE_DATETIME:
		col.Kind = typemap.KindDateTime
	case gomysql.MYSQL_TYPE_TIMESTAMP2:
		col.Kind, col.FSP = typemap.KindTimestamp, int(meta)
	case gomysql.MYSQL_TYPE_TIMESTAMP:
		col.Kind = typemap.KindTimestamp
	case gomysql.MYSQL_TYPE_TIME2:
		col.Kind, col.FSP = typemap.KindTime, int(meta)
	case gomysql.MYSQL_TYPE_TIME:
		col.Kind = typemap.KindTime
	case gomysql.MYSQL_TYPE_BIT:
		col.Kind, col.Length = typemap.KindBit, int(meta>>8)*8+int(meta&0xff)
	case gomysql.MYSQL_TYPE_JSON:
		col.Kind = typemap.KindJSON
	case gomysql.MYSQL_TYPE_GEOMETRY:
		col.Kind = typemap.KindGeometry
	case gomysql.MYSQL_TYPE_ENUM:
		values, err := b.members("ENUM", b.enums, &b.nextEnum)
		if err != nil {
			return col, err
		}
		col.Kind, col.Values = typemap.KindEnum, values
	case gomysql.MYSQL_TYPE_SET:
		values, err := b.members("SET", b.sets, &b.nextSet)
		if err != nil {
			return col, err
		}
		col.Kind, col.Values = typemap.KindSet, values
	case gomysql.MYSQL_TYPE_VARCHAR, gomysql.MYSQL_TYPE_VAR_STRING:
		return b.stringColumn(col, i, typemap.KindVarChar, typemap.KindVarBinary, int(meta))
	case gomysql.MYSQL_TYPE_STRING:
		return b.stringColumn(col, i, typemap.KindChar, typemap.KindBinary, length)
	case gomysql.MYSQL_TYPE_BLOB, gomysql.MYSQL_TYPE_TINY_BLOB, gomysql.MYSQL_TYPE_MEDIUM_BLOB, gomysql.MYSQL_TYPE_LONG_BLOB:
		return b.stringColumn(col, i, typemap.KindText, typemap.KindBlob, 0)
	default:
		return col, fmt.Errorf("%w: binlog column type %d", typemap.ErrUnsupported, typ)
	}

	if col.Kind >= typemap.KindTinyInt && col.Kind <= typemap.KindBigInt {
		col.Unsigned = b.unsigned[i]
	}
	return col, nil
}

// stringColumn resolves the text/binary pair: the binlog uses one wire type for both, and only
// the binary collation tells them apart. bytes is the declared size in bytes, 0 for BLOB/TEXT.
func (b *columnBuilder) stringColumn(col typemap.Column, i int, textKind, binaryKind typemap.Kind, bytes int) (typemap.Column, error) {
	id, ok := b.collations[i]
	if !ok {
		return col, errors.New("the binlog has no character set for this column; binlog_row_metadata must be FULL")
	}

	if id == binaryCollation {
		col.Kind, col.Length = binaryKind, bytes
		return col, nil
	}

	c, ok := b.cs[id]
	if !ok {
		return col, fmt.Errorf("unknown collation id %d", id)
	}
	col.Kind, col.Charset = textKind, c.charset
	if bytes > 0 && c.maxLen > 0 {
		col.Length = bytes / c.maxLen
	}
	return col, nil
}

// members returns the ENUM or SET members of the next such column.
func (b *columnBuilder) members(kind string, all [][]string, next *int) ([]string, error) {
	if *next >= len(all) {
		return nil, fmt.Errorf("the binlog has no %s members; binlog_row_metadata must be FULL", kind)
	}
	values := all[*next]
	*next++

	for _, v := range values {
		if !utf8.ValidString(v) {
			// Member names are raw bytes in the column character set, which the binlog does not
			// name for ENUM/SET here. Refusing beats silently writing mangled names.
			return nil, fmt.Errorf("%s member %q is not valid UTF-8; non-UTF-8 member names are not supported", kind, v)
		}
	}
	return values, nil
}

// stringMeta decodes the metadata of MYSQL_TYPE_STRING columns. The real type (STRING, ENUM,
// SET) and the byte length share two bytes, with the high bits of the length hidden in the type.
func stringMeta(meta uint16) (realType byte, length int) {
	if meta < 256 {
		return gomysql.MYSQL_TYPE_STRING, int(meta)
	}
	b0, b1 := byte(meta>>8), byte(meta&0xff)
	if b0&0x30 != 0x30 {
		return b0 | 0x30, int(uint16(b1) | uint16((b0&0x30)^0x30)<<4)
	}
	return b0, int(b1)
}
