package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/snapshot"
	"github.com/thealexjo/binwave/internal/typemap"
)

// The methods of this file read a table with SELECT, which is what a snapshot needs: the
// definition of a table, the rows of a chunk of it, and how far along the key it is. They only read: the
// sessions of the source are read-only.

// TableDefinition describes a table from information_schema in the form the binlog gives it, so that a
// row read with SELECT and a row read from the binlog are the same to the pipeline: the same
// columns, the same types, the same primary key.
//
// A table without a primary key, or with a key on a prefix of a column, cannot be read in chunks by
// key, and is an error.
func (s *Source) TableDefinition(ctx context.Context, database, table string) (*schema.Table, error) {
	if s.db == nil {
		return nil, errors.New("source is not open")
	}
	tbl := &schema.Table{Database: database, Name: table}
	err := s.query(ctx, s.readDB(), `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, CHARACTER_SET_NAME, CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, EXTRA
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
		[]any{database, table}, func(rows *sql.Rows) error {
			var (
				name, dataType, columnType, nullable, extra string
				charset                                     sql.NullString
				charLen, precision, scale, fsp              sql.NullInt64
			)
			if err := rows.Scan(&name, &dataType, &columnType, &nullable, &charset, &charLen, &precision, &scale, &fsp, &extra); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			col, err := columnOf(name, dataType, columnType, nullable == "YES", charset.String, int(charLen.Int64), int(precision.Int64), int(scale.Int64), int(fsp.Int64))
			if err != nil {
				return fmt.Errorf("column %q: %w", name, err)
			}
			tbl.Columns = append(tbl.Columns, col)
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("read the definition: %w", err)
	}
	if len(tbl.Columns) == 0 {
		return nil, errors.New("does not exist or is not visible to the user")
	}

	err = s.query(ctx, s.readDB(), `SELECT COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX`,
		[]any{database, table}, func(rows *sql.Rows) error {
			var name string
			var sub sql.NullInt64
			if err := rows.Scan(&name, &sub); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			if sub.Valid {
				return fmt.Errorf("the primary key is on a prefix of column %q, which cannot be read in chunks by key", name)
			}
			idx := tbl.ColumnIndex(name)
			if idx < 0 {
				return fmt.Errorf("the primary key names column %q, which is not one of its columns", name)
			}
			tbl.PrimaryKey = append(tbl.PrimaryKey, idx)
			return nil
		})
	if err != nil {
		return nil, err
	}
	if len(tbl.PrimaryKey) == 0 {
		return nil, errors.New("no primary key, so it cannot be read in chunks")
	}
	for _, idx := range tbl.PrimaryKey {
		switch k := tbl.Columns[idx].Kind; k {
		case typemap.KindFloat, typemap.KindDouble, typemap.KindEnum, typemap.KindSet, typemap.KindBit, typemap.KindJSON, typemap.KindGeometry:
			return nil, fmt.Errorf("the primary key has a column of type %s, which cannot be compared exactly to continue a read from where it stopped", k)
		}
	}
	if err := s.readPartitions(ctx, tbl); err != nil {
		return nil, err
	}
	return tbl, nil
}

// readPartitions fills tbl.Partitions with the leaf partitions of the table, in the order it declares
// them. A table that is not partitioned has one row with no partition name, and stays empty.
func (s *Source) readPartitions(ctx context.Context, tbl *schema.Table) error {
	err := s.query(ctx, s.readDB(), `SELECT PARTITION_NAME, SUBPARTITION_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION`,
		[]any{tbl.Database, tbl.Name}, func(rows *sql.Rows) error {
			var part, sub sql.NullString
			if err := rows.Scan(&part, &sub); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			switch {
			case sub.Valid && sub.String != "":
				// Subpartitions are the leaves: a chunk is read from one of them, and FROM t
				// PARTITION (name) takes a subpartition name just as well.
				tbl.Partitions = append(tbl.Partitions, sub.String)
			case part.Valid && part.String != "":
				tbl.Partitions = append(tbl.Partitions, part.String)
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("read the partitions of %s.%s: %w", tbl.Database, tbl.Name, err)
	}
	return nil
}

// columnOf maps a column of information_schema to the column the binlog would describe.
func columnOf(name, dataType, columnType string, nullable bool, charset string, charLen, precision, scale, fsp int) (typemap.Column, error) {
	col := typemap.Column{Name: name, Nullable: nullable}
	switch strings.ToLower(dataType) {
	case "tinyint":
		col.Kind = typemap.KindTinyInt
	case "smallint":
		col.Kind = typemap.KindSmallInt
	case "mediumint":
		col.Kind = typemap.KindMediumInt
	case "int":
		col.Kind = typemap.KindInt
	case "bigint":
		col.Kind = typemap.KindBigInt
	case "float":
		col.Kind = typemap.KindFloat
	case "double":
		col.Kind = typemap.KindDouble
	case "decimal":
		col.Kind, col.Precision, col.Scale = typemap.KindDecimal, precision, scale
	case "year":
		col.Kind = typemap.KindYear
	case "date":
		col.Kind = typemap.KindDate
	case "datetime":
		col.Kind, col.FSP = typemap.KindDateTime, fsp
	case "timestamp":
		col.Kind, col.FSP = typemap.KindTimestamp, fsp
	case "time":
		col.Kind, col.FSP = typemap.KindTime, fsp
	case "bit":
		col.Kind, col.Length = typemap.KindBit, precision
	case "json":
		col.Kind = typemap.KindJSON
	case "char":
		col.Kind, col.Charset, col.Length = typemap.KindChar, charset, charLen
	case "varchar":
		col.Kind, col.Charset, col.Length = typemap.KindVarChar, charset, charLen
	case "tinytext", "text", "mediumtext", "longtext":
		col.Kind, col.Charset = typemap.KindText, charset
	case "binary":
		col.Kind, col.Length = typemap.KindBinary, charLen
	case "varbinary":
		col.Kind, col.Length = typemap.KindVarBinary, charLen
	case "tinyblob", "blob", "mediumblob", "longblob":
		col.Kind = typemap.KindBlob
	case "enum", "set":
		col.Kind = typemap.KindEnum
		if strings.EqualFold(dataType, "set") {
			col.Kind = typemap.KindSet
		}
		members, err := members(columnType)
		if err != nil {
			return col, err
		}
		col.Values = members
	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
		col.Kind = typemap.KindGeometry
	default:
		return col, fmt.Errorf("%w: column type %s", typemap.ErrUnsupported, dataType)
	}
	if col.Kind >= typemap.KindTinyInt && col.Kind <= typemap.KindBigInt {
		col.Unsigned = strings.Contains(strings.ToLower(columnType), "unsigned")
	}
	return col, nil
}

// members reads the members of enum('a','b') or set('a','b'); a quote inside a member is doubled.
func members(columnType string) ([]string, error) {
	open := strings.IndexByte(columnType, '(')
	if open < 0 || !strings.HasSuffix(columnType, ")") {
		return nil, fmt.Errorf("cannot read the members of %q", columnType)
	}
	body := columnType[open+1 : len(columnType)-1]
	var out []string
	for i := 0; i < len(body); {
		if body[i] != '\'' {
			return nil, fmt.Errorf("cannot read the members of %q", columnType)
		}
		var b strings.Builder
		i++
		for ; i < len(body); i++ {
			if body[i] == '\'' {
				if i+1 < len(body) && body[i+1] == '\'' {
					b.WriteByte('\'')
					i++
					continue
				}
				break
			}
			b.WriteByte(body[i])
		}
		if i >= len(body) {
			return nil, fmt.Errorf("cannot read the members of %q", columnType)
		}
		i++ // the closing quote
		out = append(out, b.String())
		if i < len(body) {
			if body[i] != ',' {
				return nil, fmt.Errorf("cannot read the members of %q", columnType)
			}
			i++
		}
	}
	return out, nil
}

func queryEach(ctx context.Context, stmt *sql.Stmt, args []any, each func(*sql.Rows) error) error {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if err := each(rows); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	return nil
}

func quoteIdent(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// chunkStatement renders the statement that reads one chunk of a table: every column, in primary key
// order, past the cursor when there is one, with the size of the chunk as a parameter. A partition
// restricts the read to it (FROM t PARTITION (p)), which is what makes the cost of a chunk of a
// partitioned table the cost of one local index range instead of a merge of every partition. The text
// depends on the table, the partition and whether there is a cursor, and on nothing else, so it can be
// prepared once and kept (stmtCache).
func chunkStatement(tbl *schema.Table, partition string, withCursor bool) string {
	cols := make([]string, len(tbl.Columns))
	for i, c := range tbl.Columns {
		cols[i] = quoteIdent(c.Name)
	}
	keys := tbl.PrimaryKeyColumns()
	quoted := make([]string, len(keys))
	for i, k := range keys {
		quoted[i] = quoteIdent(k)
	}
	stmt := "SELECT " + strings.Join(cols, ", ") + " FROM " + quoteIdent(tbl.Database) + "." + quoteIdent(tbl.Name)
	if partition != "" {
		stmt += " PARTITION (" + quoteIdent(partition) + ")"
	}
	if withCursor {
		stmt += " WHERE " + keysetPredicate(quoted)
	}
	return stmt + " ORDER BY " + strings.Join(quoted, ", ") + " LIMIT ?"
}

// keysetPredicate renders "the key is past the cursor" in the form MySQL can start the index scan
// from. The tuple form ((a, b) > (?, ?)) reads as one comparison but is not turned into a range: on a
// table with a composite primary key EXPLAIN says "type: index", the whole index is scanned and the
// rows before the cursor are thrown away by the WHERE, so the cost of a chunk grows with how far the
// read has got. The expansion below is a range on the leading column, which is what stops that:
//
//	k1 > ? OR (k1 = ? AND (k2 > ? OR (k2 = ? AND k3 > ?)))
//
// The values go in the order keysetArgs renders them. The key columns are never NULL (a primary key
// column cannot be), so there is no three-valued logic to account for.
func keysetPredicate(quotedKeys []string) string {
	switch len(quotedKeys) {
	case 0:
		return ""
	case 1:
		return quotedKeys[0] + " > ?"
	default:
		k := quotedKeys[0]
		return "(" + k + " > ? OR (" + k + " = ? AND " + keysetPredicate(quotedKeys[1:]) + "))"
	}
}

// keysetArgs renders the cursor for keysetPredicate: every value but the last one twice, once for the
// "greater" comparison and once for the "equal" one. An empty cursor renders no arguments.
func keysetArgs(after []any) []any {
	if len(after) == 0 {
		return nil
	}
	args := make([]any, 0, 2*len(after))
	for i, v := range after {
		args = append(args, v)
		if i < len(after)-1 {
			args = append(args, v)
		}
	}
	return args
}

// ReadChunk reads up to req.Limit rows of the table in primary key order, starting after the key
// req.After (nil: from the start), from the partition req.Partition when it names one. Every row is a
// slice of raw source values in the forms typemap converts, one per column of the table. The second
// result is a measure of the size of the rows, in bytes, for the caller that sizes the next chunk.
//
// The read is one statement in autocommit, not a transaction: the rows are the state of the
// table at the moment each was read, which is all the snapshot algorithm asks for (see the
// watermarks in package snapshot).
func (s *Source) ReadChunk(ctx context.Context, req snapshot.ChunkRequest) (rows [][]any, size int, err error) {
	if s.db == nil {
		return nil, 0, errors.New("source is not open")
	}
	tbl, after := req.Table, req.After
	keys := tbl.PrimaryKeyColumns()
	if after != nil && len(after) != len(keys) {
		return nil, 0, fmt.Errorf("the cursor has %d values, the primary key %d columns", len(after), len(keys))
	}
	if req.Partition != "" && !slices.Contains(tbl.Partitions, req.Partition) {
		// The partition goes into the statement as an identifier, so it is never anything but a name
		// the table itself declares.
		return nil, 0, fmt.Errorf("table %s.%s has no partition %q", tbl.Database, tbl.Name, req.Partition)
	}
	stmt := chunkStatement(tbl, req.Partition, after != nil)
	args := append(keysetArgs(after), uint64(req.Limit))

	// Always through a prepared statement: the server sends the rows of one in the binary protocol,
	// which gives a FLOAT as the float it is, where the text protocol rounds it to six digits, and
	// both must not be mixed in one snapshot. The size of the chunk is a parameter too, so that the
	// statement of a table does not change with it and can be kept ready (stmtCache).
	db := s.readDB()
	prepared, err := s.chunkStmts.get(stmt, func() (*sql.Stmt, error) { return db.PrepareContext(ctx, stmt) })
	if err != nil {
		if partitionGone(err) {
			return nil, 0, partitionGoneError(req, err)
		}
		return nil, 0, fmt.Errorf("prepare a chunk of %s.%s: %w", tbl.Database, tbl.Name, err)
	}
	cols := tbl.Columns
	err = queryEach(ctx, prepared, args, func(r *sql.Rows) error {
		values := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range dest {
			dest[i] = &values[i]
		}
		if err := r.Scan(dest...); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		for i, v := range values {
			normalized, n, err := normalize(cols[i], v)
			if err != nil {
				return fmt.Errorf("column %q: %w", tbl.Columns[i].Name, err)
			}
			values[i] = normalized
			size += n
		}
		rows = append(rows, values)
		return nil
	})
	if err != nil {
		if partitionGone(err) {
			return nil, 0, partitionGoneError(req, err)
		}
		return nil, 0, fmt.Errorf("read a chunk of %s.%s: %w", tbl.Database, tbl.Name, err)
	}
	return rows, size, nil
}

// partitionGoneError reports a partition that was dropped or reorganized while the table was being
// read, in the form the reader recognizes to move on to the next one (snapshot.ErrPartitionGone). The
// server answers this way both when the statement is prepared and when it runs, because the name is
// part of the statement.
func partitionGoneError(req snapshot.ChunkRequest, err error) error {
	return fmt.Errorf("read a chunk of %s.%s partition %s: %w: %w",
		req.Table.Database, req.Table.Name, req.Partition, snapshot.ErrPartitionGone, err)
}

// KeyRange returns the smallest and the largest key of a table with a single integer primary key; ok
// is false for any other key, or a key that does not fit an int64. It reads two entries of the index.
func (s *Source) KeyRange(ctx context.Context, tbl *schema.Table) (lo, hi int64, ok bool, err error) {
	if s.db == nil {
		return 0, 0, false, errors.New("source is not open")
	}
	if len(tbl.PrimaryKey) != 1 {
		return 0, 0, false, nil
	}
	col := tbl.Columns[tbl.PrimaryKey[0]]
	if col.Kind < typemap.KindTinyInt || col.Kind > typemap.KindBigInt || (col.Kind == typemap.KindBigInt && col.Unsigned) {
		return 0, 0, false, nil
	}
	name := quoteIdent(col.Name)
	var smallest, largest sql.NullInt64
	err = s.readDB().QueryRowContext(ctx, "SELECT MIN("+name+"), MAX("+name+") FROM "+quoteIdent(tbl.Database)+"."+quoteIdent(tbl.Name)).Scan(&smallest, &largest)
	if err != nil {
		return 0, 0, false, fmt.Errorf("read the key range of %s.%s: %w", tbl.Database, tbl.Name, err)
	}
	if !smallest.Valid || !largest.Valid {
		return 0, 0, false, nil
	}
	return smallest.Int64, largest.Int64, true, nil
}

// EstimatedRows is the number of rows the server believes the table has (a statistic of InnoDB, off by
// tens of percent, but free).
func (s *Source) EstimatedRows(ctx context.Context, tbl *schema.Table) (int64, error) {
	if s.db == nil {
		return 0, errors.New("source is not open")
	}
	var n sql.NullInt64
	err := s.readDB().QueryRowContext(ctx, "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", tbl.Database, tbl.Name).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("read the size of %s.%s: %w", tbl.Database, tbl.Name, err)
	}
	return n.Int64, nil
}

// normalize turns what the driver returns into the form the binlog gives typemap, and reports the size
// of the value in bytes.
func normalize(col typemap.Column, v any) (any, int, error) {
	switch x := v.(type) {
	case nil:
		return nil, 1, nil
	case []byte:
		if col.Kind >= typemap.KindTinyInt && col.Kind <= typemap.KindBigInt {
			// The driver returns an unsigned BIGINT above the largest int64 as its digits.
			n, err := strconv.ParseUint(string(x), 10, 64)
			if err != nil {
				return nil, 0, fmt.Errorf("an integer that MySQL returned does not parse: %w", err)
			}
			return n, 8, nil
		}
		if col.Kind == typemap.KindJSON {
			compact, err := compactJSON(x)
			if err != nil {
				return nil, 0, err
			}
			return compact, len(compact), nil
		}
		return x, len(x), nil
	case string:
		return x, len(x), nil
	default:
		return x, 8, nil
	}
}

func compactJSON(text []byte) (string, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, text); err != nil {
		return "", fmt.Errorf("a JSON value that MySQL returned does not parse: %w", err)
	}
	return buf.String(), nil
}
