package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/thealexjo/binwave/internal/schema"
)

// systemSchemas are never replicated.
const systemSchemas = "('mysql', 'sys', 'information_schema', 'performance_schema')"

// checkDatabases verifies that every database named literally in tables.include is visible to
// the user. The catalog shows a database only to an account that has some privilege on it, so a
// database that is missing here does not exist or is not granted to binwave's user; both mean
// the service cannot replicate what it was asked to.
func checkDatabases(ctx context.Context, db *sql.DB, user string, want []string) error {
	if len(want) == 0 {
		return nil
	}
	rows, err := db.QueryContext(ctx, "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA")
	if err != nil {
		return fmt.Errorf("list source databases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	visible := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("list source databases: %w", err)
		}
		visible[strings.ToLower(name)] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list source databases: %w", err)
	}

	var problems []string
	for _, name := range want {
		// Database names are case sensitive on some servers; the visibility check is not worth a false alarm.
		if !visible[strings.ToLower(name)] {
			problems = append(problems, fmt.Sprintf(
				"database %q of tables.include is not visible to user %q: it does not exist, or the user has no privileges on it (grant SELECT)", name, user))
		}
	}
	if len(problems) > 0 {
		return &RequirementsError{Problems: problems}
	}
	return nil
}

// Catalog reads the base tables binwave would replicate, with their columns, unique indexes and
// foreign keys, from information_schema. With table set only that table is read. The catalog
// lists just what the user may see: a table without any privilege for the user is absent.
func (s *Source) Catalog(ctx context.Context, database, table string) (*schema.Catalog, error) {
	if s.db == nil {
		return nil, errors.New("source is not open")
	}
	where := "TABLE_SCHEMA NOT IN " + systemSchemas
	fkWhere := "CONSTRAINT_SCHEMA NOT IN " + systemSchemas
	var args []any
	if table != "" {
		where += " AND TABLE_SCHEMA = ? AND TABLE_NAME = ?"
		fkWhere += " AND CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?"
		args = []any{database, table}
	}
	include := func(db, tbl string) bool { return s.opts.Include == nil || s.opts.Include(db, tbl) }

	type key struct{ db, table string }
	byName := map[key]*schema.CatalogTable{}
	var order []key

	err := s.query(ctx, s.db, "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND "+where, args,
		func(rows *sql.Rows) error {
			var db, tbl string
			var estimate sql.NullInt64 // the server has no statistics for a table it never opened
			if err := rows.Scan(&db, &tbl, &estimate); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			if include(db, tbl) {
				k := key{db, tbl}
				byName[k] = &schema.CatalogTable{Database: db, Name: tbl, EstimatedRows: estimate.Int64}
				order = append(order, k)
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("read source tables: %w", err)
	}

	err = s.query(ctx, s.db, "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS WHERE "+where+
		" ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION", args,
		func(rows *sql.Rows) error {
			var db, tbl, col string
			if err := rows.Scan(&db, &tbl, &col); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			if t := byName[key{db, tbl}]; t != nil {
				t.Columns = append(t.Columns, col)
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("read source columns: %w", err)
	}

	// Unique indexes, columns in index order. An expression in a functional index has no column name.
	type indexKey struct {
		key
		index string
	}
	indexes := map[indexKey]*schema.UniqueKey{}
	var indexOrder []indexKey
	err = s.query(ctx, s.db, "SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, COLUMN_NAME, NULLABLE FROM information_schema.STATISTICS "+
		"WHERE NON_UNIQUE = 0 AND "+where+" ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX", args,
		func(rows *sql.Rows) error {
			var db, tbl, index, nullable string
			var col sql.NullString
			if err := rows.Scan(&db, &tbl, &index, &col, &nullable); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			t := byName[key{db, tbl}]
			if t == nil {
				return nil
			}
			if index == "PRIMARY" {
				t.PrimaryKey = append(t.PrimaryKey, col.String)
				return nil
			}
			ik := indexKey{key{db, tbl}, index}
			u := indexes[ik]
			if u == nil {
				u = &schema.UniqueKey{Name: index}
				indexes[ik] = u
				indexOrder = append(indexOrder, ik)
			}
			u.Columns = append(u.Columns, col.String)
			u.Nullable = u.Nullable || !col.Valid || nullable == "YES"
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("read source indexes: %w", err)
	}
	for _, ik := range indexOrder {
		t := byName[ik.key]
		t.UniqueKeys = append(t.UniqueKeys, *indexes[ik])
	}

	cat := &schema.Catalog{}
	sort.Slice(order, func(i, j int) bool {
		if order[i].db != order[j].db {
			return order[i].db < order[j].db
		}
		return order[i].table < order[j].table
	})
	for _, k := range order {
		cat.Tables = append(cat.Tables, *byName[k])
	}

	err = s.query(ctx, s.db, "SELECT CONSTRAINT_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, UPDATE_RULE, DELETE_RULE "+
		"FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE "+fkWhere+" ORDER BY CONSTRAINT_SCHEMA, TABLE_NAME, CONSTRAINT_NAME", args,
		func(rows *sql.Rows) error {
			var fk schema.ForeignKey
			if err := rows.Scan(&fk.Database, &fk.Table, &fk.Name, &fk.ReferencedTable, &fk.UpdateRule, &fk.DeleteRule); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			if byName[key{fk.Database, fk.Table}] != nil {
				cat.ForeignKeys = append(cat.ForeignKeys, fk)
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("read source foreign keys: %w", err)
	}
	return cat, nil
}

// query runs a statement and calls each for every row.
func (s *Source) query(ctx context.Context, db *sql.DB, statement string, args []any, each func(*sql.Rows) error) error {
	rows, err := db.QueryContext(ctx, statement, args...)
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
