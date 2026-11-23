package clickhouse

import (
	"fmt"
	"strings"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/typemap"
)

// Service columns added to every ClickHouse table: the row version and the delete marker that
// ReplacingMergeTree(_version, _is_deleted) collapses rows by.
const (
	ColVersion   = "_version"
	ColIsDeleted = "_is_deleted"
	ColSyncedAt  = "_synced_at"

	versionType   = "UInt64"
	isDeletedType = "UInt8"
	syncedAtType  = "DateTime64(3, 'UTC')"
)

var serviceColumns = map[string]bool{ColVersion: true, ColIsDeleted: true, ColSyncedAt: true}

// IsServiceColumn reports whether name is one of the columns binwave adds to every table.
func IsServiceColumn(name string) bool { return serviceColumns[name] }

// Column is a replicated column: a source column and its ClickHouse mapping.
type Column struct {
	Name        string
	SourceIndex int
	Mapping     *typemap.Mapping
}

// Table binds a source table to its ClickHouse table: target name, replicated columns and sort key.
type Table struct {
	Source   *schema.Table
	Database string
	Name     string
	Columns  []Column
	// OrderBy are the sort key column names. ReplacingMergeTree collapses rows with equal keys.
	OrderBy []string

	// Comment is put on the table when it is created; the shadow table of a resync carries a marker
	// there (see ResyncMarker), and after the swap it is on the table that took its place.
	Comment string
	// UUID, when set, is the UUID of the table: it is created with it, and a table binwave keeps for a job
	// is taken only with it.
	UUID string

	orderIdx []int // indexes into Source.Columns, for detecting key changes
}

// Resolve binds a source table to ClickHouse according to the configuration: target names,
// excluded columns, type overrides and the sort key. It fails on anything that would make the
// replica silently wrong, such as an exclusion of an unknown column.
func Resolve(src *schema.Table, cfg *config.Config, reporter typemap.Reporter) (*Table, error) {
	full := src.FullName()
	ov := cfg.Tables.Overrides[full]

	t := &Table{Source: src}
	t.Database, t.Name = TargetName(cfg, src.Database, src.Name)

	excluded, err := nameSet(src, full, "exclude_columns", ov.ExcludeColumns)
	if err != nil {
		return nil, err
	}
	overrideNames := make([]string, 0, len(ov.Columns))
	for name := range ov.Columns {
		overrideNames = append(overrideNames, name)
	}
	if _, err := nameSet(src, full, "columns", overrideNames); err != nil {
		return nil, err
	}

	for i, col := range src.Columns {
		if serviceColumns[col.Name] {
			return nil, &TableError{Table: full, Err: fmt.Errorf("table %s: column %q collides with a binwave service column; exclude it or rename it in the source", full, col.Name)}
		}
		if excluded[col.Name] {
			continue
		}
		m, err := typemap.Resolve(full, col, typemap.Options{Override: ov.Columns[col.Name].Type, Reporter: reporter})
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", full, err)
		}
		t.Columns = append(t.Columns, Column{Name: col.Name, SourceIndex: i, Mapping: m})
	}
	if len(t.Columns) == 0 {
		return nil, fmt.Errorf("table %s: every column is excluded", full)
	}
	if err := checkInsertable(full, t.Columns); err != nil {
		return nil, &TableError{Table: full, Err: err}
	}

	if err := t.resolveOrderBy(full, ov, excluded); err != nil {
		return nil, err
	}
	return t, nil
}

// TargetName is the ClickHouse database and table a source table is replicated into.
func TargetName(cfg *config.Config, database, table string) (targetDB, targetTable string) {
	targetDB, targetTable = database, table // shop.orders stays shop.orders ...
	switch {
	case cfg.Tables.TargetDatabase != "":
		targetDB = cfg.Tables.TargetDatabase // ... unless everything is collected in one database ...
	case cfg.Tables.DatabaseMap[database] != "":
		targetDB = cfg.Tables.DatabaseMap[database] // ... or this one database is renamed on its own.
	}
	if ov := cfg.Tables.Overrides[database+"."+table]; ov.TargetTable != "" {
		targetTable = ov.TargetTable
	}
	return targetDB, targetTable
}

// resyncSuffix names the shadow table of a resync.
const resyncSuffix = "__resync"

// ShadowName is the name of the shadow table of a resync of database.table, next to its target table.
func ShadowName(cfg *config.Config, database, table string) string {
	_, target := TargetName(cfg, database, table)
	return target + resyncSuffix
}

// nameSet checks that every name is a column of src and returns them as a set.
func nameSet(src *schema.Table, full, setting string, names []string) (map[string]bool, error) {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		if src.ColumnIndex(n) < 0 {
			// A typo in exclude_columns would publish a column that was meant to stay private.
			return nil, fmt.Errorf("table %s: tables.overrides.%s.%s refers to unknown column %q", full, full, setting, n)
		}
		set[n] = true
	}
	return set, nil
}

func (t *Table) resolveOrderBy(full string, ov config.TableOverride, excluded map[string]bool) error {
	orderBy := ov.OrderBy
	if len(orderBy) == 0 {
		orderBy = t.Source.PrimaryKeyColumns()
	}
	if len(orderBy) == 0 {
		return &TableError{Table: full, Err: fmt.Errorf("table %s has no primary key: set tables.overrides.%s.order_by", full, full)}
	}
	if err := requirePrimaryKey(full, t.Source, orderBy, excluded); err != nil {
		return err
	}

	for _, name := range orderBy {
		idx := t.Source.ColumnIndex(name)
		if idx < 0 {
			return fmt.Errorf("table %s: order_by refers to unknown column %q", full, name)
		}
		if excluded[name] {
			return fmt.Errorf("table %s: column %q is excluded but is part of the sort key; set order_by to other columns", full, name)
		}
		mapping := t.column(name).Mapping
		if strings.HasPrefix(mapping.Type, "Nullable(") || strings.HasPrefix(mapping.Type, "LowCardinality(Nullable(") {
			return fmt.Errorf("table %s: sort key column %q is %s; ClickHouse does not allow NULL in the sort key", full, name, mapping.Type)
		}
		t.OrderBy = append(t.OrderBy, name)
		t.orderIdx = append(t.orderIdx, idx)
	}
	return nil
}

// requirePrimaryKey rejects a sort key that leaves out a primary key column. ReplacingMergeTree
// collapses rows with equal sort keys, so a shorter or different key would merge distinct source
// rows into one and lose data without any error. Extending the key is fine.
func requirePrimaryKey(full string, src *schema.Table, orderBy []string, excluded map[string]bool) error {
	have := make(map[string]bool, len(orderBy))
	for _, n := range orderBy {
		have[n] = true
	}
	for _, pk := range src.PrimaryKeyColumns() {
		if have[pk] {
			continue
		}
		reason := ""
		if excluded[pk] {
			reason = " (it is excluded, so the table cannot be replicated without its primary key)"
		}
		return fmt.Errorf("table %s: order_by must include every primary key column, missing %q%s: a shorter or different key would merge distinct rows", full, pk, reason)
	}
	return nil
}

func (t *Table) column(name string) *Column {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

// FullName renders the quoted "database"."table" used in statements.
func (t *Table) FullName() string { return quote(t.Database) + "." + quote(t.Name) }

// expectedColumns returns the ClickHouse columns of the table in creation order, service columns last.
func (t *Table) expectedColumns() [][2]string {
	cols := make([][2]string, 0, len(t.Columns)+3)
	for _, c := range t.Columns {
		cols = append(cols, [2]string{c.Name, c.Mapping.Type})
	}
	return append(cols,
		[2]string{ColVersion, versionType},
		[2]string{ColIsDeleted, isDeletedType},
		[2]string{ColSyncedAt, syncedAtType},
	)
}

// CreateStatement renders CREATE TABLE IF NOT EXISTS for the table. It never drops or recreates anything.
func (t *Table) CreateStatement() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s", t.FullName())
	if t.UUID != "" {
		fmt.Fprintf(&b, " UUID %s", stringLiteral(t.UUID))
	}
	b.WriteString("\n(\n")
	cols := t.expectedColumns()
	comments := map[string]string{}
	for _, c := range t.Columns {
		comments[c.Name] = commentClause(c.Mapping.Column)
	}
	for i, c := range cols {
		sep := ","
		if i == len(cols)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "    %s %s%s%s\n", quote(c[0]), c[1], comments[c[0]], sep)
	}
	b.WriteString(")\n")
	fmt.Fprintf(&b, "ENGINE = ReplacingMergeTree(%s, %s)\n", quote(ColVersion), quote(ColIsDeleted))
	quoted := make([]string, len(t.OrderBy))
	for i, n := range t.OrderBy {
		quoted[i] = quote(n)
	}
	fmt.Fprintf(&b, "ORDER BY (%s)", strings.Join(quoted, ", "))
	if t.Comment != "" {
		fmt.Fprintf(&b, "\nCOMMENT %s", stringLiteral(t.Comment))
	}
	return b.String()
}

// quote renders a ClickHouse identifier in backticks.
func quote(name string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(name, `\`, `\\`), "`", "\\`") + "`"
}

// checkInsertable rejects column names the ClickHouse client library cannot insert into. It reads
// the column list of an INSERT itself: it splits on commas, strips every backtick and trims double
// quotes at the ends, so such names would fail at the first insert with a puzzling message. Better
// to say so when the table is bound. Table names are not affected.
func checkInsertable(table string, columns []Column) error {
	for _, c := range columns {
		if !Insertable(c.Name) {
			return fmt.Errorf("table %s: column %q cannot be inserted by the ClickHouse client library "+
				"(a name with a comma, a backtick or a double quote at either end is not supported); "+
				"exclude it with tables.overrides.%s.exclude_columns", table, c.Name, table)
		}
	}
	return nil
}

// Insertable reports whether the ClickHouse client library can insert into a column of this name.
func Insertable(column string) bool {
	return !strings.ContainsAny(column, ",`") && !strings.HasPrefix(column, `"`) && !strings.HasSuffix(column, `"`)
}
