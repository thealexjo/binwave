package clickhouse

import (
	"context"
	"fmt"
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/thealexjo/binwave/internal/typemap"
)

// rebuildSuffix names the table a rebuild fills before it takes the place of the original.
const rebuildSuffix = "__rebuild"

// RebuildName is the name of the table a rebuild of table fills.
func RebuildName(table string) string { return table + rebuildSuffix }

// TableShape is what the pipeline needs to know about a ClickHouse table to decide whether a change
// of the source touches its sort key.
type TableShape struct {
	// Exists is false for a table that has not been created yet (nothing was written to it).
	Exists bool
	// Key are the columns of the sort key, in order.
	Key []string
	// Columns maps every column to its ClickHouse type.
	Columns map[string]string
	// Kinds are the source types binwave recorded in the comments of the columns; a column of a
	// table made before the comments, or by hand, has none.
	Kinds map[string]typemap.Column
}

// Shape reads the sort key and the columns of a table.
func (c *Client) Shape(ctx context.Context, database, table string) (TableShape, error) {
	defs, err := c.columnDefs(ctx, database, table)
	if err != nil {
		return TableShape{}, err
	}
	if len(defs) == 0 {
		return TableShape{}, nil
	}
	_, key, err := c.tableInfo(ctx, database, table)
	if err != nil {
		return TableShape{}, err
	}
	shape := TableShape{Exists: true, Key: splitKey(key), Columns: map[string]string{}, Kinds: map[string]typemap.Column{}}
	for _, d := range defs {
		shape.Columns[d.name] = d.typ
		if kind, ok := typemap.DecodeTypeComment(d.comment); ok {
			shape.Kinds[d.name] = kind
		}
	}
	return shape, nil
}

func splitKey(key string) []string {
	key = normalizeKey(key)
	if key == "" {
		return nil
	}
	return strings.Split(key, ",")
}

// RebuildRequest is the rebuild of a table under a new sort key, already decided: the source changed
// its primary key, or renamed, dropped or retyped a column of it, and ClickHouse changes none of that
// in place. Database and Table are the ClickHouse names; Source is the MySQL table, for messages.
type RebuildRequest struct {
	Source   string
	Database string
	Table    string
	// Key is the new sort key.
	Key []string
	// Rename, Drop and Modify are the changes of the columns of the old key that the rebuild makes;
	// Modify names the column as it is called after the rename and gives its new type.
	Rename []ColumnRename
	Drop   []string
	Modify []ColumnPlan
	// DryRun logs the statements instead of executing them.
	DryRun bool
	// Progress is called while rows are copied, with the number of rows written so far.
	Progress func(rows uint64)
}

// Rebuild replaces the table with a copy under the new sort key:
//
//  1. a table <name>__rebuild is created with the columns of the original, changed as the request
//     says, and the new sort key;
//  2. the live rows of the original (FINAL, without the deleted ones: only the latest version of a
//     row says which key it has now) are copied into it, with their versions;
//  3. the copy is checked: no two rows may have collapsed into one, which would lose data;
//  4. EXCHANGE TABLES puts the copy in place, and the original, now under the other name, is dropped.
//
// The caller stops the stream around it, so nothing is written to the original meanwhile. Every
// step can be repeated after a crash: a table __rebuild with the right key is reused and filled
// again (equal versions collapse), and a table that already has the new key is done.
//
// It refuses what it cannot rebuild for certain: a table that is not the shape binwave creates (a
// partition, a TTL, a computed column, another engine) is a *TableError, and a leftover
// __rebuild table of another key, or a copy that lost rows, is a *ConflictError.
func (c *Client) Rebuild(ctx context.Context, req RebuildRequest) error {
	table := quote(req.Database) + "." + quote(req.Table)
	tmpName := RebuildName(req.Table)
	tmp := quote(req.Database) + "." + quote(tmpName)
	fail := func(format string, args ...any) error {
		return &TableError{Table: req.Source, Err: fmt.Errorf(format, args...)}
	}

	defs, err := c.columnDefs(ctx, req.Database, req.Table)
	if err != nil {
		return err
	}
	if len(defs) == 0 {
		return nil // nothing was written yet: the table is created with the new structure
	}
	engineFull, partitionKey, sortingKey, err := c.tableDetails(ctx, req.Database, req.Table)
	if err != nil {
		return err
	}
	current := splitKey(sortingKey)

	// Done already (a replay after a crash)? The table has the marker when the swap was made and the
	// old table, under the other name, is still to be dropped.
	if c.rebuilt(defs, current, req) {
		comment, _, err := c.tableComment(ctx, req.Database, req.Table)
		if err != nil || !isMarker(comment, RebuildMarker(req.Table)) {
			return err
		}
		return c.dropLeftover(ctx, req, table, tmp)
	}

	// Only a table of the shape binwave creates is rebuilt by binwave.
	switch {
	case !isBinwaveEngine(engineFull):
		return fail("table %s is not a %s, so binwave does not rebuild it under a new sort key; rebuild it by hand", table, binwaveEngine)
	case partitionKey != "" || strings.Contains(engineFull, " TTL "):
		return fail("table %s has a partition key or a TTL, which binwave does not carry over when it rebuilds a table under a new sort key; rebuild it by hand", table)
	}
	hasService := 0
	for _, d := range defs {
		if d.kind != "" && d.kind != "DEFAULT" {
			return fail("column %s of %s is %s, which binwave does not carry over when it rebuilds a table; rebuild it by hand", d.name, table, d.kind)
		}
		if d.name == ColVersion || d.name == ColIsDeleted {
			hasService++
		}
	}
	if hasService != 2 {
		return fail("table %s lacks the service columns %s and %s, so it was not made by binwave; rebuild it by hand", table, ColVersion, ColIsDeleted)
	}

	// The columns of the new table, and what to select for each.
	renames := map[string]string{}
	for _, r := range req.Rename {
		renames[r.Old] = r.New
	}
	dropped := map[string]bool{}
	for _, name := range req.Drop {
		dropped[name] = true
	}
	retyped := map[string]ColumnPlan{}
	for _, m := range req.Modify {
		retyped[m.Name] = m
	}
	var created, insert, selects []string
	types := map[string]string{}
	for _, d := range defs {
		if dropped[d.name] {
			continue
		}
		name := d.name
		if to, ok := renames[d.name]; ok {
			name = to
		}
		typ, source, comment := d.typ, quote(d.name), d.comment
		if m, ok := retyped[name]; ok {
			if m.Type != d.typ {
				typ = m.Type
				source = "accurateCast(" + quote(d.name) + ", '" + m.Type + "')"
				if m.Convert != "" {
					source = strings.ReplaceAll(m.Convert, "{col}", quote(d.name))
				}
			}
			if c := strings.TrimPrefix(commentClause(m.Kind), " COMMENT '"); c != "" {
				comment = strings.TrimSuffix(c, "'")
			}
		}
		types[name] = typ
		def := quote(name) + " " + typ
		if d.kind == "DEFAULT" {
			def += " DEFAULT " + d.expression
		}
		if comment != "" {
			def += " COMMENT '" + strings.ReplaceAll(comment, "'", "''") + "'"
		}
		created = append(created, def)
		insert = append(insert, quote(name))
		selects = append(selects, source)
	}
	if len(req.Key) == 0 {
		return fail("table %s would have no sort key after the change: set tables.overrides.%s.order_by", table, req.Source)
	}
	quotedKey := make([]string, len(req.Key))
	for i, k := range req.Key {
		typ, ok := types[k]
		if !ok {
			return fail("the new sort key of %s names column %s, which the table does not have after the change", table, k)
		}
		if strings.HasPrefix(typ, "Nullable(") || strings.HasPrefix(typ, "LowCardinality(Nullable(") {
			return fail("the new sort key of %s has column %s of type %s; ClickHouse does not allow NULL in the sort key", table, k, typ)
		}
		quotedKey[i] = quote(k)
	}

	createStmt := "CREATE TABLE " + tmp + " (" + strings.Join(created, ", ") + ") ENGINE = ReplacingMergeTree(" +
		quote(ColVersion) + ", " + quote(ColIsDeleted) + ") ORDER BY (" + strings.Join(quotedKey, ", ") + ") COMMENT " + stringLiteral(RebuildMarker(req.Table))
	copyStmt := "INSERT INTO " + tmp + " (" + strings.Join(insert, ", ") + ") SELECT " + strings.Join(selects, ", ") +
		" FROM " + table + " FINAL WHERE " + quote(ColIsDeleted) + " = 0"
	exchangeStmt := "EXCHANGE TABLES " + table + " AND " + tmp
	dropStmt := "DROP TABLE IF EXISTS " + tmp
	unmarkStmt := "ALTER TABLE " + table + " MODIFY COMMENT ''"

	if req.DryRun {
		for _, s := range []string{createStmt, copyStmt, exchangeStmt, dropStmt, unmarkStmt} {
			c.log.Info("dry run: statement not executed", "statement", s)
		}
		return &TableError{Table: req.Source, Missing: true, Err: fmt.Errorf(
			"clickhouse table %s has to be rebuilt under the new sort key (%s) and ddl.dry_run is set, so binwave did not do it (the statements are in the log); "+
				"ddl.on_missing_structure: quarantine would set the table aside and go on, at the price of the changes made meanwhile", table, strings.Join(req.Key, ", "))}
	}

	// A leftover of an earlier attempt is reused when it has the marker and the key we want, and never
	// touched otherwise: a table of that name without the marker is not binwave's copy.
	if have, err := c.Shape(ctx, req.Database, tmpName); err != nil {
		return err
	} else if have.Exists {
		comment, _, err := c.tableComment(ctx, req.Database, tmpName)
		if err != nil {
			return err
		}
		if !isMarker(comment, RebuildMarker(req.Table)) {
			return &ConflictError{Err: fmt.Errorf("ClickHouse has the table %s, which is not a copy made by a rebuild of %s; "+
				"binwave does not drop or reuse it: rename or drop it in ClickHouse and resume", tmp, table)}
		}
		if !sameKey(have.Key, req.Key) {
			return &ConflictError{Err: fmt.Errorf("ClickHouse has the table %s, left by a rebuild of %s under another sort key (%s); "+
				"binwave does not drop or reuse it: drop it in ClickHouse and resume", tmp, table, strings.Join(have.Key, ", "))}
		}
	} else if err := c.exec(ctx, createStmt); err != nil {
		return c.classifyRebuild(req, err)
	}

	var wrote uint64
	copyCtx := ch.Context(ctx, ch.WithProgress(func(p *ch.Progress) {
		wrote += p.WroteRows
		if req.Progress != nil {
			req.Progress(wrote)
		}
	}))
	if err := c.exec(copyCtx, copyStmt); err != nil {
		if !IsTransient(err) {
			if _, permanent := Permanent(err); !permanent {
				// What can fail in a copy of every row is a value: it does not convert exactly. The
				// message of ClickHouse names it, so it goes to the operator's status only.
				return &ConflictError{
					Err: fmt.Errorf("the copy of %s under the new sort key or column types stopped at a value that does not convert exactly, "+
						"so nothing was swapped and the original is untouched: MySQL, in a sql_mode that is not strict, may have truncated or changed it "+
						"where ClickHouse cannot; the leftover table %s stays for inspection", table, tmp),
					Detail: err.Error(),
				}
			}
		}
		return c.classifyRebuild(req, err)
	}

	// Two rows of the original that have the same new key would collapse into one in the copy: the
	// source guarantees they do not (its primary key is unique), so if they did, something is wrong
	// and nothing is swapped.
	var live uint64
	if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+tmp+" FINAL WHERE "+quote(ColIsDeleted)+" = 0").Scan(&live); err != nil {
		return fmt.Errorf("count the rows of %s: %w", tmp, err)
	}
	if live != wrote {
		return &ConflictError{Err: fmt.Errorf("the copy of %s under the new sort key (%s) holds %d rows but %d were written: rows with equal keys collapsed, "+
			"so the swap was not made and the original is untouched; the leftover table %s stays for inspection",
			table, strings.Join(req.Key, ", "), live, wrote, tmp)}
	}

	if err := c.exec(ctx, exchangeStmt); err != nil {
		return c.classifyRebuild(req, err)
	}
	c.log.Info("table rebuilt under the new sort key", "table", table, "sort_key", strings.Join(req.Key, ", "), "rows", live)
	return c.dropLeftover(ctx, req, table, tmp)
}

func (c *Client) exec(ctx context.Context, stmt string) error {
	if err := c.conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("%q: %w", firstLine(stmt), err)
	}
	return nil
}

func firstLine(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

// classifyRebuild sorts the failure of a statement of the rebuild like the other changes do: a
// transient failure is retried by the caller (every step can be repeated), a permanent one ends the
// service, and a refusal of ClickHouse is a problem of the table.
func (c *Client) classifyRebuild(req RebuildRequest, err error) error {
	if IsTransient(err) {
		return err
	}
	if _, permanent := Permanent(err); permanent {
		return err
	}
	return &TableError{Table: req.Source, Err: fmt.Errorf("rebuild the table under the new sort key: %w", err)}
}

// rebuilt reports whether the table already is what the request makes of it.
func (c *Client) rebuilt(defs []columnDef, current []string, req RebuildRequest) bool {
	if !sameKey(current, req.Key) {
		return false
	}
	have := map[string]string{}
	for _, d := range defs {
		have[d.name] = d.typ
	}
	for _, r := range req.Rename {
		if _, ok := have[r.Old]; ok {
			return false
		}
	}
	for _, name := range req.Drop {
		if _, ok := have[name]; ok {
			return false
		}
	}
	for _, m := range req.Modify {
		if typ, ok := have[m.Name]; ok && typ != m.Type {
			return false
		}
	}
	return true
}

// dropLeftover drops the old table after the swap, now under the name of the copy, and then removes
// the marker from the table, which ends the rebuild.
func (c *Client) dropLeftover(ctx context.Context, req RebuildRequest, table, tmp string) error {
	if req.DryRun {
		return nil
	}
	if err := c.exec(ctx, "DROP TABLE IF EXISTS "+tmp); err != nil {
		return c.classifyRebuild(req, err)
	}
	if err := c.exec(ctx, "ALTER TABLE "+table+" MODIFY COMMENT ''"); err != nil {
		return c.classifyRebuild(req, err)
	}
	return nil
}

func sameKey(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type columnDef struct {
	name, typ, kind, expression, comment string
}

// columnDefs reads the columns of a table in order, with the kind and expression of their defaults.
func (c *Client) columnDefs(ctx context.Context, database, table string) ([]columnDef, error) {
	rows, err := c.conn.Query(ctx, "SELECT name, type, default_kind, default_expression, comment FROM system.columns WHERE database = ? AND table = ? ORDER BY position", database, table)
	if err != nil {
		return nil, fmt.Errorf("read the columns of %s.%s: %w", database, table, err)
	}
	defer func() { _ = rows.Close() }()
	var out []columnDef
	for rows.Next() {
		var d columnDef
		if err := rows.Scan(&d.name, &d.typ, &d.kind, &d.expression, &d.comment); err != nil {
			return nil, fmt.Errorf("read the columns of %s.%s: %w", database, table, err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the columns of %s.%s: %w", database, table, err)
	}
	return out, nil
}

func (c *Client) tableDetails(ctx context.Context, database, table string) (engineFull, partitionKey, sortingKey string, err error) {
	row := c.conn.QueryRow(ctx, "SELECT engine_full, partition_key, sorting_key FROM system.tables WHERE database = ? AND name = ?", database, table)
	if err := row.Scan(&engineFull, &partitionKey, &sortingKey); err != nil {
		return "", "", "", fmt.Errorf("read the table %s.%s: %w", database, table, err)
	}
	return engineFull, partitionKey, sortingKey, nil
}
