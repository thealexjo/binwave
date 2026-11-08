package clickhouse

import (
	"context"
	"fmt"
	"strings"
)

// ColumnRename renames a column, keeping its data. SourceReused says that a later rename of the
// same request takes the old name again (a swap of two columns), which makes both names existing a
// sign that the request was applied whole before, not a conflict.
type ColumnRename struct {
	Old, New     string
	SourceReused bool
}

// ChangeRequest is a schema change of one table, already decided: which columns are renamed,
// dropped, added and which change type, applied in this order (a renamed column is then modified
// under its new name). Database and Table are the ClickHouse names; Source is the MySQL table,
// for messages.
type ChangeRequest struct {
	Source   string
	Database string
	Table    string
	Rename   []ColumnRename
	Drop     []string
	Add      []ColumnPlan
	Modify   []ColumnPlan
	// DryRun logs the statements instead of executing them.
	DryRun bool
}

// Change applies the change. It is safe to repeat: a column that exists is not added again, a
// column that is gone is not dropped again, a rename whose result is there is done, and a column
// that already has the type is not changed, so replaying the statement after a crash finds the
// work done. A table that does not exist is left alone: it is created, with the structure the
// binlog then shows, when its first row arrives. So is a column that ClickHouse does not have
// (excluded from replication): there is nothing to drop or rename.
//
// What it does not do: change the type of a sort key column (ClickHouse does not allow it), drop
// one or rename one, or change a type in a way that could lose a value. Those are *TableError, and the caller
// sets the table aside. A rename onto a column that exists is a *ConflictError. With DryRun nothing is executed, the statements are logged, and the structure
// that is missing is reported as such (a *TableError with Missing set).
func (c *Client) Change(ctx context.Context, req ChangeRequest) error {
	table := quote(req.Database) + "." + quote(req.Table)
	fail := func(format string, args ...any) error {
		return &TableError{Table: req.Source, Err: fmt.Errorf(format, args...)}
	}

	actual, err := c.columns(ctx, req.Database, req.Table)
	if err != nil {
		return err
	}
	if len(actual) == 0 {
		return nil
	}
	_, sortingKey, err := c.tableInfo(ctx, req.Database, req.Table)
	if err != nil {
		return err
	}
	inKey := map[string]bool{}
	for _, k := range strings.Split(normalizeKey(sortingKey), ",") {
		inKey[k] = true
	}

	var stmts []string
	present := map[string]bool{}
	for name := range actual {
		present[name] = true
	}
	for _, r := range req.Rename {
		switch {
		case !present[r.Old] && present[r.New]:
			continue // done already
		case !present[r.Old]:
			continue // not a column of ClickHouse (excluded)
		case present[r.New] && r.SourceReused:
			continue // the whole request was applied before
		case present[r.New]:
			return &ConflictError{Err: fmt.Errorf("the source renamed column %s of %s to %s, but ClickHouse table %s has both columns; "+
				"binwave does not overwrite a column: resolve it in ClickHouse and restart", r.Old, req.Source, r.New, table)}
		}
		if inKey[r.Old] {
			return fail("column %s is renamed to %s in the source, but it is part of the sort key of %s, and ClickHouse does not rename a sort key column; "+
				"ddl.mode full rebuilds the table under the new key", r.Old, r.New, table)
		}
		stmts = append(stmts, "ALTER TABLE "+table+" RENAME COLUMN "+quote(r.Old)+" TO "+quote(r.New))
		present[r.Old], present[r.New] = false, true
		actual[r.New] = actual[r.Old]
		delete(actual, r.Old)
	}
	for _, name := range req.Drop {
		if !present[name] {
			continue
		}
		if inKey[name] {
			return fail("column %s is dropped in the source, but it is part of the sort key of %s, and ClickHouse does not drop a sort key column; "+
				"ddl.mode full rebuilds the table under the new key", name, table)
		}
		stmts = append(stmts, "ALTER TABLE "+table+" DROP COLUMN IF EXISTS "+quote(name))
		present[name] = false
		delete(actual, name)
	}
	for _, col := range req.Add {
		if present[col.Name] {
			continue
		}
		stmt := "ALTER TABLE " + table + " ADD COLUMN IF NOT EXISTS " + quote(col.Name) + " " + col.Type
		if col.Default != "" {
			stmt += " " + col.Default
		}
		stmt += commentClause(col.Kind)
		switch {
		case col.First:
			stmt += " FIRST"
		case col.After != "" && present[col.After]:
			stmt += " AFTER " + quote(col.After)
		}
		stmts = append(stmts, stmt)
		present[col.Name] = true
	}
	for _, col := range req.Modify {
		current, ok := actual[col.Name]
		switch {
		case !ok:
			continue // the column is not in ClickHouse (excluded, or added by this request)
		case current == col.Type:
			// The type stays, the source's type may not (VARCHAR(10) to VARCHAR(20)): the comment says so.
			if comment := commentClause(col.Kind); comment != "" {
				stmts = append(stmts, "ALTER TABLE "+table+" COMMENT COLUMN "+quote(col.Name)+strings.TrimPrefix(comment, " COMMENT"))
			}
			continue
		case inKey[col.Name]:
			return fail("column %s changes from %s to %s, but it is part of the sort key of %s, and ClickHouse does not change the type of a sort key column in place; "+
				"ddl.mode full rebuilds the table under the new key", col.Name, current, col.Type, table)
		case !Widens(current, col.Type):
			return fail("column %s changes from %s to %s, which could lose values; binwave applies only changes that keep every value "+
				"(a wider integer, a decimal with the same or more digits, Float32 to Float64, a finer DateTime64, NOT NULL to NULL)", col.Name, current, col.Type)
		}
		stmts = append(stmts, "ALTER TABLE "+table+" MODIFY COLUMN "+quote(col.Name)+" "+col.Type+commentClause(col.Kind))
	}
	if len(stmts) == 0 {
		return nil
	}

	if req.DryRun {
		for _, s := range stmts {
			c.log.Info("dry run: statement not executed", "statement", s)
		}
		return &TableError{Table: req.Source, Missing: true, Err: fmt.Errorf(
			"clickhouse table %s lacks the change of the source (the statements are in the log) and ddl.dry_run is set, so binwave did not apply it; "+
				"ddl.on_missing_structure: quarantine would set the table aside and go on, at the price of the changes made meanwhile", table)}
	}
	for _, s := range stmts {
		if err := c.conn.Exec(ctx, s); err != nil {
			err = fmt.Errorf("apply the schema change %q: %w", s, err)
			if IsTransient(err) {
				return err // retried by the caller
			}
			if _, permanent := Permanent(err); permanent {
				return err
			}
			// ClickHouse refuses the statement itself: a problem of this table.
			return &TableError{Table: req.Source, Err: err}
		}
		c.log.Info("schema change applied to ClickHouse", "table", table, "statement", s)
	}
	return nil
}
