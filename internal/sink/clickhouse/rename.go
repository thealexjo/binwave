package clickhouse

import (
	"context"
	"fmt"
)

// RenameRequest is the rename of a table, already decided. FromDatabase, From, ToDatabase and To
// are ClickHouse names; Source is the MySQL name of the table that is renamed, for messages.
type RenameRequest struct {
	Source       string
	FromDatabase string
	From         string
	ToDatabase   string
	To           string
	// SourceReused says that a later pair of the same statement renames another table to the name
	// this pair renames away (the swap: a to old, new to a). When both names exist, the statement
	// has been applied whole before, and this pair is done: the later pair takes the name.
	SourceReused bool
	// DryRun logs the statement instead of executing it.
	DryRun bool
}

// RenameTable makes ClickHouse follow a RENAME TABLE of the source. It is safe to repeat, and it
// never overwrites:
//
//   - the table is there and the new name is free: renamed;
//   - the table is gone and the new name is there: done already (a replay after a crash);
//   - neither exists: nothing to do, the table had no rows yet and is created under its new
//     name by its first row;
//   - both exist and a later pair of the statement takes the old name again: done already, the
//     whole swap was applied before a replay;
//   - both exist otherwise: a *ConflictError. Something left a table under the new name, which does not
//     happen when the source and ClickHouse agree, and binwave does not choose which one to
//     keep.
//
// With DryRun the statement is logged and the state that is missing is reported as *TableError
// with Missing set, like Change does.
func (c *Client) RenameTable(ctx context.Context, req RenameRequest) error {
	from, err := c.columns(ctx, req.FromDatabase, req.From)
	if err != nil {
		return err
	}
	to, err := c.columns(ctx, req.ToDatabase, req.To)
	if err != nil {
		return err
	}
	fromName := quote(req.FromDatabase) + "." + quote(req.From)
	toName := quote(req.ToDatabase) + "." + quote(req.To)
	switch {
	case len(from) == 0:
		return nil // done already, or nothing was written yet
	case len(to) > 0 && req.SourceReused:
		return nil
	case len(to) > 0:
		return &ConflictError{Err: fmt.Errorf("the source renamed table %s, and ClickHouse has both %s and %s; "+
			"binwave does not overwrite or drop a table: remove or rename %s in ClickHouse (it is most likely a leftover) and restart",
			req.Source, fromName, toName, toName)}
	}

	stmt := "RENAME TABLE " + fromName + " TO " + toName
	if req.DryRun {
		c.log.Info("dry run: statement not executed", "statement", stmt)
		return &TableError{Table: req.Source, Missing: true, Err: fmt.Errorf(
			"clickhouse table %s is not renamed to %s and ddl.dry_run is set, so binwave did not apply it (the statement is in the log); "+
				"ddl.on_missing_structure: quarantine would set the table aside and go on, at the price of the changes made meanwhile", fromName, toName)}
	}
	if err := c.conn.Exec(ctx, stmt); err != nil {
		err = fmt.Errorf("apply the rename %q: %w", stmt, err)
		if IsTransient(err) {
			return err
		}
		if _, permanent := Permanent(err); permanent {
			return err
		}
		return &TableError{Table: req.Source, Err: err}
	}
	c.log.Info("table renamed in ClickHouse", "statement", stmt)
	return nil
}

// DropRequest is the drop of a table that the source dropped, already decided: a temporary table
// of an online schema change, whose DROP is mirrored.
// Database and Table are the ClickHouse names; Source is the MySQL table, for messages.
type DropRequest struct {
	Source   string
	Database string
	Table    string
	// DryRun logs the statement instead of executing it.
	DryRun bool
}

// DropTable drops the table if it exists; it is safe to repeat. The pipeline calls it to mirror a
// DROP TABLE of the source, and to remove the shadow table of a resync that was given up.
func (c *Client) DropTable(ctx context.Context, req DropRequest) error {
	stmt := "DROP TABLE IF EXISTS " + quote(req.Database) + "." + quote(req.Table)
	if req.DryRun {
		c.log.Info("dry run: statement not executed", "statement", stmt)
		return nil
	}
	if err := c.conn.Exec(ctx, stmt); err != nil {
		err = fmt.Errorf("apply the drop %q: %w", stmt, err)
		if IsTransient(err) {
			return err
		}
		if _, permanent := Permanent(err); permanent {
			return err
		}
		return &TableError{Table: req.Source, Err: err}
	}
	c.log.Info("table dropped", "statement", stmt)
	return nil
}

// TruncateRequest is the truncation of a table that the source truncated, already decided.
type TruncateRequest struct {
	Source   string
	Database string
	Table    string
	// DryRun logs the statement instead of executing it.
	DryRun bool
}

// TruncateTable empties the table if it exists; it is safe to repeat.
func (c *Client) TruncateTable(ctx context.Context, req TruncateRequest) error {
	stmt := "TRUNCATE TABLE IF EXISTS " + quote(req.Database) + "." + quote(req.Table)
	if req.DryRun {
		c.log.Info("dry run: statement not executed", "statement", stmt)
		return nil
	}
	if err := c.conn.Exec(ctx, stmt); err != nil {
		err = fmt.Errorf("apply the truncate %q: %w", stmt, err)
		if IsTransient(err) {
			return err
		}
		if _, permanent := Permanent(err); permanent {
			return err
		}
		return &TableError{Table: req.Source, Err: err}
	}
	c.log.Info("table truncated", "statement", stmt)
	return nil
}

// BinwaveTables lists the tables of a ClickHouse database that have the shape of the tables binwave
// creates: the engine ReplacingMergeTree(_version, _is_deleted) and the three service columns with
// their types. It is how the list of tables of a position saved before there was one is filled.
func (c *Client) BinwaveTables(ctx context.Context, database string) ([]string, error) {
	rows, err := c.conn.Query(ctx, "SELECT name, engine_full FROM system.tables WHERE database = ? AND name IN ("+
		"SELECT table FROM system.columns WHERE database = ? AND (name, type) IN ((?, ?), (?, ?), (?, ?)) GROUP BY table HAVING count() = 3"+
		") ORDER BY name", database, database, ColVersion, versionType, ColIsDeleted, isDeletedType, ColSyncedAt, syncedAtType)
	if err != nil {
		return nil, fmt.Errorf("list the tables of %s: %w", database, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			return nil, fmt.Errorf("list the tables of %s: %w", database, err)
		}
		if isBinwaveEngine(engine) {
			out = append(out, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list the tables of %s: %w", database, err)
	}
	return out, nil
}
