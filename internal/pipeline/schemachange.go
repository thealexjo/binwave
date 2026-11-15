package pipeline

import (
	"context"
	"fmt"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/ddl"
	"github.com/thealexjo/binwave/internal/event"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/typemap"
)

// applySchemaChange makes ClickHouse follow the schema statements of a replicated table: ALTER
// TABLE (columns added, wider types, and in ddl.mode full columns dropped and renamed) and
// RENAME TABLE. It runs at the DDL event, which is a barrier: everything before it is written
// under the old structure, and the change is made before the transaction of the statement
// commits, so the position never gets ahead of it. A replay after a crash finds the work done,
// because every change is idempotent.
//
// The types come from the statement; the binlog's own table structure, which arrives with the
// next rows of the table, is what the ClickHouse table is checked against, so a difference
// between the two reads of the type sets the table aside instead of going unnoticed.
func (p *Pipeline) applySchemaChange(ctx context.Context, e event.DDL) error {
	op, err := ddl.Parse(e.Query, e.Database)
	if err != nil {
		// A statement the parser does not read is not applied. If it changed the table, the
		// structure that arrives with the next rows shows a column ClickHouse lacks.
		p.log.Warn("unparsed DDL skipped",
			"statement", e.Query, "error", err)
		return nil
	}
	switch o := op.(type) {
	case ddl.AlterTable:
		return p.alterTable(ctx, o, e)
	case ddl.RenameTable:
		return p.renameTables(ctx, o.Pairs)
	}
	return nil
}

// alterTable applies the specifications of an ALTER TABLE of a replicated table. A rename that
// the statement contains (ALTER TABLE a RENAME TO b, which is what gh-ost's two-step cut-over
// writes) is done last, after the columns.
func (p *Pipeline) alterTable(ctx context.Context, alter ddl.AlterTable, e event.DDL) error {
	var rename []ddl.Rename
	for _, spec := range alter.Specs {
		if s, ok := spec.(ddl.RenameTo); ok {
			rename = append(rename, ddl.Rename{From: alter.Table, To: s.Table})
		}
	}
	if !p.router.Match(alter.Table.Database, alter.Table.Table) {
		// The table is not replicated, but it may be renamed into one that is.
		return p.renameTables(ctx, rename)
	}
	name := alter.Table.String()
	if skip, changed, err := p.q.skip(name, p.tx.gtid); err != nil {
		return err
	} else if changed {
		p.qChanged.Store(true)
	} else if skip {
		return p.renameTables(ctx, rename) // set aside: its changes are not followed, but its name is
	}

	req := chsink.ChangeRequest{Source: name, DryRun: p.cfg.DDL.DryRun}
	req.Database, req.Table = chsink.TargetName(p.cfg, alter.Table.Database, alter.Table.Table)

	full := p.cfg.DDL.Mode == config.DDLFull
	var pk pkSpec
	unsupported := func(what string) error {
		return p.tableFailed(name, &chsink.TableError{Table: name, Err: fmt.Errorf(
			"ALTER TABLE %s: %s is not applied to ClickHouse: ddl.mode %s applies added columns and wider types only (ddl.mode full also drops and renames columns and changes the primary key)",
			name, what, p.cfg.DDL.Mode)})
	}
	for _, spec := range alter.Specs {
		switch s := spec.(type) {
		case ddl.AddColumns:
			for _, c := range s.Columns {
				plan, skip, err := chsink.PlanColumn(p.cfg, alter.Table.Database, alter.Table.Table, c, s.Position, e.Time, p.d.Reporter)
				if err != nil {
					return p.tableFailed(name, err)
				}
				if !skip {
					req.Add = append(req.Add, *plan)
				}
				if c.PrimaryKey {
					pk.set, pk.add = true, append(pk.add, c.Name)
				}
			}
		case ddl.ModifyColumn:
			plan, skip, err := p.planModify(alter.Table, s.Column, e)
			if err != nil {
				return p.tableFailed(name, err)
			}
			if !skip {
				req.Modify = append(req.Modify, *plan)
			}
		case ddl.ChangeColumn:
			if s.Old != s.Column.Name {
				if !full {
					return unsupported("renaming column " + s.Old + " to " + s.Column.Name)
				}
				req.Rename = append(req.Rename, chsink.ColumnRename{Old: s.Old, New: s.Column.Name})
			}
			plan, skip, err := p.planModify(alter.Table, s.Column, e)
			if err != nil {
				return p.tableFailed(name, err)
			}
			if !skip {
				req.Modify = append(req.Modify, *plan)
			}
		case ddl.DropColumn:
			if !full {
				return unsupported("dropping column " + s.Name)
			}
			req.Drop = append(req.Drop, s.Name)
		case ddl.RenameColumn:
			if !full {
				return unsupported("renaming column " + s.Old + " to " + s.New)
			}
			req.Rename = append(req.Rename, chsink.ColumnRename{Old: s.Old, New: s.New})
		case ddl.DropPrimaryKey:
			pk.set = true
		case ddl.AddPrimaryKey:
			pk.set, pk.add = true, append(pk.add, s.Columns...)
		case ddl.Unsupported:
			p.log.Warn("unknown ALTER clause ignored",
				"table", name, "specification", s.What)
		}
	}
	if pk.set && !full {
		return unsupported("changing the primary key")
	}

	// A change of the sort key, and a change of a type that ClickHouse does not do in place or that
	// MySQL does by its own rules, is decided against what the table is now.
	var rebuild *chsink.RebuildRequest
	if pk.set || len(req.Rename)+len(req.Drop)+len(req.Modify) > 0 {
		var shape chsink.TableShape
		err := p.withRetry(ctx, "read the shape of "+name, func(actx context.Context) error {
			var err error
			shape, err = p.d.Sink.Shape(actx, req.Database, req.Table)
			return err
		})
		if err != nil {
			return fmt.Errorf("read the shape of table %s: %w", name, err)
		}
		if shape.Exists {
			oldKind := func(column string) (typemap.Column, bool) {
				if kind, ok := shape.Kinds[column]; ok {
					return kind, true
				}
				// A table without the comments: what the last rows showed, if there were any.
				if b := p.registry[name]; b != nil {
					for _, c := range b.src.Columns {
						if c.Name == column {
							return c, true
						}
					}
				}
				return typemap.Column{}, false
			}
			if rebuild, err = planKey(p.cfg, alter.Table, shape, &req, pk, oldKind, full); err != nil {
				return p.tableFailed(name, err)
			}
		}
	}
	for i := range req.Rename {
		for _, later := range req.Rename[i+1:] {
			req.Rename[i].SourceReused = req.Rename[i].SourceReused || later.New == req.Rename[i].Old
		}
	}
	if len(req.Add)+len(req.Modify)+len(req.Rename)+len(req.Drop) > 0 {
		err := p.withRetry(ctx, "change table "+name, func(actx context.Context) error {
			return p.d.Sink.Change(actx, req)
		})
		if err != nil {
			if chsink.IsTableProblem(err) {
				return p.tableFailed(name, err)
			}
			return fmt.Errorf("change table %s: %w", name, err)
		}
	}
	if len(req.Modify) > 0 {
		p.forgetTable(name) // what the columns are in the source changed: the next rows say
	}
	if rebuild != nil {
		if err := p.rebuildTable(ctx, name, *rebuild); err != nil {
			if chsink.IsTableProblem(err) {
				return p.tableFailed(name, err)
			}
			return fmt.Errorf("rebuild table %s: %w", name, err)
		}
	}
	return p.renameTables(ctx, rename)
}

// renameTables follows the renames of the source, pair by pair in the order written, so a swap
// (a to old, new to a) is two renames one after the other.
//
//   - Both names are not replicated: nothing to do.
//   - The old name is replicated: the ClickHouse table is renamed, data included, and the new
//     name is what the next rows are written to. If the new name is not replicated, the table
//     leaves the replication and keeps its data in ClickHouse under the new name. A quarantined
//     table stays quarantined under its new name.
//   - Only the new name is replicated: the table is replaced by one whose rows binwave never
//     saw (most likely a shadow table of an online schema change that has a name the tools do
//     not use by default and is not in tables.include), and ClickHouse cannot follow. That is not something
//     to set aside: the service stops.
//
// ddl.mode additive does not rename tables: the table is set aside under both names, because
// the rows of the new name would otherwise start a table that lacks the history.
func (p *Pipeline) renameTables(ctx context.Context, pairs []ddl.Rename) error {
	// A statement that cannot be followed is refused before anything is renamed, so ClickHouse
	// is left as it was, and the statement, repeated after the operator's change, finds it so.
	for _, r := range pairs {
		if !p.router.Match(r.From.Database, r.From.Table) && p.router.Match(r.To.Database, r.To.Table) {
			from, to := r.From.String(), r.To.String()
			return &chsink.ConflictError{Err: fmt.Errorf(
				"the source renamed table %s to %s, which is replicated, but binwave never saw the rows of %s, so ClickHouse cannot follow: "+
					"the rows of %s in ClickHouse are not the rows of the source any more. "+
					"The shadow table of an online schema change (pt-online-schema-change, gh-ost) is replicated when it has the tools' default name; "+
					"a shadow with another name has to be in tables.include before the migration starts. "+
					"To go on without this table, take it out of tables.include and restart", from, to, from, to)}
		}
	}
	for i, r := range pairs {
		fromRepl := p.router.Match(r.From.Database, r.From.Table)
		toRepl := p.router.Match(r.To.Database, r.To.Table)
		from, to := r.From.String(), r.To.String()
		if !fromRepl {
			continue // neither name is replicated
		}

		p.forgetTable(from, to)
		if skip, changed, err := p.q.skip(from, p.tx.gtid); err != nil {
			return err
		} else if changed {
			p.qChanged.Store(true)
		} else if skip {
			if p.q.move(from, to, toRepl) {
				p.log.Warn("quarantine follows renamed table", "from", from, "to", to)
				p.qChanged.Store(true)
				p.syncQuarantine()
				p.d.Observer.QuarantineChanged(p.q.active())
			}
			continue
		}

		if p.cfg.DDL.Mode != config.DDLFull {
			reason := &chsink.TableError{Table: from, Err: fmt.Errorf(
				"RENAME TABLE %s TO %s is not applied to ClickHouse: ddl.mode %s does not rename tables (ddl.mode full does)", from, to, p.cfg.DDL.Mode)}
			if err := p.tableFailed(from, reason); err != nil {
				return err
			}
			if toRepl {
				if err := p.tableFailed(to, reason); err != nil {
					return err
				}
			}
			continue
		}

		req := chsink.RenameRequest{Source: from, DryRun: p.cfg.DDL.DryRun}
		for _, later := range pairs[i+1:] {
			req.SourceReused = req.SourceReused || later.To == r.From
		}
		req.FromDatabase, req.From = chsink.TargetName(p.cfg, r.From.Database, r.From.Table)
		req.ToDatabase, req.To = chsink.TargetName(p.cfg, r.To.Database, r.To.Table)
		if req.FromDatabase == req.ToDatabase && req.From == req.To {
			p.follow(from, to, toRepl, req)
			continue // both names are written to one ClickHouse table
		}
		err := p.withRetry(ctx, "rename table "+from, func(actx context.Context) error {
			return p.d.Sink.RenameTable(actx, req)
		})
		if err != nil {
			if chsink.IsTableProblem(err) {
				if err := p.tableFailed(from, err); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("rename table %s to %s: %w", from, to, err)
		}
		p.follow(from, to, toRepl, req)
	}
	return nil
}

// follow moves a renamed table in the list of tables: under its new name if that is replicated, out of
// the list otherwise, where it keeps its data and is not binwave's any more.
func (p *Pipeline) follow(from, to string, toRepl bool, req chsink.RenameRequest) {
	if req.DryRun {
		return // nothing was renamed
	}
	p.unmanage(from)
	if toRepl {
		p.manage(to, checkpoint.Target{Database: req.ToDatabase, Table: req.To})
	}
}

// forgetChecks makes the guard look again at the replicated tables a statement names: it may have
// given them what the guard rejects (a cascading foreign key) with the columns unchanged.
func (p *Pipeline) forgetChecks(e event.DDL) {
	if p.d.Guard == nil {
		return
	}
	op, err := ddl.Parse(e.Query, e.Database)
	if err != nil {
		return
	}
	var named []ddl.Name
	dropped := false
	switch o := op.(type) {
	case ddl.CreateTable:
		named = []ddl.Name{o.Table}
	case ddl.DropTable:
		named, dropped = o.Tables, true
	case ddl.RenameTable:
		for _, pair := range o.Pairs {
			named = append(named, pair.From, pair.To)
		}
	case ddl.AlterTable:
		named = []ddl.Name{o.Table}
		for _, spec := range o.Specs {
			if r, ok := spec.(ddl.RenameTo); ok {
				named = append(named, r.Table)
			}
		}
	}
	for _, n := range named {
		if !p.router.Match(n.Database, n.Table) {
			continue
		}
		p.d.Guard.Forget(n.Database, n.Table)
		if dropped {
			delete(p.recheck, n.String())
		} else {
			p.recheck[n.String()] = true
		}
	}
}

// forgetTable drops what the pipeline holds about the tables of a rename: the next rows of
// either name bind the table again, against the structure the binlog then shows. Buffers are
// empty at a DDL event, so nothing is lost.
func (p *Pipeline) forgetTable(names ...string) {
	for _, name := range names {
		delete(p.registry, name)
		for target, owner := range p.targets {
			if owner == name {
				delete(p.targets, target)
			}
		}
	}
	p.d.Observer.TablesBound(len(p.registry))
}

// planModify plans a column whose type is redefined. Its default is not part of the change: a
// new default applies to future rows of the source, and binwave writes every value itself.
func (p *Pipeline) planModify(table ddl.Name, c ddl.Column, e event.DDL) (*chsink.ColumnPlan, bool, error) {
	c.Default = ddl.Default{Kind: ddl.DefaultNull}
	c.Generated, c.AutoIncrement = false, false
	return chsink.PlanColumn(p.cfg, table.Database, table.Table, c, ddl.Position{}, e.Time, nil)
}
