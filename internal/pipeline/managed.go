package pipeline

import (
	"context"
	"fmt"
	"sort"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// The tables binwave manages are the ClickHouse tables it created or found and wrote to, by source
// table. DROP DATABASE of the source drops the ones of the dropped database and nothing else: a table
// of the same shape that binwave never wrote to is not binwave's to drop. The list is saved with the
// position (checkpoint.Position.Tables), and every change of it is safe to repeat after a crash: a
// table bound or swapped in again is added again, one dropped or renamed again is removed again.

// loadManaged takes the list of tables from the saved position. A position saved by a version that
// kept no list gets one made of the tables binwave has written so far (adoptTables).
func (p *Pipeline) loadManaged(ctx context.Context, pos checkpoint.Position) error {
	tables, known, err := checkpoint.DecodeTables(pos.Tables)
	if err != nil {
		return err
	}
	p.managed, p.managedStale = tables, false
	if known {
		return nil
	}
	return p.adoptTables(ctx)
}

// adoptTables makes the list of tables for a position saved by a version that kept none, which means
// replication has been running: the ClickHouse tables of binwave's shape that are the targets of
// replicated tables of the source. It happens once, at the first start of a version that keeps the
// list; from then on a table gets into it only when binwave creates it or writes to it.
func (p *Pipeline) adoptTables(ctx context.Context) error {
	p.managedStale = true // saved with the next position, even when nothing is adopted
	if p.d.Snapshot == nil {
		p.log.Warn("list of tables started empty: no source catalog")
		return nil
	}
	var cat *schema.Catalog
	if err := p.fromSource(ctx, "list the tables of the source", func() error {
		var err error
		cat, err = p.d.Snapshot.Catalog(ctx, "", "")
		return err
	}); err != nil {
		return fmt.Errorf("list the tables of the source for the list of tables: %w", err)
	}
	targets := map[string][]string{} // ClickHouse database → the source tables written there
	for _, t := range cat.Tables {
		if !p.router.Match(t.Database, t.Name) {
			continue
		}
		tdb, _ := chsink.TargetName(p.cfg, t.Database, t.Name)
		targets[tdb] = append(targets[tdb], t.FullName())
	}
	databases := make([]string, 0, len(targets))
	for tdb := range targets {
		databases = append(databases, tdb)
	}
	sort.Strings(databases)
	for _, tdb := range databases {
		var names []string
		if err := p.withRetry(ctx, "list the tables of "+tdb, func(actx context.Context) error {
			var err error
			names, err = p.d.Sink.BinwaveTables(actx, tdb)
			return err
		}); err != nil {
			return fmt.Errorf("list the tables of %s for the list of tables: %w", tdb, err)
		}
		shaped := make(map[string]bool, len(names))
		for _, n := range names {
			shaped[n] = true
		}
		for _, name := range targets[tdb] {
			db, table, _ := cutQualified(name)
			_, ttable := chsink.TargetName(p.cfg, db, table)
			if shaped[ttable] {
				p.managed[name] = checkpoint.Target{Database: tdb, Table: ttable}
				p.log.Info("table adopted", "source", name, "target", tdb+"."+ttable)
			}
		}
	}
	p.log.Info("list of tables created", "tables", len(p.managed))
	return nil
}

// manage records that binwave writes the source table name into the ClickHouse table t.
func (p *Pipeline) manage(name string, t checkpoint.Target) {
	if cur, ok := p.managed[name]; ok && cur == t {
		return
	}
	p.managed[name] = t
	p.managedStale = true
}

// unmanage records that the source table name has no ClickHouse table of binwave's any more.
func (p *Pipeline) unmanage(name string) {
	if _, ok := p.managed[name]; !ok {
		return
	}
	delete(p.managed, name)
	p.managedStale = true
}

// materializeManaged puts the list of tables into the position about to be saved, if it changed. The
// list goes with the next save of the position, which every change of it is followed by: the commit of
// the transaction that bound, swapped in, renamed or dropped the table.
func (p *Pipeline) materializeManaged() {
	if p.managedStale {
		p.managedStale = false
		p.committed.Tables = checkpoint.EncodeTables(p.managed)
	}
}
