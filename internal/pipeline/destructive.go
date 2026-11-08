package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// destructiveDDL handles the statements that remove data without row events: DROP TABLE,
// TRUNCATE and DROP DATABASE, which ddl.mode full mirrors in ClickHouse (a replica does what the
// source did; binwave is not a backup, and the source is the one that decides what is dropped),
// and partition operations, which are not applied yet. ddl.mode additive removes nothing: the
// statement is logged and counted, and ClickHouse keeps the data.
//
// Everything mirrored is idempotent (IF EXISTS), so a replay after a crash finds the work done, and
// the barrier of the DDL event has written every earlier row first, so none survives a TRUNCATE.
func (p *Pipeline) destructiveDDL(ctx context.Context, e event.DDL, d destructive) error {
	if d.kind == "partition operation" {
		if !p.replicatesAny(d) {
			return nil
		}
		p.d.Observer.DestructiveDDLSkipped()
		p.log.Warn("partition DDL skipped, rows kept", "statement", e.Query)
		return nil
	}
	if d.any {
		p.d.Observer.DestructiveDDLSkipped()
		p.log.Warn("unparsed destructive DDL skipped",
			"kind", d.kind, "statement", e.Query)
		return nil
	}
	if d.kind == "DROP DATABASE" {
		return p.dropDatabase(ctx, e, d.database)
	}

	for _, name := range d.tables {
		db, table, ok := cutQualified(name)
		if !ok || !p.router.Match(db, table) {
			continue
		}
		if p.cfg.DDL.Mode != config.DDLFull {
			p.d.Observer.DestructiveDDLSkipped()
			p.log.Warn("DROP/TRUNCATE skipped (ddl.mode additive)",
				"kind", d.kind, "table", name, "statement", e.Query)
			continue
		}
		var err error
		if d.kind == "DROP TABLE" {
			err = p.dropTable(ctx, db, table)
		} else {
			err = p.truncateTable(ctx, db, table)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// replicatesAny reports whether the statement touches a table that is replicated.
func (p *Pipeline) replicatesAny(d destructive) bool {
	for _, name := range d.tables {
		if db, table, ok := cutQualified(name); ok && p.router.Match(db, table) {
			return true
		}
	}
	return false
}

func (p *Pipeline) dropTable(ctx context.Context, db, table string) error {
	name := db + "." + table
	p.forgetTable(name)
	if p.q.move(name, "", false) { // the table is gone, so is what set it aside
		p.qChanged.Store(true)
		p.syncQuarantine()
		p.d.Observer.QuarantineChanged(p.q.active())
	}
	req := chsink.DropRequest{Source: name, DryRun: p.cfg.DDL.DryRun}
	req.Database, req.Table = chsink.TargetName(p.cfg, db, table)
	err := p.withRetry(ctx, "drop table "+name, func(actx context.Context) error {
		return p.d.Sink.DropTable(actx, req)
	})
	if err != nil {
		return fmt.Errorf("drop table %s: %w", name, err)
	}
	return nil
}

func (p *Pipeline) truncateTable(ctx context.Context, db, table string) error {
	name := db + "." + table
	if skip, _, err := p.q.skip(name, p.tx.gtid); err != nil {
		return err
	} else if skip {
		return nil // set aside: its changes are not followed
	}
	req := chsink.TruncateRequest{Source: name, DryRun: p.cfg.DDL.DryRun}
	req.Database, req.Table = chsink.TargetName(p.cfg, db, table)
	err := p.withRetry(ctx, "truncate table "+name, func(actx context.Context) error {
		return p.d.Sink.TruncateTable(actx, req)
	})
	if err != nil {
		return fmt.Errorf("truncate table %s: %w", name, err)
	}
	return nil
}

// dropDatabase drops the tables of the database that binwave replicated, in ClickHouse. The
// database itself is not dropped: it may hold more than binwave's tables. Which tables came from the
// dropped database is read from ClickHouse, since the source no longer has them: the tables that
// have binwave's shape, in the ClickHouse database of that source database. With
// tables.target_database every database goes to one ClickHouse database, and only the tables this
// process has written under their source name are known to belong to the dropped database.
func (p *Pipeline) dropDatabase(ctx context.Context, e event.DDL, db string) error {
	if !p.router.SelectsDatabase(db) {
		return nil
	}
	if p.cfg.DDL.Mode != config.DDLFull {
		p.d.Observer.DestructiveDDLSkipped()
		p.log.Warn("DROP DATABASE skipped (ddl.mode additive)",
			"database", db, "statement", e.Query)
		return nil
	}
	chDB, _ := chsink.TargetName(p.cfg, db, "")
	var names []string
	err := p.withRetry(ctx, "list the tables of "+chDB, func(actx context.Context) error {
		var err error
		names, err = p.d.Sink.BinwaveTables(actx, chDB)
		return err
	})
	if err != nil {
		return fmt.Errorf("drop database %s: %w", db, err)
	}
	sort.Strings(names)
	for _, n := range names {
		table := p.sourceTableOf(db, n)
		if !p.router.Match(db, table) {
			continue
		}
		if p.cfg.Tables.TargetDatabase != "" && p.targets["`"+chDB+"`.`"+n+"`"] != db+"."+table {
			p.log.Warn("table kept: source database unknown",
				"database", db, "clickhouse_table", chDB+"."+n)
			continue
		}
		if err := p.dropTable(ctx, db, table); err != nil {
			return err
		}
	}
	return nil
}

// sourceTableOf is the source table that is written to the ClickHouse table n of the database db.
func (p *Pipeline) sourceTableOf(db, n string) string {
	for key, ov := range p.cfg.Tables.Overrides {
		if ov.TargetTable == n && strings.HasPrefix(key, db+".") {
			return strings.TrimPrefix(key, db+".")
		}
	}
	return n
}
