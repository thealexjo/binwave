package engine

import (
	"context"
	"log/slog"
	"sync"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/preflight"
	"github.com/thealexjo/binwave/internal/schema"
)

// catalogReader reads the source catalog; with a table given, only that table.
type catalogReader interface {
	Catalog(ctx context.Context, database, table string) (*schema.Catalog, error)
}

// tableGuard vets a table before the pipeline writes its first row. It runs the same per-table
// checks as the preflight, so a table created after the service started is held to the rules
// too, and it skips tables the preflight has already vetted.
type tableGuard struct {
	src catalogReader
	cfg *config.Config
	log *slog.Logger

	mu      sync.Mutex
	checked map[string]bool
}

func newTableGuard(src catalogReader, cfg *config.Config, log *slog.Logger) *tableGuard {
	return &tableGuard{src: src, cfg: cfg, log: log, checked: map[string]bool{}}
}

// markChecked records tables whose preflight report has been logged.
func (g *tableGuard) markChecked(cat *schema.Catalog) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, t := range cat.Tables {
		g.checked[t.FullName()] = true
	}
}

// CheckTable implements pipeline.Guard. A mistake of the configuration is an error, which stops
// the service. A problem of the table itself is returned as problem, for the pipeline to set the
// table aside; with tables.on_replication_error: stop it is an error too.
func (g *tableGuard) CheckTable(ctx context.Context, database, table string) (problem, err error) {
	full := database + "." + table
	g.mu.Lock()
	done := g.checked[full]
	g.mu.Unlock()
	if done {
		return nil, nil
	}

	report, cat, err := g.evaluate(ctx, database, table)
	if err != nil {
		return nil, err
	}
	if report == nil {
		// The table is gone already, or the user cannot see it. The stream carries its rows all the
		// same, so this is not a reason to stop.
		g.log.Warn("preflight: table not in catalog, keys unchecked",
			"table", full)
		return nil, nil
	}
	if len(report.Problems) > 0 {
		// The catalog is read live, while the table may be changed or renamed by the very
		// statements that follow in the binlog (the temporary table of an online schema change
		// lives seconds), and its keys are read in a query of their own: a table that changed
		// in between shows as one without a key. What is found is confirmed against a second
		// read before a table is set aside for it.
		if report, cat, err = g.evaluate(ctx, database, table); err != nil {
			return nil, err
		}
		if report == nil {
			g.log.Warn("preflight: table changed during check",
				"table", full)
			return nil, nil
		}
	}
	report.LogProblems(g.log)
	g.markChecked(cat)

	if err := report.ConfigErr(); err != nil {
		return nil, err
	}
	if problems := report.TableProblems(); len(problems) > 0 {
		e := &preflight.Error{Problems: problems}
		if g.cfg.Tables.OnReplicationError == config.OnErrorStop {
			return nil, e
		}
		return e, nil
	}
	return nil, nil
}

// evaluate reads the table from the source catalog and checks it. The report is nil when the
// catalog does not show the table.
func (g *tableGuard) evaluate(ctx context.Context, database, table string) (*preflight.Report, *schema.Catalog, error) {
	cat, err := g.src.Catalog(ctx, database, table)
	if err != nil {
		return nil, nil, err
	}
	if len(cat.Tables) == 0 {
		return nil, cat, nil
	}
	return &preflight.Report{Tables: 1, Problems: preflight.CheckTable(cat.Tables[0], cat.ForeignKeys, g.cfg)}, cat, nil
}
