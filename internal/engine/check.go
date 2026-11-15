package engine

import (
	"context"
	"fmt"
	"io"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/pipeline"
	"github.com/thealexjo/binwave/internal/preflight"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/internal/termstyle"
)

// Check connects to both databases and runs everything the service checks at start and right
// after it, without replicating and without changing anything in either database. It writes what
// it finds to out. The error is a *app.ConfigError when the configuration cannot work with these
// databases, and any other error when a database could not be reached.
func (e *Engine) Check(ctx context.Context, out io.Writer) error {
	cfg := e.cfg
	router := pipeline.NewRouter(cfg.Tables)
	// On a terminal a failed check and each problem get a color; ok stays plain.
	ts := termstyle.For(out)
	failed := ts.Failed("FAILED")

	src := mysqlsrc.New(cfg.Source.MySQL, mysqlsrc.Options{Include: router.Match, Databases: router.Databases()}, e.log)
	if err := src.Open(ctx); err != nil {
		fmt.Fprintf(out, "MySQL      %s  %v\n", failed, err)
		return classify(err)
	}
	defer func() { _ = src.Close() }()
	fmt.Fprintf(out, "MySQL      ok      %s:%d, requirements met, databases visible: %v\n",
		cfg.Source.MySQL.Host, cfg.Source.MySQL.Port, router.Databases())

	sink, err := chsink.Open(ctx, cfg.Target.ClickHouse, chsink.Options{Logger: e.log})
	if err != nil {
		err = fmt.Errorf("open ClickHouse: %w", err)
		fmt.Fprintf(out, "ClickHouse %s  %v\n", failed, err)
		return classify(err)
	}
	defer func() { _ = sink.Close() }()
	if err := e.checkClickHouseAccess(ctx, sink); err != nil {
		fmt.Fprintf(out, "ClickHouse %s  %v\n", failed, err)
		return classify(err)
	}
	fmt.Fprintf(out, "ClickHouse ok      %v, privileges granted\n", cfg.Target.ClickHouse.Addresses)
	if cfg.Checkpoint.Store == config.CheckpointFile {
		if err := checkPositionStore(cfg.Checkpoint); err != nil {
			fmt.Fprintf(out, "Position   %s  %v\n", failed, err)
			return err
		}
		fmt.Fprintf(out, "Position   ok      %s can be saved\n", cfg.Checkpoint.FilePath)
	}

	cat, err := src.Catalog(ctx, "", "")
	if err != nil {
		fmt.Fprintf(out, "Tables     %s  %v\n", failed, err)
		return classify(err)
	}
	report := preflight.Check(cat, cfg)
	quarantine := cfg.Tables.OnReplicationError == config.OnErrorQuarantine
	// A problem of one table only stops the service when tables.on_replication_error says so;
	// otherwise the table would be set aside, and the report says that instead of "error".
	stopping := len(report.ConfigProblems()) > 0 || (!quarantine && len(report.TableProblems()) > 0)
	verdict := "ok     "
	if stopping {
		verdict = failed + " "
	}
	fmt.Fprintf(out, "Tables     %s  %d checked, %d errors, %d warnings, %d would be quarantined\n",
		verdict, report.Tables, len(report.ConfigProblems())+stoppingTableProblems(report, quarantine),
		len(report.Warnings()), quarantinedTables(report, quarantine))
	for _, p := range report.Problems {
		label := fmt.Sprintf("%-10s", p.Severity.String())
		switch {
		case quarantine && p.Severity == preflight.SevError && p.Scope == preflight.ScopeTable:
			label = ts.Pending(fmt.Sprintf("%-10s", "quarantine"))
		case p.Severity == preflight.SevError:
			label = ts.Failed(label)
		default:
			label = ts.Pending(label)
		}
		fmt.Fprintf(out, "  %s %s\n", label, p)
	}
	if stopping {
		if err := report.ConfigErr(); err != nil {
			return classify(err)
		}
		return classify(&preflight.Error{Problems: report.TableProblems()})
	}
	return nil
}

func stoppingTableProblems(r *preflight.Report, quarantine bool) int {
	if quarantine {
		return 0
	}
	return len(r.TableProblems())
}

// quarantinedTables counts the tables that would be set aside.
func quarantinedTables(r *preflight.Report, quarantine bool) int {
	if !quarantine {
		return 0
	}
	tables := map[string]bool{}
	for _, p := range r.TableProblems() {
		tables[p.Table] = true
	}
	return len(tables)
}
