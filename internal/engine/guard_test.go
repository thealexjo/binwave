package engine

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/preflight"
	"github.com/thealexjo/binwave/internal/schema"
)

type fakeCatalog struct {
	cat   *schema.Catalog
	err   error
	calls int
}

func (f *fakeCatalog) Catalog(_ context.Context, database, table string) (*schema.Catalog, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := &schema.Catalog{}
	for _, t := range f.cat.Tables {
		if table == "" || (t.Database == database && t.Name == table) {
			out.Tables = append(out.Tables, t)
		}
	}
	for _, fk := range f.cat.ForeignKeys {
		if table == "" || (fk.Database == database && fk.Table == table) {
			out.ForeignKeys = append(out.ForeignKeys, fk)
		}
	}
	return out, nil
}

func guardConfig(policy config.CascadePolicy) *config.Config {
	cfg := &config.Config{}
	cfg.Tables.ForeignKeyCascades = policy
	return cfg
}

func cascadingCatalog() *schema.Catalog {
	return &schema.Catalog{
		Tables: []schema.CatalogTable{
			{Database: "shop", Name: "orders", Columns: []string{"id"}, PrimaryKey: []string{"id"}},
			{Database: "shop", Name: "keyless", Columns: []string{"a"}},
		},
		ForeignKeys: []schema.ForeignKey{{Database: "shop", Table: "orders", Name: "fk", ReferencedTable: "users", UpdateRule: "RESTRICT", DeleteRule: "CASCADE"}},
	}
}

func TestGuardReturnsTheProblemOfATableForQuarantine(t *testing.T) {
	g := newTableGuard(&fakeCatalog{cat: cascadingCatalog()}, guardConfig(config.CascadesError), slog.New(slog.DiscardHandler))

	problem, err := g.CheckTable(context.Background(), "shop", "orders")

	var pe *preflight.Error
	if err != nil || !errors.As(problem, &pe) {
		t.Fatalf("CheckTable() = (%v, %v), want the table's problem and no error: one table must not stop the service", problem, err)
	}
}

func TestGuardStopsWithThePolicyStop(t *testing.T) {
	cfg := guardConfig(config.CascadesError)
	cfg.Tables.OnReplicationError = config.OnErrorStop
	g := newTableGuard(&fakeCatalog{cat: cascadingCatalog()}, cfg, slog.New(slog.DiscardHandler))

	problem, err := g.CheckTable(context.Background(), "shop", "orders")

	var pe *preflight.Error
	var cfgErr *app.ConfigError
	if problem != nil || !errors.As(err, &pe) || !errors.As(classify(err), &cfgErr) {
		t.Fatalf("CheckTable() = (%v, %v), want a configuration error, so the process exits with the configuration code", problem, err)
	}
}

func TestGuardStopsForAMistakeOfTheConfiguration(t *testing.T) {
	cfg := guardConfig(config.CascadesIgnore)
	cfg.Tables.Overrides = map[string]config.TableOverride{"shop.orders": {ExcludeColumns: []string{"typo"}}}
	g := newTableGuard(&fakeCatalog{cat: cascadingCatalog()}, cfg, slog.New(slog.DiscardHandler))

	problem, err := g.CheckTable(context.Background(), "shop", "orders")

	var pe *preflight.Error
	if problem != nil || !errors.As(err, &pe) {
		t.Fatalf("CheckTable() = (%v, %v): a typo in the configuration is not a table's problem, whatever the policy", problem, err)
	}
}

func TestGuardLetsACascadePassUnderWarnAndIgnore(t *testing.T) {
	for _, policy := range []config.CascadePolicy{config.CascadesWarn, config.CascadesIgnore} {
		var logs bytes.Buffer
		g := newTableGuard(&fakeCatalog{cat: cascadingCatalog()}, guardConfig(policy), slog.New(slog.NewTextHandler(&logs, nil)))

		if problem, err := g.CheckTable(context.Background(), "shop", "orders"); err != nil || problem != nil {
			t.Errorf("%s: CheckTable() = (%v, %v)", policy, problem, err)
		}
		if warned := strings.Contains(logs.String(), "foreign key"); warned != (policy == config.CascadesWarn) {
			t.Errorf("%s: warned = %v, logs: %s", policy, warned, logs.String())
		}
	}
}

func TestGuardSkipsTablesThePreflightHasVetted(t *testing.T) {
	reader := &fakeCatalog{cat: cascadingCatalog()}
	g := newTableGuard(reader, guardConfig(config.CascadesError), slog.New(slog.DiscardHandler))
	g.markChecked(reader.cat, &preflight.Report{})

	if problem, err := g.CheckTable(context.Background(), "shop", "orders"); err != nil || problem != nil || reader.calls != 0 {
		t.Errorf("(%v, %v), catalog reads = %d; a vetted table must not be read again", problem, err, reader.calls)
	}
}

// TestGuardChecksAForgottenTableAgain: the table was created again, now with a cascading foreign key.
func TestGuardChecksAForgottenTableAgain(t *testing.T) {
	clean := &schema.Catalog{Tables: cascadingCatalog().Tables}
	reader := &fakeCatalog{cat: clean}
	g := newTableGuard(reader, guardConfig(config.CascadesError), slog.New(slog.DiscardHandler))
	if problem, err := g.CheckTable(context.Background(), "shop", "orders"); err != nil || problem != nil {
		t.Fatalf("CheckTable() = (%v, %v) of a clean table", problem, err)
	}

	reader.cat = cascadingCatalog()
	g.Forget("shop", "orders")
	if problem, err := g.CheckTable(context.Background(), "shop", "orders"); err != nil || problem == nil {
		t.Errorf("CheckTable() = (%v, %v) after Forget, want the cascade found", problem, err)
	}
}

func TestGuardDoesNotStopForATableTheCatalogDoesNotShow(t *testing.T) {
	g := newTableGuard(&fakeCatalog{cat: &schema.Catalog{}}, guardConfig(config.CascadesError), slog.New(slog.DiscardHandler))
	if problem, err := g.CheckTable(context.Background(), "shop", "gone"); err != nil || problem != nil {
		t.Errorf("CheckTable() = (%v, %v): the binlog carries the table's rows, the user just cannot see its catalog entry", problem, err)
	}
}

func TestGuardReportsAReadFailure(t *testing.T) {
	boom := errors.New("connection lost")
	g := newTableGuard(&fakeCatalog{err: boom}, guardConfig(config.CascadesError), slog.New(slog.DiscardHandler))
	if _, err := g.CheckTable(context.Background(), "shop", "orders"); !errors.Is(err, boom) {
		t.Errorf("CheckTable() = %v", err)
	}
}

// fakeQuarantiner records the tables the preflight sets aside.
type fakeQuarantiner struct {
	tables map[string]string
	codes  map[string]errcode.Code
}

func (f *fakeQuarantiner) Quarantine(table string, code errcode.Code, reason string) bool {
	if f.tables == nil {
		f.tables = map[string]string{}
		f.codes = map[string]errcode.Code{}
	}
	f.tables[table] = reason
	f.codes[table] = code
	return true
}

func runPreflight(t *testing.T, cfg *config.Config, cat *schema.Catalog) (*fakeQuarantiner, context.Context) {
	t.Helper()
	e := &Engine{cfg: cfg, log: slog.New(slog.DiscardHandler)}
	reader := &fakeCatalog{cat: cat}
	q := &fakeQuarantiner{}
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	e.preflight(ctx, reader, newTableGuard(reader, cfg, e.log), q, cancel)
	return q, ctx
}

// TestPreflightSetsTheTablesAsideAndTheServiceGoesOn: problems of tables do not stop the service.
func TestPreflightSetsTheTablesAsideAndTheServiceGoesOn(t *testing.T) {
	q, ctx := runPreflight(t, guardConfig(config.CascadesError), cascadingCatalog())

	if ctx.Err() != nil {
		t.Fatalf("the run was stopped by problems of single tables: %v", context.Cause(ctx))
	}
	if len(q.tables) != 2 || q.tables["shop.orders"] == "" || q.tables["shop.keyless"] == "" {
		t.Errorf("quarantined %v, want shop.orders (a cascading foreign key) and shop.keyless (no key), each with its reason", q.tables)
	}
	if !strings.Contains(q.tables["shop.orders"], "fk") {
		t.Errorf("the reason %q does not say what is wrong", q.tables["shop.orders"])
	}
	if q.codes["shop.orders"] != errcode.TBL301 || q.codes["shop.keyless"] != errcode.TBL101 {
		t.Errorf("codes = %v, want shop.orders TBL301 (cascade) and shop.keyless TBL101 (no key)", q.codes)
	}
}

func TestPreflightStopsTheRunWithThePolicyStop(t *testing.T) {
	cfg := guardConfig(config.CascadesError)
	cfg.Tables.OnReplicationError = config.OnErrorStop
	q, ctx := runPreflight(t, cfg, cascadingCatalog())

	var cfgErr *app.ConfigError
	if !errors.As(context.Cause(ctx), &cfgErr) {
		t.Fatalf("cause = %v, want a ConfigError listing the problems", context.Cause(ctx))
	}
	msg := cfgErr.Error()
	if !strings.Contains(msg, "shop.orders") || !strings.Contains(msg, "shop.keyless") {
		t.Errorf("the message must list every problem at once: %s", msg)
	}
	if len(q.tables) != 0 {
		t.Errorf("tables were quarantined under the stop policy: %v", q.tables)
	}
}

func TestPreflightStopsForAMistakeOfTheConfigurationWhateverThePolicy(t *testing.T) {
	cfg := guardConfig(config.CascadesIgnore)
	cfg.Tables.Overrides = map[string]config.TableOverride{"shop.orders": {ExcludeColumns: []string{"typo"}}}
	q, ctx := runPreflight(t, cfg, cascadingCatalog())

	var cfgErr *app.ConfigError
	if !errors.As(context.Cause(ctx), &cfgErr) || !strings.Contains(cfgErr.Error(), "typo") {
		t.Fatalf("cause = %v, want the configuration mistake", context.Cause(ctx))
	}
	if len(q.tables) != 0 {
		t.Errorf("quarantined %v: nothing is set aside when the configuration is wrong", q.tables)
	}
}

func TestPreflightLeavesTheRunAloneWhenThereAreOnlyWarnings(t *testing.T) {
	cat := cascadingCatalog()
	cat.Tables = cat.Tables[:1] // drop the keyless table
	q, ctx := runPreflight(t, guardConfig(config.CascadesWarn), cat)

	if ctx.Err() != nil || len(q.tables) != 0 {
		t.Errorf("warnings stopped the run or quarantined %v: %v", q.tables, context.Cause(ctx))
	}
}

func TestPreflightThatCannotReadTheCatalogDoesNotStopTheRun(t *testing.T) {
	cfg := guardConfig(config.CascadesError)
	e := &Engine{cfg: cfg, log: slog.New(slog.DiscardHandler)}
	reader := &fakeCatalog{err: errors.New("catalog query failed")}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	e.preflight(ctx, reader, newTableGuard(reader, cfg, e.log), &fakeQuarantiner{}, cancel)

	if ctx.Err() != nil {
		t.Errorf("a failed catalog read stopped the run: %v; each table is still vetted when it is first written", context.Cause(ctx))
	}
}

// vanishingCatalog shows a table without a key on the first read and no table on the second, like
// a table that a statement of the binlog renames while it is being checked.
type vanishingCatalog struct {
	calls int
	then  *schema.Catalog
}

func (v *vanishingCatalog) Catalog(_ context.Context, database, table string) (*schema.Catalog, error) {
	v.calls++
	if v.calls == 1 {
		return &schema.Catalog{Tables: []schema.CatalogTable{{Database: database, Name: table, Columns: []string{"a"}}}}, nil
	}
	return v.then, nil
}

func TestGuardDoesNotSetATableAsideForWhatAConcurrentStatementChanged(t *testing.T) {
	cat := &vanishingCatalog{then: &schema.Catalog{}}
	g := newTableGuard(cat, guardConfig(config.CascadesError), slog.New(slog.DiscardHandler))

	problem, err := g.CheckTable(context.Background(), "shop", "_orders_new")

	if problem != nil || err != nil {
		t.Fatalf("CheckTable() = (%v, %v): a table that vanished while it was checked has no problem", problem, err)
	}
	if cat.calls != 2 {
		t.Errorf("the catalog was read %d times, want the problem confirmed by a second read", cat.calls)
	}
}

func TestGuardConfirmsAProblemBeforeReportingIt(t *testing.T) {
	still := &schema.Catalog{Tables: []schema.CatalogTable{{Database: "shop", Name: "keyless", Columns: []string{"a"}}}}
	g := newTableGuard(&vanishingCatalog{then: still}, guardConfig(config.CascadesError), slog.New(slog.DiscardHandler))

	problem, err := g.CheckTable(context.Background(), "shop", "keyless")

	var pe *preflight.Error
	if err != nil || !errors.As(problem, &pe) {
		t.Fatalf("CheckTable() = (%v, %v), want the problem of a table that is still keyless", problem, err)
	}
}

// changingCatalog shows, in the read of every table, a shadow table of an online migration without
// its key, as a read that runs into the statements of the migration does; read alone, it has one.
type changingCatalog struct{ alone []int }

func (c *changingCatalog) Catalog(_ context.Context, _, table string) (*schema.Catalog, error) {
	shadow := schema.CatalogTable{Database: "shop", Name: "_items_gho", Columns: []string{"id"}}
	if table == "" {
		return &schema.Catalog{Tables: []schema.CatalogTable{shadow}}, nil
	}
	c.alone = append(c.alone, len(c.alone))
	shadow.PrimaryKey = []string{"id"}
	return &schema.Catalog{Tables: []schema.CatalogTable{shadow}}, nil
}

func TestPreflightDoesNotSetATableAsideForWhatAConcurrentStatementChanged(t *testing.T) {
	for _, policy := range []config.ReplicationErrorPolicy{config.OnErrorQuarantine, config.OnErrorStop} {
		t.Run(string(policy), func(t *testing.T) {
			cfg := guardConfig(config.CascadesError)
			cfg.Tables.OnReplicationError = policy
			e := &Engine{cfg: cfg, log: slog.New(slog.DiscardHandler)}
			reader := &changingCatalog{}
			q := &fakeQuarantiner{}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)

			e.preflight(ctx, reader, newTableGuard(reader, cfg, e.log), q, cancel)

			if ctx.Err() != nil || len(q.tables) != 0 {
				t.Errorf("quarantined %v, cause %v: the problem is not there when the table is read again", q.tables, context.Cause(ctx))
			}
			if len(reader.alone) == 0 {
				t.Error("the problem was not confirmed by a read of the table alone")
			}
		})
	}
}

// pausingCatalog is fakeCatalog whose first read of a table alone waits until release is closed.
type pausingCatalog struct {
	fakeCatalog
	mu      sync.Mutex
	paused  bool
	reading chan struct{}
	release chan struct{}
}

func (c *pausingCatalog) Catalog(ctx context.Context, database, table string) (*schema.Catalog, error) {
	c.mu.Lock()
	pause := table != "" && !c.paused
	c.paused = c.paused || pause
	cat, err := c.fakeCatalog.Catalog(ctx, database, table)
	c.mu.Unlock()
	if pause {
		close(c.reading)
		<-c.release
	}
	return cat, err
}

// TestATableIsCheckedBeforeItsFirstRowWhileThePreflightConfirmsItsProblem: the preflight has not set
// the table aside yet, so a row of it must not bind unchecked.
func TestATableIsCheckedBeforeItsFirstRowWhileThePreflightConfirmsItsProblem(t *testing.T) {
	cfg := guardConfig(config.CascadesError)
	e := &Engine{cfg: cfg, log: slog.New(slog.DiscardHandler)}
	reader := &pausingCatalog{fakeCatalog: fakeCatalog{cat: cascadingCatalog()}, reading: make(chan struct{}), release: make(chan struct{})}
	g := newTableGuard(reader, cfg, e.log)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.preflight(ctx, reader, g, &fakeQuarantiner{}, cancel)
	}()
	<-reader.reading

	problem, err := g.CheckTable(ctx, "shop", "orders")
	close(reader.release)
	<-done

	if err != nil || problem == nil {
		t.Errorf("CheckTable() = (%v, %v) while the preflight confirms the cascade, want the cascade found", problem, err)
	}
}

// TestATableSetAsideIsCheckedAgainBeforeItsNextRow: a released table goes back to quarantine while
// the cause is there; a table without a problem is not read again.
func TestATableSetAsideIsCheckedAgainBeforeItsNextRow(t *testing.T) {
	cat := cascadingCatalog()
	cat.Tables = append(cat.Tables, schema.CatalogTable{Database: "shop", Name: "users", Columns: []string{"id"}, PrimaryKey: []string{"id"}})
	cfg := guardConfig(config.CascadesError)
	e := &Engine{cfg: cfg, log: slog.New(slog.DiscardHandler)}
	reader := &fakeCatalog{cat: cat}
	g := newTableGuard(reader, cfg, e.log)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	e.preflight(ctx, reader, g, &fakeQuarantiner{}, cancel)

	for range 2 {
		if problem, err := g.CheckTable(ctx, "shop", "orders"); err != nil || problem == nil {
			t.Errorf("CheckTable() = (%v, %v) of a table set aside, want the cascade found", problem, err)
		}
	}
	reads := reader.calls
	if problem, err := g.CheckTable(ctx, "shop", "users"); err != nil || problem != nil || reader.calls != reads {
		t.Errorf("CheckTable() = (%v, %v), catalog reads %d, want a vetted table not read again", problem, err, reader.calls-reads)
	}
}
