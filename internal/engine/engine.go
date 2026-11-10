// Package engine assembles the production replication engine: a MySQL source, a ClickHouse sink
// and a position store, wired into a pipeline. It is the only place that knows the concrete
// components; everything else works through small interfaces.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/gtid"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/internal/pipeline"
	"github.com/thealexjo/binwave/internal/preflight"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/internal/typemap"
)

const (
	minIdlePeriod = 100 * time.Millisecond
	maxIdlePeriod = 5 * time.Second
)

// Engine runs the replication described by a configuration. It implements app.Runner.
type Engine struct {
	cfg   *config.Config
	log   *slog.Logger
	clock clock
	hub   *observability.Hub

	activeMu sync.Mutex
	active   *activeRun
}

// activeRun is what the control channel acts on while the service replicates.
type activeRun struct {
	repl *replicator
	p    *pipeline.Pipeline
	src  *mysqlsrc.Source
}

func (e *Engine) setActive(a *activeRun) {
	e.activeMu.Lock()
	e.active = a
	e.activeMu.Unlock()
}

func (e *Engine) current() *activeRun {
	e.activeMu.Lock()
	defer e.activeMu.Unlock()
	return e.active
}

// Option customizes an Engine.
type Option func(*Engine)

// WithObservability makes the engine report metrics and status to hub.
func WithObservability(hub *observability.Hub) Option {
	return func(e *Engine) { e.hub = hub }
}

// New creates an Engine.
func New(cfg *config.Config, log *slog.Logger, opts ...Option) *Engine {
	e := &Engine{cfg: cfg, log: log, clock: realClock()}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Run connects to both databases, verifies the source requirements, and replicates until ctx is
// cancelled. While the service is not ready yet, a database that is unreachable is retried for
// retry.startup_budget. Problems that a restart cannot fix are returned as *app.ConfigError.
func (e *Engine) Run(ctx context.Context, hooks app.Hooks) error {
	// The control channel exists from the first moment, like the observability server, so that
	// `binwave replication status` answers "starting" instead of "not running".
	if socket := e.cfg.Control.Socket; socket != "" {
		stop, err := control.Serve(socket, &controlBackend{e: e}, e.log)
		if err != nil {
			return err
		}
		defer stop()
	}
	return e.retryStartup(ctx, hooks, e.run)
}

// run is one attempt: it opens everything and replicates until it fails or ctx is cancelled.
func (e *Engine) run(ctx context.Context, hooks app.Hooks) error {
	cfg := e.cfg
	router := pipeline.NewRouter(cfg.Tables)

	// Reporting of replaced values (zero dates and the like) is rate limited; the summary is
	// written when the engine stops.
	logReporter := typemap.NewLogReporter(e.log, typemap.DefaultLogInterval, nil)
	defer logReporter.Flush()
	var reporter typemap.Reporter = logReporter
	var observer pipeline.Observer
	var onReconnect func()
	if e.hub != nil {
		reporter = e.hub.WrapReporter(logReporter)
		observer = e.hub
		onReconnect = e.hub.SourceReconnected
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	src := mysqlsrc.New(cfg.Source.MySQL, mysqlsrc.Options{
		Include:   router.Match,
		Databases: router.Databases(),
		// The idle heartbeat is also the timer for flushing by interval.
		IdlePeriod:      idlePeriod(time.Duration(cfg.Buffer.FlushInterval)),
		HeartbeatPeriod: time.Duration(cfg.Source.MySQL.HeartbeatPeriod),
		OnReconnect:     onReconnect,
		// A little slack over the initial load's own parallelism, for a `tables resync` alongside it.
		SnapshotPoolSize: cfg.Replication.Snapshot.Parallelism + 2,
	}, e.log)
	if err := src.Open(ctx); err != nil {
		return classify(err)
	}
	defer func() { _ = src.Close() }()

	sink, err := chsink.Open(ctx, cfg.Target.ClickHouse, chsink.Options{Logger: e.log, Writers: cfg.Buffer.InsertParallelism})
	if err != nil {
		return classify(fmt.Errorf("open ClickHouse: %w", err))
	}
	defer func() { _ = sink.Close() }()
	if err := e.checkClickHouseAccess(ctx, sink); err != nil {
		return classify(err)
	}

	store, err := e.store(ctx, sink)
	if err != nil {
		return classify(err)
	}

	// The preflight goroutine uses the source connection, so it must end before the deferred
	// closes above run: deferred calls run last-in first-out, hence this one is declared here.
	var background sync.WaitGroup
	defer func() {
		cancel(nil)
		background.Wait()
	}()

	guard := newTableGuard(src, cfg, e.log)
	p, err := pipeline.New(cfg, pipeline.Deps{
		Source:          src,
		Sink:            sink,
		Store:           store,
		Logger:          e.log,
		Reporter:        reporter,
		Guard:           guard,
		Observer:        observer,
		Snapshot:        src,
		SourceTransient: mysqlsrc.Transient,
		Retry: pipeline.RetryPolicy{
			Initial:        time.Duration(cfg.Retry.InitialDelay),
			Max:            time.Duration(cfg.Retry.MaxDelay),
			AttemptTimeout: time.Duration(cfg.Retry.AttemptTimeout),
		},
	})
	if err != nil {
		return classify(err)
	}

	// Starting means both databases are reachable and the rights are in place. Looking at every
	// table takes as long as there are tables, so it happens after the service is ready; what it
	// finds stops the service later, with a full list of the problems.
	ready := hooks.Ready
	var startPreflight, readied sync.Once
	readyOnce := func() {
		// The service is ready once, however many times replication starts.
		readied.Do(func() {
			if ready != nil {
				ready()
			}
		})
		startPreflight.Do(func() {
			background.Add(1)
			go func() {
				defer background.Done()
				e.preflight(ctx, src, guard, p, cancel)
			}()
		})
	}

	replication := newReplicator(!cfg.Replication.AutostartEnabled(), e.clock.now, hooks.Progress)
	replication.classify = func(err error) (failure.Failure, bool) {
		var fe *pipeline.FailedError
		if errors.As(err, &fe) {
			return fe.Failure, true
		}
		return failure.Failure{}, false
	}
	replication.onFailed = func(f failure.Failure) {
		red := f.Redacted() // logs carry no value of a row
		e.log.Error("["+red.Code+"] replication failed",
			"code", red.Code, "kind", red.Kind, "gtid", red.GTID, "table", red.Table, "operation", red.Op, "key", red.Key, "statement", red.Statement, "detail", red.Message)
		if e.hub != nil {
			e.hub.Failed(f)
		}
	}
	e.setActive(&activeRun{repl: replication, p: p, src: src})
	defer e.setActive(nil)
	// Where replication starts and which tables are set aside are settled before anything is
	// read: the control channel needs the list, and a service that starts paused must start from
	// the moment it was started, not from the moment it is resumed.
	if err := p.Prepare(ctx); err != nil {
		return classify(err)
	}

	if !cfg.Replication.AutostartEnabled() {
		// The service is up: both databases were reached and the rights checked, which is what
		// starting means. It reads nothing until it is told to.
		e.log.Info("replication not started (autostart: false)")
		readyOnce()
		if e.hub != nil {
			e.hub.Paused()
		}
	}

	err = replication.loop(ctx, func(rctx context.Context, req control.ResumeRequest) error {
		skip := pipeline.Skip{Count: req.SkipTransactions}
		if req.GTIDNext != "" {
			set, err := gtid.Parse(req.GTIDNext) // validated when the request was accepted
			if err != nil {
				return err
			}
			skip.GTIDs = set
		}
		p.SetSkip(skip)
		return p.Run(rctx, pipeline.Hooks{
			Ready: func() {
				readyOnce()
				if e.hub != nil {
					e.hub.Resumed()
				}
			},
			Progress: hooks.Progress,
		})
	})

	// The preflight stops the run with a cause; the pipeline has flushed and saved its position
	// on the way out, and the cause, not the cancellation, is what the operator needs to see.
	var cfgErr *app.ConfigError
	if errors.As(context.Cause(ctx), &cfgErr) {
		return cfgErr
	}
	return classify(err)
}

// quarantiner sets a table aside; the pipeline does it.
type quarantiner interface {
	Quarantine(table string, code errcode.Code, reason string) bool
}

// preflight checks the source tables against the configuration. A mistake of the configuration
// ends the run with a *app.ConfigError as the cause. A problem of a table sets that table aside
// and the rest goes on; with tables.on_replication_error: stop it ends the run like the mistake.
func (e *Engine) preflight(ctx context.Context, src catalogReader, guard *tableGuard, q quarantiner, stop context.CancelCauseFunc) {
	cat, err := src.Catalog(ctx, "", "")
	if err != nil {
		if ctx.Err() == nil {
			e.log.Warn("preflight skipped: catalog unreadable", "error", err)
		}
		return
	}
	report := preflight.Check(cat, e.cfg)
	guard.markChecked(cat, report)
	if err := guard.confirm(ctx, report); err != nil {
		return
	}
	report.Log(e.log)

	if err := report.ConfigErr(); err != nil {
		stop(&app.ConfigError{Err: err})
		return
	}
	problems := report.TableProblems()
	if len(problems) == 0 {
		return
	}
	if e.cfg.Tables.OnReplicationError == config.OnErrorStop {
		stop(&app.ConfigError{Err: &preflight.Error{Problems: problems}})
		return
	}
	// A table can have more than one problem; the code of the first one (Problems is sorted by
	// table) is the primary cause, the joined messages are the full detail.
	codes := map[string]errcode.Code{}
	reasons := map[string][]string{}
	var order []string
	for _, p := range problems {
		if _, ok := codes[p.Table]; !ok {
			codes[p.Table] = p.Code
			order = append(order, p.Table)
		}
		reasons[p.Table] = append(reasons[p.Table], p.Message)
	}
	for _, table := range order {
		q.Quarantine(table, codes[table], strings.Join(reasons[table], "; "))
	}
}

// checkClickHouseAccess verifies the privileges binwave needs on the databases it names in the
// configuration: its state database, and the target database when one is set. Without an explicit
// target database the targets are named like the source databases and only checked when a table
// is bound. Nothing is created here.
func (e *Engine) checkClickHouseAccess(ctx context.Context, sink *chsink.Client) error {
	cfg := e.cfg
	creates := !cfg.DDL.DryRun

	if cfg.Checkpoint.Store == config.CheckpointClickHouse {
		// The position store creates its table, then reads and writes it.
		if err := sink.CheckAccess(ctx, cfg.Target.ClickHouse.Database, "CREATE TABLE", "INSERT", "SELECT"); err != nil {
			return err
		}
	}
	if db := cfg.Tables.TargetDatabase; db != "" {
		privileges := []string{"INSERT"}
		if creates {
			// Tables are created, and columns added and widened as the source changes.
			privileges = append(privileges, "CREATE TABLE", "ALTER TABLE")
			if cfg.DDL.Mode == config.DDLFull {
				// Full mode mirrors what the source does to its tables: DROP TABLE and TRUNCATE, and
				// the rename of a table, for which ClickHouse asks for DROP TABLE on the source
				// (the swap of an online schema change is one).
				privileges = append(privileges, "DROP TABLE", "TRUNCATE")
			}
		}
		if err := sink.CheckAccess(ctx, db, privileges...); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) store(ctx context.Context, sink *chsink.Client) (pipeline.Store, error) {
	cp := e.cfg.Checkpoint
	switch cp.Store {
	case config.CheckpointFile:
		return checkpoint.NewFile(cp.FilePath), nil
	case config.CheckpointClickHouse:
		s := checkpoint.NewClickHouse(sink.Conn(), e.cfg.Target.ClickHouse.Database, cp.Table, nil)
		if err := s.Init(ctx); err != nil {
			return nil, fmt.Errorf("prepare the position store: %w", err)
		}
		return s, nil
	default:
		return nil, &app.ConfigError{Err: fmt.Errorf("checkpoint.store %q is not supported", cp.Store)}
	}
}

// idlePeriod is how often an idle stream reports in: the flush interval, within sane bounds.
func idlePeriod(flush time.Duration) time.Duration {
	return min(max(flush, minIdlePeriod), maxIdlePeriod)
}

// classify marks the failures that restarting cannot fix as configuration errors, so the
// process exits with EX_CONFIG and systemd does not restart it in a loop: unmet source
// requirements, an unsupported setting, a state of ClickHouse that contradicts the source
// (chsink.ConflictError), and answers of a database that repeat until someone changes
// credentials, privileges or the source itself.
func classify(err error) error {
	var req *mysqlsrc.RequirementsError
	var pre *preflight.Error
	var cfgErr *app.ConfigError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &cfgErr):
		return err
	case errors.As(err, &req), errors.As(err, &pre), errors.Is(err, pipeline.ErrUnsupported), chsink.IsConflict(err):
		return &app.ConfigError{Err: err}
	}
	if hint, ok := mysqlsrc.Permanent(err); ok {
		return &app.ConfigError{Err: fmt.Errorf("%s: %w", hint, err)}
	}
	if hint, ok := chsink.Permanent(err); ok {
		return &app.ConfigError{Err: fmt.Errorf("%s: %w", hint, err)}
	}
	return err
}
