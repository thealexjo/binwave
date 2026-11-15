package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/buildinfo"
	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/engine"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/internal/sdnotify"
)

// runDeps are the process-level dependencies of `binwave run`.
type runDeps struct {
	env       sdnotify.EnvLookup
	pid       int
	newRunner func(cfg *config.Config, log *slog.Logger, hub *observability.Hub) (app.Runner, error)
	// serve starts the metrics and health server; it returns the function that stops it.
	serve func(addr string, h http.Handler, log *slog.Logger) (stop func(), err error)
	// newChecker builds what `binwave check --connect` runs.
	newChecker func(cfg *config.Config, log *slog.Logger) (checker, error)
	// newTables builds what `binwave tables` runs.
	newTables func(cfg *config.Config, log *slog.Logger) (tablesAdmin, error)
	// newControl builds the client of the control socket for `binwave replication` and `binwave tables`.
	newControl func(socket string) controlClient
}

// checker inspects both databases without replicating.
type checker interface {
	Check(ctx context.Context, out io.Writer) error
}

// tablesAdmin manages the quarantined tables of the saved state, without a running service.
type tablesAdmin interface {
	Quarantined(ctx context.Context) ([]checkpoint.Quarantined, error)
	Release(ctx context.Context, tables []string, force bool) (released, unknown []string, err error)
}

func productionDeps() runDeps {
	return runDeps{
		env: os.LookupEnv, pid: os.Getpid(), newRunner: newEngine, serve: observability.Serve,
		newChecker: newEngineChecker, newTables: newEngineTables,
		newControl: func(socket string) controlClient { return control.Client{Socket: socket} },
	}
}

func newEngineTables(cfg *config.Config, log *slog.Logger) (tablesAdmin, error) {
	return engine.New(cfg, log), nil
}

func newEngineChecker(cfg *config.Config, log *slog.Logger) (checker, error) {
	return engine.New(cfg, log), nil
}

// newEngine wires the production replication engine.
func newEngine(cfg *config.Config, log *slog.Logger, hub *observability.Hub) (app.Runner, error) {
	return engine.New(cfg, log, engine.WithObservability(hub)), nil
}

func runRun(ctx context.Context, args []string, stderr io.Writer, deps runDeps) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", defaultConfigPath, "path to the configuration file")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "binwave run: --config must not be empty")
		fs.Usage()
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "binwave run: no positional arguments are accepted")
		fs.Usage()
		return exitUsage
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "binwave run: %v\n", err)
		return exitConfig
	}

	// Logs go to stderr only: journald and Docker collect it, binwave never writes log files.
	log, err := observability.NewLogger(stderr, cfg.Observability)
	if err != nil {
		fmt.Fprintf(stderr, "binwave run: %v\n", err)
		return exitError
	}

	notifier, err := sdnotify.New(deps.env)
	if err != nil {
		log.Error("invalid systemd environment", "error", err)
		return exitError
	}
	watchdog, err := sdnotify.WatchdogTimeout(deps.env, deps.pid)
	if err != nil {
		log.Error("invalid systemd environment", "error", err)
		return exitError
	}

	// The hub and its server exist from the first moment, so that /healthz answers (and /readyz
	// says "not yet") while the service is still waiting for a database to come up.
	info := buildinfo.Get()
	hub := observability.NewHub(observability.Options{Version: info.Version, Commit: info.Commit, Pprof: cfg.Observability.Pprof})
	stopServing, err := deps.serve(cfg.Observability.Listen, hub.Handler(), log)
	if err != nil {
		return failure(log, err)
	}
	defer stopServing()

	runner, err := deps.newRunner(cfg, log, hub)
	if err != nil {
		return failure(log, err)
	}

	log.Info("starting", "config", *configPath)
	if err := app.Run(ctx, app.Deps{
		Logger:          log,
		Notifier:        notifier,
		Runner:          runner,
		WatchdogTimeout: watchdog,
		State:           hub,
	}); err != nil {
		return failure(log, err)
	}
	log.Info("stopped")
	return exitOK
}

// failure logs err and maps it to an exit code: configuration problems must not be restarted.
func failure(log *slog.Logger, err error) int {
	log.Error("binwave stopped with an error", "error", err)

	var ce *app.ConfigError
	if errors.As(err, &ce) {
		return exitConfig
	}
	return exitError
}
