// Command binwave replicates MySQL binlog changes into ClickHouse in real time.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/buildinfo"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/internal/termstyle"
)

// Exit codes. exitConfig matches EX_CONFIG and RestartPreventExitStatus in the systemd unit.
const (
	exitOK     = 0
	exitError  = 1
	exitUsage  = 2
	exitConfig = 78
)

// defaultConfigPath is used when --config is not given. One binwave instance per host is the
// common case (deploy/systemd/binwave.service), and typing --config on every command is
// unnecessary friction for it. Several instances on one host still need an explicit, per-instance
// --config (deploy/systemd/binwave@.service): the default only helps the single-instance case. A
// var, not a const, so tests can point it at a temporary path instead of the real one.
var defaultConfigPath = "/etc/binwave/config.yaml"

const usage = `Usage: binwave <command> [flags]

Commands:
  run          Replicate MySQL changes into ClickHouse until stopped
  check        Validate a configuration file; with --connect, also check both databases
  replication  Pause, resume or show the replication of a running service
  tables       List the quarantined tables (list), release them (release <table>...) or read them again (resync <table>...)
  version      Print build information
  help         Show this help

Run 'binwave <command> -h' for command flags.
`

func main() {
	// SIGTERM is the graceful stop of systemd and Docker; SIGINT is Ctrl+C in a terminal.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run executes the CLI with the production dependencies and returns the process exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runWith(ctx, args, stdout, stderr, productionDeps())
}

// runWith executes the CLI with explicit dependencies, so tests need no real environment.
func runWith(ctx context.Context, args []string, stdout, stderr io.Writer, deps runDeps) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}

	switch args[0] {
	case "run":
		return runRun(ctx, args[1:], stderr, deps)
	case "check":
		return runCheck(ctx, args[1:], stdout, stderr, deps)
	case "replication":
		return runReplication(ctx, args[1:], stdout, stderr, deps)
	case "tables":
		return runTables(ctx, args[1:], stdout, stderr, deps)
	case "version":
		printVersion(stdout)
		return exitOK
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "binwave: unknown command %q\n\n%s", args[0], usage)
		return exitUsage
	}
}

// printVersion prints the build information; on a terminal, beside the mark. The mark is shown
// here and nowhere else: this is the one command that is about binwave itself.
func printVersion(w io.Writer) {
	mark := termstyle.For(w).Mark()
	if mark == nil {
		fmt.Fprintln(w, buildinfo.Get())
		return
	}
	fmt.Fprintln(w)
	for i, line := range mark {
		if i == len(mark)/2 {
			line += "   " + buildinfo.Get().String()
		}
		fmt.Fprintln(w, "  "+line)
	}
	fmt.Fprintln(w)
}

// parseFlags parses the flags of fs wherever they are among the positional arguments, as in
// "tables resync shop.orders --config FILE", and returns the positional ones; after "--" every
// argument is positional.
func parseFlags(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, fmt.Errorf("parse flags: %w", err)
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		if len(args) > len(rest) && args[len(args)-len(rest)-1] == "--" {
			return append(positional, rest...), nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

// repeatFlags renders the named flags the call set to other than their default, shell-quoted, for
// the commands a hint suggests: copied, they act on the same instance as the call.
func repeatFlags(fs *flag.FlagSet, names ...string) string {
	var b strings.Builder
	fs.Visit(func(f *flag.Flag) {
		if slices.Contains(names, f.Name) && f.Value.String() != f.DefValue {
			b.WriteString(" --" + f.Name + " " + shellQuote(f.Value.String()))
		}
	})
	return b.String()
}

// shellQuote quotes s for a POSIX shell, leaving a plain word as it is.
func shellQuote(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./:@%+=,-") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func runCheck(ctx context.Context, args []string, stdout, stderr io.Writer, deps runDeps) int {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", defaultConfigPath, "path to the configuration file")
	printConfig := fs.Bool("print", false, "print the effective configuration with secrets masked")
	connect := fs.Bool("connect", false, "also connect to MySQL and ClickHouse and check requirements, privileges and tables; changes nothing")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "binwave check: --config must not be empty")
		fs.Usage()
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "binwave check: no positional arguments are accepted")
		fs.Usage()
		return exitUsage
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "binwave check: %v\n", err)
		return exitConfig
	}

	if *printConfig {
		out, err := cfg.RedactedYAML()
		if err != nil {
			fmt.Fprintf(stderr, "binwave check: %v\n", err)
			return exitError
		}
		_, _ = stdout.Write(out)
	} else {
		fmt.Fprintf(stdout, "config %s is valid\n", *configPath)
	}
	if !*connect {
		return exitOK
	}

	// The report goes to stdout; the engine's own logs stay quiet unless something is wrong.
	log, err := observability.NewLogger(stderr, config.Observability{LogLevel: "warn", LogFormat: config.LogText})
	if err != nil {
		fmt.Fprintf(stderr, "binwave check: %v\n", err)
		return exitError
	}
	runner, err := deps.newChecker(cfg, log)
	if err != nil {
		fmt.Fprintf(stderr, "binwave check: %v\n", err)
		return exitError
	}
	if err := runner.Check(ctx, stdout); err != nil {
		var ce *app.ConfigError
		if errors.As(err, &ce) {
			return exitConfig
		}
		return exitError
	}
	return exitOK
}
