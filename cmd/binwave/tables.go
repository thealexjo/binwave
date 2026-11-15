package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/internal/termstyle"
)

const tablesUsage = `Usage: binwave tables <list|release|resync> [--config FILE] [flags] [table...]

  --config defaults to /etc/binwave/config.yaml.

  list                     Show the tables that are quarantined: set aside because they cannot be replicated
  release --accept-data-gap <db.table>...
                           Replicate the tables again from now on. The changes made while a table was
                           quarantined are NOT in ClickHouse: releasing does not bring them, and the
                           flag says you know. Fix the cause of the quarantine first.

  resync <db.table>...     Read the tables again from the source, into a shadow table next to each one, while
                           replication goes on, and replace the table with the copy when it is complete. The
                           old table is dropped by the swap. Only a running service can do it. A quarantined
                           table leaves quarantine when its resync begins; "binwave replication status" shows
                           how far each resync is.

With a running service the commands go through its control socket: control.socket of the
configuration, /run/binwave/binwave.sock by default; nothing else of the configuration is read, so
the secrets the service is started with are not needed. Without one, list and release work on the
saved state, with the whole configuration and its secrets; then release refuses while something
answers on observability.listen, because a service would save its own state over the change
(--force skips that check).
`

// runTables implements `binwave tables`.
func runTables(ctx context.Context, args []string, stdout, stderr io.Writer, deps runDeps) int {
	if len(args) == 0 || (args[0] != "list" && args[0] != "release" && args[0] != "resync") {
		fmt.Fprint(stderr, tablesUsage)
		return exitUsage
	}
	sub := args[0]

	fs := flag.NewFlagSet("tables "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", defaultConfigPath, "path to the configuration file")
	force := fs.Bool("force", false, "release even if the service seems to be running")
	acceptGap := fs.Bool("accept-data-gap", false, "release: acknowledge that the changes made while the tables were quarantined stay missing in ClickHouse")
	positional, err := parseFlags(fs, args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if *configPath == "" {
		fmt.Fprintf(stderr, "binwave tables %s: --config must not be empty\n", sub)
		return exitUsage
	}
	if sub == "list" && len(positional) > 0 {
		fmt.Fprintln(stderr, "binwave tables list: no positional arguments are accepted")
		return exitUsage
	}
	if (sub == "release" || sub == "resync") && len(positional) == 0 {
		fmt.Fprintf(stderr, "binwave tables %s: name the tables, as database.table\n", sub)
		return exitUsage
	}
	if sub == "release" && !*acceptGap {
		fmt.Fprintln(stderr, "binwave tables release: releasing a table does not bring back the changes made while it was quarantined; "+
			"they stay missing in ClickHouse until the table is resynchronized. Fix the cause of the quarantine, then repeat the command with --accept-data-gap")
		return exitUsage
	}

	socket, err := config.ControlSocket(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "binwave tables %s: %v\n", sub, err)
		return exitConfig
	}
	c := deps.newControl(socket)
	if sub == "resync" {
		return resyncTables(ctx, c, positional, repeatFlags(fs, "config"), stdout, stderr)
	}
	// offline is what works on the saved state without a running service: it needs the whole
	// configuration, secrets included.
	offline := func() (tablesAdmin, int) {
		cfg, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintf(stderr, "binwave tables %s: no service answers on %s, and the saved state cannot be read: %v\n", sub, socket, err)
			return nil, exitConfig
		}
		log, err := observability.NewLogger(stderr, config.Observability{LogLevel: "warn", LogFormat: config.LogText})
		if err != nil {
			fmt.Fprintf(stderr, "binwave tables %s: %v\n", sub, err)
			return nil, exitError
		}
		admin, err := deps.newTables(cfg, log)
		if err != nil {
			fmt.Fprintf(stderr, "binwave tables %s: %v\n", sub, err)
			return nil, exitError
		}
		return admin, exitOK
	}

	if sub == "list" {
		entries, err := c.Quarantined(ctx)
		if errors.Is(err, control.ErrNotRunning) {
			admin, code := offline()
			if admin == nil {
				return code
			}
			entries, err = admin.Quarantined(ctx)
		}
		if err != nil {
			fmt.Fprintf(stderr, "binwave tables list: %v\n", err)
			return exitError
		}
		if len(entries) == 0 {
			fmt.Fprintln(stdout, "no tables are quarantined")
			return exitOK
		}
		ts := termstyle.For(stdout)
		for _, en := range entries {
			fmt.Fprintf(stdout, "%s\n  since:  %s (position when it was set aside)\n  at:     %s\n  code:   %s (see docs/reference/error-codes.md)\n  reason: %s\n",
				ts.Pending(en.Table), en.Since, en.At.UTC().Format(time.RFC3339), en.Code, en.Reason)
			if en.Recoverable {
				fmt.Fprintln(stdout, "  recoverable: yes (tables.autoresync would resynchronize it on its own if enabled)")
			}
			if en.Released() {
				fmt.Fprintf(stdout, "  released: its changes up to %s are still skipped; the table replicates again after them\n", en.Until)
			}
		}
		return exitOK
	}

	released, unknown, err := c.Release(ctx, positional)
	if errors.Is(err, control.ErrNotRunning) {
		admin, code := offline()
		if admin == nil {
			return code
		}
		released, unknown, err = admin.Release(ctx, positional, *force)
	}
	if err != nil {
		fmt.Fprintf(stderr, "binwave tables release: %v\n", err)
		return exitError
	}
	for _, name := range released {
		fmt.Fprintf(stdout, "released %s: its changes after this moment are replicated. Its changes made while it was quarantined are not in ClickHouse.\n", name)
	}
	for _, name := range unknown {
		fmt.Fprintf(stderr, "%s is not quarantined\n", name)
	}
	if len(unknown) > 0 {
		return exitError
	}
	return exitOK
}

// resyncTables asks for the resyncs and reports which began.
func resyncTables(ctx context.Context, c controlClient, tables []string, again string, stdout, stderr io.Writer) int {
	results, err := c.Resync(ctx, tables)
	if errors.Is(err, control.ErrNotRunning) {
		fmt.Fprintf(stderr, "binwave tables resync: %v: a resync is run by the service, start it first\n", err)
		return exitError
	}
	if err != nil {
		fmt.Fprintf(stderr, "binwave tables resync: %v\n", err)
		return exitError
	}
	code := exitOK
	for _, r := range results {
		if r.Error != "" {
			fmt.Fprintf(stderr, "%s: not resynchronized: %s\n", r.Table, r.Error)
			code = exitError
			continue
		}
		fmt.Fprintf(stdout, "%s: resync begun. It goes on in the service; `binwave replication status%s` shows how far it is\n", r.Table, again)
	}
	return code
}
