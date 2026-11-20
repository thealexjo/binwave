package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/control"
	failinfo "github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/internal/snapshot"
	"github.com/thealexjo/binwave/internal/termstyle"
)

const replicationUsage = `Usage: binwave replication <pause|resume|status|snapshot-rate|chunk-size> [--config FILE] [--skip-transactions N] [--gtid-next GTID]

  --config defaults to /etc/binwave/config.yaml.

  pause     Stop replication: buffers are written, the position is saved and the binlog stream is
            closed. The service stays up (health and metrics answer) and reads nothing until resume.
            The source keeps its binary logs, so nothing is lost; do not pause for longer than the
            source keeps them (binlog_expire_logs_seconds).
  resume    Start replication again from the saved position. After a failure it tries the failed
            transaction once more; --skip-transactions N drops the next N transactions, starting
            with the failed one (like MySQL's sql_replica_skip_counter), and --gtid-next UUID:N
            (UUID:TAG:N for a tagged GTID) drops the transaction with that GTID (like the empty
            transaction injected with SET GTID_NEXT). A skipped transaction is NOT applied to
            ClickHouse. Skipping is possible only while replication is stopped by a failure or a
            pause.
  status    Show the state of the running service. After a failure it shows which transaction
            failed, what it was doing (statement, or table, operation and the key of the row,
            with its values: they can be personal data) and why.
  snapshot-rate N|unlimited
            Change the combined speed limit of the initial load while it runs (rows/s across every
            table it is reading at once); it does not affect "tables resync". Takes effect at once
            and is not saved: after a restart the limit is replication.snapshot.max_rows_per_second
            again. No effect once the initial load has finished.
  chunk-size N|adaptive
            Change how many rows one chunk of a snapshot read asks for: N rows in every chunk, or
            adaptive to size each chunk from how long the ones before it took. Unlike snapshot-rate
            this applies to every snapshot read, a "tables resync" included, and it overrides
            replication.snapshot.chunk_size and adaptive_chunks. The bound on the weight of a chunk
            (replication.snapshot.chunk_max_bytes) still holds, so a table with very wide rows can
            still get smaller chunks than asked. Takes effect on the next chunk of every table being
            read and is not saved: after a restart the configuration decides again.

The commands talk to a running service over its Unix socket: control.socket of the configuration,
/run/binwave/binwave.sock by default. Nothing else of the configuration is read, so the secrets the
service is started with are not needed.
`

// controlClient is what the commands need from the control channel of a running service.
type controlClient interface {
	Pause(ctx context.Context) error
	Resume(ctx context.Context, req control.ResumeRequest) error
	Status(ctx context.Context) (observability.Status, error)
	SnapshotRate(ctx context.Context, rowsPerSecond int) error
	ChunkSize(ctx context.Context, rows int) error
	Quarantined(ctx context.Context) ([]checkpoint.Quarantined, error)
	Release(ctx context.Context, tables []string) (released, unknown []string, err error)
	Resync(ctx context.Context, tables []string) ([]control.ResyncResult, error)
}

// replicationSubcommands are the commands `binwave replication` takes.
var replicationSubcommands = map[string]bool{
	"pause": true, "resume": true, "status": true, "snapshot-rate": true, "chunk-size": true,
}

// runReplication implements `binwave replication`.
func runReplication(ctx context.Context, args []string, stdout, stderr io.Writer, deps runDeps) int {
	if len(args) == 0 || !replicationSubcommands[args[0]] {
		fmt.Fprint(stderr, replicationUsage)
		return exitUsage
	}
	sub := args[0]

	fs := flag.NewFlagSet("replication "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", defaultConfigPath, "path to the configuration file")
	var req control.ResumeRequest
	if sub == "resume" {
		fs.IntVar(&req.SkipTransactions, "skip-transactions", 0, "skip this many transactions, starting with the failed one")
		fs.StringVar(&req.GTIDNext, "gtid-next", "", "skip the transaction with this GTID (uuid:sequence or uuid:tag:sequence)")
	}
	positional, err := parseFlags(fs, args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if *configPath == "" {
		fmt.Fprintf(stderr, "binwave replication %s: --config must not be empty\n", sub)
		return exitUsage
	}
	wantPositional := sub == "snapshot-rate" || sub == "chunk-size"
	if (wantPositional && len(positional) != 1) || (!wantPositional && len(positional) > 0) {
		switch sub {
		case "snapshot-rate":
			fmt.Fprintln(stderr, "binwave replication snapshot-rate: takes exactly one argument, N or unlimited")
		case "chunk-size":
			fmt.Fprintln(stderr, "binwave replication chunk-size: takes exactly one argument, N or adaptive")
		default:
			fmt.Fprintf(stderr, "binwave replication %s: no positional arguments are accepted\n", sub)
		}
		return exitUsage
	}
	var snapshotRate int
	if sub == "snapshot-rate" {
		arg := positional[0]
		if arg == "unlimited" {
			snapshotRate = 0
		} else {
			n, err := strconv.Atoi(arg)
			if err != nil || n < 0 {
				fmt.Fprintf(stderr, "binwave replication snapshot-rate: %q must be a non-negative number of rows per second, or unlimited\n", arg)
				return exitUsage
			}
			snapshotRate = n
		}
	}
	// 0 is adaptive sizing: the size of a chunk is at least one row, so it is not a size.
	var chunkSize int
	if sub == "chunk-size" {
		arg := positional[0]
		if arg != "adaptive" {
			n, err := strconv.Atoi(arg)
			if err != nil || n < 1 {
				fmt.Fprintf(stderr, "binwave replication chunk-size: %q must be a number of rows of at least 1, or adaptive\n", arg)
				return exitUsage
			}
			chunkSize = n
		}
	}

	socket, err := config.ControlSocket(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "binwave replication %s: %v\n", sub, err)
		return exitConfig
	}
	c := deps.newControl(socket)
	again := repeatFlags(fs, "config")

	switch sub {
	case "pause":
		if err := c.Pause(ctx); err != nil {
			return replicationFailure(stderr, sub, err)
		}
		fmt.Fprintf(stdout, "replication is paused. The service is up and reads nothing; `binwave replication resume%s` starts it again from the saved position.\n", again)
	case "resume":
		if err := c.Resume(ctx, req); err != nil {
			return replicationFailure(stderr, sub, err)
		}
		if req.Skips() {
			fmt.Fprintln(stdout, "the transactions are skipped: their changes are NOT applied to ClickHouse.")
		}
		fmt.Fprintf(stdout, "replication resumes from the saved position; `binwave replication status%s` shows when it runs.\n", again)
	case "snapshot-rate":
		if err := c.SnapshotRate(ctx, snapshotRate); err != nil {
			return replicationFailure(stderr, sub, err)
		}
		if snapshotRate == 0 {
			fmt.Fprintln(stdout, "the initial load's rate limit is removed.")
		} else {
			fmt.Fprintf(stdout, "the initial load's combined rate limit is now %d rows/s.\n", snapshotRate)
		}
	case "chunk-size":
		if err := c.ChunkSize(ctx, chunkSize); err != nil {
			return replicationFailure(stderr, sub, err)
		}
		if chunkSize == 0 {
			fmt.Fprintln(stdout, "the size of the chunks of snapshot reads follows how long they take again.")
		} else {
			fmt.Fprintf(stdout, "every chunk of a snapshot read now asks for %d rows (the bound on its weight still holds).\n", chunkSize)
		}
	default:
		st, err := c.Status(ctx)
		if err != nil {
			return replicationFailure(stderr, sub, err)
		}
		printStatus(stdout, st, again)
	}
	return exitOK
}

// describeResync is one line about a running resync.
// barWidth is how many cells a progress bar has. Fixed, so lines line up under each other.
const barWidth = 20

// bar renders a progress bar for a fraction of 0 to 1, or an empty one with no fill for a fraction
// that is not known (below zero).
func bar(ts termstyle.Style, fraction float64) string { return twoToneBar(ts, fraction, 0) }

// twoToneBar renders done as "#" and, after it, pending as "=": work that is finished and work that
// is complete but waiting on something else. The initial load needs both, because a table that is
// read is not yet a table that is loaded — it waits for the stream to reach the point it was read at.
// On a terminal the two zones also get the two colors of the mark, blue and orange.
func twoToneBar(ts termstyle.Style, done, pending float64) string {
	if done < 0 {
		return "[" + strings.Repeat("·", barWidth) + "]"
	}
	// A zone that has something in it gets a cell even when it rounds to none: 137 of 8000 tables is
	// a third of a cell, and rounding it away is how a bar stands still while the work moves.
	cells := func(f float64) int {
		n := int(math.Round(min(max(f, 0), 1) * barWidth))
		if n == 0 && f > 0 {
			n = 1
		}
		return n
	}
	filled := min(cells(done), barWidth)
	half := min(cells(pending), barWidth-filled)
	return "[" + ts.Done(strings.Repeat("#", filled)) + ts.Pending(strings.Repeat("=", half)) +
		strings.Repeat("·", barWidth-filled-half) + "]"
}

// printInitialLoad prints the initial load as a summary and a line per table being read: a source
// with thousands of tables must give a status of a fixed height, not one line per table.
func printInitialLoad(w io.Writer, ts termstyle.Style, l *observability.InitialLoad, statuses []snapshot.Status) {
	if l == nil {
		return
	}
	switch {
	case l.Total > 0:
		// Two zones: tables that are in (swapped) and tables that are read and only waiting for the
		// stream. A load whose stream lags has nothing swapped for a long while, and a bar that shows
		// only that would stand still although every one of those tables was read in the meantime.
		loaded := float64(l.Loaded) / float64(l.Total)
		fmt.Fprintf(w, "initial load: %d/%d tables loaded %s %.0f%%\n",
			l.Loaded, l.Total, twoToneBar(ts, loaded, float64(l.WaitingForStream)/float64(l.Total)), loaded*100)
	default:
		// A position saved before the total was recorded: how many are left is all that is known.
		fmt.Fprintf(w, "initial load: %d tables left\n", l.Left)
	}
	fmt.Fprintf(w, "              %d read and waiting for the stream, %d reading", l.WaitingForStream, l.Reading)
	if l.RowsPerSecond > 0 {
		fmt.Fprintf(w, " (%.0f rows/s together)", l.RowsPerSecond)
	}
	fmt.Fprintf(w, ", %d waiting for a slot\n", l.WaitingForSlot)

	var reading []snapshot.Status
	for _, r := range statuses {
		if r.Kind == "initial_load" && r.Reading {
			reading = append(reading, r)
		}
	}
	for _, r := range reading {
		fmt.Fprintf(w, "              %s\n", describeRead(ts, r))
	}
}

// nameWidth is the width of the table name column. Fixed, not taken from the names on screen: the
// tables being read change all the time, and a column that followed them would move every bar.
const nameWidth = 48

// tableName fits a table name to the name column: padded when shorter, cut in the middle when longer,
// so the database and the end of the table name both stay visible.
func tableName(name string) string {
	runes := []rune(name)
	if len(runes) <= nameWidth {
		return name + strings.Repeat(" ", nameWidth-len(runes))
	}
	head := (nameWidth - 1) / 2
	tail := nameWidth - 1 - head
	return string(runes[:head]) + "…" + string(runes[len(runes)-tail:])
}

// describeRead is one line for a table being read: how far, how fast, how much longer.
func describeRead(ts termstyle.Style, r snapshot.Status) string {
	line := tableName(r.Table) + " " + bar(ts, r.Fraction)
	if r.Fraction >= 0 {
		line += fmt.Sprintf(" %3.0f%%", r.Fraction*100)
	} else {
		line += "     " // the bar is empty and says so; keep the columns after it in place
	}
	line += fmt.Sprintf("  %d rows", r.Rows)
	if r.Partitions > 0 {
		// A partitioned table is read one partition at a time, and that is the unit that moves.
		line += fmt.Sprintf(" · partition %d/%d", min(r.PartitionsDone+1, r.Partitions), r.Partitions)
	}
	if r.Rows == 0 && r.RowsPerSecond == 0 {
		line += " · starting" // it has a slot, the first chunk is not in yet
	}
	if r.RowsPerSecond > 0 {
		line += fmt.Sprintf(" · %.0f rows/s", r.RowsPerSecond)
	}
	if r.RemainingSeconds >= 1 {
		line += fmt.Sprintf(" · ~%s left", (time.Duration(r.RemainingSeconds) * time.Second).Round(time.Second))
	}
	if r.Restarts > 0 {
		line += fmt.Sprintf(" · begun again %d times", r.Restarts)
	}
	return line
}

func describeResync(ts termstyle.Style, r snapshot.Status) string {
	if r.Done {
		return fmt.Sprintf("%s %s 100%%  %d rows · all read, waiting for the stream to reach the point they were read at, then the table is replaced",
			tableName(r.Table), bar(ts, 1), r.Rows)
	}
	return describeRead(ts, r)
}

func replicationFailure(stderr io.Writer, sub string, err error) int {
	switch {
	case errors.Is(err, control.ErrNotRunning):
		fmt.Fprintf(stderr, "binwave replication %s: %v. Is the service running?\n", sub, err)
	case errors.Is(err, control.ErrStarting):
		fmt.Fprintf(stderr, "binwave replication %s: %v; try again in a moment\n", sub, err)
	default:
		fmt.Fprintf(stderr, "binwave replication %s: %v\n", sub, err)
	}
	return exitError
}

// printStatus shows the status; again is what the commands it suggests need to reach the same
// instance (repeatFlags).
func printStatus(w io.Writer, st observability.Status, again string) {
	ts := termstyle.For(w)
	fmt.Fprintf(w, "state:        %s\n", stateText(ts, st.State))
	if st.Failure != nil {
		printFailure(w, st, again)
	}
	if st.PausedSince != nil {
		fmt.Fprintf(w, "paused since: %s (%s ago)\n", st.PausedSince.UTC().Format(time.RFC3339), time.Since(*st.PausedSince).Round(time.Second))
	}
	if len(st.Rebuilding) > 0 {
		fmt.Fprintf(w, "rebuilding:   %s (the source changed the sort key; no table is replicated until the copy is done)\n", strings.Join(st.Rebuilding, ", "))
	}
	if st.Position != "" {
		fmt.Fprintf(w, "position:     %s\n", st.Position)
	}
	fmt.Fprintf(w, "lag:          %.0fs\n", st.LagSeconds)
	fmt.Fprintf(w, "tables:       %d replicated, %d of them quarantined\n", st.Tables, len(st.Quarantined))
	if e := st.LastError; e != nil {
		// A failure that is still happening and one that passed read completely differently, and the
		// record alone does not say which it is: it is written on every retry and kept afterwards.
		if e.Ongoing {
			fmt.Fprintf(w, "failing:      %s: %s (for %s, %d attempts)\n",
				ts.Pending(e.Kind), e.Message, time.Since(e.Since).Round(time.Second), e.Attempts)
		} else {
			when := "resolved"
			if e.ResolvedAt != nil {
				when = fmt.Sprintf("resolved %s ago", time.Since(*e.ResolvedAt).Round(time.Second))
			}
			fmt.Fprintf(w, "last error:   %s: %s (%s, %s)\n", e.Kind, e.Message, e.Time.UTC().Format(time.RFC3339), when)
		}
	}
	// The lists of tables being read go last: their length changes as tables start and finish, and
	// everything above them keeps its place on a screen refreshed by watch.
	printInitialLoad(w, ts, st.InitialLoad, st.Resyncing)
	for _, r := range st.Resyncing {
		if r.Kind == "initial_load" {
			continue // the tables the load is reading are printed under it, above
		}
		fmt.Fprintf(w, "resyncing:    %s\n", describeResync(ts, r))
	}
}

// stateText colors the states that need a look: red for a failure, orange for a pause or a
// degraded service. A running service stays plain: the normal state should not draw the eye.
func stateText(ts termstyle.Style, s observability.State) string {
	switch s {
	case observability.StateFailed:
		return ts.Failed(string(s))
	case observability.StatePaused, observability.StateDegraded:
		return ts.Pending(string(s))
	default:
		return string(s)
	}
}

// printFailure describes the failure that stopped replication, and what can be done about it. The
// values of the key are personal data: this output goes to the operator's terminal, not to a log.
func printFailure(w io.Writer, st observability.Status, again string) {
	f := st.Failure
	ts := termstyle.For(w)
	fmt.Fprintf(w, "failed at:    %s (%s ago)\n", f.Time.UTC().Format(time.RFC3339), time.Since(f.Time).Round(time.Second))
	switch {
	case f.GTID != "":
		fmt.Fprintf(w, "transaction:  %s\n", f.GTID)
	case f.Kind == failinfo.KindResync:
		fmt.Fprintf(w, "transaction:  none: the failure is in a resync, which belongs to no transaction\n")
	default:
		fmt.Fprintf(w, "transaction:  none: the failure is at the write of buffered rows, which belong to several transactions\n")
	}
	fmt.Fprintf(w, "failure:      %s\n", ts.Failed(f.Kind))
	if f.Code != "" {
		fmt.Fprintf(w, "code:         %s (see docs/reference/error-codes.md)\n", ts.Failed(f.Code))
	}
	if f.Table != "" {
		fmt.Fprintf(w, "table:        %s\n", f.Table)
	}
	if f.Op != "" {
		fmt.Fprintf(w, "operation:    %s (%d rows in the batch)\n", f.Op, f.Rows)
	}
	if f.Key != "" {
		if len(f.KeyValues) > 0 {
			fmt.Fprintf(w, "key:          %s = (%s)\n", f.Key, strings.Join(f.KeyValues, ", "))
		} else {
			fmt.Fprintf(w, "key:          %s\n", f.Key)
		}
	}
	if f.Statement != "" {
		fmt.Fprintf(w, "statement:    %s\n", f.Statement)
	}
	fmt.Fprintf(w, "reason:       %s\n", f.Message)
	if f.Detail != "" {
		fmt.Fprintf(w, "detail:       %s\n", f.Detail)
	}
	hints := [][2]string{{"binwave replication resume" + again, "try again, after the cause is fixed"}}
	if f.Kind == failinfo.KindResync {
		hints = append(hints, [2]string{"set tables.on_replication_error: quarantine", "and restart: the resync is given up and the table quarantined"})
	}
	if f.Skippable() {
		hints = append(hints,
			[2]string{"binwave replication resume --skip-transactions 1" + again, "skip this transaction: its changes are NOT applied to ClickHouse"},
			[2]string{"binwave replication resume --gtid-next " + f.GTID + again, "the same, naming the transaction"})
	}
	width := 0
	for _, h := range hints {
		width = max(width, len(h[0]))
	}
	fmt.Fprintln(w, "what to do:")
	for _, h := range hints {
		fmt.Fprintf(w, "  %-*s  %s\n", width, h[0], h[1])
	}
	fmt.Fprintln(w)
}
