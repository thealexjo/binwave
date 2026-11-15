//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/test/testenv"
)

// shortSocket returns a socket path short enough for a Unix socket; t.TempDir is not on macOS.
func shortSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "bwi")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "c.sock")
}

func acctRows(t *testing.T, ch *testenv.ClickHouse) int {
	t.Helper()
	got, ok := clickhouseAccounts(t, ch)
	if !ok {
		return -1
	}
	return len(got)
}

// TestPauseKeepsTheServiceUpAndResumeLosesNothing: the promise of STOP REPLICA for binwave. While
// paused the service answers and reads nothing; what happens in the source meanwhile arrives
// after the resume, and the saved position is the source's.
func TestPauseKeepsTheServiceUpAndResumeLosesNothing(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "clickhouse")
	cfg.Control.Socket = shortSocket(t)
	c := control.Client{Socket: cfg.Control.Socket}
	ctx := context.Background()

	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'a', 1), (2, 'b', 2)")
	waitUntil(t, "the first rows", 60*time.Second, func() bool { return acctRows(t, ch) == 2 })

	if err := c.Pause(ctx); err != nil {
		t.Fatalf("Pause() = %v", err)
	}
	st, err := c.Status(ctx)
	if err != nil || st.State != observability.StatePaused || st.PausedSince == nil {
		t.Fatalf("Status() = %+v, %v, want paused", st, err)
	}
	if got := metricValue(t, e.hub, "binwave_paused"); got != 1 {
		t.Errorf("binwave_paused = %v", got)
	}
	e.stillRunning(t)

	// The pause left the position at the source's: everything before it was written and saved.
	if got, want := loadPosition(t, ch, cfg).GTIDSet, executedGTIDs(t, my); got != want {
		t.Errorf("position after the pause = %q, the source has executed %q", got, want)
	}

	// The source goes on; nothing is read.
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (3, 'c', 3)", "UPDATE binwave.acct SET qty = 10 WHERE id = 1", "DELETE FROM binwave.acct WHERE id = 2")
	time.Sleep(3 * time.Second)
	if got := acctRows(t, ch); got != 2 {
		t.Errorf("ClickHouse holds %d rows during the pause, want the 2 from before", got)
	}
	if got := metricValue(t, e.hub, "binwave_lag_seconds"); got < 2 {
		t.Errorf("lag = %v during a pause, want it to grow", got)
	}
	// A second pause and a stray resume are harmless; the service still answers.
	if err := c.Pause(ctx); err != nil {
		t.Errorf("a second Pause() = %v", err)
	}

	if err := c.Resume(ctx, control.ResumeRequest{}); err != nil {
		t.Fatalf("Resume() = %v", err)
	}
	waitForConvergence(t, my, ch)
	waitUntil(t, "the service to be ready again", 30*time.Second, func() bool { return e.hub.Status().State == observability.StateReady })
	// The rows come first; the position follows on the next save.
	want := executedGTIDs(t, my)
	waitUntil(t, "the position to reach "+want, 30*time.Second, func() bool { return loadPosition(t, ch, cfg).GTIDSet == want })
	e.stillRunning(t)

	// And it can pause and resume again.
	if err := c.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (4, 'd', 4)")
	if err := c.Resume(ctx, control.ResumeRequest{}); err != nil {
		t.Fatal(err)
	}
	waitForConvergence(t, my, ch)

	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}

// TestServiceWithAutostartOffComesUpAndReadsNothing: the equivalent of skip_replica_start.
func TestServiceWithAutostartOffComesUpAndReadsNothing(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)",
		"CREATE TABLE binwave.keyless (a INT)",
	)
	cfg := engineConfig(t, my, ch, "file")
	cfg.Control.Socket = shortSocket(t)
	off := false
	cfg.Replication.Autostart = &off
	c := control.Client{Socket: cfg.Control.Socket}
	ctx := context.Background()

	e := startHubEngine(t, cfg) // it is ready: both databases were reached and the rights checked

	waitUntil(t, "the service to say it is paused", 30*time.Second, func() bool { return e.hub.Status().State == observability.StatePaused })
	// The checks that run after start still run, and set aside what cannot be replicated.
	waitUntil(t, "the checks to find the keyless table", 60*time.Second, func() bool {
		return len(e.hub.Status().Quarantined) == 1
	})

	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'a', 1)")
	time.Sleep(3 * time.Second)
	if got := acctRows(t, ch); got > 0 {
		t.Errorf("ClickHouse holds %d rows, but the service was told not to start replication", got)
	}
	e.stillRunning(t)

	if err := c.Resume(ctx, control.ResumeRequest{}); err != nil {
		t.Fatal(err)
	}
	waitForConvergence(t, my, ch)

	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}

// TestStoppingAPausedServiceIsAGracefulStop: SIGTERM does not need a resume first.
func TestStoppingAPausedServiceIsAGracefulStop(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.Control.Socket = shortSocket(t)
	e := startHubEngine(t, cfg)
	if err := (control.Client{Socket: cfg.Control.Socket}).Pause(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := e.stop(); err != nil {
		t.Errorf("stopping a paused service returned %v", err)
	}
	if _, err := os.Stat(cfg.Control.Socket); !os.IsNotExist(err) {
		t.Errorf("the control socket stays after the stop: %v", err)
	}
}

// TestTablesCommandsWorkOnARunningServiceThroughTheControlChannel: the operator no longer has to
// stop the service to look at the quarantine or release a table.
func TestTablesCommandsWorkOnARunningServiceThroughTheControlChannel(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.later (id INT, v INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.Control.Socket = shortSocket(t)
	e := startHubEngine(t, cfg)
	waitUntil(t, "the keyless table to be quarantined", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })

	c := control.Client{Socket: cfg.Control.Socket}
	entries, err := c.Quarantined(context.Background())
	if err != nil || len(entries) != 1 || entries[0].Table != "binwave.later" {
		t.Fatalf("Quarantined() = %+v, %v, want the table from the running service", entries, err)
	}

	// The cause is fixed, then the table is released, all with the service running.
	mustExec(t, my, "ALTER TABLE binwave.later ADD PRIMARY KEY (id)")
	released, unknown, err := c.Release(context.Background(), []string{"binwave.later", "binwave.nope"})
	if err != nil || len(released) != 1 || len(unknown) != 1 {
		t.Fatalf("Release() = %v, %v, %v", released, unknown, err)
	}
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("still quarantined: %v", got)
	}

	mustExec(t, my, "INSERT INTO binwave.later VALUES (1, 1), (2, 2)")
	waitUntil(t, "the released table to replicate", 60*time.Second, func() bool { return chRowCount(t, ch, "later") == 2 })
	e.stillRunning(t)
}

// TestARestartShowsTheSavedPositionOnAQuietSource: after a restart the status says where replication
// stands, whether it runs or starts paused, without waiting for a change of the source to be saved.
func TestARestartShowsTheSavedPositionOnAQuietSource(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.Control.Socket = shortSocket(t)

	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'a', 1)")
	waitForConvergence(t, my, ch)
	if err := e.stop(); err != nil {
		t.Fatalf("graceful stop returned %v", err)
	}
	saved := loadPosition(t, ch, cfg)

	for _, autostart := range []bool{true, false} {
		cfg.Replication.Autostart = &autostart
		e := startHubEngine(t, cfg)
		if got := e.hub.Status().Position; got != saved.GTIDSet {
			t.Errorf("autostart %v: status position = %q, want the saved %q", autostart, got, saved.GTIDSet)
		}
		if got := metricValue(t, e.hub, "binwave_position_version"); got != float64(saved.Version) {
			t.Errorf("autostart %v: binwave_position_version = %v, want %d", autostart, got, saved.Version)
		}
		if err := e.stop(); err != nil {
			t.Fatalf("autostart %v: graceful stop returned %v", autostart, err)
		}
	}
}
