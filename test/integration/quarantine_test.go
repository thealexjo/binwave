//go:build integration

package integration

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/engine"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/test/testenv"
)

// hubEngine is an engine running in the background with its observability hub.
type hubEngine struct {
	hub  *observability.Hub
	stop func() error
	done chan error
}

func startHubEngine(t *testing.T, cfg *config.Config) *hubEngine {
	t.Helper()
	hub := observability.NewHub(observability.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{}, 1)
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- engine.New(cfg, quietLogger(), engine.WithObservability(hub)).Run(ctx, app.Hooks{
			Ready:    func() { hub.Ready(); ready <- struct{}{} },
			Progress: hub.Progress,
		})
	}()
	t.Cleanup(func() { stopAndWait(t, cancel, finished) })
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("the engine stopped before it was ready: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the engine did not become ready")
	}
	return &hubEngine{hub: hub, done: done, stop: func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(60 * time.Second):
			t.Fatal("the engine did not stop")
			return nil
		}
	}}
}

func (e *hubEngine) quarantinedTables() []string {
	var out []string
	for _, q := range e.hub.Status().Quarantined {
		out = append(out, q.Table)
	}
	return out
}

func (e *hubEngine) stillRunning(t *testing.T) {
	t.Helper()
	select {
	case err := <-e.done:
		t.Fatalf("the engine stopped: %v", err)
	default:
	}
}

func chRowCount(t *testing.T, ch *testenv.ClickHouse, table string) int {
	t.Helper()
	var n uint64
	if err := ch.DB.QueryRow("SELECT count() FROM (SELECT * FROM replica." + table + " FINAL WHERE _is_deleted = 0)").Scan(&n); err != nil {
		return -1 // the table may not exist
	}
	return int(n)
}

// TestTablesThatCannotBeReplicatedAreSetAsideAndTheRestGoesOn: the case the quarantine is for.
// A table without a key and a table with a cascading foreign key are set aside, loudly; the
// healthy table replicates as if nothing was wrong; a restart keeps the quarantine; releasing
// the table brings it back.
func TestTablesThatCannotBeReplicatedAreSetAsideAndTheRestGoesOn(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.fine (id INT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE binwave.keyless (a INT, b INT)",
		"CREATE TABLE binwave.parent (id INT PRIMARY KEY)",
		"CREATE TABLE binwave.child (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_c FOREIGN KEY (parent_id) REFERENCES binwave.parent (id) ON DELETE CASCADE)",
	)
	cfg := engineConfig(t, my, ch, "file")

	e := startHubEngine(t, cfg)
	waitUntil(t, "the problem tables to be quarantined", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 2 })
	if got := strings.Join(e.quarantinedTables(), ","); got != "binwave.child,binwave.keyless" {
		t.Fatalf("quarantined %s, want the keyless table and the table with the cascade", got)
	}
	st := e.hub.Status()
	for _, q := range st.Quarantined {
		if q.Reason == "" || q.At.IsZero() {
			t.Errorf("entry %+v lacks a reason or a time", q)
		}
	}

	// The healthy table, and the parent (which has nothing wrong), replicate; the others do not.
	mustExec(t, my,
		"INSERT INTO binwave.fine VALUES (1, 10), (2, 20)",
		"INSERT INTO binwave.parent VALUES (1)",
		"INSERT INTO binwave.keyless VALUES (1, 1), (2, 2)",
		"INSERT INTO binwave.child VALUES (1, 1)",
	)
	waitUntil(t, "the healthy tables to replicate", 60*time.Second, func() bool {
		return chRowCount(t, ch, "fine") == 2 && chRowCount(t, ch, "parent") == 1
	})
	waitUntil(t, "the skipped rows to be counted", 30*time.Second, func() bool {
		return metricValue(t, e.hub, "binwave_quarantine_skipped_rows_total") >= 3
	})
	if got := chRowCount(t, ch, "keyless"); got > 0 {
		t.Errorf("the quarantined table holds %d rows in ClickHouse", got)
	}
	if got := metricValue(t, e.hub, "binwave_quarantined_tables"); got != 2 {
		t.Errorf("binwave_quarantined_tables = %v, want 2", got)
	}
	e.stillRunning(t)

	if err := e.stop(); err != nil {
		t.Fatalf("graceful stop returned %v", err)
	}

	// The quarantine is in the saved state: a restart keeps it, and the operator can read it.
	admin := engine.New(cfg, quietLogger())
	entries, err := admin.Quarantined(context.Background())
	if err != nil || len(entries) != 2 {
		t.Fatalf("Quarantined() = %+v, %v, want the two tables", entries, err)
	}

	// Releasing takes a table out; releasing what is not quarantined is reported.
	released, unknown, err := admin.Release(context.Background(), []string{"binwave.keyless", "binwave.nope"}, true)
	if err != nil || len(released) != 1 || released[0] != "binwave.keyless" || len(unknown) != 1 || unknown[0] != "binwave.nope" {
		t.Fatalf("Release() = %v, %v, %v", released, unknown, err)
	}
	entries, _ = admin.Quarantined(context.Background())
	var stillQuarantined, fenced []string
	for _, en := range entries {
		if en.Released() {
			fenced = append(fenced, en.Table)
		} else {
			stillQuarantined = append(stillQuarantined, en.Table)
		}
	}
	if strings.Join(stillQuarantined, ",") != "binwave.child" || strings.Join(fenced, ",") != "binwave.keyless" {
		t.Errorf("after the release: quarantined %v, released with a fence %v; want the child quarantined and the keyless table fenced", stillQuarantined, fenced)
	}
}

// TestReleaseRefusesWhileTheServiceRuns: the running service would save its own state over it.
func TestReleaseRefusesWhileTheServiceRuns(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	cfg := engineConfig(t, my, ch, "file")

	// Something answers on the observability address.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	cfg.Observability.Listen = ln.Addr().String()

	_, _, err = engine.New(cfg, quietLogger()).Release(context.Background(), []string{"binwave.x"}, false)
	if err == nil || !strings.Contains(err.Error(), "seems to be running") {
		t.Fatalf("Release() = %v, want a refusal", err)
	}

	// Nothing answers: the release goes on (and finds nothing to release).
	_ = ln.Close()
	if _, unknown, err := engine.New(cfg, quietLogger()).Release(context.Background(), []string{"binwave.x"}, false); err != nil || len(unknown) != 1 {
		t.Errorf("Release() = %v, %v, want the unknown table reported", unknown, err)
	}
}

// TestHandMadeTableThatDoesNotMatchIsQuarantined: the ClickHouse table of an operator has a
// column of another type than the source. The table is set aside, the others go on.
func TestHandMadeTableThatDoesNotMatchIsQuarantined(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.hand (id INT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE binwave.fine (id INT PRIMARY KEY, v INT NOT NULL)",
	)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS replica",
		"CREATE TABLE replica.hand (id Int32, v String, _version UInt64, _is_deleted UInt8, _synced_at DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY id",
	} {
		if _, err := ch.DB.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}

	e := startHubEngine(t, engineConfig(t, my, ch, "file"))
	mustExec(t, my, "INSERT INTO binwave.hand VALUES (1, 1)", "INSERT INTO binwave.fine VALUES (1, 1)")

	waitUntil(t, "the table to be quarantined and the other to replicate", 60*time.Second, func() bool {
		return strings.Join(e.quarantinedTables(), ",") == "binwave.hand" && chRowCount(t, ch, "fine") == 1
	})
	reason := e.hub.Status().Quarantined[0].Reason
	if !strings.Contains(reason, "column v") {
		t.Errorf("the reason %q does not name the column that differs", reason)
	}
	e.stillRunning(t)
}

// TestTableThatVanishesFromClickHouseIsQuarantined (with ddl.on_missing_structure: quarantine): a column or the whole table is dropped in
// ClickHouse after replication started. The insert fails with an exception that says so; that is
// the table's problem, not the service's.
func TestTableThatVanishesFromClickHouseIsQuarantined(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.gone (id INT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE binwave.fine (id INT PRIMARY KEY, v INT NOT NULL)",
	)
	cfg := engineConfig(t, my, ch, "file")
	cfg.DDL.OnMissingStructure = config.OnMissingQuarantine // a missing table is set aside only when that is asked for
	e := startHubEngine(t, cfg)

	mustExec(t, my, "INSERT INTO binwave.gone VALUES (1, 1)", "INSERT INTO binwave.fine VALUES (1, 1)")
	waitUntil(t, "both tables to replicate", 60*time.Second, func() bool {
		return chRowCount(t, ch, "gone") == 1 && chRowCount(t, ch, "fine") == 1
	})

	if _, err := ch.DB.Exec("DROP TABLE replica.gone"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, my, "INSERT INTO binwave.gone VALUES (2, 2)", "INSERT INTO binwave.fine VALUES (2, 2)")

	waitUntil(t, "the vanished table to be quarantined and the other to go on", 60*time.Second, func() bool {
		return strings.Join(e.quarantinedTables(), ",") == "binwave.gone" && chRowCount(t, ch, "fine") == 2
	})
	e.stillRunning(t)
	if reason := e.hub.Status().Quarantined[0].Reason; !strings.Contains(reason, "does not exist") {
		t.Errorf("the reason %q does not say the table is gone", reason)
	}
}

// TestTableCreatedLaterThatCannotBeReplicatedIsQuarantined: the same rules for a table that
// appears while the service runs.
func TestTableCreatedLaterThatCannotBeReplicatedIsQuarantined(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.fine (id INT PRIMARY KEY, v INT NOT NULL)")
	e := startHubEngine(t, engineConfig(t, my, ch, "file"))

	mustExec(t, my,
		"CREATE TABLE binwave.late_keyless (a INT)",
		"INSERT INTO binwave.late_keyless VALUES (1)",
		"INSERT INTO binwave.fine VALUES (1, 1)",
	)

	waitUntil(t, "the new table to be quarantined and the old one to replicate", 60*time.Second, func() bool {
		return strings.Join(e.quarantinedTables(), ",") == "binwave.late_keyless" && chRowCount(t, ch, "fine") == 1
	})
	e.stillRunning(t)
}

// TestReleaseAfterAStopDoesNotReplayTheTimeBeforeTheFix: the trap of releasing a table on a
// stopped service. The changes of the table that the service had not read when it stopped are
// read after the start, with the structure of their time (no key). The fence of the release keeps
// them out: only what happens after the release is replicated.
func TestReleaseAfterAStopDoesNotReplayTheTimeBeforeTheFix(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.later (id INT, v INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")

	e := startHubEngine(t, cfg)
	waitUntil(t, "the keyless table to be quarantined", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })
	// Stopped right after the changes, whether or not the service has read them.
	mustExec(t, my, "INSERT INTO binwave.later VALUES (1, 1), (2, 2)")
	if err := e.stop(); err != nil {
		t.Fatal(err)
	}

	// The cause is fixed after those changes in the binlog; then the table is released.
	mustExec(t, my, "ALTER TABLE binwave.later ADD PRIMARY KEY (id)")
	released, _, err := engine.New(cfg, quietLogger()).Release(context.Background(), []string{"binwave.later"}, true)
	if err != nil || len(released) != 1 {
		t.Fatalf("Release() = %v, %v", released, err)
	}

	e = startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.later VALUES (3, 3), (4, 4)")
	waitUntil(t, "the rows after the release", 60*time.Second, func() bool { return chRowCount(t, ch, "later") == 2 })
	if got := e.quarantinedTables(); len(got) != 0 {
		t.Errorf("quarantined again: %v; the changes before the fix must not be replayed with the old structure", got)
	}
	var ids []int32
	rows, err := ch.DB.Query("SELECT id FROM replica.later FINAL WHERE _is_deleted = 0 ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 3 || ids[1] != 4 {
		t.Errorf("ClickHouse holds %v, want only the rows written after the release", ids)
	}
}

// TestTableThatVanishesFromClickHouseStopsTheServiceByDefault: the default loses nothing.
func TestTableThatVanishesFromClickHouseStopsTheServiceByDefault(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.gone (id INT PRIMARY KEY, v INT NOT NULL)")
	e := startHubEngine(t, engineConfig(t, my, ch, "file"))
	mustExec(t, my, "INSERT INTO binwave.gone VALUES (1, 1)")
	waitUntil(t, "the first row", 60*time.Second, func() bool { return chRowCount(t, ch, "gone") == 1 })

	if _, err := ch.DB.Exec("DROP TABLE replica.gone"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, my, "INSERT INTO binwave.gone VALUES (2, 2)")

	f := waitFailedHub(t, e)
	if !strings.Contains(f.Message, "does not exist") {
		t.Fatalf("the failure does not name the missing table: %s", f.Message)
	}
	if got := len(e.hub.Status().Quarantined); got != 0 {
		t.Errorf("%d tables quarantined although replication failed", got)
	}
}
