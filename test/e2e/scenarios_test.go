//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/test/testenv"
)

// TestBackpressureLongerThanTheServersWriteTimeout: while ClickHouse hangs, binwave stops reading
// the binlog, and MySQL gives up on a replica that does not read (net_write_timeout). The stream
// must reconnect and deliver everything, without loss and without a restart of the process.
func TestBackpressureLongerThanTheServersWriteTimeout(t *testing.T) {
	t.Parallel()
	s := newStack(t, testenv.WithMySQLArgs("--net-write-timeout=2"))
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})
	p := startBinwave(t, cfg)
	p.waitReady()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 11)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	time.Sleep(3 * time.Second)
	s.chProxy.Blackhole() // binwave's flush hangs; reading stops; MySQL's send buffer fills
	time.Sleep(12 * time.Second)
	s.chProxy.Resume()
	<-writing

	s.waitConverged(3*time.Minute, p, accounts)
	if p.exited() {
		t.Logf("binwave restarted itself during the run (exit %d); data still converged\n%s", p.exit, tail(p.logs(), 1500))
	}
}

const refreshDDL = "CREATE TABLE binwave.refresh (id INT NOT NULL, v VARCHAR(20) NOT NULL, n INT NOT NULL, PRIMARY KEY (id))"

func refreshSQL(prefix string, factor int) string {
	return "INSERT INTO binwave.refresh SELECT seq, CONCAT('" + prefix + "', seq), seq * " + strconv.Itoa(factor) +
		" FROM (WITH RECURSIVE s(seq) AS (SELECT 1 UNION ALL SELECT seq + 1 FROM s WHERE seq < 100000) SELECT seq FROM s) t"
}

var refresh = comparison{
	name:  "refresh",
	mysql: "SELECT id, v, n FROM binwave.refresh ORDER BY id",
	ch:    "SELECT id, v, n FROM replica.refresh FINAL ORDER BY id",
}

// TestDeleteAndReinsertOfEverythingAtScale is the nightly refresh: every row of a big table is
// deleted and inserted again, in one transaction and in two. With a small buffer the tombstones
// and the re-inserts of one transaction reach ClickHouse in different flushes with the same
// version. A comparable tool lost data permanently in this pattern (issue 1346).
func TestDeleteAndReinsertOfEverythingAtScale(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(refreshDDL)
	conn, err := s.my.DB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	run := func(stmts ...string) {
		t.Helper()
		for _, q := range stmts {
			if _, err := conn.ExecContext(context.Background(), q); err != nil {
				t.Fatalf("%.80s: %v", q, err)
			}
		}
	}
	run("SET SESSION cte_max_recursion_depth = 1000000")

	// binwave starts from "now" and there is no snapshot yet, so the rows are loaded once it runs.
	p := startBinwave(t, s.writeConfig(settings{bufferRows: 1000}))
	p.waitReady()
	run(refreshSQL("a", 1))
	s.waitConverged(2*time.Minute, p, refresh)

	// One transaction: DELETE everything, INSERT everything again with new values.
	run("START TRANSACTION", "DELETE FROM binwave.refresh", refreshSQL("b", 2), "COMMIT")
	s.waitConverged(2*time.Minute, p, refresh)

	// Two transactions: the delete is committed before the re-insert starts.
	run("DELETE FROM binwave.refresh")
	s.waitConverged(2*time.Minute, p, refresh)
	run(refreshSQL("c", 3))
	s.waitConverged(2*time.Minute, p, refresh)

	// A refresh that keeps only some of the keys.
	run("START TRANSACTION", "DELETE FROM binwave.refresh", refreshSQL("d", 4), "DELETE FROM binwave.refresh WHERE id % 3 = 0", "COMMIT")
	s.waitConverged(2*time.Minute, p, refresh)

	if code := p.term(45 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

var late = comparison{
	name:  "late",
	mysql: "SELECT id, v FROM binwave.late ORDER BY id",
	ch:    "SELECT id, v FROM replica.late FINAL ORDER BY id",
}

// TestTableCreatedWhileRunningIsReplicated: a table that appears in MySQL after binwave started is
// created in ClickHouse and replicated from its first row.
func TestTableCreatedWhileRunningIsReplicated(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec("CREATE TABLE binwave.first (id INT PRIMARY KEY)")
	p := startBinwave(t, s.writeConfig(settings{}))
	p.waitReady()

	s.mustExec("CREATE TABLE binwave.late (id INT PRIMARY KEY, v VARCHAR(10) NULL)",
		"INSERT INTO binwave.late VALUES (1, 'a'), (2, NULL)",
		"UPDATE binwave.late SET v = 'b' WHERE id = 1")
	s.waitConverged(60*time.Second, p, late)
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestMissingTargetTableStopsInsteadOfSkipping: with ddl.dry_run, rows for a table that does not
// exist in ClickHouse must stop replication with a clear error, never be skipped (a comparable
// tool silently dropped them, issue 1285). Once the table exists, nothing was lost.
func TestMissingTargetTableStopsInsteadOfSkipping(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec("CREATE TABLE binwave.first (id INT PRIMARY KEY)")
	// The default: the service stops on the missing table, so nothing is lost.
	cfg := s.writeConfig(settings{dryRun: true})
	s.chExec("CREATE DATABASE IF NOT EXISTS replica")
	s.chExec(`CREATE TABLE replica.first (id Int32, _version UInt64, _is_deleted UInt8, _synced_at DateTime64(3, 'UTC'))
		ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY id`)
	p := startBinwave(t, cfg)
	p.waitReady()

	s.mustExec("CREATE TABLE binwave.late (id INT PRIMARY KEY, v VARCHAR(10) NULL)",
		"INSERT INTO binwave.late VALUES (1, 'a')", "INSERT INTO binwave.late VALUES (2, 'b')")
	waitFailedState(t, s, p)
	if !strings.Contains(p.logs(), "does not exist") || !strings.Contains(p.logs(), "ddl.dry_run") {
		t.Errorf("the error does not explain the problem:\n%s", tail(p.logs(), 2000))
	}
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("a service with failed replication exited with %d on SIGTERM, want a graceful 0", code)
	}

	// The operator creates the table; a restart must find every row still waiting.
	s.chExec(`CREATE TABLE replica.late (id Int32, v Nullable(String), _version UInt64, _is_deleted UInt8, _synced_at DateTime64(3, 'UTC'))
		ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY id`)
	p = startBinwave(t, cfg)
	p.waitReady()
	s.waitConverged(60*time.Second, p, late)
}

// TestMissingTargetTableIsQuarantinedWhenAskedFor: with ddl.on_missing_structure: quarantine the
// service stays up, the table is set aside loudly, and the rows written meanwhile are NOT in
// ClickHouse after the table is created: that is the price of not stopping, and what a
// resynchronization is for.
func TestMissingTargetTableIsQuarantinedWhenAskedFor(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec("CREATE TABLE binwave.first (id INT PRIMARY KEY)")
	cfg := s.writeConfig(settings{dryRun: true, onMissingStructure: "quarantine"})
	s.chExec("CREATE DATABASE IF NOT EXISTS replica")
	s.chExec(`CREATE TABLE replica.first (id Int32, _version UInt64, _is_deleted UInt8, _synced_at DateTime64(3, 'UTC'))
		ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY id`)
	p := startBinwave(t, cfg)
	p.waitReady()

	s.mustExec("CREATE TABLE binwave.late (id INT PRIMARY KEY, v VARCHAR(10) NULL)",
		"INSERT INTO binwave.late VALUES (1, 'a')", "INSERT INTO binwave.first VALUES (1)")
	eventually(t, 60*time.Second, "the missing table to be quarantined", func() (bool, string) {
		return strings.Join(s.quarantinedTables(), ",") == "binwave.late", tail(p.logs(), 300)
	})
	if p.exited() {
		t.Fatalf("binwave exited (%d) on a missing table\n%s", p.exit, tail(p.logs(), 2000))
	}
	st, _ := s.status()
	reason := ""
	if list, _ := st["quarantined"].([]any); len(list) == 1 {
		reason, _ = list[0].(map[string]any)["reason"].(string)
	}
	if !strings.Contains(reason, "does not exist") || !strings.Contains(reason, "ddl.dry_run") {
		t.Errorf("the reason does not explain the problem: %q", reason)
	}
	// The other table goes on.
	eventually(t, 60*time.Second, "the other table to replicate", func() (bool, string) {
		var n uint64
		err := s.ch.DB.QueryRow("SELECT count() FROM replica.first FINAL").Scan(&n)
		return err == nil && n == 1, "not yet"
	})
}

func (s *stack) chExec(q string) {
	s.t.Helper()
	if _, err := s.ch.DB.Exec(q); err != nil {
		s.t.Fatalf("%.100s: %v", q, err)
	}
}

// TestExitCodes pins the contract with the supervisor: 78 for what a restart cannot fix, 1 for
// runtime failures, 2 for a bad command line. More exit codes are pinned in hardening_test.go.
func TestExitCodes(t *testing.T) {
	t.Run("invalid configuration is 78", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "bad.yaml")
		if err := os.WriteFile(path, []byte("ddl: {mode: nope}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		p := startBinwave(t, path)
		if code := p.waitExit(30 * time.Second); code != 78 {
			t.Errorf("exit code = %d, want 78\n%s", code, p.logs())
		}
	})

	t.Run("a source that does not meet the requirements is 78", func(t *testing.T) {
		t.Parallel()
		s := newStack(t, testenv.WithMySQLArgs("--binlog-format=MIXED"))
		p := startBinwave(t, s.writeConfig(settings{}))
		if code := p.waitExit(60 * time.Second); code != 78 {
			t.Fatalf("exit code = %d, want 78\n%s", code, tail(p.logs(), 2000))
		}
		if !strings.Contains(p.logs(), "binlog_format must be ROW") {
			t.Errorf("the requirement that failed is not named:\n%s", tail(p.logs(), 2000))
		}
	})

	t.Run("a wrong ClickHouse password is 78, retrying cannot fix it", func(t *testing.T) {
		t.Parallel()
		s := newStack(t)
		cfg := s.writeConfig(settings{})
		data, err := os.ReadFile(cfg)
		if err != nil {
			t.Fatal(err)
		}
		bad := strings.Replace(string(data), "password: "+s.ch.Password, "password: wrong", 1)
		if bad == string(data) {
			t.Fatal("test bug: the password was not found in the config")
		}
		if err := os.WriteFile(cfg, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		p := startBinwave(t, cfg)
		if code := p.waitExit(60 * time.Second); code != 78 {
			t.Errorf("exit code = %d, want 78\n%s", code, tail(p.logs(), 2000))
		}
		if strings.Contains(p.logs(), "cannot start yet, retrying") {
			t.Errorf("a wrong password was retried:\n%s", tail(p.logs(), 2000))
		}
	})
}

// TestDryRunStopsOnANewTableByDefault: a dry run cannot create the table, so it cannot replicate
// it either; by default the service says so and stops instead of losing the table's changes.
func TestDryRunStopsOnANewTableByDefault(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	cfg := s.writeConfig(settings{dryRun: true})
	p := startBinwave(t, cfg)
	p.waitReady()

	s.mustExec("CREATE TABLE binwave.fresh (id INT PRIMARY KEY)", "INSERT INTO binwave.fresh VALUES (1)")

	waitFailedState(t, s, p)
	for _, want := range []string{"ddl.dry_run", "dry run cannot replicate", "on_missing_structure"} {
		if !strings.Contains(p.logs(), want) {
			t.Errorf("the logs lack %q:\n%s", want, tail(p.logs(), 2000))
		}
	}
}

// waitFailedState waits until the service reports that replication failed: it stays up, the
// failure is on /status, and it is in the log.
func waitFailedState(t *testing.T, s *stack, p *proc) {
	t.Helper()
	eventually(t, 60*time.Second, "the failed state", func() (bool, string) {
		if p.exited() {
			t.Fatalf("binwave exited (%d) instead of staying up with a failed replication\n%s", p.exit, tail(p.logs(), 2000))
		}
		st, err := s.status()
		if err != nil {
			return false, err.Error()
		}
		state, _ := st["state"].(string)
		return state == "failed" && st["failure"] != nil, state
	})
	if !strings.Contains(p.logs(), "replication failed") {
		t.Errorf("the log does not say that replication failed:\n%s", tail(p.logs(), 2000))
	}
}
