//go:build e2e

package e2e

import (
	"bytes"
	"database/sql"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func (p *proc) waitStarted() { p.t.Helper(); p.waitLog("msg=ready", 90*time.Second) }

// TestStartWaitsForADatabaseThatIsDown: the service is started while one database is down, as
// after a reboot of the whole host. It must keep trying, say why, and start replicating when the
// database returns, without an operator.
func TestStartWaitsForADatabaseThatIsDown(t *testing.T) {
	for _, down := range []string{"ClickHouse", "MySQL"} {
		t.Run(down+" is down", func(t *testing.T) {
			t.Parallel()
			s := newStack(t)
			s.mustExec(accountDDL)
			proxy := s.chProxy
			if down == "MySQL" {
				proxy = s.myProxy
			}
			proxy.Refuse()

			p := startBinwave(t, s.writeConfig(settings{}))
			p.waitLog("cannot start yet, retrying", 60*time.Second)
			time.Sleep(time.Second)
			if p.exited() {
				t.Fatalf("binwave exited (%d) while a database was down\n%s", p.exit, tail(p.logs(), 2000))
			}
			if strings.Contains(p.logs(), "msg=ready") {
				t.Fatal("the service claims to be ready with a database down")
			}

			proxy.Resume()
			p.waitStarted()
			s.mustExec("INSERT INTO binwave.acct VALUES (1, 'ann', 1, 10.00, NULL, NOW(3))")
			s.waitConverged(2*time.Minute, p, accounts)
			if code := p.term(30 * time.Second); code != 0 {
				t.Errorf("graceful stop exited with %d", code)
			}
		})
	}
}

// TestStartGivesUpAfterTheStartupBudget: a database that stays down beyond retry.startup_budget
// ends the process with a runtime error, so that the supervisor decides, not with the
// configuration code.
func TestStartGivesUpAfterTheStartupBudget(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.chProxy.Refuse()

	p := startBinwave(t, s.writeConfig(settings{startupBudget: "4s"}))

	if code := p.waitExit(60 * time.Second); code != 1 {
		t.Errorf("exit code = %d, want 1\n%s", code, tail(p.logs(), 2000))
	}
	if !strings.Contains(p.logs(), "retry.startup_budget") {
		t.Errorf("the budget is not named in the logs:\n%s", tail(p.logs(), 2000))
	}
}

func TestPermanentStartFailuresAreExitCode78WithoutRetries(t *testing.T) {
	t.Run("wrong MySQL password", func(t *testing.T) {
		t.Parallel()
		s := newStack(t)
		p := startBinwave(t, s.writeConfig(settings{mysqlUser: s.my.User, mysqlPassword: "wrong"}))
		if code := p.waitExit(60 * time.Second); code != 78 {
			t.Errorf("exit code = %d, want 78\n%s", code, tail(p.logs(), 2000))
		}
		if strings.Contains(p.logs(), "cannot start yet, retrying") {
			t.Errorf("a wrong password was retried:\n%s", tail(p.logs(), 2000))
		}
	})

	t.Run("purged binary logs", func(t *testing.T) {
		t.Parallel()
		s := newStack(t)
		s.mustExec(accountDDL, "INSERT INTO binwave.acct VALUES (1, 'a', 1, 1.00, NULL, NOW(3))")
		var uuid string
		if err := s.my.DB.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
			t.Fatal(err)
		}
		purgeBinaryLogs(t, rootDB(t, s))

		p := startBinwave(t, s.writeConfig(settings{store: "file", extra: "replication: {initial_position: gtid, gtid: \"" + uuid + ":1\"}"}))

		if code := p.waitExit(60 * time.Second); code != 78 {
			t.Errorf("exit code = %d, want 78\n%s", code, tail(p.logs(), 2000))
		}
		if !strings.Contains(p.logs(), "purged") {
			t.Errorf("the cause is not explained:\n%s", tail(p.logs(), 2000))
		}
	})
}

// TestTableWithoutAKeyStopsTheServiceAfterStartWithAFullListWithThePolicyStop: with the strict
// policy the service starts, then finds tables it cannot replicate and stops gracefully with the
// configuration code, listing all of them, so that one restart is enough to learn everything
// that needs fixing.
func TestTableWithoutAKeyStopsTheServiceAfterStartWithAFullListWithThePolicyStop(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL, "CREATE TABLE binwave.keyless_a (a INT)", "CREATE TABLE binwave.keyless_b (b INT)")

	p := startBinwave(t, s.writeConfig(settings{onReplicationError: "stop"}))

	if code := p.waitExit(60 * time.Second); code != 78 {
		t.Fatalf("exit code = %d, want 78\n%s", code, tail(p.logs(), 2500))
	}
	logs := p.logs()
	for _, want := range []string{"msg=ready", "binwave.keyless_a", "binwave.keyless_b", "order_by"} {
		if !strings.Contains(logs, want) {
			t.Errorf("the logs lack %q:\n%s", want, tail(logs, 2500))
		}
	}
	if strings.Contains(logs, "binwave.acct:") || strings.Contains(logs, "table=binwave.acct") {
		t.Errorf("a healthy table is reported as a problem:\n%s", tail(logs, 2500))
	}
}

func TestCheckConnectReport(t *testing.T) {
	check := func(t *testing.T, cfg string) (string, int) {
		t.Helper()
		cmd := exec.Command(binaryPath, "check", "--connect", "--config", cfg)
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		return out.String(), exitCode(err)
	}

	t.Run("healthy", func(t *testing.T) {
		t.Parallel()
		s := newStack(t)
		s.mustExec(accountDDL)
		out, code := check(t, s.writeConfig(settings{}))
		if code != 0 || !strings.Contains(out, "Tables     ok") {
			t.Errorf("exit code = %d, output:\n%s", code, out)
		}
	})

	t.Run("a table that cannot be replicated is 78 with the policy stop", func(t *testing.T) {
		t.Parallel()
		s := newStack(t)
		s.mustExec(accountDDL, "CREATE TABLE binwave.keyless (a INT)")
		out, code := check(t, s.writeConfig(settings{onReplicationError: "stop"}))
		if code != 78 || !strings.Contains(out, "binwave.keyless") {
			t.Errorf("exit code = %d, output:\n%s", code, out)
		}
	})

	t.Run("a table that cannot be replicated would be quarantined by default", func(t *testing.T) {
		t.Parallel()
		s := newStack(t)
		s.mustExec(accountDDL, "CREATE TABLE binwave.keyless (a INT)")
		out, code := check(t, s.writeConfig(settings{}))
		if code != 0 || !strings.Contains(out, "quarantine") || !strings.Contains(out, "binwave.keyless") {
			t.Errorf("exit code = %d, output:\n%s", code, out)
		}
	})

	t.Run("a database that is down is 1", func(t *testing.T) {
		t.Parallel()
		s := newStack(t)
		s.chProxy.Refuse()
		out, code := check(t, s.writeConfig(settings{}))
		if code != 1 || !strings.Contains(out, "ClickHouse FAILED") {
			t.Errorf("exit code = %d, output:\n%s", code, out)
		}
	})

	t.Run("a wrong password is 78", func(t *testing.T) {
		t.Parallel()
		s := newStack(t)
		out, code := check(t, s.writeConfig(settings{mysqlUser: s.my.User, mysqlPassword: "wrong"}))
		if code != 78 || !strings.Contains(out, "MySQL      FAILED") {
			t.Errorf("exit code = %d, output:\n%s", code, out)
		}
	})
}

// purgeBinaryLogs removes every binary log but the newest, so the GTIDs written so far can no
// longer be served. BEFORE NOW() would not do: a log written to during the current second is not
// "before" it.
func purgeBinaryLogs(t *testing.T, root *sql.DB) {
	t.Helper()
	if _, err := root.Exec("FLUSH BINARY LOGS"); err != nil {
		t.Fatal(err)
	}
	rows, err := root.Query("SHOW BINARY LOGS")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	newest := ""
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		newest = string(vals[0].([]byte))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec("PURGE BINARY LOGS TO '" + newest + "'"); err != nil {
		t.Fatal(err)
	}
}
