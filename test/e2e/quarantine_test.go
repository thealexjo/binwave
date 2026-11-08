//go:build e2e

package e2e

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// binwaveCmd runs a binwave command to its end and returns its output and exit code.
func binwaveCmd(t *testing.T, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(binaryPath, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), exitCode(err)
}

func (s *stack) quarantinedTables() []string {
	st, err := s.status()
	if err != nil {
		return nil
	}
	list, _ := st["quarantined"].([]any)
	var out []string
	for _, e := range list {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m["table"].(string))
		}
	}
	return out
}

// TestAServiceWithATableItCannotReplicateStaysUp: the promise of the quarantine, with the real
// process. The table without a key is set aside, loudly, and the process does not exit; the other
// table replicates; a kill -9 and a restart change nothing.
func TestAServiceWithATableItCannotReplicateStaysUp(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL, "CREATE TABLE binwave.keyless (a INT, b INT)")
	cfg := s.writeConfig(settings{})

	p := startBinwave(t, cfg)
	eventually(t, 60*time.Second, "the keyless table to be quarantined", func() (bool, string) {
		return strings.Join(s.quarantinedTables(), ",") == "binwave.keyless", tail(p.logs(), 300)
	})
	if p.exited() {
		t.Fatalf("the process exited (%d) because of one table\n%s", p.exit, tail(p.logs(), 2000))
	}
	for _, want := range []string{"table quarantined", "binwave.keyless"} {
		if !strings.Contains(p.logs(), want) {
			t.Errorf("the logs lack %q:\n%s", want, tail(p.logs(), 2000))
		}
	}
	if v, err := s.metric("binwave_quarantined_tables"); err != nil || v != 1 {
		t.Errorf("binwave_quarantined_tables = %v %v", v, err)
	}

	s.mustExec(
		"INSERT INTO binwave.acct VALUES (1, 'ann', 1, 1.00, NULL, NOW(3))",
		"INSERT INTO binwave.keyless VALUES (1, 1), (2, 2), (3, 3)",
	)
	s.waitConverged(time.Minute, p, accounts)
	eventually(t, 30*time.Second, "the skipped rows to be counted", func() (bool, string) {
		v, err := s.metric("binwave_quarantine_skipped_rows_total")
		return err == nil && v >= 3, "not counted yet"
	})

	// kill -9 and restart: the table is still quarantined, the service still replicates.
	p.kill9()
	p = startBinwave(t, cfg)
	p.waitStarted()
	s.mustExec("INSERT INTO binwave.acct VALUES (2, 'bob', 2, 2.00, NULL, NOW(3))")
	s.waitConverged(time.Minute, p, accounts)
	if got := strings.Join(s.quarantinedTables(), ","); got != "binwave.keyless" {
		t.Errorf("quarantined after the restart: %q", got)
	}
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestTheOperatorFlowWithARunningService: list, fix the cause, release, all through the control
// channel of the running service; the table replicates again from then on, with the gap
// documented rather than hidden.
func TestTheOperatorFlowWithARunningService(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec("CREATE TABLE binwave.later (id INT, v INT NOT NULL)")
	cfg := s.writeConfig(settings{store: "file"})

	p := startBinwave(t, cfg)
	eventually(t, 60*time.Second, "the keyless table to be quarantined", func() (bool, string) {
		return strings.Join(s.quarantinedTables(), ",") == "binwave.later", tail(p.logs(), 300)
	})
	s.mustExec("INSERT INTO binwave.later VALUES (1, 1), (2, 2)") // lost for ClickHouse: the table is set aside

	out, code := binwaveCmd(t, "tables", "list", "--config", cfg)
	if code != 0 || !strings.Contains(out, "binwave.later") || !strings.Contains(out, "reason:") {
		t.Errorf("tables list on a running service: exit %d, output:\n%s", code, out)
	}

	// The operator fixes the cause: the table gets a key. Then releases it, without stopping anything.
	s.mustExec("ALTER TABLE binwave.later ADD PRIMARY KEY (id)")
	out, code = binwaveCmd(t, "tables", "release", "--accept-data-gap", "--config", cfg, "binwave.later")
	if code != 0 || !strings.Contains(out, "released binwave.later") || !strings.Contains(out, "not in ClickHouse") {
		t.Fatalf("tables release on a running service: exit %d, output:\n%s", code, out)
	}
	if p.exited() {
		t.Fatalf("the service exited (%d) on a release", p.exit)
	}
	if got := s.quarantinedTables(); len(got) != 0 {
		t.Errorf("still quarantined: %v", got)
	}

	s.mustExec("INSERT INTO binwave.later VALUES (3, 3), (4, 4)")
	s.expectIDs(t, "later", []int32{3, 4}) // the gap: rows 1 and 2 were never in ClickHouse
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestReleaseNeedsTheAcknowledgementOfTheDataGap: the command refuses without the flag and names it.
func TestReleaseNeedsTheAcknowledgementOfTheDataGap(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	cfg := s.writeConfig(settings{store: "file"})

	out, code := binwaveCmd(t, "tables", "release", "--config", cfg, "binwave.later")

	if code != 2 || !strings.Contains(out, "--accept-data-gap") || !strings.Contains(out, "stay missing in ClickHouse") {
		t.Errorf("exit %d, output:\n%s", code, out)
	}
}

// TestTheOperatorFlowWithAStoppedService: the same with the service stopped, on the saved state.
func TestTheOperatorFlowWithAStoppedService(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec("CREATE TABLE binwave.later (id INT, v INT NOT NULL)")
	cfg := s.writeConfig(settings{store: "file"})

	p := startBinwave(t, cfg)
	eventually(t, 60*time.Second, "the keyless table to be quarantined", func() (bool, string) {
		return strings.Join(s.quarantinedTables(), ",") == "binwave.later", tail(p.logs(), 300)
	})
	s.mustExec("INSERT INTO binwave.later VALUES (1, 1), (2, 2)")
	// Stopped right after the rows, whether or not the service has read them: the fence of the
	// release keeps the changes of the time before the fix out.
	if code := p.term(30 * time.Second); code != 0 {
		t.Fatalf("graceful stop exited with %d", code)
	}

	out, code := binwaveCmd(t, "tables", "list", "--config", cfg)
	if code != 0 || !strings.Contains(out, "binwave.later") || !strings.Contains(out, "reason:") {
		t.Errorf("tables list: exit %d, output:\n%s", code, out)
	}
	s.mustExec("ALTER TABLE binwave.later ADD PRIMARY KEY (id)")
	out, code = binwaveCmd(t, "tables", "release", "--accept-data-gap", "--config", cfg, "binwave.later")
	if code != 0 || !strings.Contains(out, "released binwave.later") {
		t.Fatalf("tables release: exit %d, output:\n%s", code, out)
	}
	// Until the stream passes the fence the list shows the table as released, not as quarantined.
	if out, _ := binwaveCmd(t, "tables", "list", "--config", cfg); !strings.Contains(out, "binwave.later") || !strings.Contains(out, "still skipped") {
		t.Errorf("after the release the list shows: %s", out)
	}

	p = startBinwave(t, cfg)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(tail(p.logs(), 4000))
		}
	})
	p.waitStarted()
	s.mustExec("INSERT INTO binwave.later VALUES (3, 3), (4, 4)")
	s.expectIDs(t, "later", []int32{3, 4})
	if got := s.quarantinedTables(); len(got) != 0 {
		t.Errorf("still quarantined: %v", got)
	}
}

// expectIDs waits until the ids of the live rows of a ClickHouse table are exactly want.
func (s *stack) expectIDs(t *testing.T, table string, want []int32) {
	t.Helper()
	var got []int32
	eventually(t, 60*time.Second, "the rows of "+table, func() (bool, string) {
		got = nil
		rows, err := s.ch.DB.Query("SELECT id FROM replica." + table + " FINAL WHERE _is_deleted = 0 ORDER BY id")
		if err != nil {
			return false, err.Error()
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id int32
			if err := rows.Scan(&id); err != nil {
				return false, err.Error()
			}
			got = append(got, id)
		}
		if err := rows.Err(); err != nil {
			return false, err.Error()
		}
		if len(got) != len(want) {
			return false, "have rows"
		}
		for i := range want {
			if got[i] != want[i] {
				return false, "different rows"
			}
		}
		return true, ""
	})
}
