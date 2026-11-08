//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"
)

func (s *stack) ready() (int, string) {
	code, body, err := s.httpGet("/readyz")
	if err != nil {
		return 0, err.Error()
	}
	return code, strings.TrimSpace(body)
}

// TestPauseAndResumeWithTheRealBinary: STOP REPLICA for binwave. The process stays up and answers
// while paused and reads nothing; after the resume everything the source did meanwhile arrives.
func TestPauseAndResumeWithTheRealBinary(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})

	p := startBinwave(t, cfg)
	p.waitStarted()
	s.mustExec("INSERT INTO binwave.acct VALUES (1, 'ann', 1, 1.00, NULL, NOW(3)), (2, 'bob', 2, 2.00, NULL, NOW(3))")
	s.waitConverged(time.Minute, p, accounts)

	if out, code := binwaveCmd(t, "replication", "pause", "--config", cfg); code != 0 || !strings.Contains(out, "replication is paused") {
		t.Fatalf("pause: exit %d\n%s", code, out)
	}
	if p.exited() {
		t.Fatalf("the process exited (%d) on a pause\n%s", p.exit, tail(p.logs(), 1500))
	}
	if code, body, err := s.httpGet("/healthz"); err != nil || code != 200 {
		t.Errorf("/healthz while paused = %d %q %v: a paused service is alive", code, body, err)
	}
	if code, body := s.ready(); code != 503 || body != "paused" {
		t.Errorf("/readyz while paused = %d %q, want 503 paused", code, body)
	}
	out, code := binwaveCmd(t, "replication", "status", "--config", cfg)
	if code != 0 || !strings.Contains(out, "state:        paused") {
		t.Errorf("status: exit %d\n%s", code, out)
	}

	// The source goes on. Nothing is read.
	s.mustExec("INSERT INTO binwave.acct VALUES (3, 'cy', 3, 3.00, NULL, NOW(3))", "UPDATE binwave.acct SET qty = 99 WHERE id = 1", "DELETE FROM binwave.acct WHERE id = 2")
	time.Sleep(3 * time.Second)
	if diffs, err := s.compare(accounts); err != nil || len(diffs) == 0 {
		t.Errorf("ClickHouse matches MySQL during a pause (diffs %v, err %v): the service is reading", diffs, err)
	}
	if lag, err := s.metric("binwave_lag_seconds"); err != nil || lag < 2 {
		t.Errorf("lag = %v %v while paused, want it to grow", lag, err)
	}

	if out, code := binwaveCmd(t, "replication", "resume", "--config", cfg); code != 0 {
		t.Fatalf("resume: exit %d\n%s", code, out)
	}
	s.waitConverged(time.Minute, p, accounts)
	eventually(t, 30*time.Second, "/readyz to turn 200", func() (bool, string) {
		c, body := s.ready()
		return c == 200, body
	})

	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestServiceThatStartsPausedAndIsStoppedWhilePaused: replication.autostart false, and SIGTERM
// without a resume first.
func TestServiceThatStartsPausedAndIsStoppedWhilePaused(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{extra: "replication: {autostart: false}"})

	p := startBinwave(t, cfg)
	eventually(t, 60*time.Second, "the service to say it is paused", func() (bool, string) {
		st, err := s.status()
		return err == nil && st["state"] == "paused", tail(p.logs(), 300)
	})
	s.mustExec("INSERT INTO binwave.acct VALUES (1, 'ann', 1, 1.00, NULL, NOW(3))")
	time.Sleep(2 * time.Second)
	// The ClickHouse table does not even exist: nothing was read, nothing was prepared.
	if diffs, err := s.compare(accounts); err == nil && len(diffs) == 0 {
		t.Error("a service that was told not to start replication replicated a change")
	}

	// What happened before the resume is replicated after it: the start point was fixed at the start.
	if out, code := binwaveCmd(t, "replication", "resume", "--config", cfg); code != 0 {
		t.Fatalf("resume: exit %d\n%s", code, out)
	}
	s.waitConverged(time.Minute, p, accounts)

	// Paused again and stopped: a graceful stop, exit 0, and the socket is removed.
	if _, code := binwaveCmd(t, "replication", "pause", "--config", cfg); code != 0 {
		t.Fatal("pause failed")
	}
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("SIGTERM while paused exited with %d\n%s", code, tail(p.logs(), 1500))
	}
}

// TestReplicationCommandsWithoutARunningService: the exit code says what is wrong.
func TestReplicationCommandsWithoutARunningService(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	cfg := s.writeConfig(settings{})

	out, code := binwaveCmd(t, "replication", "pause", "--config", cfg)
	if code != 1 || !strings.Contains(out, "Is the service running?") {
		t.Errorf("exit %d\n%s", code, out)
	}
}

// TestKillNineWhilePausedResumesOnRestart: a pause set by a command is not remembered; like MySQL
// without skip_replica_start, a restarted service replicates again. replication.autostart false
// is the way to have it wait.
func TestKillNineWhilePausedResumesOnRestart(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})

	p := startBinwave(t, cfg)
	p.waitStarted()
	if _, code := binwaveCmd(t, "replication", "pause", "--config", cfg); code != 0 {
		t.Fatal("pause failed")
	}
	s.mustExec("INSERT INTO binwave.acct VALUES (1, 'ann', 1, 1.00, NULL, NOW(3))")
	p.kill9()

	p = startBinwave(t, cfg)
	p.waitStarted()
	s.waitConverged(time.Minute, p, accounts)
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}
