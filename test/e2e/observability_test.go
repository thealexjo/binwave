//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/observability"
)

// eventually polls until check returns true, and fails with what it last saw.
func eventually(t *testing.T, within time.Duration, what string, check func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(within)
	last := ""
	for time.Now().Before(deadline) {
		ok, seen := check()
		if ok {
			return
		}
		last = seen
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s; last seen: %s", within, what, last)
}

// TestHealthAndReadinessWhileADatabaseIsDown: a service that waits for a database is alive, not
// ready, and says what it is waiting for; once the database is back it becomes ready by itself.
func TestHealthAndReadinessWhileADatabaseIsDown(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	s.chProxy.Refuse()

	p := startBinwave(t, s.writeConfig(settings{}))

	eventually(t, 60*time.Second, "the status to name the failed start", func() (bool, string) {
		st, err := s.status()
		if err != nil {
			return false, err.Error()
		}
		le, _ := st["last_error"].(map[string]any)
		return st["state"] == "starting" && le != nil && le["kind"] == "startup", tail(strings.TrimSpace(p.logs()), 300)
	})
	if code, body, err := s.httpGet("/healthz"); err != nil || code != 200 {
		t.Errorf("/healthz while starting = %d %q %v, want 200: a service waiting for a database is alive", code, body, err)
	}
	if code, _, err := s.httpGet("/readyz"); err != nil || code != 503 {
		t.Errorf("/readyz while starting = %d %v, want 503", code, err)
	}
	if v, err := s.metric("binwave_startup_failures_total"); err != nil || v < 1 {
		t.Errorf("binwave_startup_failures_total = %v %v, want at least 1", v, err)
	}
	if v, err := s.metric("binwave_ready"); err != nil || v != 0 {
		t.Errorf("binwave_ready = %v %v while starting", v, err)
	}

	s.chProxy.Resume()
	eventually(t, 60*time.Second, "/readyz to turn 200", func() (bool, string) {
		code, body, err := s.httpGet("/readyz")
		return err == nil && code == 200, body
	})
	st, err := s.status()
	if err != nil || st["state"] != "ready" {
		t.Errorf("status = %v %v", st, err)
	}

	s.mustExec("INSERT INTO binwave.acct VALUES (1, 'ann', 1, 1.00, NULL, NOW(3))")
	s.waitConverged(time.Minute, p, accounts)
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestLagRetriesAndHealthDuringAClickHouseHang: while ClickHouse accepts connections and never
// answers, the lag of the replica grows, the retries are counted and the process stays healthy
// (it is waiting, not stuck); afterwards the lag returns to zero and stays there on a quiet source.
func TestLagRetriesAndHealthDuringAClickHouseHang(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	p := startBinwave(t, s.writeConfig(settings{}))
	p.waitStarted()

	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 21)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	eventually(t, 30*time.Second, "rows to be replicated before the hang", func() (bool, string) {
		v, err := s.metric("binwave_rows_written_total")
		return err == nil && v > 0, "rows_written not above 0"
	})

	s.chProxy.Blackhole()
	var maxLag float64
	eventually(t, 25*time.Second, "the lag to grow while ClickHouse hangs", func() (bool, string) {
		lag, err := s.metric("binwave_lag_seconds")
		if err != nil {
			return false, err.Error()
		}
		if code, body, err := s.httpGet("/healthz"); err != nil || code != 200 {
			t.Errorf("/healthz during a ClickHouse hang = %d %q %v: the process is waiting, not stuck", code, body, err)
		}
		maxLag = max(maxLag, lag)
		return lag >= 5, "lag " + time.Duration(lag*float64(time.Second)).String()
	})
	// The hung request is cut after attempt_timeout (10s in these scenarios) and retried.
	eventually(t, 30*time.Second, "the retry to be counted", func() (bool, string) {
		v, err := s.metric(`binwave_clickhouse_retries_total{reason="transient"}`)
		return err == nil && v >= 1, "no retry yet"
	})
	if st, err := s.status(); err != nil || st["last_error"] == nil {
		t.Errorf("status during the hang = %v %v, want the last error", st, err)
	}

	s.chProxy.Resume()
	<-writing
	s.waitConverged(2*time.Minute, p, accounts)

	// The source is quiet now: the lag is 0, and the status says the replica caught up.
	eventually(t, 30*time.Second, "the lag to return to 0", func() (bool, string) {
		lag, err := s.metric("binwave_lag_seconds")
		st, err2 := s.status()
		if err != nil || err2 != nil {
			return false, "scrape failed"
		}
		return lag == 0 && st["caught_up"] == true, "lag " + time.Duration(lag*float64(time.Second)).String()
	})
	time.Sleep(6 * time.Second)
	if lag, err := s.metric("binwave_lag_seconds"); err != nil || lag != 0 {
		t.Errorf("lag of a quiet source = %v %v, want 0 however long it stays quiet", lag, err)
	}
	if age, err := s.metric("binwave_position_saved_age_seconds"); err != nil || age < 5 {
		t.Errorf("position age = %v %v, want it to keep growing on a quiet source", age, err)
	}
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestReplicationSurvivesAClickHouseRestart: an operator restarts ClickHouse while replication runs.
// What binwave sees is its connections reset, then refused, then a server that answers again — and
// after that the stream has to go on by itself: the position must move again, the rows must arrive,
// and nothing may be left waiting for a human.
func TestReplicationSurvivesAClickHouseRestart(t *testing.T) {
	s := newStack(t)
	s.mustExec(accountDDL)
	p := startBinwave(t, s.writeConfig(settings{}))
	p.waitReady()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 77)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	eventually(t, 30*time.Second, "rows to be replicated before the restart", func() (bool, string) {
		v, err := s.metric("binwave_rows_written_total")
		return err == nil && v > 0, "rows_written not above 0"
	})
	before, err := s.status()
	if err != nil {
		t.Fatal(err)
	}
	positionBefore, _ := before["position"].(string)
	transactionsBefore, err := s.metric("binwave_transactions_total")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("before the restart: position %s, transactions %.0f", positionBefore, transactionsBefore)

	s.ch.Restart(t)
	// A real server keeps its address across a restart; a container does not, so the proxy binwave
	// talks to is pointed at the new one. Nothing about binwave's configuration changes.
	s.chProxy.Retarget(s.ch.Addr)
	t.Log("clickhouse restarted")

	// This is the whole point: without any operator action, the stream goes on.
	eventually(t, 2*time.Minute, "the position to move after the restart", func() (bool, string) {
		st, err := s.status()
		if err != nil {
			return false, err.Error()
		}
		now, _ := st["position"].(string)
		tx, err2 := s.metric("binwave_transactions_total")
		if err2 != nil {
			return false, err2.Error()
		}
		return now != positionBefore && tx > transactionsBefore,
			fmt.Sprintf("position %s (was %s), transactions %.0f (was %.0f), state %v", now, positionBefore, tx, transactionsBefore, st["state"])
	})

	<-writing
	s.waitConverged(2*time.Minute, p, accounts)

	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestReplicationSurvivesALongClickHouseOutage: a restart of a server that holds thousands of tables
// is not seconds but minutes, and while it is down every connection is refused. The retries have to
// outlast that and pick up by themselves — the operator is not supposed to do anything.
func TestReplicationSurvivesALongClickHouseOutage(t *testing.T) {
	s := newStack(t)
	s.mustExec(accountDDL)
	p := startBinwave(t, s.writeConfig(settings{}))
	p.waitReady()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	w := newWorkload(s.my.DB, 91)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	eventually(t, 30*time.Second, "rows to be replicated before the outage", func() (bool, string) {
		v, err := s.metric("binwave_rows_written_total")
		return err == nil && v > 0, "rows_written not above 0"
	})
	before, err := s.status()
	if err != nil {
		t.Fatal(err)
	}
	positionBefore, _ := before["position"].(string)

	// Down for a minute: longer than the attempt timeout and many rounds of backoff.
	s.chProxy.Refuse()
	eventually(t, 45*time.Second, "the state to say the data path is failing", func() (bool, string) {
		st, err := s.status()
		if err != nil {
			return false, err.Error()
		}
		code, _, err := s.httpGet("/readyz")
		if err != nil {
			return false, err.Error()
		}
		degraded, err := s.metric("binwave_degraded")
		if err != nil {
			return false, err.Error()
		}
		e, _ := st["last_error"].(map[string]any)
		ongoing, _ := e["ongoing"].(bool)
		return st["state"] == string(observability.StateDegraded) && code == 503 && degraded == 1 && ongoing,
			fmt.Sprintf("state %v, /readyz %d, degraded %v, last error %v", st["state"], code, degraded, st["last_error"])
	})
	if secs, err := s.metric("binwave_degraded_seconds"); err != nil || secs <= 0 {
		t.Errorf("binwave_degraded_seconds = %v %v, want the time the data path has been failing", secs, err)
	}
	if ready, err := s.metric("binwave_ready"); err != nil || ready != 0 {
		t.Errorf("binwave_ready = %v %v while degraded, want 0", ready, err)
	}
	time.Sleep(20 * time.Second)
	s.chProxy.Resume()

	// Back to ready by itself, and the record of the failure stops claiming to be happening.
	eventually(t, 60*time.Second, "the state to return to ready", func() (bool, string) {
		st, err := s.status()
		if err != nil {
			return false, err.Error()
		}
		e, _ := st["last_error"].(map[string]any)
		ongoing, _ := e["ongoing"].(bool)
		return st["state"] == string(observability.StateReady) && !ongoing,
			fmt.Sprintf("state %v, last error %v", st["state"], st["last_error"])
	})

	eventually(t, 2*time.Minute, "the position to move after the outage", func() (bool, string) {
		st, err := s.status()
		if err != nil {
			return false, err.Error()
		}
		now, _ := st["position"].(string)
		return now != positionBefore, fmt.Sprintf("position %s (was %s), state %v", now, positionBefore, st["state"])
	})

	cancel()
	<-writing
	s.waitConverged(2*time.Minute, p, accounts)
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}
