//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// supervisor runs binwave the way the systemd unit does: Restart=on-failure with
// RestartPreventExitStatus=78. A process that exits with any other non-zero code is started again
// after restartDelay; 0 and 78 end the supervision.
type supervisor struct {
	t   *testing.T
	cfg string

	mu       sync.Mutex
	procs    []*proc
	exits    []int
	stopping bool
	done     chan struct{}
}

// restartDelay stands for RestartSec of the unit (5s there), shortened for the tests.
const restartDelay = time.Second

func supervise(t *testing.T, cfg string) *supervisor {
	t.Helper()
	s := &supervisor{t: t, cfg: cfg, done: make(chan struct{})}
	first := startBinwave(t, cfg)
	s.procs = append(s.procs, first)
	go s.loop(first)
	t.Cleanup(func() {
		s.stop()
		if t.Failed() {
			t.Logf("binwave logs:\n%s", tail(s.logs(), 6000))
		}
	})
	return s
}

func (s *supervisor) loop(p *proc) {
	defer close(s.done)
	for {
		<-p.done
		s.mu.Lock()
		s.exits = append(s.exits, p.exit)
		if s.stopping || p.exit == 0 || p.exit == 78 {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		time.Sleep(restartDelay)
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return
		}
		p = startBinwave(s.t, s.cfg)
		s.procs = append(s.procs, p)
		s.mu.Unlock()
	}
}

// current is the process running now (or the last one, when supervision ended).
func (s *supervisor) current() *proc {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.procs[len(s.procs)-1]
}

// exitCodes are the codes of the processes that exited so far.
func (s *supervisor) exitCodes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.exits...)
}

// given reports whether supervision ended: a process exited with 0 or 78.
func (s *supervisor) gaveUp() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// logs are the outputs of every process, in order.
func (s *supervisor) logs() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	for i, p := range s.procs {
		fmt.Fprintf(&b, "---- process %d ----\n%s", i+1, p.logs())
	}
	return b.String()
}

func (s *supervisor) stop() {
	s.mu.Lock()
	s.stopping = true
	p := s.procs[len(s.procs)-1]
	s.mu.Unlock()
	if !p.exited() {
		_ = p.term(60 * time.Second)
	}
	<-s.done
}

// convergeWithin is how long a supervised binwave has to converge after the outage of a scenario.
const convergeWithin = 3 * time.Minute

// waitConvergedSupervised is waitConverged for a supervised binwave: an exit is not a failure as long
// as the supervisor restarts it, only giving up is.
func (st *stack) waitConvergedSupervised(sup *supervisor, cs ...comparison) {
	st.t.Helper()
	within := convergeWithin
	var last []string
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if sup.gaveUp() {
			st.t.Fatalf("binwave exited with %v and is not restarted: an operator is needed\n%s", sup.exitCodes(), tail(sup.logs(), 4000))
		}
		last = nil
		for _, c := range cs {
			d, err := st.compare(c)
			if err != nil {
				last = append(last, c.name+": "+err.Error())
				continue
			}
			last = append(last, d...)
		}
		if len(last) == 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	st.t.Fatalf("ClickHouse did not converge with MySQL within %v (exits %v):\n  %s\nlogs:\n%s",
		within, sup.exitCodes(), strings.Join(last, "\n  "), tail(sup.logs(), 4000))
}

// assertHealthyAfterRecovery checks what an operator looks at once the fault is over: the service is
// ready, nothing is reported as still failing, and the position moves.
func (st *stack) assertHealthyAfterRecovery(sup *supervisor) {
	st.t.Helper()
	eventually(st.t, time.Minute, "the service to report itself healthy", func() (bool, string) {
		status, err := st.status()
		if err != nil {
			return false, err.Error()
		}
		state, _ := status["state"].(string)
		if state != "ready" {
			return false, fmt.Sprintf("state %q, last error %v", state, status["last_error"])
		}
		if le, ok := status["last_error"].(map[string]any); ok && le["ongoing"] == true {
			return false, fmt.Sprintf("state ready, but the last error is still reported as ongoing: %v", le)
		}
		code, _, err := st.httpGet("/readyz")
		if err != nil || code != 200 {
			return false, fmt.Sprintf("/readyz %d %v", code, err)
		}
		return true, ""
	})
	st.t.Logf("exit codes along the way: %v", sup.exitCodes())
}

// writeFor runs a workload against db for d.
func writeFor(st *stack, seed int64, d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	newWorkload(st.my.DB, seed).run(ctx)
}

// TestASourceRestartIsRecoveredFromWithoutAnOperator: MySQL is restarted the way an operator does it
// (clean shutdown, start) while changes are written. binwave may exit and be restarted by its
// supervisor, but nobody else is supposed to do anything: the data converges and the status is clean.
func TestASourceRestartIsRecoveredFromWithoutAnOperator(t *testing.T) {
	st := newStack(t)
	st.mustExec(accountDDL)
	sup := supervise(t, st.writeConfig(settings{}))
	sup.current().waitReady()

	writeFor(st, 101, 5*time.Second)
	st.my.Restart(t)
	st.myProxy.Retarget(fmt.Sprintf("%s:%d", st.my.Host, st.my.Port))
	t.Log("mysql restarted")
	writeFor(st, 102, 5*time.Second)

	st.waitConvergedSupervised(sup, accounts)
	st.assertHealthyAfterRecovery(sup)
}

// TestALongSourceOutageIsRecoveredFromWithoutAnOperator: the source refuses connections for longer
// than the reconnects inside the binlog client last (a host reboot, a long maintenance).
func TestALongSourceOutageIsRecoveredFromWithoutAnOperator(t *testing.T) {
	st := newStack(t)
	st.mustExec(accountDDL)
	sup := supervise(t, st.writeConfig(settings{}))
	sup.current().waitReady()

	writeFor(st, 201, 3*time.Second)
	st.myProxy.Refuse()
	t.Log("mysql refuses connections")
	writeFor(st, 202, 45*time.Second) // straight to MySQL: only binwave is cut off
	st.myProxy.Resume()
	t.Log("mysql is reachable again")
	writeFor(st, 203, 3*time.Second)

	st.waitConvergedSupervised(sup, accounts)
	st.assertHealthyAfterRecovery(sup)
}

// TestASourceRestartDuringTheInitialLoad: the load reads chunks over ordinary connections besides the
// binlog stream, and both are cut by the restart.
func TestASourceRestartDuringTheInitialLoad(t *testing.T) {
	st := newStack(t)
	st.mustExec(accountDDL)
	bulkAccounts(t, st, 200000)

	sup := supervise(t, st.writeConfig(settings{extra: "replication: {initial_position: snapshot, snapshot: {parallelism: 2, max_rows_per_second: 20000}}"}))
	sup.current().waitReady()
	eventually(t, time.Minute, "the initial load to be reading", func() (bool, string) {
		v, err := st.metric(`binwave_resync_rows_read_total`)
		return err == nil && v > 20000, fmt.Sprintf("rows read %v %v", v, err)
	})

	st.my.Restart(t)
	st.myProxy.Retarget(fmt.Sprintf("%s:%d", st.my.Host, st.my.Port))
	t.Log("mysql restarted during the load")
	writeFor(st, 301, 5*time.Second)

	eventually(t, 3*time.Minute, "the initial load to finish", func() (bool, string) {
		if sup.gaveUp() {
			t.Fatalf("binwave exited with %v and is not restarted\n%s", sup.exitCodes(), tail(sup.logs(), 4000))
		}
		status, err := st.status()
		if err != nil {
			return false, err.Error()
		}
		return status["initial_load"] == nil, fmt.Sprintf("state %v, load %v, last error %v", status["state"], status["initial_load"], status["last_error"])
	})
	st.waitConvergedSupervised(sup, accounts)
	st.assertHealthyAfterRecovery(sup)
}

// TestASourceRestartDuringAResync: the same for a resync an operator asked for.
func TestASourceRestartDuringAResync(t *testing.T) {
	st := newStack(t)
	st.mustExec(accountDDL)
	// A resync an operator asks for is not rate limited; small chunks of a fixed size make it last
	// long enough to be cut in the middle.
	cfg := st.writeConfig(settings{extra: "replication: {snapshot: {chunk_size: 100, adaptive_chunks: false}}"})
	sup := supervise(t, cfg)
	sup.current().waitReady()
	bulkAccounts(t, st, 200000) // through the stream: the table is replicated before the resync
	st.waitConvergedSupervised(sup, accounts)

	if out, code := binwaveCmd(t, "tables", "resync", "--config", cfg, "binwave.acct"); code != 0 {
		t.Fatalf("tables resync: exit %d\n%s", code, out)
	}
	eventually(t, time.Minute, "the resync to be reading", func() (bool, string) {
		v, err := st.metric("binwave_resyncing_tables")
		return err == nil && v == 1, fmt.Sprintf("resyncing %v %v", v, err)
	})
	time.Sleep(2 * time.Second)

	st.my.Restart(t)
	st.myProxy.Retarget(fmt.Sprintf("%s:%d", st.my.Host, st.my.Port))
	t.Log("mysql restarted during the resync")
	writeFor(st, 401, 5*time.Second)

	eventually(t, 3*time.Minute, "the resync to finish", func() (bool, string) {
		if sup.gaveUp() {
			t.Fatalf("binwave exited with %v and is not restarted\n%s", sup.exitCodes(), tail(sup.logs(), 4000))
		}
		v, err := st.metric("binwave_resyncing_tables")
		return err == nil && v == 0, fmt.Sprintf("resyncing %v %v", v, err)
	})
	st.waitConvergedSupervised(sup, accounts)
	st.assertHealthyAfterRecovery(sup)
}

// TestAClickHouseRestartDuringAResync: the target side of the same.
func TestAClickHouseRestartDuringAResync(t *testing.T) {
	st := newStack(t)
	st.mustExec(accountDDL)
	// A resync an operator asks for is not rate limited; small chunks of a fixed size make it last
	// long enough to be cut in the middle.
	cfg := st.writeConfig(settings{extra: "replication: {snapshot: {chunk_size: 100, adaptive_chunks: false}}"})
	sup := supervise(t, cfg)
	sup.current().waitReady()
	bulkAccounts(t, st, 200000) // through the stream: the table is replicated before the resync
	st.waitConvergedSupervised(sup, accounts)

	if out, code := binwaveCmd(t, "tables", "resync", "--config", cfg, "binwave.acct"); code != 0 {
		t.Fatalf("tables resync: exit %d\n%s", code, out)
	}
	eventually(t, time.Minute, "the resync to be reading", func() (bool, string) {
		v, err := st.metric("binwave_resyncing_tables")
		return err == nil && v == 1, fmt.Sprintf("resyncing %v %v", v, err)
	})
	time.Sleep(2 * time.Second)

	st.ch.Restart(t)
	st.chProxy.Retarget(st.ch.Addr)
	t.Log("clickhouse restarted during the resync")
	writeFor(st, 501, 5*time.Second)

	eventually(t, 3*time.Minute, "the resync to finish", func() (bool, string) {
		if sup.gaveUp() {
			t.Fatalf("binwave exited with %v and is not restarted\n%s", sup.exitCodes(), tail(sup.logs(), 4000))
		}
		v, err := st.metric("binwave_resyncing_tables")
		return err == nil && v == 0, fmt.Sprintf("resyncing %v %v", v, err)
	})
	st.waitConvergedSupervised(sup, accounts)
	st.assertHealthyAfterRecovery(sup)
}

// TestBinwaveStartedWhileClickHouseIsDown: a host reboot brings binwave up before ClickHouse. The
// start is retried within retry.startup_budget, and once it succeeds nothing is left reported as failing.
func TestBinwaveStartedWhileClickHouseIsDown(t *testing.T) {
	st := newStack(t)
	st.mustExec(accountDDL)
	st.chProxy.Refuse()
	sup := supervise(t, st.writeConfig(settings{}))
	time.Sleep(10 * time.Second)
	st.chProxy.Resume()
	sup.current().waitReady()

	writeFor(st, 601, 3*time.Second)
	st.waitConvergedSupervised(sup, accounts)
	st.assertHealthyAfterRecovery(sup)
}

// TestBinwaveStartedWhileMySQLIsDown: the same with the source.
func TestBinwaveStartedWhileMySQLIsDown(t *testing.T) {
	st := newStack(t)
	st.mustExec(accountDDL)
	st.myProxy.Refuse()
	sup := supervise(t, st.writeConfig(settings{}))
	time.Sleep(10 * time.Second)
	st.myProxy.Resume()
	sup.current().waitReady()

	writeFor(st, 701, 3*time.Second)
	st.waitConvergedSupervised(sup, accounts)
	st.assertHealthyAfterRecovery(sup)
}

// bulkAccounts fills binwave.acct with n rows in one statement.
func bulkAccounts(t *testing.T, st *stack, n int) {
	t.Helper()
	ctx := context.Background()
	conn, err := st.my.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range []string{
		"SET SESSION cte_max_recursion_depth = 1000000",
		fmt.Sprintf(`INSERT INTO binwave.acct
			WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < %d)
			SELECT 100000 + n, CONCAT('bulk', n), n, n / 100, NULL, '2024-01-02 03:04:05.678' FROM seq`, n),
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s\n%v", stmt, err)
		}
	}
}
