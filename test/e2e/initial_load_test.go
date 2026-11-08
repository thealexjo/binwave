//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// TestInitialLoadUnderLoadAndKillNine: rows already in the source before binwave ever starts (well past
// what fits in one process's lifetime to read alone) are loaded while four workers keep changing the
// table, the process is killed in the middle of it more than once, and each new process continues the
// load from its saved cursor. When the writing stops ClickHouse holds exactly what MySQL holds.
func TestInitialLoadUnderLoadAndKillNine(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)

	ctx := context.Background()
	conn, err := s.my.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"SET SESSION cte_max_recursion_depth = 1000000",
		`INSERT INTO binwave.acct
			WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 300000)
			SELECT 100000 + n, CONCAT('bulk', n), n, n / 100, NULL, '2024-01-02 03:04:05.678' FROM seq`,
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s\n%v", stmt, err)
		}
	}
	_ = conn.Close()

	cfg := s.writeConfig(settings{extra: "replication: {initial_position: snapshot, snapshot: {parallelism: 2}}"})
	p := startBinwave(t, cfg)
	p.waitReady()

	wctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 2718)
	writing := make(chan struct{})
	go func() { w.run(wctx); close(writing) }()

	rng := rand.New(rand.NewSource(11))
	kills, resumed := 0, 0
	for s.loadInProgress() && kills < 4 {
		time.Sleep(time.Duration(100+rng.Intn(500)) * time.Millisecond)
		p.kill9()
		kills++
		p = startBinwave(t, cfg)
		p.waitReady()
		// resumeResync's own per-table line is Debug now (hundreds of tables would drown everything
		// else at Info after a restart); the aggregate "resyncs resumed" is what fires at Info exactly
		// when a saved resync was picked up, which is what this loop is checking for.
		if strings.Contains(p.logs(), "resyncs resumed") {
			resumed++
		}
	}
	<-writing

	if resumed == 0 {
		t.Fatalf("no process continued the initial load after a kill (%d kills): the scenario proved nothing", kills)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for s.loadInProgress() && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
	}
	if s.loadInProgress() {
		t.Fatalf("the initial load did not finish\n%s", tail(p.logs(), 3000))
	}
	s.waitConverged(3*time.Minute, p, accounts)
	t.Logf("%d kills, %d restarts continued the initial load", kills, resumed)

	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
	if !strings.Contains(p.logs(), "stopped cleanly") {
		t.Errorf("the last process did not stop cleanly\n%s", tail(p.logs(), 1500))
	}
}

// loadInProgress reports whether the initial load still has tables to load.
func (s *stack) loadInProgress() bool {
	st, err := s.status()
	if err != nil {
		s.t.Fatalf("read status: %v", err)
	}
	// The status summarizes the load rather than listing every table of it, so the summary is what
	// says whether anything is left: tables only waiting for a slot are counted, not named.
	load, ok := st["initial_load"].(map[string]any)
	if !ok {
		return false
	}
	left, _ := load["left"].(float64)
	return left > 0
}

// TestAClickHouseRestartDuringTheInitialLoad: the load is where the most paths of the pipeline run at
// once — chunks of several tables, shadow tables being created, tables being replaced, and a position
// that carries the state of every resync. An operator restarting ClickHouse in the middle of it must
// not leave replication standing still afterwards: the load finishes and the position moves again,
// with nobody doing anything about it.
func TestAClickHouseRestartDuringTheInitialLoad(t *testing.T) {
	s := newStack(t)
	s.mustExec(accountDDL)
	// Several tables, so that the load has a queue and slots turn over while the server goes away.
	for i := range 6 {
		s.mustExec(fmt.Sprintf("CREATE TABLE binwave.t%d (id INT PRIMARY KEY, v VARCHAR(40) NOT NULL)", i))
		s.mustExec(fmt.Sprintf("INSERT INTO binwave.t%d WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 900) SELECT n, CONCAT('v', n) FROM seq", i))
	}
	ctx := context.Background()
	conn, err := s.my.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"SET SESSION cte_max_recursion_depth = 1000000",
		`INSERT INTO binwave.acct
			WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 120000)
			SELECT 100000 + n, CONCAT('bulk', n), n, n / 100, NULL, '2024-01-02 03:04:05.678' FROM seq`,
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s\n%v", stmt, err)
		}
	}
	_ = conn.Close()

	cfg := s.writeConfig(settings{extra: "replication: {initial_position: snapshot, snapshot: {parallelism: 2}}"})
	p := startBinwave(t, cfg)
	p.waitReady()

	wctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	w := newWorkload(s.my.DB, 4242)
	writing := make(chan struct{})
	go func() { w.run(wctx); close(writing) }()

	// In the middle of the load: some tables are read, some are still waiting for a slot.
	eventually(t, 60*time.Second, "the load to be under way", func() (bool, string) {
		st, err := s.status()
		if err != nil {
			return false, err.Error()
		}
		load, ok := st["initial_load"].(map[string]any)
		if !ok {
			return false, "no initial load in the status"
		}
		loaded, _ := load["loaded"].(float64)
		return loaded >= 1, fmt.Sprintf("loaded %v of %v", load["loaded"], load["total"])
	})
	before, err := s.status()
	if err != nil {
		t.Fatal(err)
	}
	positionBefore, _ := before["position"].(string)

	s.ch.Restart(t)
	s.chProxy.Retarget(s.ch.Addr)
	t.Logf("clickhouse restarted while loading; position was %s", positionBefore)

	eventually(t, 3*time.Minute, "the position to move after the restart", func() (bool, string) {
		st, err := s.status()
		if err != nil {
			return false, err.Error()
		}
		now, _ := st["position"].(string)
		return now != positionBefore, fmt.Sprintf("position %s, state %v, load %v", now, st["state"], st["initial_load"])
	})
	eventually(t, 5*time.Minute, "the initial load to finish", func() (bool, string) {
		if !s.loadInProgress() {
			return true, ""
		}
		st, _ := s.status()
		return false, fmt.Sprintf("load %v", st["initial_load"])
	})

	cancel()
	<-writing
	s.waitConverged(3*time.Minute, p, accounts)
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}
