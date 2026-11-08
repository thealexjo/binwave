//go:build e2e

package e2e

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// TestResyncUnderLoadAndKillNine: a table with a few hundred thousand rows is read again while four
// writers change it, and the process is killed in the middle of the resync, more than once. Each new
// process continues from the saved cursor, and when the writing stops ClickHouse holds exactly what MySQL
// holds, with no shadow table left.
func TestResyncUnderLoadAndKillNine(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})

	p := startBinwave(t, cfg)
	p.waitReady()

	// Rows beyond the key space of the workload, so that most of the table is only ever seen by the snapshot.
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
	s.waitConverged(4*time.Minute, p, accounts)

	wctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 3141)
	writing := make(chan struct{})
	go func() { w.run(wctx); close(writing) }()

	if out, code := binwaveCmd(t, "tables", "resync", "--config", cfg, "binwave.acct"); code != 0 || !strings.Contains(out, "resync begun") {
		t.Fatalf("tables resync: exit %d\n%s", code, out)
	}

	rng := rand.New(rand.NewSource(9))
	kills, resumed := 0, 0
	for s.shadowExists() && kills < 4 {
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
		t.Fatalf("no process continued a resync after a kill (%d kills): the scenario proved nothing", kills)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for s.shadowExists() && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
	}
	if s.shadowExists() {
		t.Fatalf("the resync did not finish\n%s", tail(p.logs(), 3000))
	}
	s.waitConverged(3*time.Minute, p, accounts)
	t.Logf("%d kills, %d restarts continued the resync", kills, resumed)

	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
	if !strings.Contains(p.logs(), "stopped cleanly") {
		t.Errorf("the last process did not stop cleanly\n%s", tail(p.logs(), 1500))
	}
}

// shadowExists reports whether ClickHouse has the shadow table of a resync of binwave.acct.
func (s *stack) shadowExists() bool {
	var n uint64
	q := "SELECT count() FROM system.tables WHERE database = 'replica' AND name = 'acct__resync'"
	if err := s.ch.DB.QueryRow(q).Scan(&n); err != nil {
		s.t.Fatalf("look for the shadow table: %v", err)
	}
	return n > 0
}
