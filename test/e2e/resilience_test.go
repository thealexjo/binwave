//go:build e2e

package e2e

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// TestKillNineUnderLoadConverges is the at-least-once contract with a real process: while random
// changes are written, binwave is killed with SIGKILL again and again, at moments nobody chose,
// and restarted each time. When the writing stops, ClickHouse must hold exactly what MySQL holds.
func TestKillNineUnderLoadConverges(t *testing.T) {
	for _, store := range []string{"clickhouse", "file"} {
		t.Run(store, func(t *testing.T) {
			t.Parallel()
			s := newStack(t)
			s.mustExec(accountDDL)
			cfg := s.writeConfig(settings{store: store})

			p := startBinwave(t, cfg)
			p.waitReady()

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			w := newWorkload(s.my.DB, 42)
			writing := make(chan struct{})
			go func() { w.run(ctx); close(writing) }()

			rng := rand.New(rand.NewSource(7))
			kills := 0
			for ctx.Err() == nil {
				select {
				case <-time.After(time.Duration(800+rng.Intn(1500)) * time.Millisecond):
				case <-ctx.Done():
				}
				if ctx.Err() != nil {
					break
				}
				p.kill9()
				kills++
				p = startBinwave(t, cfg)
				p.waitReady()
			}
			<-writing

			if kills < 4 {
				t.Fatalf("only %d kills; the scenario proved little", kills)
			}
			t.Logf("%d operations, %d of them failed by design, %d kills", w.ops.Load(), w.failed.Load(), kills)
			s.waitConverged(2*time.Minute, p, accounts)

			if code := p.term(30 * time.Second); code != 0 {
				t.Errorf("graceful stop after the run exited with %d\n%s", code, tail(p.logs(), 2000))
			}
		})
	}
}

// TestFrozenSourceMakesTheProcessExit: a MySQL that stops answering without closing the connection
// must make binwave exit with an error, so the supervisor restarts it, instead of looking
// healthy while replication is stuck. After the source is back, the restarted process catches up.
func TestFrozenSourceMakesTheProcessExit(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})

	p := startBinwave(t, cfg)
	p.waitReady()
	s.mustExec("INSERT INTO binwave.acct VALUES (1, 'ann', 1, 1.00, NULL, NOW(3))")
	s.waitConverged(30*time.Second, p, accounts)

	s.myProxy.Blackhole()
	frozenAt := time.Now()
	code := p.waitExit(60 * time.Second)
	if code == 0 {
		t.Fatalf("binwave exited cleanly on a frozen source\n%s", tail(p.logs(), 2000))
	}
	if !strings.Contains(p.logs(), "went silent") {
		t.Errorf("the exit does not say why; logs:\n%s", tail(p.logs(), 2000))
	}
	if took := time.Since(frozenAt); took > 40*time.Second {
		t.Errorf("the frozen source was noticed after %v", took.Round(time.Second))
	}

	s.myProxy.Resume()
	s.mustExec("INSERT INTO binwave.acct VALUES (2, 'bob', 2, 2.00, NULL, NOW(3))")
	p = startBinwave(t, cfg)
	p.waitReady()
	s.waitConverged(60*time.Second, p, accounts)
}

// TestClickHouseOutagesUnderLoadAreRiddenOut: ClickHouse refuses connections, then hangs without
// answering, while changes keep being written. binwave must stay up, retry, and deliver everything
// once ClickHouse is back.
func TestClickHouseOutagesUnderLoadAreRiddenOut(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})
	p := startBinwave(t, cfg)
	p.waitReady()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 99)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	time.Sleep(3 * time.Second)
	s.chProxy.Refuse() // connection refused, open connections reset
	time.Sleep(5 * time.Second)
	if p.exited() {
		t.Fatalf("binwave exited (%d) during a ClickHouse outage\n%s", p.exit, tail(p.logs(), 2000))
	}
	s.chProxy.Resume()
	time.Sleep(3 * time.Second)

	s.chProxy.Blackhole() // ClickHouse accepts and never answers
	time.Sleep(8 * time.Second)
	if p.exited() {
		t.Fatalf("binwave exited (%d) while ClickHouse hung\n%s", p.exit, tail(p.logs(), 2000))
	}
	s.chProxy.Resume()
	<-writing

	s.waitConverged(3*time.Minute, p, accounts)
	if !strings.Contains(p.logs(), "did not accept the request, retrying") {
		t.Error("the outage was survived without a single logged retry: the fault was not injected")
	}
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestSigtermUnderLoadStopsCleanly: SIGTERM in the middle of a stream of changes flushes, saves the
// position and exits with 0; a restart continues without loss.
func TestSigtermUnderLoadStopsCleanly(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})
	p := startBinwave(t, cfg)
	p.waitReady()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 5)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	time.Sleep(4 * time.Second)
	if code := p.term(45 * time.Second); code != 0 {
		t.Fatalf("SIGTERM under load exited with %d, want 0\n%s", code, tail(p.logs(), 2500))
	}
	if !strings.Contains(p.logs(), "stopped cleanly") {
		t.Errorf("no clean stop in the logs:\n%s", tail(p.logs(), 2000))
	}
	<-writing

	p = startBinwave(t, cfg)
	p.waitReady()
	s.waitConverged(2*time.Minute, p, accounts)
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("second graceful stop exited with %d", code)
	}
}
