//go:build e2e

package e2e

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thealexjo/binwave/test/migrationsim"
)

// The scenarios run the online schema change tools' statements through the simulation of
// test/migrationsim, which is checked statement for statement against what the real
// pt-online-schema-change and gh-ost wrote to the binlog. The real tools are not part of the
// pipelines (see migration_real_test.go).

// migrationTool runs one ALTER of binwave.acct the way a tool does, and returns when the
// migration is over.
type migrationTool struct {
	name string
	run  func(ctx context.Context, s *stack, alter string) error
}

func simPtOSC(dropSwap bool, o migrationsim.Options) migrationTool {
	return migrationTool{"pt-online-schema-change", func(ctx context.Context, s *stack, alter string) error {
		return migrationsim.New(rootDB(s.t, s), "binwave", "acct").PtOSC(ctx, alter, migrationsim.PtOSCOptions{Options: o, DropSwap: dropSwap})
	}}
}

func simGhost(twoStep bool, o migrationsim.Options) migrationTool {
	name := "gh-ost"
	if twoStep {
		name += " two-step"
	}
	return migrationTool{name, func(ctx context.Context, s *stack, alter string) error {
		return migrationsim.New(rootDB(s.t, s), "binwave", "acct").Ghost(ctx, alter, migrationsim.GhostOptions{Options: o, TwoStep: twoStep})
	}}
}

// slowCopy makes the copy last long enough for binwave to be killed in it.
var slowCopy = migrationsim.Options{ChunkSize: 50, ChunkPause: 40 * time.Millisecond}

// acctWith renders binwave.acct with one more column, in one canonical text form on both sides.
func acctWith(column string) comparison {
	return comparison{
		name:  "acct+" + column,
		mysql: "SELECT id, owner, qty, CAST(balance AS CHAR), note, CAST(updated_at AS CHAR), " + column + " FROM binwave.acct ORDER BY id",
		ch:    "SELECT id, owner, qty, toDecimalString(balance, 2), note, toString(updated_at), " + column + " FROM replica.acct FINAL ORDER BY id",
	}
}

// migrationSteps is a column added, a type widened, the column renamed and dropped: the changes
// of the tools' everyday use, one migration each. The workload does not touch the column.
var migrationSteps = []struct {
	alter string
	check comparison
}{
	{"ADD COLUMN extra INT NOT NULL DEFAULT 5", acctWith("extra")},
	{"MODIFY COLUMN qty BIGINT NOT NULL", acctWith("extra")},
	{"CHANGE COLUMN extra extra2 INT NOT NULL DEFAULT 5", acctWith("extra2")},
	{"DROP COLUMN extra2", accounts},
}

// TestOnlineSchemaChangeUnderLoad: each tool migrates the table while random changes are written
// to it, and after every migration ClickHouse holds exactly what MySQL holds, in the new
// structure. Only binwave.acct is in tables.include: the tools' shadow tables are followed
// because of their names, and the swap is a rename in ClickHouse.
func TestOnlineSchemaChangeUnderLoad(t *testing.T) {
	t.Parallel()
	for _, tl := range []migrationTool{simPtOSC(false, slowCopy), simGhost(false, slowCopy), simGhost(true, slowCopy)} {
		t.Run(tl.name, func(t *testing.T) { t.Parallel(); migrateUnderLoad(t, tl, false) })
	}
}

// TestOnlineSchemaChangeUnderLoadAndKillNine is the same, with binwave killed again and again while
// the migrations run: at the creation of the shadow, in the middle of its filling, at the swap, at
// the drop of the old table.
func TestOnlineSchemaChangeUnderLoadAndKillNine(t *testing.T) {
	t.Parallel()
	for _, tl := range []migrationTool{simPtOSC(false, slowCopy), simGhost(false, slowCopy), simGhost(true, slowCopy)} {
		t.Run(tl.name, func(t *testing.T) { t.Parallel(); migrateUnderLoad(t, tl, true) })
	}
}

func migrateUnderLoad(t *testing.T, tl migrationTool, kills bool) {
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{include: "binwave.acct", store: "file"})
	p := startBinwave(t, cfg)
	p.waitReady()

	// Some rows to begin with, so the first migration copies something.
	seed := newWorkload(s.my.DB, 1)
	seedCtx, stopSeed := context.WithTimeout(context.Background(), 4*time.Second)
	seed.run(seedCtx)
	stopSeed()

	killed := 0
	rng := rand.New(rand.NewSource(7))
	for i, step := range migrationSteps {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		w := newWorkload(s.my.DB, int64(100+i))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); w.run(ctx) }()

		toolDone := make(chan error, 1)
		go func() { toolDone <- tl.run(ctx, s, step.alter) }()

		var err error
	migrating:
		for {
			wait := 24 * time.Hour // without kills, wait for the tool
			if kills {
				wait = time.Duration(300+rng.Intn(700)) * time.Millisecond
			}
			select {
			case err = <-toolDone:
				break migrating
			case <-time.After(wait):
				p.kill9()
				killed++
				p = startBinwave(t, cfg)
				p.waitReady()
			}
		}
		cancel()
		wg.Wait()
		if err != nil {
			t.Fatalf("%s %q: %v\nbinwave logs:\n%s", tl.name, step.alter, err, tail(p.logs(), 2500))
		}

		s.waitConverged(3*time.Minute, p, step.check)
		if p.exited() {
			t.Fatalf("binwave exited (%d) at %q\n%s", p.exit, step.alter, tail(p.logs(), 2500))
		}
		if v, err := s.metric("binwave_quarantined_tables"); err != nil || v != 0 {
			t.Fatalf("after %q: binwave_quarantined_tables = %v (%v)\n%s", step.alter, v, err, tail(p.logs(), 2500))
		}
		for _, leftover := range []string{"_acct_new", "_acct_old", "_acct_gho", "_acct_del"} {
			var n uint64
			if err := s.ch.DB.QueryRow("SELECT count() FROM system.tables WHERE database = 'replica' AND name = ?", leftover).Scan(&n); err != nil || n != 0 {
				t.Errorf("after %q: ClickHouse still has %s (%v)", step.alter, leftover, err)
			}
		}
	}
	if kills && killed < 4 {
		t.Fatalf("only %d kills; the scenario proved little", killed)
	}
	if strings.Contains(p.logs(), "level=ERROR") && strings.Contains(p.logs(), "quarantined") {
		t.Errorf("binwave logged a quarantine:\n%s", tail(p.logs(), 2500))
	}
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}
