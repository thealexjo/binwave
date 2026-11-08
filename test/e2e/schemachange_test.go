//go:build e2e

package e2e

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"
)

var accountsWithExtra = comparison{
	name:  "acct",
	mysql: "SELECT id, owner, qty, CAST(balance AS CHAR), note, CAST(updated_at AS CHAR), extra FROM binwave.acct ORDER BY id",
	ch:    "SELECT id, owner, qty, toDecimalString(balance, 2), note, toString(updated_at), extra FROM replica.acct FINAL ORDER BY id",
}

// TestSchemaChangeUnderLoadAndKillNine: a column is added and another widened while random changes
// are written and binwave is killed again and again, at moments nobody chose. Whatever the moment,
// the restarted process finds the changes done or does them, and when the writing stops
// ClickHouse holds exactly what MySQL holds, including the new column of every old row.
func TestSchemaChangeUnderLoadAndKillNine(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})

	p := startBinwave(t, cfg)
	p.waitReady()

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 314)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	altered := make(chan struct{})
	go func() {
		defer close(altered)
		for _, stmt := range []string{
			"ALTER TABLE binwave.acct ADD COLUMN extra INT NOT NULL DEFAULT 5",
			"ALTER TABLE binwave.acct MODIFY COLUMN qty BIGINT NOT NULL",
		} {
			select {
			case <-time.After(4 * time.Second):
			case <-ctx.Done():
				return
			}
			if _, err := s.my.DB.Exec(stmt); err != nil {
				t.Errorf("%s: %v", stmt, err)
			}
		}
	}()

	rng := rand.New(rand.NewSource(11))
	kills := 0
	for ctx.Err() == nil {
		select {
		case <-time.After(time.Duration(1200+rng.Intn(1800)) * time.Millisecond):
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
	<-altered

	if kills < 4 {
		t.Fatalf("only %d kills; the scenario proved little", kills)
	}
	s.waitConverged(3*time.Minute, p, accountsWithExtra)
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestPrimaryKeyChangeUnderLoadAndKillNine: the primary key of a table that is being written and
// read changes in place, while binwave is killed again and again. The table is rebuilt in ClickHouse
// at the statement, and whatever the moment of a kill (before the rebuild, in it, after it, before
// the position was saved), the restarted process finds the work done or does it, and when the
// writing stops ClickHouse holds exactly what MySQL holds.
func TestPrimaryKeyChangeUnderLoadAndKillNine(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})

	p := startBinwave(t, cfg)
	p.waitReady()

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 2718)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	altered := make(chan struct{})
	go func() {
		defer close(altered)
		select {
		case <-time.After(6 * time.Second):
		case <-ctx.Done():
			return
		}
		if _, err := s.my.DB.Exec("ALTER TABLE binwave.acct DROP PRIMARY KEY, ADD PRIMARY KEY (id, owner)"); err != nil {
			t.Errorf("ALTER: %v", err)
		}
	}()

	rng := rand.New(rand.NewSource(5))
	kills := 0
	for ctx.Err() == nil {
		select {
		case <-time.After(time.Duration(1000+rng.Intn(1800)) * time.Millisecond):
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
	<-altered

	if kills < 4 {
		t.Fatalf("only %d kills; the scenario proved little", kills)
	}
	// The key is (id, owner) now, so an id can be there more than once: order by both, the owner as
	// bytes because ClickHouse compares strings that way and MySQL by collation.
	byBoth := comparison{
		name:  "acct",
		mysql: "SELECT id, owner, qty, CAST(balance AS CHAR), note, CAST(updated_at AS CHAR) FROM binwave.acct ORDER BY id, BINARY owner",
		ch:    "SELECT id, owner, qty, toDecimalString(balance, 2), note, toString(updated_at) FROM replica.acct FINAL ORDER BY id, owner",
	}
	s.waitConverged(3*time.Minute, p, byBoth)
	var key string
	if err := s.ch.DB.QueryRow("SELECT sorting_key FROM system.tables WHERE database = 'replica' AND name = 'acct'").Scan(&key); err != nil || strings.ReplaceAll(strings.Trim(key, "()"), " ", "") != "id,owner" {
		t.Errorf("sort key %q (%v), want id,owner: the table was not rebuilt", key, err)
	}
	if v, err := s.metric("binwave_quarantined_tables"); err != nil || v != 0 {
		t.Errorf("binwave_quarantined_tables = %v (%v)\n%s", v, err, tail(p.logs(), 2500))
	}
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}

// TestTypeChangeThatNeedsACopyUnderLoadAndKillNine: a column becomes unsigned while the table is written
// and binwave is killed again and again. MySQL converts it in place; ClickHouse cannot, so the
// table is rebuilt with every value converted exactly, at the statement, whatever the moment of a kill.
func TestTypeChangeThatNeedsACopyUnderLoadAndKillNine(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.mustExec(accountDDL)
	cfg := s.writeConfig(settings{})

	p := startBinwave(t, cfg)
	p.waitReady()

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Second)
	defer cancel()
	w := newWorkload(s.my.DB, 4242)
	writing := make(chan struct{})
	go func() { w.run(ctx); close(writing) }()

	altered := make(chan struct{})
	go func() {
		defer close(altered)
		select {
		case <-time.After(6 * time.Second):
		case <-ctx.Done():
			return
		}
		if _, err := s.my.DB.Exec("ALTER TABLE binwave.acct MODIFY COLUMN qty INT UNSIGNED NOT NULL"); err != nil {
			t.Errorf("ALTER: %v", err)
		}
	}()

	rng := rand.New(rand.NewSource(9))
	kills := 0
	for ctx.Err() == nil {
		select {
		case <-time.After(time.Duration(1000+rng.Intn(1800)) * time.Millisecond):
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
	<-altered

	if kills < 4 {
		t.Fatalf("only %d kills; the scenario proved little", kills)
	}
	s.waitConverged(3*time.Minute, p, accounts)
	var typ string
	if err := s.ch.DB.QueryRow("SELECT type FROM system.columns WHERE database = 'replica' AND table = 'acct' AND name = 'qty'").Scan(&typ); err != nil || typ != "UInt32" {
		t.Errorf("type of qty %q (%v), want UInt32: the table was not rebuilt", typ, err)
	}
	if v, err := s.metric("binwave_quarantined_tables"); err != nil || v != 0 {
		t.Errorf("binwave_quarantined_tables = %v (%v)\n%s", v, err, tail(p.logs(), 2500))
	}
	if code := p.term(30 * time.Second); code != 0 {
		t.Errorf("graceful stop exited with %d", code)
	}
}
