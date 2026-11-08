package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/event"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// tables.autoresync resynchronizes, without an operator, a table quarantined for a reason a resync
// fixes on its own (checkpoint.Quarantined.Recoverable, decided by chsink.Recoverable when the table
// was first quarantined). It never touches an ordinary quarantine, and gives up loudly, bounded by
// maxAutoresyncAttempts, if the automatic resync itself keeps failing.

// TestAutoresyncFixesAGeneratedColumnOnItsOwn: an ADD COLUMN whose value for existing rows binwave
// cannot reproduce (a generated column) quarantines the table with Recoverable set; tables.autoresync
// resynchronizes it without an operator, and the observer sees it happen despite the self-resolve.
func TestAutoresyncFixesAGeneratedColumnOnItsOwn(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.cfg.Tables.Autoresync = true
	h.snapshot = &fakeSnapshot{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, executed: fence(2)}
	h.obs = &fakeObserver{}
	h.source.script = []scriptedTx{
		uuidTx(1, insertRows(items, 1, "a"), insertRows(items, 2, "b")),
		uuidTx(2, event.DDL{Database: "shop", Query: "ALTER TABLE items ADD COLUMN total INT GENERATED ALWAYS AS (1) STORED", Time: epoch}),
	}
	h.source.until = func() bool { return h.sink.swapped() > 0 }

	if err := h.run(t); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	entries, err := checkpoint.DecodeQuarantine(h.store.current().Quarantine)
	if err != nil || len(entries) != 0 {
		t.Fatalf("quarantine = %v, %v, want it resolved by autoresync", entries, err)
	}
	obs := h.obs.(*fakeObserver)
	if got := obs.sequence("autoresync-resolved"); len(got) != 1 || got[0] != "autoresync-resolved shop.items true 1" {
		t.Errorf("autoresync-resolved = %v, want one true after one attempt", got)
	}
	if got := live(t, h, "shop.items"); !equal(got, map[string]string{"1|": "a", "2|": "b"}) {
		t.Errorf("rows = %v", got)
	}
}

// TestStartResyncRefusesAStructureAheadOfTheStreamButNotForAQuarantinedTable: an operator's
// `tables resync` of a table that is not quarantined is refused when the source's current structure
// is not what the stream has processed yet (a DDL statement it has not reached). The same check must
// not block a table that IS quarantined: it is quarantined exactly because the stream already
// reached and failed on that statement, so its stale registry entry does not mean the stream is
// behind — this is what tables.autoresync (and a manual resync of the same table) relies on.
func TestStartResyncRefusesAStructureAheadOfTheStreamButNotForAQuarantinedTable(t *testing.T) {
	items := itemsTable()
	h := newHarness(t)
	h.snapshot = &fakeSnapshot{}
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	p.q.setSince(fence(0))
	t.Cleanup(func() {
		for _, j := range p.resyncs {
			p.stopJob(j)
		}
	})

	// A live row binds the table under its current structure.
	b, err := p.bind(context.Background(), items)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.sink.Ensure(context.Background(), b.table, chsink.DDL{}); err != nil {
		t.Fatal(err)
	}

	newer := itemsTable()
	newer.Columns = append(newer.Columns, newer.Columns[len(newer.Columns)-1])
	newer.Columns[len(newer.Columns)-1].Name = "extra"

	err = p.startResync(context.Background(), "shop.items", newer, "", 0, resyncOrigin{})
	if err == nil || !strings.Contains(err.Error(), "is not the one the stream has reached") {
		t.Fatalf("startResync() on a healthy table with a newer source structure = %v, want a refusal", err)
	}

	p.q.add("shop.items", errcode.TBL101, "some reason", false, epoch)
	if err := p.startResync(context.Background(), "shop.items", newer, "", 0, resyncOrigin{}); err != nil {
		t.Errorf("startResync() on a quarantined table with the same stale registry entry = %v, want it accepted", err)
	}
}

// TestAutoresyncNeverTouchesAnOrdinaryQuarantine: a table quarantined for a reason a resync cannot
// fix (no key at all) is left alone, even with tables.autoresync on.
func TestAutoresyncNeverTouchesAnOrdinaryQuarantine(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Autoresync = true
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	p.q.setSince(fence(0))
	p.q.add("shop.keyless", errcode.TBL101, "the table has no primary key and no unique key over NOT NULL columns", false, epoch)

	if err := p.autoresyncQuarantined(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, active := p.resyncs["shop.keyless"]; active {
		t.Error("autoresync started a resync of a non-recoverable quarantine")
	}
	if len(p.Quarantined()) != 1 {
		t.Errorf("quarantined = %v, want the table to stay quarantined untouched", p.Quarantined())
	}
}

// TestAutoresyncGivesUpAfterMaxAttempts: a table whose automatic resync keeps failing to even begin
// (the table cannot be read) is retried up to maxAutoresyncAttempts times and then left alone, so a
// misclassified or persistently broken table does not get hammered every heartbeat forever.
func TestAutoresyncGivesUpAfterMaxAttempts(t *testing.T) {
	h := newHarness(t)
	h.cfg.Tables.Autoresync = true
	h.snapshot = &fakeSnapshot{defErr: errors.New("connection refused")}
	h.obs = &fakeObserver{}
	p := h.pipeline(t)
	p.versions = chsink.NewVersioner(0)
	p.autoresyncAttempts = map[string]int{}
	p.q.setSince(fence(0))
	p.q.add("shop.items", errcode.TBL201, "a generated column: cannot reproduce existing values", true, epoch)

	ctx := context.Background()
	for i := 0; i < maxAutoresyncAttempts+2; i++ {
		if err := p.autoresyncQuarantined(ctx); err != nil {
			t.Fatal(err)
		}
	}

	if got := p.autoresyncAttempts["shop.items"]; got != maxAutoresyncAttempts {
		t.Errorf("attempts = %d, want exactly %d (no more tried after the cap)", got, maxAutoresyncAttempts)
	}
	if len(p.Quarantined()) != 1 {
		t.Errorf("quarantined = %v, want the table still quarantined", p.Quarantined())
	}
}
