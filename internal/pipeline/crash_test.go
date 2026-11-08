package pipeline

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/event"
)

// crashWorkload is a stream that exercises everything replay safety depends on: inserts,
// updates, a primary key change, deletes and a revived key, an empty transaction, a
// transaction big enough to be flushed in pieces, a DDL, and a source clock that steps back.
func crashWorkload() []scriptedTx {
	items := itemsTable()
	big := []event.Event{}
	for id := int32(4); id <= 8; id++ {
		big = append(big, insertRows(items, id, fmt.Sprintf("n%d", id)))
	}
	return []scriptedTx{
		tx(1, insertRows(items, 1, "a"), insertRows(items, 2, "b")),
		tx(2, updateRows(items, 1, "a", "a2")),
		tx(3),
		tx(4, updateKey(items, 2, 20, "b"), insertRows(items, 3, "c")),
		txAt(5, epoch, updateRows(items, 3, "c", "c2")), // committed later, stamped earlier
		tx(6, deleteRows(items, 20, "b")),
		tx(7, big...),
		tx(8, insertRows(items, 20, "again")),
		tx(9, event.DDL{Database: "shop", Query: "ALTER TABLE items ADD INDEX i (name)"}),
		tx(10, updateRows(items, 8, "n8", "n8x")),
	}
}

// expectedState applies the workload the way MySQL would, with no ClickHouse in between.
func expectedState() map[string][]any {
	rows := map[int32]string{
		1: "a2", 3: "c2", 4: "n4", 5: "n5", 6: "n6", 7: "n7", 8: "n8x", 20: "again",
	}
	out := map[string][]any{}
	for id, name := range rows {
		out[strconv.Itoa(int(id))+"|"] = []any{id, name}
	}
	return out
}

type crashMode struct {
	name string
	// setup configures the harness: how often flushes happen.
	setup func(h *harness)
}

var crashModes = []crashMode{
	{"flush at every commit", func(h *harness) { h.cfg.Buffer.MaxRows = 3; h.eagerFlush() }},
	{"flush only by size and at the end", func(h *harness) { h.cfg.Buffer.MaxRows = 3 }},
	{"flush only at the end", func(*harness) {}},
}

func crashHarness(t *testing.T, m crashMode) *harness {
	t.Helper()
	h := newHarness(t)
	h.source.script = crashWorkload()
	m.setup(h)
	h.installInvariants(t)
	return h
}

// installInvariants checks, at the moment every position is saved, that the data it covers is
// already written: the position must never get ahead of the data.
func (h *harness) installInvariants(t *testing.T) {
	t.Helper()
	var times []time.Time
	var hasRows []bool
	for _, tx := range h.source.script {
		times = append(times, tx.events[0].(event.Begin).CommitTime)
		rows := false
		for _, ev := range tx.events {
			if _, ok := ev.(event.Rows); ok {
				rows = true
			}
		}
		hasRows = append(hasRows, rows)
	}
	versions := versionsOf(t, times...)

	h.store.onSave = func(pos checkpoint.Position) {
		var covered int
		if _, err := fmt.Sscanf(pos.GTIDSet, "u:1-%d", &covered); err != nil {
			return // the start position "u:1-0" and friends cover nothing
		}
		for n := 1; n <= covered; n++ {
			if hasRows[n-1] && !h.sink.hasVersion(versions[n-1]) {
				t.Errorf("position %s saved, but transaction %d (version %d) is not in ClickHouse", pos.GTIDSet, n, versions[n-1])
			}
		}
	}
}

type crash struct {
	in    int  // operations from now
	after bool // the operation takes effect, but its caller is told it failed
}

// execute runs the pipeline through the given crashes and then to completion.
func execute(t *testing.T, h *harness, crashes ...crash) {
	t.Helper()
	for _, c := range crashes {
		h.faults.crashIn(c.in, c.after)
		err := h.run(t)
		h.faults.revive()
		if err == nil {
			return // finished before the crash point
		}
		if !errors.Is(err, errCrash) {
			t.Fatalf("Run() = %v, want the injected crash", err)
		}
	}
	if err := h.run(t); err != nil {
		t.Fatalf("Run() after the restart = %v", err)
	}
}

type outcome struct {
	rows     map[string][]any
	position checkpoint.Position
}

func outcomeOf(h *harness) outcome {
	pos := h.store.current()
	return outcome{rows: h.sink.visibleItems(), position: pos}
}

func TestReferenceRunMatchesWhatMySQLHolds(t *testing.T) {
	for _, m := range crashModes {
		t.Run(m.name, func(t *testing.T) {
			h := crashHarness(t, m)
			execute(t, h)
			got := outcomeOf(h)
			if !reflect.DeepEqual(got.rows, expectedState()) {
				t.Errorf("ClickHouse holds\n%v\nwant\n%v", got.rows, expectedState())
			}
			if got.position.GTIDSet != "u:1-10" {
				t.Errorf("final position = %q, want u:1-10", got.position.GTIDSet)
			}
		})
	}
}

// TestCrashAtAnyPointConverges is the at-least-once contract: whatever moment the process dies
// at, before or after any single write to ClickHouse or to the position store, the restarted
// process ends in exactly the state of a run that never crashed.
func TestCrashAtAnyPointConverges(t *testing.T) {
	for _, m := range crashModes {
		t.Run(m.name, func(t *testing.T) {
			ref := crashHarness(t, m)
			execute(t, ref)
			want := outcomeOf(ref)
			ops := ref.faults.count()
			if ops < 5 {
				t.Fatalf("the reference run made only %d writes; the test would prove little", ops)
			}

			for at := 1; at <= ops; at++ {
				for _, after := range []bool{false, true} {
					h := crashHarness(t, m)
					execute(t, h, crash{in: at, after: after})

					got := outcomeOf(h)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("crash at write %d (after=%v): state differs from the run without a crash\n got %+v\nwant %+v", at, after, got, want)
					}
				}
			}
			t.Logf("%d crash points checked in each of two variants", ops)
		})
	}
}

// TestRepeatedCrashesConverge crashes again during the recovery, at several depths.
func TestRepeatedCrashesConverge(t *testing.T) {
	for _, m := range crashModes {
		t.Run(m.name, func(t *testing.T) {
			ref := crashHarness(t, m)
			execute(t, ref)
			want := outcomeOf(ref)
			ops := ref.faults.count()

			for first := 1; first <= ops; first++ {
				for _, second := range []int{1, 2, 3, 5} {
					h := crashHarness(t, m)
					execute(t, h,
						crash{in: first, after: first%2 == 0},
						crash{in: second, after: second%2 == 1},
					)
					if got := outcomeOf(h); !reflect.DeepEqual(got, want) {
						t.Fatalf("crashes at %d then %d: state differs\n got %+v\nwant %+v", first, second, got, want)
					}
				}
			}
		})
	}
}

// TestNoForeignVersionsAppearAfterRecovery: replay reproduces the first run's versions exactly,
// so the set of versions in ClickHouse never grows beyond one per transaction.
func TestNoForeignVersionsAppearAfterRecovery(t *testing.T) {
	m := crashModes[1]
	ref := crashHarness(t, m)
	execute(t, ref)
	allowed := ref.sink.versions
	ops := ref.faults.count()

	for at := 1; at <= ops; at++ {
		h := crashHarness(t, m)
		execute(t, h, crash{in: at, after: at%2 == 0})
		for v := range h.sink.versions {
			if !allowed[v] {
				t.Fatalf("crash at write %d left version %d in ClickHouse that the run without a crash never wrote", at, v)
			}
		}
	}
}
