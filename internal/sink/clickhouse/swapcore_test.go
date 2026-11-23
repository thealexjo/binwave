package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// The swap is checked against a model of the tables of a database, whose adversary can:
//
//   - finish an exchange sent before (its answer lost, by an earlier attempt or a process that crashed)
//     right before any statement of the swap, as long as the swap goes on while it runs;
//   - lose the answer of the swap's own exchange, the exchange made or still running;
//   - fail the reading of the queries that run;
//   - crash the swap before any statement, after which it begins again as a restart does;
//   - put another table under the name of the copy right before any statement, the table that was there
//     renamed away, as somebody working in ClickHouse while the swap runs.
//
// Whatever it does, the copy and the other table are never dropped, and a swap that ends puts the copy
// in the place of the table, unmarked, with nothing left of it.

type modelTable struct{ uuid, comment string }

type modelExchange struct {
	a, b   string
	rename bool
}

type swapModel struct {
	tables  map[string]modelTable
	dropped []string // UUIDs
	pending *modelExchange
	step    int

	completeAt, crashAt, meddleAt int // the step before which it happens; 0: never
	lose                          int // the swap's first exchange: 0 answered, 1 made and its answer lost, 2 still running
	failRunning                   bool
	meddleName                    string // the name the adversary puts another table under; empty: the copy's
	meddleComment                 string // the comment of that table; empty: "made by hand"
	changes                       int    // the statements of the swap that change a table
}

var errCrash = errors.New("crash")

func refused(code int32, format string, args ...any) error {
	return &ch.Exception{Code: code, Message: fmt.Sprintf(format, args...)}
}

func (m *swapModel) apply(x modelExchange) error {
	a, aok := m.tables[x.a]
	b, bok := m.tables[x.b]
	switch {
	case x.rename && bok && !aok:
		m.tables[x.a] = b
		delete(m.tables, x.b)
	case !x.rename && aok && bok:
		m.tables[x.a], m.tables[x.b] = b, a
	default:
		return refused(60, "exchange %v: a table is missing or taken", x)
	}
	return nil
}

// before is what the adversary does before a statement of the swap.
func (m *swapModel) before() error {
	m.step++
	if m.pending != nil && m.step == m.completeAt {
		_ = m.apply(*m.pending)
		m.pending = nil
	}
	if m.step == m.meddleAt {
		name := m.meddleName
		if name == "" {
			name = "t__copy"
		}
		if t, ok := m.tables[name]; ok {
			m.tables["elsewhere"] = t
		}
		comment := m.meddleComment
		if comment == "" {
			comment = "made by hand"
		}
		m.tables[name] = modelTable{uuid: fromHand, comment: comment}
	}
	if m.step == m.crashAt {
		return errCrash
	}
	return nil
}

func (m *swapModel) running(context.Context, string) (bool, error) {
	if m.failRunning {
		m.failRunning = false
		return false, io.ErrUnexpectedEOF
	}
	if m.pending != nil {
		// It ends right after it was seen running: the swap that waits sees it over.
		_ = m.apply(*m.pending)
		m.pending = nil
		return true, nil
	}
	return false, nil
}

func (m *swapModel) read(_ context.Context, _ string, names ...string) (map[string]swapTable, error) {
	if err := m.before(); err != nil {
		return nil, err
	}
	got := map[string]swapTable{}
	for _, n := range names {
		if t, ok := m.tables[n]; ok {
			got[n] = swapTable{exists: true, comment: t.comment, uuid: t.uuid}
		}
	}
	return got, nil
}

func (m *swapModel) setComment(_ context.Context, _, table, comment string) error {
	if err := m.before(); err != nil {
		return err
	}
	m.changes++
	t, ok := m.tables[table]
	if !ok {
		return refused(60, "no table %s", table)
	}
	t.comment = comment
	m.tables[table] = t
	return nil
}

func (m *swapModel) exchange(_ context.Context, _, a, b string, rename bool) error {
	if err := m.before(); err != nil {
		return err
	}
	m.changes++
	x := modelExchange{a: a, b: b, rename: rename}
	switch m.lose {
	case 1:
		m.lose = 0
		_ = m.apply(x)
		return io.ErrUnexpectedEOF
	case 2:
		m.lose = 0
		m.pending = &x
		return io.ErrUnexpectedEOF
	}
	return m.apply(x)
}

func (m *swapModel) rename(_ context.Context, _, from, to string) error {
	if err := m.before(); err != nil {
		return err
	}
	m.changes++
	t, ok := m.tables[from]
	if _, taken := m.tables[to]; !ok || taken {
		return refused(57, "rename %s to %s: a table is missing or taken", from, to)
	}
	m.tables[to] = t
	delete(m.tables, from)
	return nil
}

func (m *swapModel) drop(_ context.Context, _, table string) error {
	if err := m.before(); err != nil {
		return err
	}
	m.changes++
	if t, ok := m.tables[table]; ok {
		m.dropped = append(m.dropped, t.uuid)
		delete(m.tables, table)
	}
	return nil
}

func (m *swapModel) dropping(context.Context, string) ([]string, error) {
	if err := m.before(); err != nil {
		return nil, err
	}
	var names []string
	for name := range m.tables {
		if strings.HasPrefix(name, dropPrefix) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names, nil
}

const (
	oldUUID  = "0a000000-0000-0000-0000-000000000000"
	copyUUID = "0c000000-0000-0000-0000-000000000000"
	fromHand = "f0000000-0000-0000-0000-000000000000"
	leftUUID = "0d000000-0000-0000-0000-000000000000" // what a drop a crash interrupted left
	takenBy  = "0e000000-0000-0000-0000-000000000000" // a table made by hand under the name of a drop
)

// adversaries runs check for every way the adversary can act on a model of the tables.
func adversaries(t *testing.T, tables map[string]modelTable, pendingRename bool, check func(m *swapModel, what string)) int {
	t.Helper()
	runs := 0
	for _, pending := range []bool{false, true} {
		for _, failRunning := range []bool{false, true} {
			for lose := 0; lose <= 2; lose++ {
				for completeAt := 0; completeAt <= 8; completeAt++ {
					if !pending && completeAt > 0 {
						continue
					}
					for crashAt := 0; crashAt <= 10; crashAt++ {
						for meddleAt := 0; meddleAt <= 10; meddleAt++ {
							m := &swapModel{
								tables: map[string]modelTable{}, completeAt: completeAt, crashAt: crashAt, meddleAt: meddleAt,
								lose: lose, failRunning: failRunning,
							}
							for n, t := range tables {
								m.tables[n] = t
							}
							if pending {
								// The exchange (or the rename) a crashed process sent, still running.
								m.pending = &modelExchange{a: "t", b: "t__copy", rename: pendingRename}
							}
							check(m, fmt.Sprintf("pending %v, failing to read the queries %v, lose %d, complete at %d, crash at %d, meddle at %d",
								pending, failRunning, lose, completeAt, crashAt, meddleAt))
							runs++
						}
					}
				}
			}
		}
	}
	return runs
}

func TestASwapLosesNothingWhateverHappensAroundIt(t *testing.T) {
	marker := ResyncMarker("t")
	replacing := Replacing(marker, oldUUID, copyUUID)
	states := []struct {
		name   string
		tables map[string]modelTable
		lost   bool // the copy is gone: nothing may be swapped in
	}{
		{"in a database without UUIDs", map[string]modelTable{"t": {zeroUUID, ""}, "t__copy": {zeroUUID, marker}}, false},
		{"with no table to replace, in a database without UUIDs", map[string]modelTable{"t__copy": {zeroUUID, marker}}, false},
		{"before the exchange", map[string]modelTable{"t": {oldUUID, ""}, "t__copy": {copyUUID, marker}}, false},
		{"before the exchange, the marker naming the tables", map[string]modelTable{"t": {oldUUID, ""}, "t__copy": {copyUUID, replacing}}, false},
		{"after the exchange", map[string]modelTable{"t": {copyUUID, replacing}, "t__copy": {oldUUID, ""}}, false},
		{"after the old table was renamed to be dropped", map[string]modelTable{"t": {copyUUID, replacing}, dropName(oldUUID): {oldUUID, ""}}, false},
		{"after the old table was dropped", map[string]modelTable{"t": {copyUUID, replacing}}, false},
		{"with no table to replace", map[string]modelTable{"t__copy": {copyUUID, marker}}, false},
		{"with the copy lost", map[string]modelTable{"t": {oldUUID, ""}}, true},
		{"after the swap was finished", map[string]modelTable{"t": {copyUUID, ""}}, false},
		{"after the exchange, with what an interrupted drop of another copy left", map[string]modelTable{
			"t": {copyUUID, replacing}, "t__copy": {oldUUID, ""}, dropName(leftUUID): {leftUUID, ResyncMarker("u")},
		}, false},
		{"after the exchange, the name of the drop taken by a table made by hand", map[string]modelTable{
			"t": {copyUUID, replacing}, "t__copy": {oldUUID, ""}, dropName(oldUUID): {takenBy, ""},
		}, false},
	}
	log := slog.New(slog.DiscardHandler)
	runs := 0
	for _, known := range []bool{false, true} {
		// The UUID of the copy known to the job or not; and the table put under the name of the copy carrying
		// the marker of the job, a copy of its comment, which only the UUID tells from the copy.
		for _, marked := range []bool{false, true} {
			if marked && !known {
				continue
			}
			for _, st := range states {
				finished := st.name == "after the swap was finished"
				_, oldThere := st.tables["t"]
				runs += adversaries(t, st.tables, !oldThere, func(m *swapModel, what string) {
					if finished && m.pending != nil {
						return // its exchange is over
					}
					if marked {
						m.meddleComment = marker
					}
					what = fmt.Sprintf("%s, the UUID of the copy known %v, the table put in its name marked %v, %s", st.name, known, marked, what)
					runSwapModel(t, m, log, marker, what, st.lost, finished, known)
				})
			}
		}
	}
	t.Logf("%d runs", runs)
}

func runSwapModel(t *testing.T, m *swapModel, log *slog.Logger, marker, what string, lostCopy, finished, known bool) {
	t.Helper()
	initial := map[string]modelTable{}
	for n, tb := range m.tables {
		initial[n] = tb
	}
	spec := swapSpec{database: "db", table: "t", copy: "t__copy", marker: marker}
	if known {
		spec.copyUUID = copyUUID // saved with the job: a restart knows it
	}
	outcome, done, stopped := swapDone, false, false
	for range 20 {
		copied, got, err := swapCopy(context.Background(), m, log, spec)
		if err == nil {
			outcome, done = got, true
			break
		}
		if IsConflict(err) || errors.Is(err, errNoIdentity) {
			stopped = true
			break
		}
		if errors.Is(err, errCrash) {
			// A restart: it reads the marks as SwapMarks does, waiting while an exchange runs, and knows
			// nothing of what the attempts before it sent.
			for {
				if running, _ := m.running(context.Background(), ""); !running {
					break
				}
			}
			spec.copied = ""
			continue
		}
		spec.copied = copied
	}

	for _, uuid := range m.dropped {
		if uuid != oldUUID && uuid != leftUUID {
			t.Fatalf("%s: dropped the table %s; tables %v", what, uuid, m.tables)
		}
	}
	meddled := m.meddleAt > 0 && m.meddleAt <= m.step
	if finished {
		// The swap was finished, and its end not saved: nothing is changed. With the UUID of the copy known
		// the table is told to be the copy, and the swap ends as done.
		if m.changes > 0 || len(m.dropped) > 0 || m.tables["t"].uuid != copyUUID || known && (!done || outcome != swapDone) {
			t.Fatalf("%s: outcome %d (done %v), %d changes, dropped %v, tables %v: want nothing changed, done when the copy is known", what, outcome, done, m.changes, m.dropped, m.tables)
		}
		return
	}
	if hasUUID(initial, zeroUUID) {
		// Nothing tells one table from another: no statement of the swap changes a table, and it does not end
		// as done.
		if m.changes > 0 || len(m.dropped) > 0 || done && outcome == swapDone {
			t.Fatalf("%s: %d changes, dropped %v, done %v with tables %v: want nothing changed and the swap stopped", what, m.changes, m.dropped, done && outcome == swapDone, m.tables)
		}
		return
	}
	if initial[dropName(oldUUID)].uuid == takenBy {
		// A table under the name of a drop that binwave did not put there: never moved, and the swap stops.
		if m.tables[dropName(oldUUID)].uuid != takenBy || (!stopped && outcome == swapDone) {
			t.Fatalf("%s: stopped %v, tables %v: want the table under the name of the drop kept there, and the swap stopped", what, stopped, m.tables)
		}
		return
	}
	if !done && !stopped {
		t.Fatalf("%s: the swap never ended; tables %v", what, m.tables)
	}
	if lostCopy {
		want := swapLost
		if meddled {
			want = swapNoCopy // another table under the name of the copy
		}
		if outcome != want || stopped || m.tables["t"].uuid != oldUUID {
			t.Fatalf("%s: outcome %d (stopped %v) with tables %v, want the copy lost and the table as it was", what, outcome, stopped, m.tables)
		}
		return
	}
	if meddled && (stopped || outcome != swapDone) {
		// The copy was moved away, another table put under its name: the swap stops, and nothing is lost.
		if !hasUUID(m.tables, copyUUID) || !hasUUID(m.tables, fromHand) || len(m.dropped) > 0 && !hasUUID(m.tables, copyUUID) {
			t.Fatalf("%s: outcome %d with tables %v, dropped %v: want everything kept", what, outcome, m.tables, m.dropped)
		}
		return
	}
	if stopped || outcome != swapDone {
		t.Fatalf("%s: outcome %d (stopped %v); tables %v", what, outcome, stopped, m.tables)
	}
	if got := m.tables["t"]; got.uuid != copyUUID || got.comment != "" {
		t.Fatalf("%s: the table is %+v, want the copy, unmarked; tables %v", what, got, m.tables)
	}
	for n := range m.tables {
		if strings.HasPrefix(n, "binwave_drop_") {
			t.Fatalf("%s: %s left; tables %v", what, n, m.tables)
		}
	}
	if meddled && !hasUUID(m.tables, fromHand) {
		t.Fatalf("%s: the table made by hand is gone; tables %v", what, m.tables)
	}
}

// TestADroppedCopyTakesNothingElseWhateverHappensAroundIt: the copy of a job that is given up is dropped
// by its identity. Whatever the adversary does, nothing else is dropped, and what a crash interrupted is
// dropped by the drop repeated, or by the next swap or drop in the database (its sweep).
func TestADroppedCopyTakesNothingElseWhateverHappensAroundIt(t *testing.T) {
	marker := ResyncMarker("t")
	log := slog.New(slog.DiscardHandler)
	runs := 0
	for _, st := range []struct {
		name   string
		tables map[string]modelTable
	}{
		{"a copy", map[string]modelTable{"t": {oldUUID, ""}, "t__copy": {copyUUID, marker}}},
		{"a copy, the marker naming the tables", map[string]modelTable{"t": {oldUUID, ""}, "t__copy": {copyUUID, Replacing(marker, oldUUID, copyUUID)}}},
		{"a copy renamed to be dropped", map[string]modelTable{"t": {oldUUID, ""}, dropName(copyUUID): {copyUUID, marker}}},
	} {
		for _, known := range []bool{false, true} {
			for _, marked := range []bool{false, true} {
				if marked && !known {
					continue // without the UUID, a copy of the marker is not told from the copy
				}
				runs += adversaries(t, st.tables, false, func(m *swapModel, what string) {
					what = fmt.Sprintf("%s, the UUID of the copy known %v, the table put in its name marked %v, %s", st.name, known, marked, what)
					checkDroppedCopy(t, m, log, marker, what, known, marked)
				})
			}
		}
	}
	t.Logf("%d runs", runs)
}

func checkDroppedCopy(t *testing.T, m *swapModel, log *slog.Logger, marker, what string, known, marked bool) {
	t.Helper()
	m.lose, m.pending = 0, nil // nothing is exchanged here
	if marked {
		m.meddleComment = marker
	}
	spec := swapSpec{database: "db", table: "t", copy: "t__copy", marker: marker}
	if known {
		spec.copyUUID = copyUUID
	}
	stopped := false
	for range 20 {
		_, err := dropCopy(context.Background(), m, log, spec)
		if err == nil {
			break
		}
		if IsConflict(err) {
			stopped = true
			break
		}
	}
	meddled := m.meddleAt > 0 && m.meddleAt <= m.step
	for _, uuid := range m.dropped {
		if uuid != copyUUID {
			t.Fatalf("%s: dropped the table %s; tables %v", what, uuid, m.tables)
		}
	}
	if m.tables["t"].uuid != oldUUID {
		t.Fatalf("%s: the table is %+v; tables %v", what, m.tables["t"], m.tables)
	}
	if meddled && !hasUUID(m.tables, fromHand) {
		t.Fatalf("%s: the table made by hand is gone; tables %v", what, m.tables)
	}
	if !meddled && !stopped && hasUUID(m.tables, copyUUID) {
		t.Fatalf("%s: the copy is left; tables %v", what, m.tables)
	}
}

// TestASweepTakesNoTableThatTookTheNameOfADropAfterItWasListed: the sweep lists what is under the names of
// drops, and another table takes one of them, the table that was there renamed away: the sweep reads each
// again right before it drops it, and leaves the other table.
func TestASweepTakesNoTableThatTookTheNameOfADropAfterItWasListed(t *testing.T) {
	for meddleAt := 1; meddleAt <= 2; meddleAt++ { // before the listing; between the listing and the read
		m := &swapModel{
			tables:     map[string]modelTable{"t": {oldUUID, ""}, dropName(leftUUID): {leftUUID, ResyncMarker("u")}, dropName(copyUUID): {copyUUID, ""}},
			meddleAt:   meddleAt,
			meddleName: dropName(leftUUID),
		}
		if err := sweep(context.Background(), m, "db"); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(m.dropped, []string{copyUUID}) || !hasUUID(m.tables, fromHand) || !hasUUID(m.tables, leftUUID) {
			t.Errorf("another table under the name of a drop before step %d: dropped %v, tables %v: want only %s dropped", meddleAt, m.dropped, m.tables, copyUUID)
		}
	}
}

// TestNothingWithoutAUUIDIsDropped: in a database that gives its tables no UUID, every table has the same
// one, and the name of a drop made of it tells nothing: no drop takes such a table.
func TestNothingWithoutAUUIDIsDropped(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	m := &swapModel{tables: map[string]modelTable{"t__copy": {zeroUUID, ResyncMarker("t")}, dropName(zeroUUID): {zeroUUID, ""}}}
	if err := sweep(context.Background(), m, "db"); err != nil || m.changes > 0 {
		t.Errorf("sweep() = %v with %d changes, tables %v: want nothing dropped", err, m.changes, m.tables)
	}
	if err := dropByIdentity(context.Background(), m, log, "db", "t__copy", zeroUUID); !errors.Is(err, errNoIdentity) || m.changes > 0 {
		t.Errorf("dropByIdentity() = %v with %d changes, tables %v: want it refused, nothing changed", err, m.changes, m.tables)
	}
	if _, err := dropCopy(context.Background(), m, log, swapSpec{database: "db", table: "t", copy: "t__copy", marker: ResyncMarker("t")}); err != nil || m.changes > 0 {
		t.Errorf("dropCopy() = %v with %d changes, tables %v: want nothing dropped", err, m.changes, m.tables)
	}
}

func hasUUID(tables map[string]modelTable, uuid string) bool {
	for _, t := range tables {
		if t.uuid == uuid {
			return true
		}
	}
	return false
}

// TestADryRunLogsTheSwapItself: the plan of a dry run is the swap run on a model of the tables, every query
// it would send in order, the UUID of a copy not made yet shown by its description.
func TestADryRunLogsTheSwapItself(t *testing.T) {
	spec := swapSpec{database: "db", table: "t", copy: "t__rebuild", marker: RebuildMarker("t")}
	var out strings.Builder
	for _, c := range []struct {
		name   string
		tables map[string]swapTable
		made   bool
	}{
		{"a rebuild", map[string]swapTable{"t": {exists: true, uuid: oldUUID}}, true},
		{"a rebuild whose swap a crash interrupted", map[string]swapTable{
			"t":          {exists: true, uuid: copyUUID, comment: Replacing(spec.marker, oldUUID, copyUUID)},
			"t__rebuild": {exists: true, uuid: oldUUID},
		}, false},
		{"a rebuild whose swap a crash interrupted after the old table was renamed to be dropped", map[string]swapTable{
			"t":               {exists: true, uuid: copyUUID, comment: Replacing(spec.marker, oldUUID, copyUUID)},
			dropName(oldUUID): {exists: true, uuid: oldUUID},
		}, false},
	} {
		var lines []string
		plan := newPlan(slog.New(recorder{&lines}), spec, c.tables, c.made)
		if _, _, err := swapCopy(context.Background(), plan, slog.New(slog.DiscardHandler), spec); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		fmt.Fprintf(&out, "-- %s\n%s\n", c.name, strings.Join(lines, "\n"))
	}
	path := filepath.Join("testdata", "swap_plan.golden.txt")
	if *update {
		if err := os.WriteFile(path, []byte(out.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != string(want) {
		t.Errorf("the plan:\n%s\nwant:\n%s", out.String(), want)
	}
}

// recorder keeps the records it is given, a line each, its attributes after it.
type recorder struct{ lines *[]string }

func (r recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r recorder) Handle(_ context.Context, rec slog.Record) error {
	line := rec.Message
	rec.Attrs(func(a slog.Attr) bool {
		line += "\n    " + a.Key + ": " + a.Value.String()
		return true
	})
	*r.lines = append(*r.lines, line)
	return nil
}

func (r recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r recorder) WithGroup(string) slog.Handler      { return r }
