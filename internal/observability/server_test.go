package observability

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/failure"

	snap "github.com/thealexjo/binwave/internal/snapshot"
)

// TestSummarizeResyncsKeepsTheStatusAPageLong: with an entry per table of the source, the status
// names only what an operator watches — the resyncs asked for and the tables being read — and counts
// the rest. Without this the document is megabytes and the terminal output thousands of lines.
func TestSummarizeResyncsKeepsTheStatusAPageLong(t *testing.T) {
	statuses := []snap.Status{
		{Table: "shop.orders", Kind: "resync", Rows: 10},                                          // an operator asked for it
		{Table: "shop.fixed", Kind: "autoresync", Rows: 20, Done: true},                           // so did tables.autoresync
		{Table: "a.reading", Kind: "initial_load", Reading: true, Rows: 100, RowsPerSecond: 1500}, //
		{Table: "b.reading", Kind: "initial_load", Reading: true, Rows: 200, RowsPerSecond: 2500}, //
		{Table: "c.read", Kind: "initial_load", Done: true, Rows: 300},                            // waiting for the stream
		{Table: "d.waiting", Kind: "initial_load"},                                                // waiting for a slot
		{Table: "e.waiting", Kind: "initial_load"},                                                //
	}

	named, load := summarizeResyncs(statuses, 10)

	var tables []string
	for _, r := range named {
		tables = append(tables, r.Table)
	}
	want := []string{"shop.orders", "shop.fixed", "a.reading", "b.reading"}
	if !reflect.DeepEqual(tables, want) {
		t.Errorf("named = %v, want %v: the ones only waiting are counted, not named", tables, want)
	}
	if load == nil {
		t.Fatal("initial load = nil, want a summary")
	}
	got := *load
	expect := InitialLoad{Total: 10, Loaded: 5, Left: 5, Reading: 2, WaitingForStream: 1, WaitingForSlot: 2, RowsPerSecond: 4000}
	if got != expect {
		t.Errorf("initial load = %+v, want %+v", got, expect)
	}
}

// TestSummarizeResyncsWithoutASavedTotal: a position written before the total was recorded gives no
// denominator, so the summary says how many are left and claims no percentage.
func TestSummarizeResyncsWithoutASavedTotal(t *testing.T) {
	_, load := summarizeResyncs([]snap.Status{{Table: "a.b", Kind: "initial_load"}}, 0)
	if load == nil || load.Total != 0 || load.Loaded != 0 || load.Left != 1 {
		t.Errorf("initial load = %+v, want only Left known", load)
	}
}

// TestSummarizeResyncsWithoutAnInitialLoad: no summary at all when nothing is being loaded.
func TestSummarizeResyncsWithoutAnInitialLoad(t *testing.T) {
	named, load := summarizeResyncs([]snap.Status{{Table: "shop.orders", Kind: "resync"}}, 0)
	if load != nil {
		t.Errorf("initial load = %+v, want none", load)
	}
	if len(named) != 1 {
		t.Errorf("named = %v, want the resync itself", named)
	}
}

// TestPprofIsServedOnlyWhenItIsAskedFor: the profiles of a running process are what answers "where is
// the time going" without a rebuild, and they also say a good deal about the process, so they are off
// until the configuration asks for them.
func TestPprofIsServedOnlyWhenItIsAskedFor(t *testing.T) {
	for _, on := range []bool{false, true} {
		h := NewHub(Options{Pprof: on})
		rec := httptest.NewRecorder()
		h.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
		want := http.StatusNotFound
		if on {
			want = http.StatusOK
		}
		if rec.Code != want {
			t.Errorf("pprof=%v: /debug/pprof/ = %d, want %d", on, rec.Code, want)
		}
		// Whatever the setting, the endpoints the operator relies on answer.
		for _, path := range []string{"/metrics", "/healthz", "/status"} {
			rec := httptest.NewRecorder()
			h.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusOK && rec.Code != http.StatusServiceUnavailable {
				t.Errorf("pprof=%v: %s = %d", on, path, rec.Code)
			}
		}
	}
}

// metricOf reads one series of /metrics; the series of this package are all plain gauges.
func metricOf(t *testing.T, h *Hub, series string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		rest, ok := strings.CutPrefix(line, series+" ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			t.Fatalf("series %s: %v", series, err)
		}
		return v
	}
	t.Fatalf("series %s is not exposed", series)
	return 0
}

// TestAServiceWhoseDataPathKeepsFailingIsNotReady: the state is what an operator and a monitor look
// at, and while requests to ClickHouse keep failing the service replicates nothing — the position
// stands still and the lag grows. Before this it said "ready" throughout an outage, and the only
// other sign was an error record that is kept forever and so says nothing about now.
func TestAServiceWhoseDataPathKeepsFailingIsNotReady(t *testing.T) {
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	h := NewHub(Options{Now: func() time.Time { return now }})
	h.Ready()

	if got := h.Status().State; got != StateReady {
		t.Fatalf("state = %q, want ready", got)
	}

	// The first failures are a hiccup, not a state: a restart of ClickHouse takes seconds.
	h.Retry("insert into shop.items", "transient", errors.New("connection refused"))
	now = now.Add(degradedAfter - time.Second)
	h.Retry("insert into shop.items", "transient", errors.New("connection refused"))
	if got := h.Status().State; got != StateReady {
		t.Errorf("state after %s of failing = %q, want ready: a hiccup must not flip the state", degradedAfter-time.Second, got)
	}

	// Past the grace period it is no longer a hiccup.
	now = now.Add(2 * time.Second)
	h.Retry("insert into shop.items", "transient", errors.New("connection refused"))
	st := h.Status()
	if st.State != StateDegraded {
		t.Errorf("state after %s of failing = %q, want degraded", degradedAfter+time.Second, st.State)
	}
	if st.LastError == nil || !st.LastError.Ongoing || st.LastError.Attempts != 3 {
		t.Errorf("last error = %+v, want it ongoing with the attempts counted", st.LastError)
	}
	if ready := metricOf(t, h, "binwave_ready"); ready != 0 {
		t.Errorf("binwave_ready = %v while degraded, want 0", ready)
	}
	if degraded := metricOf(t, h, "binwave_degraded"); degraded != 1 {
		t.Errorf("binwave_degraded = %v, want 1", degraded)
	}
	if secs := metricOf(t, h, "binwave_degraded_seconds"); secs != (degradedAfter + time.Second).Seconds() {
		t.Errorf("binwave_degraded_seconds = %v, want %v", secs, (degradedAfter + time.Second).Seconds())
	}

	// One success ends it, and the record stops claiming to be happening.
	now = now.Add(time.Second)
	h.Recovered()
	st = h.Status()
	if st.State != StateReady {
		t.Errorf("state after a success = %q, want ready", st.State)
	}
	if st.LastError == nil || st.LastError.Ongoing || st.LastError.ResolvedAt == nil {
		t.Errorf("last error = %+v, want it kept as history and marked resolved", st.LastError)
	}
	if degraded := metricOf(t, h, "binwave_degraded"); degraded != 0 {
		t.Errorf("binwave_degraded = %v after a success, want 0", degraded)
	}
}

// TestAStateThatSaysMoreThanDegradedWins: paused, failed, starting and stopping say something an
// operator has to know first; a failing data path must not hide them.
func TestAStateThatSaysMoreThanDegradedWins(t *testing.T) {
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	h := NewHub(Options{Now: func() time.Time { return now }})
	h.Ready()
	h.Retry("insert into shop.items", "transient", errors.New("connection refused"))
	now = now.Add(2 * degradedAfter)
	if got := h.Status().State; got != StateDegraded {
		t.Fatalf("state = %q, want degraded", got)
	}

	h.Paused()
	if got := h.Status().State; got != StatePaused {
		t.Errorf("state = %q while paused, want paused", got)
	}
	h.Ready()
	h.Failed(failure.Failure{Time: now, Kind: "write", Message: "a row that cannot be written"})
	if got := h.Status().State; got != StateFailed {
		t.Errorf("state = %q while failed, want failed", got)
	}
}

// TestTheInitialLoadIsVisibleInTheMetrics: the initial load is the longest and least obvious period in
// the life of the service — on a source with thousands of tables it is hours — and its progress used to
// be only in /status. A dashboard could not draw it, and an alert on the lag could not tell a load,
// where a huge lag is expected, from a stuck stream.
func TestTheInitialLoadIsVisibleInTheMetrics(t *testing.T) {
	h := NewHub(Options{})
	h.Ready()
	if covered := metricOf(t, h, "binwave_initial_load_covered_tables"); covered != 0 {
		t.Errorf("covered = %v with no initial load, want 0", covered)
	}

	// Ten tables in the load: five are in place, the rest are spread over the other states.
	h.PositionSaved(checkpoint.Position{GTIDSet: "u:1-5", LoadTotal: 10}, time.Time{}, time.Time{})
	h.ResyncChanged([]snap.Status{
		{Table: "shop.r1", Kind: "initial_load", Reading: true},
		{Table: "shop.r2", Kind: "initial_load", Reading: true},
		{Table: "shop.w1", Kind: "initial_load", Done: true},
		{Table: "shop.s1", Kind: "initial_load"},
		{Table: "shop.s2", Kind: "initial_load"},
		{Table: "shop.orders", Kind: "resync"}, // an operator's resync is not part of the load
	})

	for series, want := range map[string]float64{
		"binwave_initial_load_covered_tables":                     10,
		`binwave_initial_load_tables{state="loaded"}`:             5,
		`binwave_initial_load_tables{state="reading"}`:            2,
		`binwave_initial_load_tables{state="waiting_for_stream"}`: 1,
		`binwave_initial_load_tables{state="waiting_for_slot"}`:   2,
		"binwave_resyncing_tables":                                1,
	} {
		if got := metricOf(t, h, series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
}

// TestAStartThatSucceededEndsTheStartupError: the start is retried while a database is down, and each
// failed attempt is recorded. Once the service is ready, the record is history: before, it kept saying
// "failing: startup: connection refused" for as long as the process lived.
func TestAStartThatSucceededEndsTheStartupError(t *testing.T) {
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	h := NewHub(Options{Now: func() time.Time { return now }})
	h.StartupFailed(errors.New("connection refused"))
	now = now.Add(5 * time.Second)
	h.StartupFailed(errors.New("connection refused"))
	now = now.Add(5 * time.Second)

	h.Ready()

	st := h.Status()
	if st.LastError == nil || st.LastError.Kind != "startup" || st.LastError.Attempts != 2 {
		t.Fatalf("last error = %+v, want the startup failures kept as history", st.LastError)
	}
	if st.LastError.Ongoing || st.LastError.ResolvedAt == nil || !st.LastError.ResolvedAt.Equal(now) {
		t.Errorf("last error = %+v, want it resolved when the service became ready", st.LastError)
	}
}

// TestBecomingReadyAgainDoesNotResolveAnErrorOfTheDataPath: Ready is also what the service reports
// after a pause ends; an outage of ClickHouse that is still going on is not over because of that.
func TestBecomingReadyAgainDoesNotResolveAnErrorOfTheDataPath(t *testing.T) {
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	h := NewHub(Options{Now: func() time.Time { return now }})
	h.Ready()
	h.Retry("insert into shop.items", "transient", errors.New("connection refused"))

	h.Ready()

	if st := h.Status(); st.LastError == nil || !st.LastError.Ongoing {
		t.Errorf("last error = %+v, want it still ongoing", st.LastError)
	}
}

// TestResumingEndsTheErrorThatStoppedReplication: an overload outlasts its budget and replication
// stops; the operator resumes it. What failed is not failing any more until it fails again, and the
// service must not stay degraded and unready while every insert succeeds.
func TestResumingEndsTheErrorThatStoppedReplication(t *testing.T) {
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	h := NewHub(Options{Now: func() time.Time { return now }})
	h.Ready()
	h.Retry("insert into shop.items", "overload", errors.New("too many parts"))
	now = now.Add(2 * degradedAfter)
	h.Failed(failure.Failure{Time: now, Kind: "write", Message: "ClickHouse stayed overloaded"})

	now = now.Add(time.Minute)
	h.Resumed()

	st := h.Status()
	if st.State != StateReady {
		t.Errorf("state after resume = %q, want ready", st.State)
	}
	if st.LastError == nil || st.LastError.Ongoing || st.LastError.ResolvedAt == nil {
		t.Errorf("last error = %+v, want it kept as history and marked resolved", st.LastError)
	}
	if ready := metricOf(t, h, "binwave_ready"); ready != 1 {
		t.Errorf("binwave_ready = %v after resume, want 1", ready)
	}
}
