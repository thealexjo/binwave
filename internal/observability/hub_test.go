package observability

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil/promlint"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/failure"
	snap "github.com/thealexjo/binwave/internal/snapshot"
	"github.com/thealexjo/binwave/internal/typemap"
)

// clock is a settable time source.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newTestHub() (*Hub, *clock) {
	c := &clock{t: t0}
	return NewHub(Options{Now: c.Now, Version: "v1", Commit: "abc"}), c
}

func scrape(t *testing.T, h *Hub) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d", rec.Code)
	}
	return rec.Body.String()
}

// value finds the value of a series in the text format.
func value(t *testing.T, text, series string) string {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			return rest
		}
	}
	t.Fatalf("series %q not found in:\n%s", series, text)
	return ""
}

func TestCountersFollowWhatThePipelineReports(t *testing.T) {
	h, _ := newTestHub()

	h.TransactionBegan(t0)
	h.RowsRead("shop.items", 3)
	h.TransactionCommitted()
	h.BufferChanged(3, 120)
	h.Inserted("shop.items", 2, 40*time.Millisecond)
	h.Retry("insert", "overload", errors.New("too many parts"))
	h.Retry("insert", "transient", errors.New("reset"))
	h.Retry("insert", "transient", errors.New("reset"))
	h.TablesBound(4)
	h.DestructiveDDLSkipped()
	h.SourceReconnected()
	h.StartupFailed(errors.New("refused"))
	h.WrapReporter(nil).Report(typemap.Adjustment{Kind: typemap.AdjustZeroDate})
	h.WrapReporter(nil).Report(typemap.Adjustment{Kind: typemap.AdjustZeroDate})

	text := scrape(t, h)
	for series, want := range map[string]string{
		"binwave_transactions_total":                           "1",
		"binwave_rows_read_total":                              "3",
		"binwave_rows_written_total":                           "2",
		"binwave_inserts_total":                                "1",
		`binwave_clickhouse_retries_total{reason="overload"}`:  "1",
		`binwave_clickhouse_retries_total{reason="transient"}`: "2",
		"binwave_buffer_rows":                                  "3",
		"binwave_buffer_bytes":                                 "120",
		"binwave_tables":                                       "4",
		"binwave_destructive_ddl_skipped_total":                "1",
		"binwave_source_reconnects_total":                      "1",
		"binwave_startup_failures_total":                       "1",
		`binwave_value_adjustments_total{kind="zero_date"}`:    "2",
		`binwave_build_info{commit="abc",version="v1"}`:        "1",
		"binwave_insert_duration_seconds_count":                "1",
	} {
		if got := value(t, text, series); got != want {
			t.Errorf("%s = %s, want %s", series, got, want)
		}
	}
	for _, name := range []string{"go_goroutines", "process_resident_memory_bytes"} {
		if !strings.Contains(text, name) {
			t.Errorf("the standard runtime metric %s is missing", name)
		}
	}
	families, err := h.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	if problems, err := promlint.NewWithMetricFamilies(families).Lint(); err != nil || len(problems) > 0 {
		t.Errorf("metric naming problems: %v %v", problems, err)
	}
}

func TestLagMetricFollowsTheRule(t *testing.T) {
	h, c := newTestHub()
	if got := value(t, scrape(t, h), "binwave_lag_seconds"); got != "0" {
		t.Errorf("lag before anything = %s", got)
	}

	// A backlog: a transaction committed 90 seconds ago is being processed.
	h.TransactionBegan(c.Now().Add(-90 * time.Second))
	if got := value(t, scrape(t, h), "binwave_lag_seconds"); got != "90" {
		t.Errorf("lag with a backlog = %s, want 90", got)
	}

	// It is written and saved; the stream is still busy.
	h.PositionSaved(checkpoint.Position{GTIDSet: "u:1-5", Version: 7}, c.Now().Add(-90*time.Second), time.Time{})
	c.Advance(time.Second)
	if got := value(t, scrape(t, h), "binwave_lag_seconds"); got != "91" {
		t.Errorf("lag right after a save while busy = %s, want 91", got)
	}

	// Nothing more to send: the lag is 0 however old the last change was.
	h.Idle()
	c.Advance(time.Hour)
	text := scrape(t, h)
	if got := value(t, text, "binwave_lag_seconds"); got != "0" {
		t.Errorf("lag of a quiet source = %s, want 0", got)
	}
	// The position metrics keep growing with time, which is what they are for.
	if got := value(t, text, "binwave_position_saved_age_seconds"); got != "3601" {
		t.Errorf("position age = %s, want 3601", got)
	}
	if got := value(t, text, "binwave_position_version"); got != "7" {
		t.Errorf("position version = %s", got)
	}
}

func TestReadyGaugeAndStatusFollowTheState(t *testing.T) {
	h, c := newTestHub()
	ready := func() string { return value(t, scrape(t, h), "binwave_ready") }

	if ready() != "0" || h.Status().State != StateStarting {
		t.Errorf("a new hub is not starting: %v", h.Status())
	}
	h.Ready()
	if ready() != "1" || h.Status().State != StateReady {
		t.Errorf("not ready after Ready: %v", h.Status())
	}
	c.Advance(time.Minute)
	h.Stopping()
	if ready() != "0" || h.Status().State != StateStopping {
		t.Errorf("still ready after Stopping: %v", h.Status())
	}
}

func get(t *testing.T, h *Hub, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, strings.TrimSpace(string(body))
}

func TestHealthAndReadinessEndpoints(t *testing.T) {
	h, c := newTestHub()

	// Starting: alive (a database that is down is being retried), not ready.
	if code, body := get(t, h, "/healthz"); code != 200 {
		t.Errorf("/healthz while starting = %d %s", code, body)
	}
	if code, _ := get(t, h, "/readyz"); code != 503 {
		t.Errorf("/readyz while starting = %d, want 503", code)
	}

	h.Ready()
	if code, _ := get(t, h, "/readyz"); code != 200 {
		t.Errorf("/readyz when ready = %d", code)
	}

	// Progress keeps it healthy, silence for half a minute does not.
	c.Advance(20 * time.Second)
	h.Progress()
	c.Advance(20 * time.Second)
	if code, body := get(t, h, "/healthz"); code != 200 {
		t.Errorf("/healthz with recent progress = %d %s", code, body)
	}
	c.Advance(31 * time.Second)
	if code, body := get(t, h, "/healthz"); code != 503 || !strings.Contains(body, "no progress") {
		t.Errorf("/healthz without progress = %d %q, want 503 and a reason", code, body)
	}

	h.Progress()
	h.Stopping()
	if code, _ := get(t, h, "/readyz"); code != 503 {
		t.Errorf("/readyz while stopping = %d, want 503", code)
	}
	if code, _ := get(t, h, "/healthz"); code != 200 {
		t.Errorf("/healthz while stopping and making progress = %d", code)
	}
}

func TestStatusReportsPositionLagAndLastError(t *testing.T) {
	h, c := newTestHub()
	h.Ready()
	h.TablesBound(2)
	h.PositionSaved(checkpoint.Position{GTIDSet: "u:1-9", Version: 3}, c.Now(), time.Time{})
	h.Idle()
	h.Retry("insert", "transient", errors.New("connection reset"))

	code, body := get(t, h, "/status")
	if code != 200 {
		t.Fatalf("/status = %d", code)
	}
	var st Status
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, body)
	}
	if st.State != StateReady || st.Position != "u:1-9" || st.Tables != 2 || !st.CaughtUp || st.LagSeconds != 0 {
		t.Errorf("status = %+v", st)
	}
	if st.LastError == nil || st.LastError.Kind != "clickhouse" || st.LastError.Message != "connection reset" {
		t.Errorf("last error = %+v", st.LastError)
	}
	if st.Version != "v1" || st.Commit != "abc" {
		t.Errorf("build = %s %s", st.Version, st.Commit)
	}
}

func TestLastErrorIsBoundedAndKeepsValidUTF8(t *testing.T) {
	h, _ := newTestHub()
	h.RecordError("startup", errors.New(strings.Repeat("я", 400)))

	msg := h.Status().LastError.Message
	if len(msg) > maxErrorLength+3 || !strings.HasSuffix(msg, "...") {
		t.Errorf("message of %d bytes is not bounded: %q", len(msg), msg[len(msg)-20:])
	}
}

func TestEndpointsAreReadOnly(t *testing.T) {
	h, _ := newTestHub()
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/status", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /status = %d, want 405", rec.Code)
	}
}

func TestServeListensBeforeItReturnsAndStopsCleanly(t *testing.T) {
	h, _ := newTestHub()
	log := slog.New(slog.DiscardHandler)

	stop, err := Serve("127.0.0.1:0", h.Handler(), log)
	if err != nil {
		t.Fatal(err)
	}
	stop()

	// An address that is taken is an error at start.
	busy := httptest.NewServer(http.NotFoundHandler())
	defer busy.Close()
	if _, err := Serve(strings.TrimPrefix(busy.URL, "http://"), h.Handler(), log); err == nil || !strings.Contains(err.Error(), "observability.listen") {
		t.Errorf("Serve on a taken address = %v, want an error naming the setting", err)
	}
}

func TestQuarantinedTablesAreCountedAndListed(t *testing.T) {
	h, _ := newTestHub()
	if got := value(t, scrape(t, h), "binwave_quarantined_tables"); got != "0" {
		t.Errorf("quarantined tables before any = %s", got)
	}

	h.QuarantineChanged([]checkpoint.Quarantined{
		{Table: "shop.b", Since: "u:1-5", Reason: "no key"},
		{Table: "shop.a", Since: "u:1-4", Reason: "cannot convert"},
	})
	h.RowsSkipped("shop.a", 7)
	h.RowsSkipped("shop.b", 3)

	text := scrape(t, h)
	if got := value(t, text, "binwave_quarantined_tables"); got != "2" {
		t.Errorf("quarantined tables = %s, want 2", got)
	}
	if got := value(t, text, "binwave_quarantine_skipped_rows_total"); got != "10" {
		t.Errorf("skipped rows = %s, want 10", got)
	}

	_, body := get(t, h, "/status")
	var st Status
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Quarantined) != 2 || st.Quarantined[0].Table != "shop.a" || st.Quarantined[1].Reason != "no key" {
		t.Errorf("status lists %+v, want both tables sorted with their reasons", st.Quarantined)
	}

	// Released: the list empties, and /status shows an empty list, not null.
	h.QuarantineChanged(nil)
	if got := value(t, scrape(t, h), "binwave_quarantined_tables"); got != "0" {
		t.Errorf("quarantined tables after release = %s", got)
	}
	_, body = get(t, h, "/status")
	if !strings.Contains(body, `"quarantined": []`) {
		t.Errorf("status without quarantine must show an empty list:\n%s", body)
	}
}

func TestPausedServiceIsUpNotReadyAndItsLagGrows(t *testing.T) {
	h, c := newTestHub()
	h.Ready()
	h.Idle() // caught up: lag 0
	if got := value(t, scrape(t, h), "binwave_lag_seconds"); got != "0" {
		t.Fatalf("lag before the pause = %s", got)
	}

	h.Paused()
	c.Advance(90 * time.Second)
	h.Progress() // the paused service is alive and keeps reporting progress

	text := scrape(t, h)
	if got := value(t, text, "binwave_paused"); got != "1" {
		t.Errorf("binwave_paused = %s", got)
	}
	if got := value(t, text, "binwave_ready"); got != "0" {
		t.Errorf("binwave_ready = %s, want 0: a paused service replicates nothing", got)
	}
	if got := value(t, text, "binwave_lag_seconds"); got != "90" {
		t.Errorf("lag = %s, want 90: a paused service must not look caught up", got)
	}
	if code, body := get(t, h, "/readyz"); code != 503 || body != "paused" {
		t.Errorf("/readyz = %d %q, want 503 paused", code, body)
	}
	if code, _ := get(t, h, "/healthz"); code != 200 {
		t.Errorf("/healthz = %d: a paused service is alive", code)
	}
	st := h.Status()
	if st.State != StatePaused || st.PausedSince == nil || !st.PausedSince.Equal(t0) {
		t.Errorf("status = %+v", st)
	}

	h.Resumed()
	text = scrape(t, h)
	if value(t, text, "binwave_paused") != "0" || value(t, text, "binwave_ready") != "1" || h.Status().PausedSince != nil {
		t.Errorf("after the resume: paused %s ready %s since %v", value(t, text, "binwave_paused"), value(t, text, "binwave_ready"), h.Status().PausedSince)
	}
}

func TestFailedServiceIsUpNotReadyAndTheMetricsPortHidesTheKey(t *testing.T) {
	h, c := newTestHub()
	h.Ready()
	f := failure.Failure{
		Time: t0, GTID: "u:5", Kind: failure.KindRows, Table: "shop.orders", Op: "insert", Rows: 2,
		Key: "PRIMARY KEY (id)", KeyValues: []string{"42"}, Message: "the value cannot be converted", Detail: "value: secret",
	}

	h.Failed(f)
	c.Advance(30 * time.Second)
	h.Progress()

	text := scrape(t, h)
	if value(t, text, "binwave_replication_failed") != "1" || value(t, text, "binwave_ready") != "0" || value(t, text, "binwave_paused") != "0" {
		t.Errorf("failed %s ready %s paused %s", value(t, text, "binwave_replication_failed"), value(t, text, "binwave_ready"), value(t, text, "binwave_paused"))
	}
	if got := value(t, text, "binwave_lag_seconds"); got != "30" {
		t.Errorf("lag = %s, want 30: a failed service must not look caught up", got)
	}
	if code, body := get(t, h, "/readyz"); code != 503 || body != "failed" {
		t.Errorf("/readyz = %d %q", code, body)
	}
	if code, _ := get(t, h, "/healthz"); code != 200 {
		t.Errorf("/healthz = %d: a failed service is alive, restarting it would hit the same failure", code)
	}
	if st := h.Status(); st.Failure == nil || len(st.Failure.KeyValues) != 1 || st.Failure.Detail == "" {
		t.Errorf("the control channel gets the whole failure: %+v", st.Failure)
	}
	code, body := get(t, h, "/status")
	if code != 200 || strings.Contains(body, "42") || strings.Contains(body, "secret") || !strings.Contains(body, "PRIMARY KEY (id)") {
		t.Errorf("/status on the metrics port: %d\n%s", code, body)
	}

	h.Resumed()
	if h.Status().Failure != nil || value(t, scrape(t, h), "binwave_replication_failed") != "0" {
		t.Error("the failure is still there after the resume")
	}
}

func TestResyncsAreCountedAndListed(t *testing.T) {
	h, _ := newTestHub()

	h.ResyncRowsRead("shop.a", 100)
	h.ResyncChanged([]snap.Status{{Table: "shop.a", Rows: 100, Fraction: 0.5}})
	h.ResyncRowsRead("shop.a", 150)
	// A table read between two publications of the statuses is counted all the same.
	h.ResyncRowsRead("shop.b", 30)
	h.ResyncChanged([]snap.Status{{Table: "shop.a", Rows: 250, Fraction: 1, Done: true}})
	text := scrape(t, h)
	if got := value(t, text, "binwave_resyncing_tables"); got != "1" {
		t.Errorf("resyncing tables = %s, want 1", got)
	}
	if got := value(t, text, "binwave_resync_rows_read_total"); got != "280" {
		t.Errorf("rows read = %s, want 280: every chunk is counted once", got)
	}
	_, body := get(t, h, "/status")
	var st Status
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Resyncing) != 1 || st.Resyncing[0].Rows != 250 || !st.Resyncing[0].Done {
		t.Errorf("status lists %+v", st.Resyncing)
	}

	h.ResyncFinished("shop.a", true)
	h.ResyncChanged(nil)
	text = scrape(t, h)
	if got := value(t, text, "binwave_table_resyncs_total"); got != "1" {
		t.Errorf("resyncs = %s, want 1", got)
	}
	if got := value(t, text, "binwave_resyncing_tables"); got != "0" {
		t.Errorf("resyncing tables after = %s", got)
	}
	// One that was given up is not counted as done.
	h.ResyncFinished("shop.b", false)
	if got := value(t, scrape(t, h), "binwave_table_resyncs_total"); got != "1" {
		t.Errorf("resyncs after a given up one = %s, want 1", got)
	}
}
