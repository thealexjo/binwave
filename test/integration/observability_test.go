//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/engine"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/test/testenv"
)

// metricValue reads one series from the hub's /metrics.
func metricValue(t *testing.T, hub *observability.Hub, series string) float64 {
	t.Helper()
	v, ok := findMetric(t, hub, series)
	if !ok {
		t.Fatalf("series %s is not exposed", series)
	}
	return v
}

// findMetric is metricValue for a series that may not exist yet: a labelled counter appears with
// its first increment.
func findMetric(t *testing.T, hub *observability.Hub, series string) (float64, bool) {
	t.Helper()
	rec := httptest.NewRecorder()
	hub.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			if err != nil {
				t.Fatal(err)
			}
			return v, true
		}
	}
	return 0, false
}

// TestEngineReportsWhatItDoes runs the engine against real servers and compares the metrics and
// the status with what was done to the source.
func TestEngineReportsWhatItDoes(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "clickhouse")

	hub := observability.NewHub(observability.Options{Version: "test", Commit: "abc"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- engine.New(cfg, quietLogger(), engine.WithObservability(hub)).Run(ctx, app.Hooks{
			Ready:    func() { hub.Ready(); ready <- struct{}{} },
			Progress: hub.Progress,
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("the engine stopped before it was ready: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the engine did not become ready")
	}

	// One transaction of five rows.
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3), (4, 'd', 4), (5, 'e', 5)")
	want := executedGTIDs(t, my)
	waitUntil(t, "the position to reach the source's", 60*time.Second, func() bool {
		return hub.Status().Position == want
	})

	for series, want := range map[string]float64{
		"binwave_rows_read_total":    5,
		"binwave_rows_written_total": 5,
		"binwave_transactions_total": 1,
		"binwave_tables":             1,
		"binwave_ready":              1,
		"binwave_buffer_rows":        0,
	} {
		if got := metricValue(t, hub, series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
	if got := metricValue(t, hub, "binwave_inserts_total"); got < 1 {
		t.Errorf("binwave_inserts_total = %v", got)
	}

	// The source is quiet: the replica has caught up, and says so.
	waitUntil(t, "the lag to be 0 on a quiet source", 30*time.Second, func() bool {
		return metricValue(t, hub, "binwave_lag_seconds") == 0 && hub.Status().CaughtUp
	})
	if age := time.Now().Unix() - int64(metricValue(t, hub, "binwave_position_commit_timestamp_seconds")); age < 0 || age > 60 {
		t.Errorf("the saved position says the last commit was %ds ago", age)
	}

	rec := httptest.NewRecorder()
	hub.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	var st observability.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil || st.State != observability.StateReady || st.Tables != 1 || st.LastError != nil {
		t.Errorf("status = %+v (%v)", st, err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}

// TestValueAdjustmentsAreCounted: a zero date is replaced, logged, and counted.
func TestValueAdjustmentsAreCounted(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.zd (id INT PRIMARY KEY, d DATE NOT NULL)")
	cfg := engineConfig(t, my, ch, "file")

	hub := observability.NewHub(observability.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, 1)
	go func() {
		_ = engine.New(cfg, quietLogger(), engine.WithObservability(hub)).Run(ctx, app.Hooks{
			Ready:    func() { ready <- struct{}{} },
			Progress: hub.Progress,
		})
	}()
	select {
	case <-ready:
	case <-time.After(60 * time.Second):
		t.Fatal("the engine did not become ready")
	}

	// A session that allows zero dates, as legacy applications do.
	c, err := my.DB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.ExecContext(context.Background(), "SET SESSION sql_mode = ''"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(context.Background(), "INSERT INTO binwave.zd VALUES (1, '0000-00-00'), (2, '2024-01-01')"); err != nil {
		t.Fatal(err)
	}

	waitUntil(t, "the adjustment to be counted", 60*time.Second, func() bool {
		v, ok := findMetric(t, hub, `binwave_value_adjustments_total{kind="zero_date"}`)
		return ok && v >= 1
	})
}
