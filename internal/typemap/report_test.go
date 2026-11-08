package typemap

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) attrs(i int) map[string]any {
	out := map[string]any{}
	h.records[i].Attrs(func(a slog.Attr) bool {
		out[a.Key] = a.Value.Any()
		return true
	})
	return out
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func TestLogReporterRateLimits(t *testing.T) {
	h := &captureHandler{}
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	r := NewLogReporter(slog.New(h), time.Minute, clock.Now)

	zero := Adjustment{Table: "shop.orders", Column: "paid_at", Kind: AdjustZeroDate, Original: "0000-00-00", Replacement: "1900-01-01"}

	r.Report(zero)
	if len(h.records) != 1 || h.records[0].Level != slog.LevelWarn || h.records[0].Message != "value adjusted" {
		t.Fatalf("first occurrence must be logged immediately as a warning, got %+v", h.records)
	}
	first := h.attrs(0)
	for key, want := range map[string]any{
		"table": "shop.orders", "column": "paid_at", "kind": "zero_date",
		"original": "0000-00-00", "replacement": "1900-01-01", "count": int64(1),
	} {
		if first[key] != want {
			t.Errorf("attr %s = %v, want %v", key, first[key], want)
		}
	}

	for range 1000 {
		clock.Advance(time.Millisecond)
		r.Report(zero)
	}
	if len(h.records) != 1 {
		t.Fatalf("repeats within the interval must be suppressed, got %d records", len(h.records))
	}

	// Another column or kind is a separate stream.
	r.Report(Adjustment{Table: "shop.orders", Column: "shipped_at", Kind: AdjustZeroDate})
	r.Report(Adjustment{Table: "shop.orders", Column: "paid_at", Kind: AdjustOutOfRange})
	if len(h.records) != 3 {
		t.Fatalf("distinct column/kind must log its first occurrence, got %d records", len(h.records))
	}

	clock.Advance(time.Minute)
	r.Report(zero)
	if len(h.records) != 4 || h.records[3].Message != "values adjusted (summary)" {
		t.Fatalf("summary expected after the interval, got %d records", len(h.records))
	}
	if got := h.attrs(3)["count"]; got != int64(1001) {
		t.Errorf("summary count = %v, want 1001", got)
	}

	r.Report(zero)
	r.Report(zero)
	r.Flush()
	if len(h.records) != 5 || h.attrs(4)["count"] != int64(2) {
		t.Fatalf("Flush must log pending counts, got %d records", len(h.records))
	}
	r.Flush()
	if len(h.records) != 5 {
		t.Error("Flush without pending adjustments must not log")
	}
}

func TestLogReporterConcurrent(t *testing.T) {
	h := &captureHandler{}
	r := NewLogReporter(slog.New(h), time.Hour, nil)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				r.Report(Adjustment{Table: "t", Column: "c", Kind: AdjustZeroDate})
			}
		})
	}
	wg.Wait()
	r.Flush()

	if len(h.records) != 2 {
		t.Fatalf("got %d records, want first occurrence + one summary", len(h.records))
	}
	if got := h.attrs(0)["count"].(int64) + h.attrs(1)["count"].(int64); got != 8000 {
		t.Errorf("total count = %d, want 8000", got)
	}
}
