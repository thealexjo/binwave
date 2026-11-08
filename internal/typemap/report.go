package typemap

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// AdjustmentKind says why a value was replaced during conversion.
type AdjustmentKind string

// Adjustment kinds.
const (
	// AdjustZeroDate: 0000-00-00 (or a zero TIMESTAMP) replaced with the type minimum.
	AdjustZeroDate AdjustmentKind = "zero_date"
	// AdjustInvalidDate: a date with zero parts (2024-00-15) or a non-existent day replaced with the type minimum.
	AdjustInvalidDate AdjustmentKind = "invalid_date"
	// AdjustOutOfRange: a valid date outside the ClickHouse type range clamped to its minimum or maximum.
	AdjustOutOfRange AdjustmentKind = "out_of_range"
	// AdjustDDLTimestamp: a column added with DEFAULT CURRENT_TIMESTAMP gives its existing rows the
	// time the source executed the ALTER TABLE, which is known to the second; binwave uses the
	// time of the statement in the binlog.
	AdjustDDLTimestamp AdjustmentKind = "ddl_default_timestamp"
	// AdjustNullSet: NULL in a nullable SET column written as an empty array (ClickHouse arrays cannot be NULL).
	AdjustNullSet AdjustmentKind = "null_set"
)

// Adjustment is a value binwave replaced instead of failing.
type Adjustment struct {
	Table       string
	Column      string
	Kind        AdjustmentKind
	Original    string
	Replacement string
}

// Reporter receives adjustments. Implementations must be safe for concurrent use.
type Reporter interface {
	Report(a Adjustment)
}

type nopReporter struct{}

func (nopReporter) Report(Adjustment) {}

// DefaultLogInterval is how often repeated adjustments of one column are summarized.
const DefaultLogInterval = time.Minute

// LogReporter logs adjustments without flooding: the first occurrence per table, column and
// kind is logged immediately; repeats are counted and summarized at most once per interval.
type LogReporter struct {
	logger   *slog.Logger
	interval time.Duration
	now      func() time.Time

	mu      sync.Mutex
	entries map[adjustmentKey]*adjustmentEntry
}

type adjustmentKey struct {
	table, column string
	kind          AdjustmentKind
}

type adjustmentEntry struct {
	lastLogged time.Time
	suppressed int64
	last       Adjustment
}

// NewLogReporter creates a LogReporter. now may be nil to use time.Now.
func NewLogReporter(logger *slog.Logger, interval time.Duration, now func() time.Time) *LogReporter {
	if now == nil {
		now = time.Now
	}
	return &LogReporter{
		logger:   logger,
		interval: interval,
		now:      now,
		entries:  make(map[adjustmentKey]*adjustmentEntry),
	}
}

// Report implements Reporter.
func (r *LogReporter) Report(a Adjustment) {
	key := adjustmentKey{a.Table, a.Column, a.Kind}
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()

	e, seen := r.entries[key]
	if !seen {
		r.entries[key] = &adjustmentEntry{lastLogged: now}
		r.log("value adjusted", a, 1)
		return
	}

	e.suppressed++
	e.last = a
	if now.Sub(e.lastLogged) >= r.interval {
		r.log("values adjusted (summary)", a, e.suppressed)
		e.suppressed = 0
		e.lastLogged = now
	}
}

// Flush logs pending summaries; call it on shutdown.
func (r *LogReporter) Flush() {
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, e := range r.entries {
		if e.suppressed > 0 {
			r.log("values adjusted (summary)", e.last, e.suppressed)
			e.suppressed = 0
			e.lastLogged = now
		}
	}
}

func (r *LogReporter) log(msg string, a Adjustment, count int64) {
	r.logger.LogAttrs(context.Background(), slog.LevelWarn, msg,
		slog.String("table", a.Table),
		slog.String("column", a.Column),
		slog.String("kind", string(a.Kind)),
		slog.String("original", a.Original),
		slog.String("replacement", a.Replacement),
		slog.Int64("count", count),
	)
}
