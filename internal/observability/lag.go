package observability

import "time"

// lagTracker computes the replication lag from what the pipeline reports.
//
// The lag is how far behind the source ClickHouse is, in seconds:
//
//   - 0 while the source has nothing more to send: everything binwave has read is written to
//     ClickHouse and its position is saved (the source stayed quiet for an idle period);
//   - otherwise now minus the time the source committed the oldest transaction that is not yet
//     written and saved; while everything read so far is saved but the stream is still busy, the
//     commit time of the last saved transaction.
//
// So a quiet source shows 0, not a lag that grows with the time since its last change, and a
// backlog shows how old its oldest unwritten change is. The commit time is the source's clock,
// the current time is binwave's: a difference between the two clocks shows up in the lag.
type lagTracker struct {
	// pending is when the source committed the oldest transaction not covered by a saved position.
	pending time.Time
	// applied is when it committed the last transaction a saved position covers.
	applied time.Time
	// idle is true from an idle report until the next transaction begins.
	idle bool
}

// began records the start of a transaction committed at commit.
func (l *lagTracker) began(commit time.Time) {
	l.idle = false
	if l.pending.IsZero() {
		l.pending = commit
	}
}

// saved records that a position was saved; open is when the source committed the transaction
// still being read, if any.
func (l *lagTracker) saved(applied, open time.Time) {
	l.applied = applied
	l.pending = open
}

// quiet records that the source had nothing to send and nothing is waiting to be written.
func (l *lagTracker) quiet() {
	l.idle = true
	l.pending = time.Time{}
}

// lag returns the lag at now.
func (l *lagTracker) lag(now time.Time) time.Duration {
	if l.idle {
		return 0
	}
	oldest := l.pending
	if oldest.IsZero() {
		oldest = l.applied
	}
	if oldest.IsZero() {
		return 0
	}
	return max(now.Sub(oldest), 0)
}
