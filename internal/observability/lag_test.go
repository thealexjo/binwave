package observability

import (
	"testing"
	"time"
)

var t0 = time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)

func TestLagTracker(t *testing.T) {
	var l lagTracker

	if got := l.lag(t0); got != 0 {
		t.Errorf("before anything happened: lag = %v, want 0", got)
	}

	// A backlog: the first transaction read was committed a minute ago.
	l.began(t0.Add(-time.Minute))
	l.began(t0.Add(-50 * time.Second)) // a later one does not move the oldest
	if got := l.lag(t0); got != time.Minute {
		t.Errorf("backlog: lag = %v, want 1m0s, the age of the oldest unwritten transaction", got)
	}

	// Both are written and saved, but the stream is still busy.
	l.saved(t0.Add(-50*time.Second), time.Time{})
	if got := l.lag(t0); got != 50*time.Second {
		t.Errorf("saved but busy: lag = %v, want 50s, the age of the last saved transaction", got)
	}

	// The source has nothing more to send.
	l.quiet()
	if got := l.lag(t0.Add(time.Hour)); got != 0 {
		t.Errorf("quiet source: lag = %v, want 0 however long ago its last change was", got)
	}

	// A new change wakes it up.
	l.began(t0.Add(time.Hour - 2*time.Second))
	if got := l.lag(t0.Add(time.Hour)); got != 2*time.Second {
		t.Errorf("after a change: lag = %v, want 2s", got)
	}
}

func TestLagStaysBehindAnOpenTransactionWhoseRowsAreWritten(t *testing.T) {
	var l lagTracker
	l.began(t0.Add(-10 * time.Second)) // transaction 1
	l.saved(t0.Add(-10*time.Second), time.Time{})
	l.began(t0.Add(-5 * time.Second)) // a big transaction 2, rows written early

	// Saving the position while transaction 2 is open must not make the lag look smaller than it is.
	l.saved(t0.Add(-10*time.Second), t0.Add(-5*time.Second))

	if got := l.lag(t0); got != 5*time.Second {
		t.Errorf("lag = %v, want 5s, counted from the open transaction", got)
	}
}

func TestLagIsNeverNegative(t *testing.T) {
	var l lagTracker
	l.began(t0.Add(time.Hour)) // the source's clock is ahead
	if got := l.lag(t0); got != 0 {
		t.Errorf("lag = %v, want 0", got)
	}
}
