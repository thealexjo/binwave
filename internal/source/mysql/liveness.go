package mysql

import (
	"errors"
	"time"
)

// ErrSourceSilent means the source sent nothing, not even a server heartbeat, for longer than the
// liveness timeout. A server that is merely idle still sends heartbeats, so silence means a
// frozen server, a network partition or a half-open connection: the stream must fail so the
// process is restarted, instead of reporting a healthy idle stream while replication is stuck.
var ErrSourceSilent = errors.New("the source went silent")

// liveness tracks when the source last proved it is alive: by delivering an event or a server
// heartbeat. Time spent inside the consumer's handler does not count as silence, because while
// the consumer blocks (backpressure) the source is not given a chance to speak.
type liveness struct {
	timeout time.Duration
	now     func() time.Time
	last    time.Time
}

func newLiveness(timeout time.Duration, now func() time.Time) *liveness {
	return &liveness{timeout: timeout, now: now, last: now()}
}

// touch records a sign of life, or the end of time spent away from the source.
func (l *liveness) touch() { l.last = l.now() }

// credit excuses time spent away from the source without treating it as a sign of life. The
// consumer blocking while it handles our own idle heartbeat is such time: the source could not
// speak, but it has not spoken either.
func (l *liveness) credit(d time.Duration) { l.last = l.last.Add(d) }

// silence reports how long the source has been quiet and whether that exceeds the timeout.
func (l *liveness) silence() (time.Duration, bool) {
	d := l.now().Sub(l.last)
	return d, d > l.timeout
}
