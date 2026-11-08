package mysql

import (
	"testing"
	"time"
)

type manualClock struct{ t time.Time }

func (c *manualClock) now() time.Time          { return c.t }
func (c *manualClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestLivenessTimesOutOnlyAfterTheLimit(t *testing.T) {
	clock := &manualClock{t: time.Unix(1_000, 0)}
	l := newLiveness(15*time.Second, clock.now)

	clock.advance(15 * time.Second)
	if d, silent := l.silence(); silent {
		t.Errorf("silent after exactly the timeout (%v); the limit is exclusive", d)
	}
	clock.advance(time.Nanosecond)
	if _, silent := l.silence(); !silent {
		t.Error("not silent after the timeout passed")
	}
}

func TestLivenessTouchResetsTheClock(t *testing.T) {
	clock := &manualClock{t: time.Unix(1_000, 0)}
	l := newLiveness(10*time.Second, clock.now)

	clock.advance(9 * time.Second)
	l.touch() // an event or heartbeat arrived
	clock.advance(9 * time.Second)
	if d, silent := l.silence(); silent || d != 9*time.Second {
		t.Errorf("silence = %v, silent = %v; want 9s and not silent", d, silent)
	}
}

// TestLivenessIgnoresTimeSpentInTheConsumer models backpressure: the consumer blocks for far
// longer than the timeout, and the stream must not be blamed when it resumes.
func TestLivenessIgnoresTimeSpentInTheConsumer(t *testing.T) {
	clock := &manualClock{t: time.Unix(1_000, 0)}
	l := newLiveness(10*time.Second, clock.now)

	l.touch()
	clock.advance(10 * time.Minute) // the handler was blocked by a slow ClickHouse
	l.touch()                       // the loop marks the return from the handler
	if _, silent := l.silence(); silent {
		t.Error("time spent blocked in the handler was counted as source silence")
	}
}

// TestLivenessCreditExcusesTimeWithoutProvingLife: the time the consumer spends on our own idle
// heartbeat is excused, but the source still has to speak eventually.
func TestLivenessCreditExcusesTimeWithoutProvingLife(t *testing.T) {
	clock := &manualClock{t: time.Unix(1_000, 0)}
	l := newLiveness(10*time.Second, clock.now)

	clock.advance(6 * time.Second) // silence so far
	clock.advance(30 * time.Second)
	l.credit(30 * time.Second) // the consumer was busy for 30 of those seconds
	if d, silent := l.silence(); silent || d != 6*time.Second {
		t.Errorf("silence = %v, silent = %v; want the 6s the source really was quiet", d, silent)
	}

	clock.advance(5 * time.Second)
	if _, silent := l.silence(); !silent {
		t.Error("the source stayed quiet past the timeout but was not reported: an idle heartbeat must not count as life")
	}
}
