package clickhouse

import (
	"math"
	"strings"
	"testing"
	"time"
)

var epoch = time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

func nextVersion(t *testing.T, v *Versioner, at time.Time) uint64 {
	t.Helper()
	n, err := v.Next(at)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestVersionerFollowsTheCommitClock(t *testing.T) {
	v := NewVersioner(0)

	a := nextVersion(t, v, epoch)
	b := nextVersion(t, v, epoch.Add(time.Second))

	if a != uint64(epoch.UnixMicro())<<12 {
		t.Errorf("first version = %d, want commit microseconds << 12", a)
	}
	if b <= a {
		t.Errorf("a later commit got a lower version: %d <= %d", b, a)
	}
}

func TestVersionerNeverRepeatsOrGoesBack(t *testing.T) {
	v := NewVersioner(0)

	// Transactions in the same microsecond, then a clock that steps back: every one still gets a
	// strictly higher version than the one before, because binlog order is commit order.
	times := []time.Time{epoch, epoch, epoch, epoch.Add(-time.Hour), epoch.Add(-time.Hour), epoch.Add(-2 * time.Hour)}
	var prev uint64
	for i, at := range times {
		got := nextVersion(t, v, at)
		if got <= prev {
			t.Fatalf("transaction %d: version %d is not above the previous %d", i, got, prev)
		}
		prev = got
	}
}

func TestVersionerReturnsToTheClockOnceItCatchesUp(t *testing.T) {
	v := NewVersioner(0)
	nextVersion(t, v, epoch)
	nextVersion(t, v, epoch.Add(-time.Minute)) // clock stepped back

	later := epoch.Add(time.Hour)
	if got := nextVersion(t, v, later); got != uint64(later.UnixMicro())<<12 {
		t.Errorf("version after the clock recovered = %d, want the clock value", got)
	}
}

// TestVersionerReplayFromACheckpointReproducesEveryVersion is the property replay safety rests on:
// resuming from a saved version yields exactly the versions the first run produced.
func TestVersionerReplayFromACheckpointReproducesEveryVersion(t *testing.T) {
	commits := []time.Time{
		epoch, epoch, epoch.Add(-time.Hour), epoch.Add(-time.Hour), epoch.Add(time.Second),
		epoch.Add(-time.Minute), epoch.Add(2 * time.Second),
	}

	first := NewVersioner(0)
	var all []uint64
	for _, at := range commits {
		all = append(all, nextVersion(t, first, at))
	}

	// A checkpoint after transaction k stores all[k]; a restart replays the rest.
	for k := range commits {
		resumed := NewVersioner(all[k])
		for i := k + 1; i < len(commits); i++ {
			if got := nextVersion(t, resumed, commits[i]); got != all[i] {
				t.Fatalf("resume after %d: transaction %d got %d, first run had %d", k, i, got, all[i])
			}
		}
	}
}

func TestVersionerWithoutTheSavedVersionCanRegress(t *testing.T) {
	// The counter-example that motivates persisting the version: after a clock step back,
	// forgetting the state gives a new transaction a version below what was already written.
	first := NewVersioner(0)
	nextVersion(t, first, epoch)
	written := nextVersion(t, first, epoch.Add(-time.Hour)) // chain continues above epoch

	forgetful := NewVersioner(0)
	if got := nextVersion(t, forgetful, epoch.Add(-time.Hour)); got >= written {
		t.Errorf("expected a regression without state, got %d >= %d", got, written)
	}
}

func TestVersionerLast(t *testing.T) {
	v := NewVersioner(77)
	if v.Last() != 77 {
		t.Errorf("Last() = %d", v.Last())
	}
	n := nextVersion(t, v, epoch)
	if v.Last() != n {
		t.Errorf("Last() = %d, want %d", v.Last(), n)
	}
}

func TestVersionerErrors(t *testing.T) {
	tests := []struct {
		name    string
		v       *Versioner
		at      time.Time
		wantMsg string
	}{
		{"no commit time", NewVersioner(0), time.Time{}, "no commit timestamp"},
		{"before the epoch", NewVersioner(0), time.Unix(-1, 0), "outside the supported range"},
		{"beyond the range", NewVersioner(0), time.UnixMicro(1 << 52), "outside the supported range"},
		{"exhausted", NewVersioner(math.MaxUint64), epoch, "exhausted"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.v.Last()
			_, err := tt.v.Next(tt.at)
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Next() = %v, want error containing %q", err, tt.wantMsg)
			}
			if tt.v.Last() != before {
				t.Error("a failed Next changed the state")
			}
		})
	}
}
