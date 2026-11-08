package snapshot

import (
	"context"
	"sync"
	"time"
)

// Budget paces the rows several Readers read together, so that reading N tables at once does not
// read the source N times faster than reading one table alone would: every row any of them reads
// counts against the one shared average. Safe for concurrent use. The limit can be changed at any
// time with SetRate, including from or to unlimited, which is why NewBudget always returns a usable
// object rather than nil for "unlimited": there would be nothing later to call SetRate on.
type Budget struct {
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error

	mu      sync.Mutex
	perSec  float64
	rows    int64
	started time.Time
}

// NewBudget creates a Budget limiting the combined read speed to rowsPerSecond; 0 or less means no
// limit.
func NewBudget(rowsPerSecond int, now func() time.Time, sleep func(context.Context, time.Duration) error) *Budget {
	return &Budget{perSec: float64(rowsPerSecond), now: now, sleep: sleep}
}

// SetRate changes the limit; 0 or less removes it. Pacing restarts from this moment: rows read
// before the change are not counted against the new rate, so tightening the limit never produces one
// large wait to "catch up" on a history that was never paced to begin with.
func (b *Budget) SetRate(rowsPerSecond int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.perSec = float64(rowsPerSecond)
	b.rows, b.started = 0, time.Time{}
}

// Rate reports the limit currently in effect; 0 means unlimited.
func (b *Budget) Rate() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return int(b.perSec)
}

// wait blocks until the rows already accounted for keep the shared average at or under the limit.
// Called before a Reader reads its next chunk, so the chunk about to be read is not yet counted:
// like a single Reader's own throttle, it corrects itself chunk by chunk rather than row by row.
func (b *Budget) wait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	perSec, rows, started := b.perSec, b.rows, b.started
	b.mu.Unlock()
	if perSec <= 0 || rows == 0 {
		return nil
	}
	due := time.Duration(float64(rows) / perSec * float64(time.Second))
	if wait := due - b.now().Sub(started); wait > 0 {
		return b.sleep(ctx, wait)
	}
	return nil
}

// add accounts for n more rows read, by any Reader sharing this Budget. Rows read while unlimited are
// not counted at all, so turning a limit on later paces only what is read from that moment on.
func (b *Budget) add(n int) {
	if b == nil || n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.perSec <= 0 {
		return
	}
	if b.rows == 0 {
		b.started = b.now()
	}
	b.rows += int64(n)
}
