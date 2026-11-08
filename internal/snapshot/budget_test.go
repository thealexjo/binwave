package snapshot

import (
	"context"
	"testing"
	"time"
)

// TestBudgetKeepsTheCombinedSpeedUnderTheLimit: two readers sharing one Budget read the source
// together no faster than one would with the same MaxRowsPerSecond alone.
func TestBudgetKeepsTheCombinedSpeedUnderTheLimit(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	src1 := &fakeSource{clock: c, keys: sequence(10_000, 1), perRow: func(int) time.Duration { return time.Millisecond }, rowSize: 10}
	src2 := &fakeSource{clock: c, keys: sequence(10_000, 1), perRow: func(int) time.Duration { return time.Millisecond }, rowSize: 10}
	budget := NewBudget(5000, c.Now, c.Sleep)
	opts := func() Options {
		return Options{Now: c.Now, Sleep: c.Sleep, Budget: budget, Sizer: SizerOptions{Initial: 1000, Max: 1000}}
	}
	r1 := NewReader(src1, idTable, nil, opts())
	r2 := NewReader(src2, idTable, nil, opts())

	total1, _ := readAll(t, r1)
	total2, _ := readAll(t, r2)

	took := c.now.Sub(time.Unix(0, 0)).Seconds()
	if speed := float64(total1+total2) / took; speed > 5100 {
		t.Errorf("the two readers together read %d rows in %.1fs = %.0f a second, the shared limit is 5000", total1+total2, took, speed)
	}
}

// TestZeroBudgetIsUnlimitedAndSetRateCanChangeIt: 0 means no limit, but the object still exists, so a
// limit can be turned on later (SetLoadRate) without recreating it. A nil *Budget also accepts every
// method, for callers (a `tables resync` job) that pass no budget at all.
func TestZeroBudgetIsUnlimitedAndSetRateCanChangeIt(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	b := NewBudget(0, c.Now, c.Sleep)
	if b == nil {
		t.Fatal("NewBudget(0, ...) = nil, want a usable object so SetRate can turn a limit on later")
	}
	if got := b.Rate(); got != 0 {
		t.Errorf("Rate() = %d, want 0 (unlimited)", got)
	}
	b.add(1_000_000) // unlimited: nothing is paced, so nothing should be accounted for either
	if err := b.wait(context.Background()); err != nil {
		t.Errorf("wait() while unlimited = %v", err)
	}
	if c.now != time.Unix(0, 0) {
		t.Errorf("wait() slept while unlimited: now = %v", c.now)
	}

	b.SetRate(1)
	b.add(1)
	if err := b.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.now.Sub(time.Unix(0, 0)) < time.Second {
		t.Errorf("after SetRate(1) and one more row, wait() did not pace to roughly 1 row/s: now = %v", c.now)
	}

	var nilBudget *Budget
	if err := nilBudget.wait(context.Background()); err != nil {
		t.Errorf("wait() on a nil budget = %v", err)
	}
	nilBudget.add(100)   // must not panic
	nilBudget.SetRate(5) // must not panic
	if got := nilBudget.Rate(); got != 0 {
		t.Errorf("Rate() on a nil budget = %d, want 0", got)
	}
}
