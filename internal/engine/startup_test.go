package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/observability"
)

// fakeClock is a clock whose time moves only when something sleeps, and whose timers fire only
// when a test says so.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
	timers []*fakeTimer
}

type fakeTimer struct {
	f       func()
	stopped bool
}

func (c *fakeClock) clock() clock {
	return clock{
		now: func() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now },
		sleep: func(ctx context.Context, d time.Duration) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.sleeps = append(c.sleeps, d)
			c.now = c.now.Add(d)
			return ctx.Err()
		},
		afterFunc: func(_ time.Duration, f func()) func() bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			t := &fakeTimer{f: f}
			c.timers = append(c.timers, t)
			return func() bool { c.mu.Lock(); defer c.mu.Unlock(); was := !t.stopped; t.stopped = true; return was }
		},
	}
}

// fire runs the deadline of the latest attempt, as if the startup budget had run out.
func (c *fakeClock) fire() {
	c.mu.Lock()
	t := c.timers[len(c.timers)-1]
	c.mu.Unlock()
	if !t.stopped {
		t.f()
	}
}

func startupEngine(budget time.Duration) (*Engine, *fakeClock) {
	fc := &fakeClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	cfg := &config.Config{}
	cfg.Retry.StartupBudget = config.Duration(budget)
	cfg.Retry.InitialDelay = config.Duration(time.Second)
	cfg.Retry.MaxDelay = config.Duration(30 * time.Second)
	return &Engine{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil)), clock: fc.clock()}, fc
}

func TestStartupRetriesWhileADatabaseIsDownAndForwardsReadyOnce(t *testing.T) {
	e, fc := startupEngine(10 * time.Minute)
	var attempts, readied int
	hooks := app.Hooks{Ready: func() { readied++ }}

	err := e.retryStartup(context.Background(), hooks, func(_ context.Context, h app.Hooks) error {
		attempts++
		if attempts < 3 {
			return errors.New("dial tcp: connection refused")
		}
		h.Ready()
		return nil
	})
	if err != nil {
		t.Fatalf("retryStartup() = %v", err)
	}
	if attempts != 3 || readied != 1 {
		t.Errorf("attempts = %d, Ready calls = %d; want 3 and 1", attempts, readied)
	}
	if want := []time.Duration{time.Second, 2 * time.Second}; !equalDurations(fc.sleeps, want) {
		t.Errorf("waited %v between attempts, want %v", fc.sleeps, want)
	}
}

func TestStartupStopsAtOnceOnAnErrorThatARetryCannotFix(t *testing.T) {
	e, fc := startupEngine(10 * time.Minute)
	var attempts int
	want := &app.ConfigError{Err: errors.New("access denied")}

	err := e.retryStartup(context.Background(), app.Hooks{}, func(context.Context, app.Hooks) error {
		attempts++
		return want
	})

	if !errors.Is(err, want) || attempts != 1 || len(fc.sleeps) != 0 {
		t.Errorf("err = %v, attempts = %d, sleeps = %v; want the config error after one attempt", err, attempts, fc.sleeps)
	}
}

func TestStartupGivesUpAfterTheBudget(t *testing.T) {
	e, fc := startupEngine(time.Minute)
	var attempts int
	cause := errors.New("dial tcp: connection refused")

	err := e.retryStartup(context.Background(), app.Hooks{}, func(context.Context, app.Hooks) error {
		attempts++
		return cause
	})

	var cfgErr *app.ConfigError
	if err == nil || errors.As(err, &cfgErr) {
		t.Fatalf("err = %v, want a plain error: an outage is not a configuration problem, so systemd may restart", err)
	}
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "retry.startup_budget") {
		t.Errorf("err = %q, want it to name the budget and keep the last cause", err)
	}
	var waited time.Duration
	for _, d := range fc.sleeps {
		waited += d
		if d > 30*time.Second {
			t.Errorf("waited %v once, the cap is 30s", d)
		}
	}
	if waited > time.Minute {
		t.Errorf("waited %v in total, more than the budget", waited)
	}
	if attempts < 3 {
		t.Errorf("only %d attempts in a minute of budget", attempts)
	}
}

func TestStartupDoesNotRetryAFailureAfterTheServiceWasReady(t *testing.T) {
	e, fc := startupEngine(10 * time.Minute)
	var attempts int
	cause := errors.New("binlog stream failed")

	err := e.retryStartup(context.Background(), app.Hooks{}, func(_ context.Context, h app.Hooks) error {
		attempts++
		h.Ready()
		return cause
	})

	if !errors.Is(err, cause) || attempts != 1 || len(fc.sleeps) != 0 {
		t.Errorf("err = %v, attempts = %d; from READY on, restarts are the supervisor's job", err, attempts)
	}
}

func TestStartupCutsAnAttemptThatHangsPastTheBudget(t *testing.T) {
	e, fc := startupEngine(time.Minute)
	entered := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- e.retryStartup(context.Background(), app.Hooks{}, func(ctx context.Context, _ app.Hooks) error {
			close(entered)
			<-ctx.Done() // a database that accepts the connection and never answers
			return ctx.Err()
		})
	}()
	<-entered
	fc.fire()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "retry.startup_budget") {
			t.Errorf("err = %v, want the budget error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the hanging attempt was not cut off")
	}
}

// TestStartupFailsWhenAnAttemptCutByTheBudgetReturnsNil: a pipeline waiting for its first event
// takes the cancellation for a graceful stop and returns nil; the budget still ran out, so the
// process must fail and be restarted, not exit 0.
func TestStartupFailsWhenAnAttemptCutByTheBudgetReturnsNil(t *testing.T) {
	e, fc := startupEngine(time.Minute)
	entered := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- e.retryStartup(context.Background(), app.Hooks{}, func(ctx context.Context, _ app.Hooks) error {
			close(entered)
			<-ctx.Done()
			return nil
		})
	}()
	<-entered
	fc.fire()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "retry.startup_budget") {
			t.Errorf("err = %v, want the budget error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the attempt was not cut off")
	}
}

func TestStartupStopsWhenTheProcessIsAskedToStop(t *testing.T) {
	e, _ := startupEngine(10 * time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	var attempts int

	err := e.retryStartup(ctx, app.Hooks{}, func(context.Context, app.Hooks) error {
		attempts++
		cancel()
		return context.Canceled
	})

	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Errorf("err = %v, attempts = %d", err, attempts)
	}
}

func equalDurations(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestStartupFailuresAreVisibleInTheStatus(t *testing.T) {
	e, _ := startupEngine(10 * time.Minute)
	e.hub = observability.NewHub(observability.Options{})
	var attempts int

	_ = e.retryStartup(context.Background(), app.Hooks{}, func(_ context.Context, h app.Hooks) error {
		attempts++
		if attempts < 3 {
			return errors.New("dial tcp: connection refused")
		}
		h.Ready()
		return nil
	})

	st := e.hub.Status()
	if st.LastError == nil || st.LastError.Kind != "startup" || !strings.Contains(st.LastError.Message, "connection refused") {
		t.Errorf("last error = %+v, want the failed start attempt", st.LastError)
	}
}
