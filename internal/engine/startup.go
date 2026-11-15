package engine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/thealexjo/binwave/internal/app"
)

// clock is what the startup retry needs from time; tests replace it.
type clock struct {
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
	// afterFunc runs f after d unless stop is called first.
	afterFunc func(d time.Duration, f func()) (stop func() bool)
}

func realClock() clock {
	return clock{
		now: time.Now,
		sleep: func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
		afterFunc: func(d time.Duration, f func()) func() bool {
			return time.AfterFunc(d, f).Stop
		},
	}
}

// states of one start attempt.
const (
	attemptPending int32 = iota
	attemptReady
	attemptExpired
)

// retryStartup runs attempt until the service becomes ready, retrying while a database is not
// reachable. The service starts when both databases accepted the connection and the rights
// checks passed; until then a failure that may pass by itself is retried with a delay that grows
// from retry.initial_delay to retry.max_delay, for retry.startup_budget, so a restart of the whole host does not need the databases to come
// up first. A failure that a retry cannot fix (*app.ConfigError) returns at once, and so does
// any failure after the service was ready: from there the supervisor is in charge of restarts.
func (e *Engine) retryStartup(ctx context.Context, hooks app.Hooks, attempt func(context.Context, app.Hooks) error) error {
	budget := time.Duration(e.cfg.Retry.StartupBudget)
	began := e.clock.now()
	delay := time.Duration(e.cfg.Retry.InitialDelay)
	maxDelay := time.Duration(e.cfg.Retry.MaxDelay)

	for n := 1; ; n++ {
		var state atomic.Int32
		actx, cancel := context.WithCancel(ctx)
		stop := e.clock.afterFunc(max(budget-e.clock.now().Sub(began), 0), func() {
			// Ready wins a race with the deadline: a running service is never cut off by it.
			if state.CompareAndSwap(attemptPending, attemptExpired) {
				cancel()
			}
		})

		h := hooks
		h.Ready = func() {
			if state.CompareAndSwap(attemptPending, attemptReady) {
				stop()
			}
			if hooks.Ready != nil {
				hooks.Ready()
			}
		}
		err := attempt(actx, h)
		stop()
		cancel()

		// An attempt the budget cut off has failed, whatever it returned: a pipeline waiting for its
		// first event takes the cancellation for a graceful stop and returns nil.
		if state.Load() == attemptExpired && ctx.Err() == nil {
			if e.hub != nil && err != nil {
				e.hub.StartupFailed(err)
			}
			if err == nil {
				return fmt.Errorf("the service did not start within retry.startup_budget (%s, %d attempts)", budget, n)
			}
			return fmt.Errorf("the service did not start within retry.startup_budget (%s, %d attempts): %w", budget, n, err)
		}
		var cfgErr *app.ConfigError
		switch {
		case err == nil, state.Load() == attemptReady, ctx.Err() != nil, errors.As(err, &cfgErr):
			return err
		}

		if e.hub != nil {
			e.hub.StartupFailed(err)
		}
		elapsed := e.clock.now().Sub(began)
		if elapsed+delay >= budget {
			return fmt.Errorf("the service did not start within retry.startup_budget (%s, %d attempts): %w",
				budget, n, err)
		}
		e.log.Warn("cannot start yet, retrying", "attempt", n, "retry_in", delay,
			"budget_left", (budget - elapsed).Round(time.Second), "error", err)
		if e.clock.sleep(ctx, delay) != nil {
			return err
		}
		delay = min(delay*2, maxDelay)
	}
}
