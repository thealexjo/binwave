package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// ErrOverloaded is returned when ClickHouse keeps answering "overloaded" for longer than
// retry.overload_budget. Retrying an outage of the network is right, retrying an overload
// forever is not: it holds back every table and hides the problem from the operator.
var ErrOverloaded = errors.New("ClickHouse stayed overloaded beyond retry.overload_budget")

// withRetry runs fn until it succeeds, retrying transient ClickHouse failures with exponential
// backoff. Network failures are retried without limit; overload errors only for the configured
// budget of consecutive time. A failure that a retry cannot fix, or the end of ctx, returns the error. While it
// waits or a call is in flight it reports progress, so the watchdog only fires when the
// pipeline is really stuck: a call that hangs is cut by the attempt timeout and retried.
func (p *Pipeline) withRetry(ctx context.Context, what string, fn func(context.Context) error) error {
	return p.withRetryTimeout(ctx, what, p.retry.AttemptTimeout, fn)
}

// withRetryTimeout is withRetry with its own limit for one attempt, for the operations that take
// longer than an insert.
func (p *Pipeline) withRetryTimeout(ctx context.Context, what string, timeout time.Duration, fn func(context.Context) error) error {
	delay := p.retry.Initial
	budget := time.Duration(p.cfg.Retry.OverloadBudget)
	var overloadedSince time.Time
	for attempt := 1; ; attempt++ {
		err := p.attempt(ctx, timeout, fn)
		if err == nil {
			// The run of failures ended: what the status reports as failing has to stop being that. It
			// is ended by any request that succeeds, not only by the one that failed: that one can give
			// up with an error a retry cannot fix, or be cut short by a pause.
			if p.failing.CompareAndSwap(true, false) {
				p.d.Observer.Recovered()
			}
			return nil
		}
		if ctx.Err() != nil || !chsink.IsTransient(err) {
			return err
		}

		if chsink.IsOverload(err) {
			if overloadedSince.IsZero() {
				overloadedSince = p.d.Now()
			}
			if elapsed := p.d.Now().Sub(overloadedSince); elapsed >= budget {
				return fmt.Errorf("%w: %s kept failing for %s: %w", ErrOverloaded, what, elapsed.Round(time.Second), err)
			}
		} else {
			overloadedSince = time.Time{} // another kind of failure: the count starts over
		}

		reason := ReasonTransient
		if chsink.IsOverload(err) {
			reason = ReasonOverload
		}
		p.failing.Store(true)
		p.d.Observer.Retry(what, reason, err)
		p.log.Warn("ClickHouse did not accept the request, retrying",
			"operation", what, "attempt", attempt, "retry_in", delay, "error", err)
		if p.d.Sleep(ctx, delay) != nil {
			return err
		}
		delay = min(delay*2, p.retry.Max)
	}
}

// attempt runs fn once under the attempt timeout, with a keepalive while it is in flight.
func (p *Pipeline) attempt(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	actx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// A call that finishes before the first keepalive tick would otherwise report nothing at all, and
	// a long run of such calls is exactly what a busy pipeline is made of.
	p.progress()

	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(keepaliveInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				p.progress()
			}
		}
	}()
	defer func() {
		close(done)
		<-finished
	}()

	return fn(actx)
}

// sleepWithProgress waits for d, reporting progress every few seconds.
func (p *Pipeline) sleepWithProgress(ctx context.Context, d time.Duration) error {
	for d > 0 {
		p.progress()
		chunk := min(d, sleepChunk)
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait to retry: %w", ctx.Err())
		case <-time.After(chunk):
		}
		d -= chunk
	}
	return nil
}
