// Package app owns the process lifecycle of `binwave run`: it starts the replication engine,
// reports state to systemd (READY, STOPPING, watchdog) and classifies failures into exit codes.
package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Hooks are callbacks the engine uses to report its state to the process supervisor.
type Hooks struct {
	// Ready is called once, after the source requirements are verified and replication has started.
	Ready func()
	// Progress is called while the engine is alive and doing its job: an event was handled, a batch
	// was flushed, or an idle poll completed. The watchdog is pinged only while Progress keeps
	// being called, so a hung engine gets restarted. Long operations must call it periodically.
	Progress func()
}

// Runner is the replication engine. Run blocks until ctx is cancelled and then returns nil after a
// graceful stop (flush and position commit), or returns the error that stopped it.
type Runner interface {
	Run(ctx context.Context, hooks Hooks) error
}

// Notifier reports process state to systemd. sdnotify.Notifier implements it; a no-op without a socket.
type Notifier interface {
	Ready() error
	Stopping() error
	Watchdog() error
}

// ConfigError marks a failure that a restart cannot fix: an invalid configuration or unmet
// source requirements. The process exits with the EX_CONFIG code so systemd does not restart it.
type ConfigError struct{ Err error }

func (e *ConfigError) Error() string { return e.Err.Error() }
func (e *ConfigError) Unwrap() error { return e.Err }

// StateSink is told about the process state, for health endpoints and metrics. Optional.
type StateSink interface {
	Ready()
	Stopping()
	Progress()
}

// Deps are the collaborators of Run.
type Deps struct {
	Logger   *slog.Logger
	Notifier Notifier
	Runner   Runner
	// State, if set, follows the same events as the notifier.
	State StateSink

	// WatchdogTimeout is systemd's WatchdogSec; zero disables the watchdog.
	WatchdogTimeout time.Duration
	// Now and NewTicker are injectable for deterministic tests; they default to the time package.
	Now       func() time.Time
	NewTicker func(d time.Duration) (ticks <-chan time.Time, stop func())
}

// Run executes the engine until ctx is cancelled or the engine fails.
func Run(ctx context.Context, d Deps) error {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.NewTicker == nil {
		d.NewTicker = func(interval time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(interval)
			return t.C, t.Stop
		}
	}

	var lastProgress atomic.Int64
	touch := func() {
		lastProgress.Store(d.Now().UnixNano())
		if d.State != nil {
			d.State.Progress()
		}
	}
	touch()

	// The supervision goroutines are owned by Run and stopped through done before it returns.
	done := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(done)
		wg.Wait()
	}()

	// STOPPING=1 goes out as soon as shutdown begins, and is guaranteed before Run returns
	// even when the engine finishes faster than the goroutine notices the cancellation.
	var stopOnce sync.Once
	announceStop := func() {
		stopOnce.Do(func() {
			d.Logger.Info("shutting down")
			if d.State != nil {
				d.State.Stopping()
			}
			if err := d.Notifier.Stopping(); err != nil {
				d.Logger.Warn("notify STOPPING failed", "error", err)
			}
		})
	}
	wg.Go(func() {
		select {
		case <-ctx.Done():
			announceStop()
		case <-done:
		}
	})

	if d.WatchdogTimeout > 0 {
		wg.Go(func() { superviseWatchdog(d, &lastProgress, done) })
	}

	var readyOnce sync.Once
	hooks := Hooks{
		Ready: func() {
			readyOnce.Do(func() {
				d.Logger.Info("replication started")
				if d.State != nil {
					d.State.Ready()
				}
				if err := d.Notifier.Ready(); err != nil {
					d.Logger.Warn("notify READY failed", "error", err)
				}
			})
		},
		Progress: touch,
	}

	err := d.Runner.Run(ctx, hooks)
	if ctx.Err() != nil {
		announceStop()
	}
	if err != nil && ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return nil // cancellation surfaced as an error is still a graceful stop
	}
	return err
}

// superviseWatchdog pings systemd twice per timeout, but only while the engine reports progress
// more recently than half a timeout ago: a stalled engine stops the pings and gets killed.
func superviseWatchdog(d Deps, lastProgress *atomic.Int64, done <-chan struct{}) {
	interval := d.WatchdogTimeout / 2
	ticks, stop := d.NewTicker(interval)
	defer stop()

	for {
		select {
		case <-done:
			return
		case <-ticks:
			idle := d.Now().Sub(time.Unix(0, lastProgress.Load()))
			if idle > interval {
				d.Logger.Warn("watchdog ping skipped: no progress", "idle", idle.Round(time.Millisecond))
				continue
			}
			if err := d.Notifier.Watchdog(); err != nil {
				d.Logger.Warn("watchdog ping failed", "error", err)
			}
		}
	}
}
