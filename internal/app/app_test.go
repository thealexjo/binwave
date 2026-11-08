package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// recorder is a Notifier that records calls and signals each one.
type recorder struct {
	mu    sync.Mutex
	calls []string
	event chan string
}

func newRecorder() *recorder { return &recorder{event: make(chan string, 32)} }

func (r *recorder) record(name string) error {
	r.mu.Lock()
	r.calls = append(r.calls, name)
	r.mu.Unlock()
	r.event <- name
	return nil
}

func (r *recorder) Ready() error    { return r.record("ready") }
func (r *recorder) Stopping() error { return r.record("stopping") }
func (r *recorder) Watchdog() error { return r.record("watchdog") }

func (r *recorder) count(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.calls {
		if c == name {
			n++
		}
	}
	return n
}

func (r *recorder) waitFor(t *testing.T, name string) {
	t.Helper()
	select {
	case got := <-r.event:
		if got != name {
			t.Fatalf("got notification %q, want %q", got, name)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %q", name)
	}
}

// runnerFunc adapts a function to Runner.
type runnerFunc func(ctx context.Context, h Hooks) error

func (f runnerFunc) Run(ctx context.Context, h Hooks) error { return f(ctx, h) }

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestReadyIsSentOnceWhenEngineReportsIt(t *testing.T) {
	n := newRecorder()
	err := Run(context.Background(), Deps{
		Logger: quietLogger(), Notifier: n,
		Runner: runnerFunc(func(_ context.Context, h Hooks) error {
			h.Ready()
			h.Ready()
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if got := n.count("ready"); got != 1 {
		t.Errorf("READY sent %d times, want 1", got)
	}
}

func TestReadyIsNotSentIfEngineNeverStarts(t *testing.T) {
	n := newRecorder()
	want := errors.New("source requirements not met")
	err := Run(context.Background(), Deps{
		Logger: quietLogger(), Notifier: n,
		Runner: runnerFunc(func(context.Context, Hooks) error { return want }),
	})
	if !errors.Is(err, want) {
		t.Fatalf("Run() = %v, want %v", err, want)
	}
	if n.count("ready") != 0 {
		t.Error("READY sent although the engine failed before starting")
	}
}

func TestShutdownSendsStoppingAndReturnsNil(t *testing.T) {
	n := newRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, Deps{
			Logger: quietLogger(), Notifier: n,
			Runner: runnerFunc(func(ctx context.Context, h Hooks) error {
				h.Ready()
				close(started)
				<-ctx.Done()
				return nil
			}),
		})
	}()

	<-started
	n.waitFor(t, "ready")
	cancel()
	n.waitFor(t, "stopping")

	if err := <-result; err != nil {
		t.Errorf("Run() = %v, want nil after a graceful stop", err)
	}
}

func TestCancellationSurfacedAsErrorIsGraceful(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := Run(ctx, Deps{
		Logger: quietLogger(), Notifier: newRecorder(),
		Runner: runnerFunc(func(ctx context.Context, _ Hooks) error {
			cancel()
			return ctx.Err()
		}),
	})
	if err != nil {
		t.Errorf("Run() = %v, want nil", err)
	}
}

func TestCancellationErrorWithoutCancelledContextIsAFailure(t *testing.T) {
	err := Run(context.Background(), Deps{
		Logger: quietLogger(), Notifier: newRecorder(),
		Runner: runnerFunc(func(context.Context, Hooks) error { return context.Canceled }),
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run() = %v, want the engine error", err)
	}
}

func TestConfigErrorIsDetectable(t *testing.T) {
	inner := errors.New("binlog_format must be ROW")
	var err error = &ConfigError{Err: inner}

	var ce *ConfigError
	if !errors.As(err, &ce) || !errors.Is(err, inner) {
		t.Error("ConfigError must be matchable and unwrap to its cause")
	}
	if err.Error() != inner.Error() {
		t.Errorf("Error() = %q", err.Error())
	}
}

// fakeClock and manualTicker make the watchdog deterministic.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// logSignal reports every log message on a channel.
type logSignal struct{ msgs chan string }

func (h logSignal) Enabled(context.Context, slog.Level) bool { return true }
func (h logSignal) Handle(_ context.Context, r slog.Record) error {
	h.msgs <- r.Message
	return nil
}
func (h logSignal) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h logSignal) WithGroup(string) slog.Handler      { return h }

func TestWatchdogPingsOnlyWhileEngineMakesProgress(t *testing.T) {
	n := newRecorder()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	ticks := make(chan time.Time)
	logs := logSignal{msgs: make(chan string, 64)}

	var hooks Hooks
	hooksReady := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, Deps{
			Logger: slog.New(logs), Notifier: n,
			WatchdogTimeout: 60 * time.Second, // ping interval 30s
			Now:             clock.Now,
			NewTicker: func(d time.Duration) (<-chan time.Time, func()) {
				if d != 30*time.Second {
					t.Errorf("ticker interval = %v, want 30s", d)
				}
				return ticks, func() {}
			},
			Runner: runnerFunc(func(ctx context.Context, h Hooks) error {
				hooks = h
				close(hooksReady)
				<-ctx.Done()
				return nil
			}),
		})
	}()
	<-hooksReady

	// Fresh progress: the ping goes out.
	clock.Advance(10 * time.Second)
	ticks <- clock.Now()
	n.waitFor(t, "watchdog")

	// The engine stalls for longer than one interval: the ping is skipped.
	clock.Advance(40 * time.Second)
	ticks <- clock.Now()
	waitForLog(t, logs, "watchdog ping skipped: no progress")
	if got := n.count("watchdog"); got != 1 {
		t.Fatalf("watchdog pings = %d after a stall, want 1", got)
	}

	// The engine recovers: pings resume.
	hooks.Progress()
	ticks <- clock.Now()
	n.waitFor(t, "watchdog")

	cancel()
	if err := <-result; err != nil {
		t.Errorf("Run() = %v", err)
	}
}

func waitForLog(t *testing.T, logs logSignal, want string) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case msg := <-logs.msgs:
			if msg == want {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for log %q", want)
		}
	}
}

func TestWatchdogDisabledStartsNoTicker(t *testing.T) {
	err := Run(context.Background(), Deps{
		Logger: quietLogger(), Notifier: newRecorder(),
		NewTicker: func(time.Duration) (<-chan time.Time, func()) {
			t.Error("ticker created although the watchdog is disabled")
			return nil, func() {}
		},
		Runner: runnerFunc(func(context.Context, Hooks) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// stateRecorder is a StateSink that records the order of what it was told.
type stateRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (s *stateRecorder) add(c string) { s.mu.Lock(); s.calls = append(s.calls, c); s.mu.Unlock() }
func (s *stateRecorder) Ready()       { s.add("ready") }
func (s *stateRecorder) Stopping()    { s.add("stopping") }
func (s *stateRecorder) Progress()    { s.add("progress") }

func (s *stateRecorder) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func TestStateSinkFollowsReadyProgressAndStopping(t *testing.T) {
	sink := &stateRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	err := Run(ctx, Deps{
		Logger: quietLogger(), Notifier: newRecorder(), State: sink,
		Runner: runnerFunc(func(ctx context.Context, h Hooks) error {
			h.Ready()
			h.Ready() // only once
			h.Progress()
			cancel()
			<-ctx.Done()
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	got := sink.seen()
	count := func(name string) (n int) {
		for _, c := range got {
			if c == name {
				n++
			}
		}
		return n
	}
	if count("ready") != 1 || count("stopping") != 1 || count("progress") < 2 {
		t.Errorf("the sink was told %v, want one ready, one stopping and progress (the initial one and the engine's)", got)
	}
	if got[len(got)-1] != "stopping" {
		t.Errorf("the last thing the sink hears is %q, want stopping", got[len(got)-1])
	}
}
