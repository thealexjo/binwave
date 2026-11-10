package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/failure"
)

// pipelineStub is a "pipeline" that runs until its context ends, then stops gracefully.
type pipelineStub struct {
	runs    atomic.Int32
	running chan struct{} // receives once per run, when it has started
	failOn  int32         // the run number that fails when stopped, 0 for none
}

func newStub() *pipelineStub { return &pipelineStub{running: make(chan struct{}, 8)} }

func (s *pipelineStub) run(ctx context.Context, _ control.ResumeRequest) error {
	n := s.runs.Add(1)
	s.running <- struct{}{}
	<-ctx.Done()
	if n == s.failOn {
		return errors.New("final flush failed")
	}
	return nil
}

func (s *pipelineStub) waitRunning(t *testing.T) {
	t.Helper()
	select {
	case <-s.running:
	case <-time.After(5 * time.Second):
		t.Fatal("the pipeline did not start")
	}
}

func testReplicator(startPaused bool) (*replicator, *atomic.Int32) {
	var waits atomic.Int32
	r := newReplicator(startPaused, time.Now, func() { waits.Add(1) })
	r.tick = 10 * time.Millisecond
	return r, &waits
}

func runLoop(ctx context.Context, r *replicator, s *pipelineStub) chan error {
	done := make(chan error, 1)
	go func() { done <- r.loop(ctx, s.run) }()
	return done
}

func TestPauseStopsThePipelineAndResumeStartsItAgain(t *testing.T) {
	r, waits := testReplicator(false)
	s := newStub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runLoop(ctx, r, s)
	s.waitRunning(t)

	if err := r.Pause(ctx); err != nil {
		t.Fatalf("Pause() = %v", err)
	}
	if st := r.State(); !st.Paused || st.Since.IsZero() {
		t.Errorf("state after the pause = %+v", st)
	}
	select {
	case err := <-done:
		t.Fatalf("the loop ended (%v): a pause must leave the service running", err)
	case <-time.After(100 * time.Millisecond):
	}
	if waits.Load() == 0 {
		t.Error("a paused service reported no progress, so a watchdog would take it for hung")
	}
	if s.runs.Load() != 1 {
		t.Errorf("the pipeline ran %d times while paused, want 1", s.runs.Load())
	}

	r.Resume(control.ResumeRequest{})
	s.waitRunning(t)
	if r.State().Paused {
		t.Error("still paused after the resume")
	}
	if s.runs.Load() != 2 {
		t.Errorf("the pipeline ran %d times, want a second run after the resume", s.runs.Load())
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("loop() = %v after a stop", err)
	}
}

func TestPausingTwiceAndResumingARunningServiceAreHarmless(t *testing.T) {
	r, _ := testReplicator(false)
	s := newStub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runLoop(ctx, r, s)
	s.waitRunning(t)

	r.Resume(control.ResumeRequest{}) // not paused: nothing happens
	if err := r.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Pause(ctx); err != nil {
		t.Errorf("the second Pause() = %v, want it to do nothing", err)
	}
	if s.runs.Load() != 1 {
		t.Errorf("runs = %d", s.runs.Load())
	}
}

func TestAServiceThatStartsPausedRunsNothingUntilResumed(t *testing.T) {
	r, waits := testReplicator(true)
	s := newStub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runLoop(ctx, r, s)

	time.Sleep(100 * time.Millisecond)
	if s.runs.Load() != 0 {
		t.Fatal("the pipeline ran although the service starts paused")
	}
	if !r.State().Paused || waits.Load() == 0 {
		t.Errorf("state %+v, progress reports %d", r.State(), waits.Load())
	}

	r.Resume(control.ResumeRequest{})
	s.waitRunning(t)
	cancel()
	if err := <-done; err != nil {
		t.Error(err)
	}
}

func TestStoppingAPausedServiceEndsTheLoopCleanly(t *testing.T) {
	r, _ := testReplicator(true)
	s := newStub()
	ctx, cancel := context.WithCancel(context.Background())
	done := runLoop(ctx, r, s)

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("loop() = %v, want a clean end: SIGTERM of a paused service is a graceful stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a paused service did not stop")
	}
}

func TestAPipelineThatCannotStopCleanlyFailsTheService(t *testing.T) {
	r, _ := testReplicator(false)
	s := newStub()
	s.failOn = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runLoop(ctx, r, s)
	s.waitRunning(t)

	pauseErr := r.Pause(ctx)

	if !errors.Is(pauseErr, errServiceStopped) {
		t.Errorf("Pause() = %v, want the report that the service stopped", pauseErr)
	}
	if err := <-done; err == nil || err.Error() != "final flush failed" {
		t.Errorf("loop() = %v, want the failure of the pipeline: what could not be written must not be forgotten", err)
	}
}

func TestPauseBeforeThePipelineRunsTakesEffectAtOnce(t *testing.T) {
	r, _ := testReplicator(false)
	s := newStub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Requested before the loop started: the first run is never made.
	r.mu.Lock()
	r.pauseRequested = true
	r.mu.Unlock()
	runLoop(ctx, r, s)

	waitFor(t, "the pause to take effect", func() bool { return r.State().Paused })
	if s.runs.Load() != 0 {
		t.Errorf("the pipeline ran %d times although a pause was requested first", s.runs.Load())
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// failingStub fails its first run with a failure of the data, and keeps what it was asked at every run.
type failingStub struct {
	runs atomic.Int32
	reqs chan control.ResumeRequest
	err  error
}

func (s *failingStub) run(ctx context.Context, req control.ResumeRequest) error {
	n := s.runs.Add(1)
	s.reqs <- req
	if n == 1 {
		return s.err
	}
	<-ctx.Done()
	return nil
}

var errData = errors.New("a transaction the target cannot take")

// failureReplicator returns a replicator and the number of failures reported to onFailed so far.
func failureReplicator() (*replicator, func() int) {
	r, _ := testReplicator(false)
	var mu sync.Mutex
	var seen []failure.Failure
	r.classify = func(err error) (failure.Failure, bool) {
		if errors.Is(err, errData) {
			return failure.Failure{GTID: "u:5", Kind: failure.KindDDL, Message: err.Error()}, true
		}
		return failure.Failure{}, false
	}
	r.onFailed = func(f failure.Failure) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, f)
	}
	return r, func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(seen)
	}
}

// TestFailureStopsThePipelineAndTheServiceWaitsForTheOperator: the process does not exit; it waits
// like a pause, with the failure in its state, and a resume runs the pipeline again with what the
// operator asked for.
func TestFailureStopsThePipelineAndTheServiceWaitsForTheOperator(t *testing.T) {
	r, seen := failureReplicator()
	s := &failingStub{reqs: make(chan control.ResumeRequest, 8), err: errData}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.loop(ctx, s.run) }()
	<-s.reqs // the run that fails

	waitFor(t, "the failure reported", func() bool { st := r.State(); return st.Paused && st.Failure != nil && seen() > 0 })
	if st := r.State(); st.Failure.GTID != "u:5" || seen() != 1 {
		t.Errorf("state %+v, reported %d failures", st, seen())
	}
	select {
	case err := <-done:
		t.Fatalf("the loop ended with %v: a failure must not end the process", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := r.Pause(ctx); err != nil {
		t.Errorf("Pause() of a failed service = %v, want it to do nothing", err)
	}

	r.Resume(control.ResumeRequest{SkipTransactions: 1, GTIDNext: "u:5"})
	got := <-s.reqs
	if got.SkipTransactions != 1 || got.GTIDNext != "u:5" {
		t.Errorf("the pipeline was started with %+v", got)
	}
	if st := r.State(); st.Paused || st.Failure != nil {
		t.Errorf("state after the resume: %+v", st)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("loop() = %v", err)
	}
}

func TestOtherErrorsStillEndTheRun(t *testing.T) {
	r, seen := failureReplicator()
	boom := errors.New("ClickHouse is unreachable")
	s := &failingStub{reqs: make(chan control.ResumeRequest, 8), err: boom}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := r.loop(ctx, s.run)

	if !errors.Is(err, boom) || seen() != 0 {
		t.Errorf("loop() = %v, failures %d: only failures of the data are held for the operator", err, seen())
	}
}

// TestWorkWhilePausedHoldsAResumeBack: what is saved while paused is not overwritten by a pipeline
// that a resume starts in the middle of it.
func TestWorkWhilePausedHoldsAResumeBack(t *testing.T) {
	r, _ := testReplicator(true)
	s := newStub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runLoop(ctx, r, s)

	var runsDuring int32
	ran, err := r.whilePaused(func() error {
		go r.Resume(control.ResumeRequest{})
		time.Sleep(100 * time.Millisecond)
		runsDuring = s.runs.Load()
		return nil
	})
	if !ran || err != nil {
		t.Fatalf("whilePaused() = %v, %v, want the work done", ran, err)
	}
	if runsDuring != 0 {
		t.Errorf("the pipeline ran %d times during the work, want the resume to wait for it", runsDuring)
	}
	s.waitRunning(t)

	ran, err = r.whilePaused(func() error {
		t.Error("the work was done while replication runs")
		return nil
	})
	if ran || err != nil {
		t.Errorf("whilePaused() while running = %v, %v, want nothing done", ran, err)
	}
}
