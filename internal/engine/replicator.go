package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/failure"
)

// errPaused is the cause with which a running pipeline is asked to stop for a pause.
var errPaused = errors.New("replication paused")

// errServiceStopped is returned to a caller that waits for a pause the service ends before.
var errServiceStopped = errors.New("the service stopped")

// errPauseCancelled is returned to a caller that waits for a pause a resume cancels before it takes effect.
var errPauseCancelled = errors.New("the pause was cancelled by a resume")

// pausedTick is how often a paused service reports progress: it is alive and waiting, which the
// watchdog and /healthz must be able to tell from a hang.
const pausedTick = time.Second

// ReplicationState is what the replicator tells about itself.
type ReplicationState struct {
	// Paused is true while nothing runs: after a pause, and after a failure.
	Paused bool
	Since  time.Time
	// Failure is set when the pause is a failure that the operator has to look at.
	Failure *failure.Failure
}

// pauseWait is one pause: done is closed when it takes effect, and failed says it is a failure.
type pauseWait struct {
	done   chan struct{}
	failed bool
}

func newPauseWait() *pauseWait { return &pauseWait{done: make(chan struct{})} }

// replicator runs the replication in a loop that can be paused, like MySQL's STOP REPLICA: a
// pause stops the pipeline the way a graceful shutdown does (buffers written, position saved,
// stream closed) and the process goes on, waiting; a resume starts the pipeline again from the
// saved position. Nothing is lost while paused, because the source keeps its binary logs.
//
// A failure of the data path (see pipeline.FailedError) does not end the process either: the
// replicator stops the pipeline, records the failure and waits like a pause does, until the
// operator resumes, retrying the transaction or skipping it.
type replicator struct {
	now    func() time.Time
	onWait func() // called while paused, about every pausedTick
	tick   time.Duration
	// classify tells a failure of the data path from an error that ends the run.
	classify func(error) (failure.Failure, bool)
	// onFailed is called when a failure has stopped replication.
	onFailed func(failure.Failure)

	mu             sync.Mutex
	paused         bool
	pauseRequested bool
	since          time.Time
	failure        *failure.Failure
	pending        control.ResumeRequest   // what the resume that ends the wait asked for
	cancelRun      context.CancelCauseFunc // of the pipeline that runs, nil between runs
	pause          *pauseWait              // the pause to come, or the one in effect
	pauseCancelled chan struct{}           // closed when a resume cancels the pause asked for
	stopped        chan struct{}           // closed when the loop ends
	resume         chan struct{}           // wakes the loop up; the state says whether to resume
}

func newReplicator(startPaused bool, now func() time.Time, onWait func()) *replicator {
	r := &replicator{
		now: now, onWait: onWait, tick: pausedTick,
		pause: newPauseWait(), stopped: make(chan struct{}), resume: make(chan struct{}, 1),
	}
	if startPaused {
		r.paused, r.since = true, now()
		close(r.pause.done)
	}
	return r
}

// State returns whether replication is paused.
func (r *replicator) State() ReplicationState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ReplicationState{Paused: r.paused, Since: r.since, Failure: r.failure}
}

// whilePaused runs fn if replication is paused, and keeps it paused until fn returns: a resume waits.
// fn must not call the replicator.
func (r *replicator) whilePaused(fn func() error) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.paused {
		return false, nil
	}
	return true, fn()
}

// Pause stops the pipeline and waits until it has stopped. It reports whether this call paused
// replication: not when it was paused or failed already, nor when a failure stopped it first. It
// fails when the pipeline cannot stop cleanly, which stops the service as any failure of the
// pipeline does, and when a resume cancels the pause before it takes effect.
func (r *replicator) Pause(ctx context.Context) (bool, error) {
	r.mu.Lock()
	if r.paused {
		r.mu.Unlock()
		return false, nil
	}
	r.pauseRequested = true
	if r.pauseCancelled == nil {
		r.pauseCancelled = make(chan struct{})
	}
	cancel, wait, cancelled := r.cancelRun, r.pause, r.pauseCancelled
	r.mu.Unlock()

	if cancel != nil {
		cancel(errPaused)
	}
	select {
	case <-wait.done:
		return !wait.failed, nil
	case <-cancelled:
		return false, errPauseCancelled
	case <-r.stopped:
		return false, errServiceStopped
	case <-ctx.Done():
		return false, fmt.Errorf("wait for the pause: %w", ctx.Err())
	}
}

// Resume starts replication again. It does nothing when replication is not paused, except cancel a
// pause that has not taken effect yet. The request says which transactions to drop; the pipeline is
// told when it starts. The state changes here, under the lock, so that a second resume finds
// replication running and a pause after it waits for its own resume.
func (r *replicator) Resume(req control.ResumeRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.paused {
		if r.pauseRequested {
			r.pauseRequested = false
			close(r.pauseCancelled)
			r.pauseCancelled = nil
		}
		return
	}
	r.pending = req
	r.paused, r.pauseRequested = false, false
	r.failure = nil
	r.since = time.Time{}
	r.pause = newPauseWait()
	select {
	case r.resume <- struct{}{}:
	default:
	}
}

// loop runs the pipeline with run until ctx ends or run fails. run must stop when its context
// ends, and return nil after a graceful stop. It is called again after every resume, with what the
// resume asked for.
func (r *replicator) loop(ctx context.Context, run func(ctx context.Context, req control.ResumeRequest) error) error {
	defer close(r.stopped)

	for {
		r.mu.Lock()
		if r.pauseRequested && !r.paused {
			r.enterPause()
		}
		paused := r.paused
		r.mu.Unlock()

		if paused && !r.waitForResume(ctx) {
			return nil // asked to stop while paused: nothing is running, nothing to flush
		}
		// Taken at every run: a resume may come before the loop waits for it.
		r.mu.Lock()
		req := r.pending
		r.pending = control.ResumeRequest{}
		r.mu.Unlock()

		rctx, cancel := context.WithCancelCause(ctx)
		r.mu.Lock()
		r.cancelRun = cancel
		if r.pauseRequested {
			cancel(errPaused)
		}
		r.mu.Unlock()

		err := run(rctx, req)

		r.mu.Lock()
		r.cancelRun = nil
		pausing := errors.Is(context.Cause(rctx), errPaused) && ctx.Err() == nil
		r.mu.Unlock()
		cancel(nil)

		if f, failed := r.failureOf(err); failed && ctx.Err() == nil {
			r.enterFailed(f)
			continue
		}
		if !pausing || err != nil {
			return err
		}
	}
}

func (r *replicator) failureOf(err error) (failure.Failure, bool) {
	if err == nil || r.classify == nil {
		return failure.Failure{}, false
	}
	return r.classify(err)
}

// enterFailed records that a failure has stopped replication: it waits like a pause.
func (r *replicator) enterFailed(f failure.Failure) {
	r.mu.Lock()
	r.failure = &f
	if !r.paused {
		r.pause.failed = true
		r.enterPause()
	}
	r.mu.Unlock()
	if r.onFailed != nil {
		r.onFailed(f)
	}
}

// enterPause records that the pause has taken effect. mu is held.
func (r *replicator) enterPause() {
	r.paused, r.since = true, r.now()
	r.pauseRequested, r.pauseCancelled = false, nil
	close(r.pause.done)
}

// waitForResume blocks while paused, reporting progress. It returns false when ctx ends first.
func (r *replicator) waitForResume(ctx context.Context) bool {
	t := time.NewTicker(r.tick)
	defer t.Stop()
	for {
		if r.onWait != nil {
			r.onWait()
		}
		select {
		case <-ctx.Done():
			return false
		case <-r.resume:
			r.mu.Lock()
			resumed := !r.paused // a signal left by a resume that found replication running already is not one
			r.mu.Unlock()
			if resumed {
				return true
			}
		case <-t.C:
		}
	}
}
