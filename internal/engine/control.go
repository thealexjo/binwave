package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/gtid"
	"github.com/thealexjo/binwave/internal/observability"
)

// controlBackend offers the running engine to the control channel.
type controlBackend struct{ e *Engine }

// run returns what the commands act on, or ErrStarting before it exists.
func (b *controlBackend) run() (*activeRun, error) {
	a := b.e.current()
	if a == nil {
		return nil, control.ErrStarting
	}
	return a, nil
}

func (b *controlBackend) Pause(ctx context.Context) error {
	a, err := b.run()
	if err != nil {
		return err
	}
	if err := a.repl.Pause(ctx); err != nil {
		return fmt.Errorf("pause replication: %w", err)
	}
	if b.e.hub != nil {
		b.e.hub.Paused()
	}
	b.e.log.Info("replication paused")
	return nil
}

func (b *controlBackend) Resume(_ context.Context, req control.ResumeRequest) error {
	a, err := b.run()
	if err != nil {
		return err
	}
	state := a.repl.State()
	if req.SkipTransactions < 0 {
		return errors.New("--skip-transactions must not be negative")
	}
	if req.GTIDNext != "" {
		if _, err := gtid.Parse(req.GTIDNext); err != nil {
			return fmt.Errorf("--gtid-next: %w", err)
		}
	}
	if req.Skips() {
		// Like MySQL, which skips only while the replica is stopped: dropping a transaction that
		// nobody has looked at would lose data quietly.
		if !state.Paused {
			return errors.New("replication is running: transactions are skipped only after it has failed or been paused, " +
				"when `binwave replication status` shows what is being skipped")
		}
		if state.Failure != nil && !state.Failure.Skippable() {
			return fmt.Errorf("the failure is at the write of buffered rows of %s, which belong to no single transaction, so there is nothing to skip: "+
				"fix ClickHouse and resume without --skip-transactions or --gtid-next", state.Failure.Table)
		}
	}
	a.repl.Resume(req)
	b.e.log.Info("replication resume requested by the operator", "skip_transactions", req.SkipTransactions, "gtid_next", req.GTIDNext)
	return nil
}

func (b *controlBackend) Status() (observability.Status, error) {
	a, err := b.run()
	if err != nil {
		if b.e.hub != nil {
			return b.e.hub.Status(), nil // still starting: the hub says so
		}
		return observability.Status{}, err
	}
	if b.e.hub != nil {
		return b.e.hub.Status(), nil
	}
	st := observability.Status{State: observability.StateReady, Quarantined: a.p.Quarantined()}
	if rs := a.repl.State(); rs.Paused {
		st.State = observability.StatePaused
		since := rs.Since
		st.PausedSince = &since
		if rs.Failure != nil {
			st.State = observability.StateFailed
			st.Failure = rs.Failure
		}
	}
	return st, nil
}

func (b *controlBackend) Quarantined(context.Context) ([]checkpoint.Quarantined, error) {
	a, err := b.run()
	if err != nil {
		return nil, err
	}
	return a.p.Quarantined(), nil
}

func (b *controlBackend) Release(ctx context.Context, tables []string) (released, unknown []string, err error) {
	a, err := b.run()
	if err != nil {
		return nil, nil, err
	}
	// The fence: what the source has executed now. Changes of the released tables up to it stay
	// dropped, so nothing from the time before the cause was fixed is replayed.
	fence, err := a.src.ExecutedGTIDSet(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("read the position of the source for the fence: %w", err)
	}
	released, unknown = a.p.Release(tables, fence)
	// A running pipeline saves the change with its next position. A paused one has no next
	// position, so the list is written now, and a resume waits until it is.
	if len(released) > 0 {
		if _, err := a.repl.whilePaused(func() error { return a.p.SaveQuarantine(ctx) }); err != nil {
			return nil, nil, err
		}
	}
	return released, unknown, nil
}

func (b *controlBackend) Resync(ctx context.Context, tables []string) ([]control.ResyncResult, error) {
	a, err := b.run()
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, errors.New("name the tables to resynchronize, as database.table")
	}
	results := make([]control.ResyncResult, 0, len(tables))
	for _, t := range tables {
		res := control.ResyncResult{Table: t}
		if err := a.p.StartResync(ctx, t); err != nil {
			res.Error = err.Error()
			b.e.log.Warn("a resync was asked for and cannot begin", "table", t, "error", err)
		}
		results = append(results, res)
	}
	return results, nil
}

func (b *controlBackend) SnapshotRate(_ context.Context, rowsPerSecond int) error {
	a, err := b.run()
	if err != nil {
		return err
	}
	if rowsPerSecond < 0 {
		return errors.New("rows_per_second must not be negative")
	}
	if err := a.p.SetLoadRate(rowsPerSecond); err != nil {
		return fmt.Errorf("change the initial load's rate limit: %w", err)
	}
	return nil
}

func (b *controlBackend) ChunkSize(_ context.Context, rows int) error {
	a, err := b.run()
	if err != nil {
		return err
	}
	if err := a.p.SetChunkSize(rows); err != nil {
		return fmt.Errorf("change the size of the chunks of snapshot reads: %w", err)
	}
	return nil
}
