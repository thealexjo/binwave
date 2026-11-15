package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/pipeline"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
)

// ErrServiceRunning is returned when a state change is asked for while the service seems to run:
// the service would write its own state over the change.
var ErrServiceRunning = errors.New("the service seems to be running")

// openStore opens the position store without creating anything, and returns what closes it.
func (e *Engine) openStore(ctx context.Context) (pipeline.Store, func(), error) {
	cp := e.cfg.Checkpoint
	if cp.Store == config.CheckpointFile {
		return checkpoint.NewFile(cp.FilePath), func() {}, nil
	}
	sink, err := chsink.Open(ctx, e.cfg.Target.ClickHouse, chsink.Options{Logger: e.log})
	if err != nil {
		return nil, nil, classify(fmt.Errorf("open ClickHouse: %w", err))
	}
	store := checkpoint.NewClickHouse(sink.Conn(), e.cfg.Target.ClickHouse.Database, cp.Table, nil)
	return store, func() { _ = sink.Close() }, nil
}

// Quarantined returns the tables that are set aside in the saved state. A running service is asked
// over its control socket instead (control.Client).
func (e *Engine) Quarantined(ctx context.Context) ([]checkpoint.Quarantined, error) {
	store, closeStore, err := e.openStore(ctx)
	if err != nil {
		return nil, err
	}
	defer closeStore()

	pos, found, err := store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load the saved state: %w", err)
	}
	if !found {
		return nil, nil
	}
	return checkpoint.DecodeQuarantine(pos.Quarantine)
}

// Release takes tables out of quarantine in the saved state: from then on their changes are
// replicated again. The changes made while they were quarantined are not in ClickHouse, and
// releasing does not bring them. unknown are the requested tables that were not quarantined. A
// running service does it itself, over its control socket; unless force is set, this refuses while
// the service seems to run, because the service saves its own state over it.
func (e *Engine) Release(ctx context.Context, tables []string, force bool) (released, unknown []string, err error) {
	if !force && e.serviceAnswers() {
		return nil, nil, fmt.Errorf("%w: observability.listen %s answers. Stop the service first; releasing tables in a running service would be undone by its next save (--force skips this check)",
			ErrServiceRunning, e.cfg.Observability.Listen)
	}

	store, closeStore, err := e.openStore(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer closeStore()

	pos, found, err := store.Load(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("load the saved state: %w", err)
	}
	if !found {
		return nil, tables, nil
	}
	entries, err := checkpoint.DecodeQuarantine(pos.Quarantine)
	if err != nil {
		return nil, nil, err
	}

	var wanted []checkpoint.Quarantined
	for _, en := range entries {
		if slices.Contains(tables, en.Table) && !en.Released() {
			wanted = append(wanted, en)
		}
	}
	for _, t := range tables {
		if !slices.ContainsFunc(wanted, func(en checkpoint.Quarantined) bool { return en.Table == t }) {
			unknown = append(unknown, t)
		}
	}
	if len(wanted) == 0 {
		return nil, unknown, nil
	}

	// The fence: what the source has executed now. Changes of the released tables up to it stay
	// dropped, so what happened before the cause was fixed is not replayed after the start.
	fence, err := e.sourcePosition(ctx)
	if err != nil {
		return nil, nil, err
	}
	var kept []checkpoint.Quarantined
	for _, en := range entries {
		if slices.ContainsFunc(wanted, func(w checkpoint.Quarantined) bool { return w.Table == en.Table }) {
			en.Until = fence
			released = append(released, en.Table)
		}
		kept = append(kept, en)
	}

	pos.Quarantine = checkpoint.EncodeQuarantine(kept)
	if err := store.Save(ctx, pos); err != nil {
		return nil, nil, fmt.Errorf("save the state with the released tables: %w", err)
	}
	return released, unknown, nil
}

// sourcePosition reads what the source has executed, over a short connection of its own.
func (e *Engine) sourcePosition(ctx context.Context) (string, error) {
	src := mysqlsrc.New(e.cfg.Source.MySQL, mysqlsrc.Options{}, e.log)
	if err := src.Open(ctx); err != nil {
		return "", fmt.Errorf("connect to the source to read the release fence: %w", classify(err))
	}
	defer func() { _ = src.Close() }()
	fence, err := src.ExecutedGTIDSet(ctx)
	if err != nil {
		return "", fmt.Errorf("read the release fence: %w", err)
	}
	return fence, nil
}

// serviceAnswers reports whether something listens on the observability address. That is the
// best a command can tell about a service that may run on another terminal or another host.
func (e *Engine) serviceAnswers() bool {
	host, port, err := net.SplitHostPort(e.cfg.Observability.Listen)
	if err != nil {
		return false
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(context.Background(), "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
