package engine

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/pipeline"
)

// backendWith returns the control backend of an engine whose replicator is in the state given.
func backendWith(t *testing.T, startPaused bool, f *failure.Failure) (*controlBackend, *replicator) {
	t.Helper()
	e := New(&config.Config{}, slog.New(slog.DiscardHandler))
	r, _ := testReplicator(startPaused)
	if f != nil {
		r.enterFailed(*f)
	}
	e.setActive(&activeRun{repl: r})
	return &controlBackend{e: e}, r
}

// TestSnapshotRateValidatesAndRefusesWhileNotRunning: the control backend rejects a negative rate
// before it ever reaches the pipeline, and surfaces the pipeline's own refusal while the pipeline is
// not actually running (SetLoadRate needs Run to have set up the budget it changes).
func TestSnapshotRateValidatesAndRefusesWhileNotRunning(t *testing.T) {
	e := New(&config.Config{}, slog.New(slog.DiscardHandler))
	p, err := pipeline.New(&config.Config{}, pipeline.Deps{})
	if err != nil {
		t.Fatal(err)
	}
	e.setActive(&activeRun{p: p})
	b := &controlBackend{e: e}

	if err := b.SnapshotRate(context.Background(), -1); err == nil {
		t.Error("a negative rate was accepted")
	}
	if err := b.SnapshotRate(context.Background(), 100); !errors.Is(err, pipeline.ErrNotRunning) {
		t.Errorf("SnapshotRate() before Run() = %v, want pipeline.ErrNotRunning", err)
	}
}

func TestResumeThatSkipsIsRefusedWhileReplicationRuns(t *testing.T) {
	b, _ := backendWith(t, false, nil)

	for _, req := range []control.ResumeRequest{{SkipTransactions: 1}, {GTIDNext: "6d4d8f60-1111-2222-3333-444455556666:5"}} {
		err := b.Resume(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "replication is running") {
			t.Errorf("Resume(%+v) = %v, want a refusal: nobody has looked at what would be skipped", req, err)
		}
	}
}

func TestResumeThatSkipsAFailureOfAWriteIsRefused(t *testing.T) {
	b, _ := backendWith(t, false, &failure.Failure{Kind: failure.KindWrite, Table: "shop.orders"})

	err := b.Resume(context.Background(), control.ResumeRequest{SkipTransactions: 1})

	if err == nil || !strings.Contains(err.Error(), "nothing to skip") {
		t.Errorf("Resume() = %v", err)
	}
	if err := b.Resume(context.Background(), control.ResumeRequest{}); err != nil {
		t.Errorf("a plain resume of the same failure = %v, want it accepted", err)
	}
}

func TestResumeChecksItsArguments(t *testing.T) {
	b, _ := backendWith(t, true, nil)

	if err := b.Resume(context.Background(), control.ResumeRequest{SkipTransactions: -1}); err == nil {
		t.Error("a negative count was accepted")
	}
	if err := b.Resume(context.Background(), control.ResumeRequest{GTIDNext: "garbage"}); err == nil || !strings.Contains(err.Error(), "--gtid-next") {
		t.Errorf("a garbage GTID: %v", err)
	}
}

func TestResumeOfAFailedTransactionPassesTheSkipOn(t *testing.T) {
	b, r := backendWith(t, false, &failure.Failure{Kind: failure.KindDDL, GTID: "6d4d8f60-1111-2222-3333-444455556666:5"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan control.ResumeRequest, 1)
	done := make(chan error, 1)
	go func() {
		done <- r.loop(ctx, func(rctx context.Context, req control.ResumeRequest) error {
			got <- req
			<-rctx.Done()
			return nil
		})
	}()

	if err := b.Resume(ctx, control.ResumeRequest{SkipTransactions: 1, GTIDNext: "6d4d8f60-1111-2222-3333-444455556666:5"}); err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-got:
		if req.SkipTransactions != 1 || req.GTIDNext == "" {
			t.Errorf("the pipeline was started with %+v", req)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pipeline did not start")
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("loop() = %v", err)
	}
}

// TestTheDefaultSocketWithoutItsDirectoryLeavesTheChannelOff: outside systemd and Docker there is no
// /run/binwave, and the service runs without the channel; a socket that is set must be served.
func TestTheDefaultSocketWithoutItsDirectoryLeavesTheChannelOff(t *testing.T) {
	if isDir(filepath.Dir(config.DefaultControlSocket)) {
		t.Skipf("%s exists on this host", filepath.Dir(config.DefaultControlSocket))
	}
	var logs strings.Builder
	e := New(&config.Config{Control: config.Control{Socket: config.DefaultControlSocket}}, slog.New(slog.NewTextHandler(&logs, nil)))
	stop, err := e.serveControl()
	if err != nil {
		t.Fatalf("serveControl() = %v, want the channel off", err)
	}
	stop()
	if !strings.Contains(logs.String(), "control channel is off") {
		t.Errorf("no warning that the channel is off:\n%s", logs.String())
	}

	dir, err := os.MkdirTemp("/tmp", "bwe") // a Unix socket path is short
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	missing := filepath.Join(dir, "missing", "c.sock")
	e = New(&config.Config{Control: config.Control{Socket: missing}}, slog.New(slog.DiscardHandler))
	var cfgErr *app.ConfigError
	if _, err := e.serveControl(); !errors.As(err, &cfgErr) {
		t.Errorf("serveControl() = %v, want a configuration error: a restart does not make the directory", err)
	}

	served := filepath.Join(dir, "c.sock")
	e = New(&config.Config{Control: config.Control{Socket: served}}, slog.New(slog.DiscardHandler))
	stop, err = e.serveControl()
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if _, err := (control.Client{Socket: served}).Quarantined(context.Background()); errors.Is(err, control.ErrNotRunning) {
		t.Errorf("the socket that is set is not served: %v", err)
	}
}
