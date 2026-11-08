package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/observability"
)

// fakeBackend records the calls and answers from fields.
type fakeBackend struct {
	mu      sync.Mutex
	calls   []string
	err     error
	entries []checkpoint.Quarantined
}

func (f *fakeBackend) note(c string) { f.mu.Lock(); f.calls = append(f.calls, c); f.mu.Unlock() }

func (f *fakeBackend) Pause(context.Context) error { f.note("pause"); return f.err }
func (f *fakeBackend) Resume(_ context.Context, req ResumeRequest) error {
	f.note(fmt.Sprintf("resume skip=%d gtid=%s", req.SkipTransactions, req.GTIDNext))
	return f.err
}

func (f *fakeBackend) Status() (observability.Status, error) {
	return observability.Status{State: observability.StatePaused, Position: "u:1-5"}, f.err
}

func (f *fakeBackend) Quarantined(context.Context) ([]checkpoint.Quarantined, error) {
	return f.entries, f.err
}

func (f *fakeBackend) Release(_ context.Context, tables []string) (released, unknown []string, err error) {
	f.note("release")
	for _, t := range tables {
		if t == "shop.orders" {
			released = append(released, t)
		} else {
			unknown = append(unknown, t)
		}
	}
	return released, unknown, f.err
}

func (f *fakeBackend) SnapshotRate(_ context.Context, n int) error {
	f.note(fmt.Sprintf("snapshot-rate %d", n))
	return f.err
}

func (f *fakeBackend) ChunkSize(_ context.Context, rows int) error {
	f.note(fmt.Sprintf("chunk-size %d", rows))
	return f.err
}

func (f *fakeBackend) Resync(_ context.Context, tables []string) ([]ResyncResult, error) {
	f.note("resync " + strings.Join(tables, ","))
	var out []ResyncResult
	for _, t := range tables {
		r := ResyncResult{Table: t}
		if t != "shop.orders" {
			r.Error = "not replicated"
		}
		out = append(out, r)
	}
	return out, f.err
}

// shortDir is a directory with a path short enough for a Unix socket; t.TempDir is not on macOS.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "bwc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func serve(t *testing.T, b Backend) (Client, string) {
	t.Helper()
	path := filepath.Join(shortDir(t), "c.sock")
	stop, err := Serve(path, b, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	return Client{Socket: path}, path
}

func TestCommandsReachTheBackend(t *testing.T) {
	b := &fakeBackend{entries: []checkpoint.Quarantined{{Table: "shop.orders", Reason: "no key", At: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}}}
	c, _ := serve(t, b)
	ctx := context.Background()

	if err := c.Pause(ctx); err != nil {
		t.Fatalf("Pause() = %v", err)
	}
	if err := c.Resume(ctx, ResumeRequest{SkipTransactions: 2, GTIDNext: "u:7"}); err != nil {
		t.Fatalf("Resume() = %v", err)
	}
	st, err := c.Status(ctx)
	if err != nil || st.State != observability.StatePaused || st.Position != "u:1-5" {
		t.Errorf("Status() = %+v, %v", st, err)
	}
	entries, err := c.Quarantined(ctx)
	if err != nil || len(entries) != 1 || entries[0].Reason != "no key" {
		t.Errorf("Quarantined() = %+v, %v", entries, err)
	}
	released, unknown, err := c.Release(ctx, []string{"shop.orders", "shop.nope"})
	if err != nil || !reflect.DeepEqual(released, []string{"shop.orders"}) || !reflect.DeepEqual(unknown, []string{"shop.nope"}) {
		t.Errorf("Release() = %v, %v, %v", released, unknown, err)
	}
	if want := []string{"pause", "resume skip=2 gtid=u:7", "release"}; !reflect.DeepEqual(b.calls, want) {
		t.Errorf("the backend saw %v, want %v", b.calls, want)
	}
}

func TestAnEmptyQuarantineIsAnEmptyList(t *testing.T) {
	c, _ := serve(t, &fakeBackend{})
	entries, err := c.Quarantined(context.Background())
	if err != nil || entries == nil || len(entries) != 0 {
		t.Errorf("Quarantined() = %#v, %v", entries, err)
	}
}

func TestErrorsOfTheBackendReachTheClientWithTheirText(t *testing.T) {
	c, _ := serve(t, &fakeBackend{err: errors.New("the final flush failed")})
	err := c.Pause(context.Background())
	if err == nil || err.Error() != "the final flush failed" {
		t.Errorf("Pause() = %v, want the text of the backend's error", err)
	}

	starting, _ := serve(t, &fakeBackend{err: ErrStarting})
	if err := starting.Pause(context.Background()); !errors.Is(err, ErrStarting) {
		t.Errorf("Pause() = %v, want ErrStarting so that the command can say to wait", err)
	}
}

func TestNoServiceMeansErrNotRunning(t *testing.T) {
	c := Client{Socket: filepath.Join(shortDir(t), "nobody.sock")}
	if err := c.Pause(context.Background()); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Pause() = %v, want ErrNotRunning", err)
	}
}

// TestSocketIsPrivateToTheUser: nobody else on the host may pause the replication.
func TestSocketIsPrivateToTheUser(t *testing.T) {
	_, path := serve(t, &fakeBackend{})
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("the socket has mode %o, want no access for group and others", mode)
	}
}

func TestStaleSocketIsReplacedAndALiveOneIsNot(t *testing.T) {
	dir := shortDir(t)
	path := filepath.Join(dir, "c.sock")
	log := slog.New(slog.DiscardHandler)

	// A socket file left by a process that died.
	first, err := Serve(path, &fakeBackend{}, log)
	if err != nil {
		t.Fatal(err)
	}
	// Serving again while the first one lives is refused.
	if _, err := Serve(path, &fakeBackend{}, log); err == nil {
		t.Fatal("a second service took the socket of a live one")
	}
	first()
	if err := os.WriteFile(path, nil, 0o600); err != nil { // a stale file where the socket was
		t.Fatal(err)
	}
	stop, err := Serve(path, &fakeBackend{}, log)
	if err != nil {
		t.Fatalf("Serve() over a stale file = %v", err)
	}
	stop()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the socket file stays after stop: %v", err)
	}
}

func TestMalformedReleaseRequestIsRejected(t *testing.T) {
	c, _ := serve(t, &fakeBackend{})
	err := c.do(context.Background(), "POST", "/v1/tables/release", "not an object", nil)
	if err == nil {
		t.Error("a request that is not a release request was accepted")
	}
}

func TestReleaseWithoutTheAcknowledgementIsRefused(t *testing.T) {
	b := &fakeBackend{}
	c, _ := serve(t, b)

	// A client that does not acknowledge the data gap, sending the request by hand.
	err := c.do(context.Background(), "POST", "/v1/tables/release", releaseRequest{Tables: []string{"shop.orders"}}, nil)

	if err == nil || !strings.Contains(err.Error(), "--accept-data-gap") {
		t.Errorf("error = %v, want the refusal that names the flag", err)
	}
	if len(b.calls) != 0 {
		t.Errorf("the backend released tables (%v) although the gap was not acknowledged", b.calls)
	}
}

func TestResyncReachesTheBackendAndCarriesTheReasonPerTable(t *testing.T) {
	b := &fakeBackend{}
	c, _ := serve(t, b)

	results, err := c.Resync(context.Background(), []string{"shop.orders", "shop.nope"})
	if err != nil {
		t.Fatalf("Resync() = %v", err)
	}
	want := []ResyncResult{{Table: "shop.orders"}, {Table: "shop.nope", Error: "not replicated"}}
	if !reflect.DeepEqual(results, want) {
		t.Errorf("Resync() = %+v, want %+v", results, want)
	}
	if want := []string{"resync shop.orders,shop.nope"}; !reflect.DeepEqual(b.calls, want) {
		t.Errorf("the backend saw %v, want %v", b.calls, want)
	}
}

func TestSnapshotRateReachesTheBackend(t *testing.T) {
	b := &fakeBackend{}
	c, _ := serve(t, b)

	if err := c.SnapshotRate(context.Background(), 5000); err != nil {
		t.Fatalf("SnapshotRate(5000) = %v", err)
	}
	if err := c.SnapshotRate(context.Background(), 0); err != nil {
		t.Fatalf("SnapshotRate(0) = %v", err)
	}
	if want := []string{"snapshot-rate 5000", "snapshot-rate 0"}; !reflect.DeepEqual(b.calls, want) {
		t.Errorf("the backend saw %v, want %v", b.calls, want)
	}

	failing := &fakeBackend{err: errors.New("no service to change the rate of")}
	fc, _ := serve(t, failing)
	if err := fc.SnapshotRate(context.Background(), 100); err == nil {
		t.Error("SnapshotRate() with a failing backend returned nil")
	}
}

func TestChunkSizeReachesTheBackend(t *testing.T) {
	b := &fakeBackend{}
	c, _ := serve(t, b)

	if err := c.ChunkSize(context.Background(), 5000); err != nil {
		t.Fatalf("ChunkSize(5000) = %v", err)
	}
	if err := c.ChunkSize(context.Background(), 0); err != nil {
		t.Fatalf("ChunkSize(0) = %v", err)
	}
	if want := []string{"chunk-size 5000", "chunk-size 0"}; !reflect.DeepEqual(b.calls, want) {
		t.Errorf("the backend saw %v, want %v", b.calls, want)
	}

	failing := &fakeBackend{err: errors.New("no service to change the chunks of")}
	fc, _ := serve(t, failing)
	if err := fc.ChunkSize(context.Background(), 1000); err == nil {
		t.Error("ChunkSize() with a failing backend returned nil")
	}
}
