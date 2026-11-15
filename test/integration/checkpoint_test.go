//go:build integration

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/test/testenv"
)

// positionStore is the contract both stores fulfil.
type positionStore interface {
	Load(ctx context.Context) (checkpoint.Position, bool, error)
	Save(ctx context.Context, pos checkpoint.Position) error
}

// checkStoreContract exercises what the pipeline relies on. reopen returns a fresh store on the
// same storage, like a process restart.
func checkStoreContract(t *testing.T, reopen func() positionStore) {
	t.Helper()
	ctx := context.Background()

	store := reopen()
	if pos, found, err := store.Load(ctx); err != nil || found || pos != (checkpoint.Position{}) {
		t.Fatalf("Load() on empty storage = (%+v, %v, %v)", pos, found, err)
	}

	const uuid = "6d4d8f60-1111-2222-3333-444455556666"
	first := checkpoint.Position{GTIDSet: uuid + ":1-10", Version: 100}
	second := checkpoint.Position{
		GTIDSet: uuid + ":1-25", Version: 1<<63 + 250, // beyond float64 precision
		Quarantine: checkpoint.EncodeQuarantine([]checkpoint.Quarantined{{Table: "shop.orders", Since: uuid + ":1-20", Reason: "no key"}}),
		Tables:     checkpoint.EncodeTables(map[string]checkpoint.Target{"shop.items": {Database: "shop", Table: "items"}}),
	}
	if err := store.Save(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, second); err != nil {
		t.Fatal(err)
	}

	// A restarted process sees the latest position, version included.
	restarted := reopen()
	got, found, err := restarted.Load(ctx)
	if err != nil || !found || got != second {
		t.Fatalf("Load() after restart = (%+v, %v, %v), want %+v", got, found, err, second)
	}

	// It can keep saving, and the next restart sees that.
	third := checkpoint.Position{GTIDSet: uuid + ":1-40", Version: 1<<63 + 400, Tables: checkpoint.EncodeTables(nil)}
	if err := restarted.Save(ctx, third); err != nil {
		t.Fatal(err)
	}
	if got, _, err := reopen().Load(ctx); err != nil || got != third {
		t.Fatalf("Load() after the second restart = (%+v, %v), want %+v", got, err, third)
	}
}

func TestFileStoreContract(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	checkStoreContract(t, func() positionStore { return checkpoint.NewFile(path) })
}

func TestClickHouseStoreContract(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)

	checkStoreContract(t, func() positionStore {
		store := checkpoint.NewClickHouse(sink.Conn(), targetDB, "binwave_state", nil)
		if err := store.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		return store
	})
}

func TestClickHouseStoreKeepsTheLatestPositionBeforeMerges(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()

	store := checkpoint.NewClickHouse(sink.Conn(), targetDB, "binwave_state", nil)
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	// Many saves create many unmerged rows; Load must still return the newest.
	var last checkpoint.Position
	for i := 1; i <= 30; i++ {
		last = checkpoint.Position{GTIDSet: fmt.Sprintf("6d4d8f60-1111-2222-3333-444455556666:1-%d", i), Version: uint64(i) * 1000}
		if err := store.Save(ctx, last); err != nil {
			t.Fatal(err)
		}
	}

	got, found, err := checkpoint.NewClickHouse(sink.Conn(), targetDB, "binwave_state", nil).Load(ctx)
	if err != nil || !found || got != last {
		t.Fatalf("Load() = (%+v, %v, %v), want %+v", got, found, err, last)
	}
}

func TestClickHouseStoreInitKeepsExistingPosition(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()

	store := checkpoint.NewClickHouse(sink.Conn(), targetDB, "binwave_state", nil)
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	saved := checkpoint.Position{GTIDSet: "6d4d8f60-1111-2222-3333-444455556666:1-7", Version: 7}
	if err := store.Save(ctx, saved); err != nil {
		t.Fatal(err)
	}

	// Starting again runs Init on the existing table: nothing may be lost.
	again := checkpoint.NewClickHouse(sink.Conn(), targetDB, "binwave_state", nil)
	if err := again.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if got, found, err := again.Load(ctx); err != nil || !found || got != saved {
		t.Fatalf("Load() = (%+v, %v, %v), want %+v", got, found, err, saved)
	}
}

// TestClickHouseStoreUpgradesAStateTableOfAnEarlierVersion: a state table made before tables could
// be quarantined has no such column; Init adds it and keeps the saved position.
func TestClickHouseStoreUpgradesAStateTableOfAnEarlierVersion(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()

	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS replica",
		"CREATE TABLE replica.old_state (id String, seq UInt64, gtid_set String, version UInt64, updated_at DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(seq) ORDER BY id",
		"INSERT INTO replica.old_state VALUES ('position', 7, 'u:1-9', 900, now64(3))",
	} {
		if _, err := ch.DB.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}

	store := checkpoint.NewClickHouse(sink.Conn(), "replica", "old_state", nil)
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	pos, found, err := store.Load(ctx)
	if err != nil || !found || pos.GTIDSet != "u:1-9" || pos.Version != 900 || pos.Quarantine != "" || pos.Tables != "" {
		t.Fatalf("Load() after the upgrade = (%+v, %v, %v), want the old position without quarantine", pos, found, err)
	}

	q := checkpoint.EncodeQuarantine([]checkpoint.Quarantined{{Table: "a.b", Reason: "r"}})
	if err := store.Save(ctx, checkpoint.Position{GTIDSet: "u:1-10", Version: 901, Quarantine: q}); err != nil {
		t.Fatal(err)
	}
	if got, _, err := store.Load(ctx); err != nil || got.Quarantine != q {
		t.Errorf("Load() = (%+v, %v), want the quarantine saved", got, err)
	}
	// Running Init again changes nothing.
	if err := store.Init(ctx); err != nil {
		t.Errorf("a second Init failed: %v", err)
	}
}
