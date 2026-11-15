package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileLoadWithoutFile(t *testing.T) {
	store := NewFile(filepath.Join(t.TempDir(), "checkpoint.json"))

	pos, found, err := store.Load(context.Background())
	if err != nil || found || pos != (Position{}) {
		t.Fatalf("Load() = (%+v, %v, %v), want nothing found", pos, found, err)
	}
}

func TestFileSaveThenLoad(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	store := NewFile(path)

	want := Position{GTIDSet: "6d4d8f60-1111-2222-3333-444455556666:1-100,7a7a7a7a-1111-2222-3333-444455556666:1-5", Version: 1<<63 + 12345}
	if err := store.Save(ctx, want); err != nil {
		t.Fatal(err)
	}

	// A fresh store on the same file sees it, as after a process restart. The version must survive
	// exactly, including values beyond what a float64 can hold.
	got, found, err := NewFile(path).Load(ctx)
	if err != nil || !found || got != want {
		t.Fatalf("Load() = (%+v, %v, %v), want %+v", got, found, err, want)
	}
}

func TestFileSaveReplacesThePreviousPosition(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewFile(filepath.Join(dir, "checkpoint.json"))

	for i, set := range []string{"u:1-1", "u:1-2", "u:1-3"} {
		if err := store.Save(ctx, Position{GTIDSet: set, Version: uint64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	got, _, err := store.Load(ctx)
	if err != nil || got != (Position{GTIDSet: "u:1-3", Version: 3}) {
		t.Fatalf("Load() = (%+v, %v)", got, err)
	}

	// No temporary files are left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("directory holds %v, want only checkpoint.json", names)
	}
}

func TestFileIsPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	if err := NewFile(path).Save(context.Background(), Position{GTIDSet: "u:1"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("checkpoint file mode = %o, want no group/other access", perm)
	}
}

func TestFileRejectsDamagedContent(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantMsg string
	}{
		{"not json", "u:1-5", "damaged"},
		{"truncated", `{"version":1,"gtid_se`, "damaged"},
		{"empty", "", "damaged"},
		{"unknown format", `{"format":9,"gtid_set":"u:1"}`, "unsupported format 9"},
		{"no format", `{"gtid_set":"u:1"}`, "unsupported format 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "checkpoint.json")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}

			_, found, err := NewFile(path).Load(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Load() = (found=%v, %v), want error containing %q", found, err, tt.wantMsg)
			}
		})
	}
}

func TestFileSaveFailsInAMissingDirectory(t *testing.T) {
	store := NewFile(filepath.Join(t.TempDir(), "no", "such", "dir", "checkpoint.json"))
	if err := store.Save(context.Background(), Position{GTIDSet: "u:1"}); err == nil {
		t.Fatal("Save() succeeded in a missing directory")
	}
}

func TestFileFailedSaveKeepsThePreviousPosition(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	store := NewFile(path)
	if err := store.Save(ctx, Position{GTIDSet: "u:1-10", Version: 10}); err != nil {
		t.Fatal(err)
	}

	// Make the directory unwritable so the temporary file cannot be created.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}

	if err := store.Save(ctx, Position{GTIDSet: "u:1-20", Version: 20}); err == nil {
		t.Fatal("Save() succeeded in an unwritable directory")
	}
	got, _, err := store.Load(ctx)
	if err != nil || got != (Position{GTIDSet: "u:1-10", Version: 10}) {
		t.Errorf("Load() = (%+v, %v), want the previous position", got, err)
	}
}

func TestFileKeepsTheQuarantineWithThePosition(t *testing.T) {
	ctx := context.Background()
	f := NewFile(filepath.Join(t.TempDir(), "cp.json"))
	pos := Position{GTIDSet: "u:1-5", Version: 5, Quarantine: EncodeQuarantine([]Quarantined{{Table: "a.b", Reason: "r"}})}

	if err := f.Save(ctx, pos); err != nil {
		t.Fatal(err)
	}
	got, found, err := f.Load(ctx)
	if err != nil || !found || got != pos {
		t.Fatalf("Load() = (%+v, %v, %v), want %+v", got, found, err, pos)
	}
}

func TestFileKeepsTheListOfTablesWithThePosition(t *testing.T) {
	ctx := context.Background()
	f := NewFile(filepath.Join(t.TempDir(), "cp.json"))
	pos := Position{GTIDSet: "u:1-5", Version: 5, Tables: EncodeTables(map[string]Target{"shop.orders": {Database: "shop", Table: "orders"}})}

	if err := f.Save(ctx, pos); err != nil {
		t.Fatal(err)
	}
	got, found, err := f.Load(ctx)
	if err != nil || !found || got != pos {
		t.Fatalf("Load() = (%+v, %v, %v), want %+v", got, found, err, pos)
	}
}

// A file written before tables could be quarantined has no such field and stays valid.
func TestFileWrittenByAnEarlierVersionLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	if err := os.WriteFile(path, []byte(`{"format":1,"gtid_set":"u:1-5","version":7}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, found, err := NewFile(path).Load(context.Background())
	if err != nil || !found || got != (Position{GTIDSet: "u:1-5", Version: 7}) {
		t.Fatalf("Load() = (%+v, %v, %v)", got, found, err)
	}
}
