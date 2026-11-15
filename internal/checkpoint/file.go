package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Position is what binwave persists: the executed GTID set to resume after, and the row version
// of the last transaction in it. The version keeps row versions monotone across restarts, see
// chsink.Versioner.
type Position struct {
	GTIDSet string
	Version uint64
	// Quarantine lists the tables that are not being replicated, as EncodeQuarantine renders
	// it. It is saved with the position because the two belong together: the position says how
	// far the stream has been read, the list says which tables' changes were left out of it.
	Quarantine string
	// Resync lists the tables being read again from the source, as EncodeResyncs renders it.
	Resync string
	// LoadTotal is how many tables the initial load (replication.initial_position: snapshot) covered
	// when it began. Resync says how many are left; the two together are what "N of M tables loaded"
	// needs, which is why the total is saved and not counted from the list. Zero once the load is
	// done, and in a position saved before there was an initial load.
	LoadTotal int
	// Tables lists the ClickHouse tables binwave manages, as EncodeTables renders it: what DROP DATABASE
	// of the source drops. Empty in a position saved before there was a list.
	Tables string
}

// fileFormat is the on-disk document. Format lets a future change be detected.
type fileFormat struct {
	Format  int    `json:"format"`
	GTIDSet string `json:"gtid_set"`
	Version uint64 `json:"version"`
	// Quarantine is absent in files written before tables could be quarantined.
	Quarantine string `json:"quarantine,omitempty"`
	// Resync is absent in files written before tables could be resynchronized.
	Resync string `json:"resync,omitempty"`
	// LoadTotal is absent in files written before the initial load counted its tables.
	LoadTotal int `json:"load_total,omitempty"`
	// Tables is absent in files written before binwave kept a list of its tables.
	Tables string `json:"tables,omitempty"`
}

const fileFormatVersion = 1

// File stores the position in a JSON file, replaced atomically on every save. It is meant for
// development and tests; production keeps the position in ClickHouse.
type File struct {
	path string
}

// NewFile creates a store backed by the file at path.
func NewFile(path string) *File { return &File{path: path} }

// Load returns the saved position. found is false when nothing has been saved yet. A damaged
// file is an error: starting from "now" instead would silently skip data.
func (f *File) Load(_ context.Context) (pos Position, found bool, err error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return Position{}, false, nil
	}
	if err != nil {
		return Position{}, false, fmt.Errorf("read checkpoint %s: %w", f.path, err)
	}

	var doc fileFormat
	if err := json.Unmarshal(data, &doc); err != nil {
		return Position{}, false, fmt.Errorf("checkpoint %s is damaged: %w", f.path, err)
	}
	if doc.Format != fileFormatVersion {
		return Position{}, false, fmt.Errorf("checkpoint %s has unsupported format %d", f.path, doc.Format)
	}
	return Position{GTIDSet: doc.GTIDSet, Version: doc.Version, Quarantine: doc.Quarantine, Resync: doc.Resync, LoadTotal: doc.LoadTotal, Tables: doc.Tables}, true, nil
}

// Save durably replaces the stored position: the new content is written to a temporary file,
// flushed to disk, and renamed over the old one, so a crash leaves either the old or the new
// position and never a partial file.
func (f *File) Save(_ context.Context, pos Position) error {
	data, err := json.Marshal(fileFormat{Format: fileFormatVersion, GTIDSet: pos.GTIDSet, Version: pos.Version, Quarantine: pos.Quarantine, Resync: pos.Resync, LoadTotal: pos.LoadTotal, Tables: pos.Tables})
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}

	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".checkpoint-*")
	if err != nil {
		return fmt.Errorf("create temporary checkpoint in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("sync checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		cleanup()
		return fmt.Errorf("replace checkpoint %s: %w", f.path, err)
	}
	return syncDir(dir)
}

// syncDir flushes the directory entry so the rename itself survives a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open checkpoint directory: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint directory: %w", err)
	}
	return nil
}
