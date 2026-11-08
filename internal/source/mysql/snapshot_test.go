package mysql

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/typemap"
)

func partitioned(t *schema.Table, names ...string) *schema.Table {
	t.Partitions = names
	return t
}

func table(cols []string, key ...int) *schema.Table {
	t := &schema.Table{Database: "shop", Name: "t", PrimaryKey: key}
	for _, c := range cols {
		t.Columns = append(t.Columns, typemap.Column{Name: c, Kind: typemap.KindBigInt})
	}
	return t
}

// TestChunkStatementStartsTheScanAtTheCursor: the tuple form ((a, b) > (?, ?)) is not turned into a
// range by MySQL, which makes every chunk of a table with a composite key cost more than the one
// before it (measured: "type: index", the whole primary key scanned). The statement must compare the
// leading column on its own so that the scan starts at the cursor.
func TestChunkStatementStartsTheScanAtTheCursor(t *testing.T) {
	tests := map[string]struct {
		tbl        *schema.Table
		partition  string
		withCursor bool
		want       string
	}{
		"one key column, from the start": {
			tbl:  table([]string{"id", "name"}, 0),
			want: "SELECT `id`, `name` FROM `shop`.`t` ORDER BY `id` LIMIT ?",
		},
		"one key column, past a cursor": {
			tbl: table([]string{"id", "name"}, 0), withCursor: true,
			want: "SELECT `id`, `name` FROM `shop`.`t` WHERE `id` > ? ORDER BY `id` LIMIT ?",
		},
		"two key columns, past a cursor": {
			tbl: table([]string{"id", "at", "name"}, 0, 1), withCursor: true,
			want: "SELECT `id`, `at`, `name` FROM `shop`.`t` WHERE (`id` > ? OR (`id` = ? AND `at` > ?)) " +
				"ORDER BY `id`, `at` LIMIT ?",
		},
		"three key columns, past a cursor": {
			tbl: table([]string{"a", "b", "c"}, 0, 1, 2), withCursor: true,
			want: "SELECT `a`, `b`, `c` FROM `shop`.`t` WHERE (`a` > ? OR (`a` = ? AND (`b` > ? OR (`b` = ? AND `c` > ?)))) " +
				"ORDER BY `a`, `b`, `c` LIMIT ?",
		},
		"one partition of a partitioned table": {
			tbl: partitioned(table([]string{"id", "at", "v"}, 0, 1), "p1", "p2"), partition: "p2", withCursor: true,
			want: "SELECT `id`, `at`, `v` FROM `shop`.`t` PARTITION (`p2`) " +
				"WHERE (`id` > ? OR (`id` = ? AND `at` > ?)) ORDER BY `id`, `at` LIMIT ?",
		},
		"the first chunk of a partition": {
			tbl: partitioned(table([]string{"id"}, 0), "p1"), partition: "p1",
			want: "SELECT `id` FROM `shop`.`t` PARTITION (`p1`) ORDER BY `id` LIMIT ?",
		},
		"a name with a backtick is quoted": {
			tbl: table([]string{"we`ird"}, 0), withCursor: true,
			want: "SELECT `we``ird` FROM `shop`.`t` WHERE `we``ird` > ? ORDER BY `we``ird` LIMIT ?",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := chunkStatement(tt.tbl, tt.partition, tt.withCursor); got != tt.want {
				t.Errorf("statement:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

// TestKeysetArgsMatchThePlaceholdersOfThePredicate: every value but the last is compared twice, and a
// mismatch here reads rows from the wrong place with no error at all.
func TestKeysetArgsMatchThePlaceholdersOfThePredicate(t *testing.T) {
	tests := []struct {
		after []any
		want  []any
	}{
		{after: nil, want: nil},
		{after: []any{int64(7)}, want: []any{int64(7)}},
		{after: []any{int64(7), "d"}, want: []any{int64(7), int64(7), "d"}},
		{after: []any{int64(7), "d", int64(3)}, want: []any{int64(7), int64(7), "d", "d", int64(3)}},
	}
	for _, tt := range tests {
		got := keysetArgs(tt.after)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("keysetArgs(%v) = %v, want %v", tt.after, got, tt.want)
		}
		keys := make([]string, len(tt.after))
		for i := range keys {
			keys[i] = "k"
		}
		if marks := placeholders(keysetPredicate(keys)); marks != len(got) {
			t.Errorf("the predicate for %d key columns has %d placeholders for %d values", len(keys), marks, len(got))
		}
	}
}

func placeholders(s string) int {
	n := 0
	for _, r := range s {
		if r == '?' {
			n++
		}
	}
	return n
}

type fakeStmt struct {
	name   string
	closed bool
}

func TestStmtCacheKeepsTheWorkingSetAndClosesWhatItDrops(t *testing.T) {
	var mu sync.Mutex
	var closed []string
	c := newStmtCache(2, func(s *fakeStmt) error {
		mu.Lock()
		defer mu.Unlock()
		s.closed = true
		closed = append(closed, s.name)
		return nil
	})
	prepares := 0
	get := func(key string) *fakeStmt {
		t.Helper()
		s, err := c.get(key, func() (*fakeStmt, error) {
			prepares++
			return &fakeStmt{name: key}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	a := get("a")
	get("b")
	if again := get("a"); again != a || prepares != 2 {
		t.Errorf("a statement already prepared was prepared again: %d prepares", prepares)
	}
	// "b" is the least recently used, so it is the one that goes when "c" arrives.
	get("c")
	if !reflect.DeepEqual(closed, []string{"b"}) {
		t.Errorf("closed %v, want only b", closed)
	}
	if get("a") != a {
		t.Error("the statement used last was dropped")
	}

	if err := c.closeAll(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(closed, []string{"b", "a", "c"}) {
		t.Errorf("closed %v after closeAll, want every statement", closed)
	}
	// Nothing is kept: closing twice must not close a statement that is already gone.
	if err := c.closeAll(); err != nil {
		t.Fatal(err)
	}
	if len(closed) != 3 {
		t.Errorf("closed %v, want the three statements once each", closed)
	}
}

func TestStmtCacheDoesNotKeepAStatementItCouldNotPrepare(t *testing.T) {
	c := newStmtCache(2, func(*fakeStmt) error { return nil })
	boom := errors.New("too many connections")
	if _, err := c.get("a", func() (*fakeStmt, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("get() = %v, want the error of prepare", err)
	}
	prepared := false
	if _, err := c.get("a", func() (*fakeStmt, error) { prepared = true; return &fakeStmt{name: "a"}, nil }); err != nil {
		t.Fatal(err)
	}
	if !prepared {
		t.Error("a failed prepare was cached: the statement would never be prepared again")
	}
}
