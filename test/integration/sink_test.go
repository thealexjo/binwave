//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/typemap"
	"github.com/thealexjo/binwave/test/testenv"
)

// kvTable is shop.kv(k INT PRIMARY KEY, v VARCHAR(20) NULL).
func kvTable() *schema.Table {
	return &schema.Table{
		Database: "shop", Name: "kv",
		Columns: []typemap.Column{
			{Name: "k", Kind: typemap.KindInt},
			{Name: "v", Kind: typemap.KindVarChar, Length: 20, Charset: "utf8mb4", Nullable: true},
		},
		PrimaryKey: []int{0},
	}
}

func kvRows(op event.Op, before, after []any) event.Rows {
	return event.Rows{Table: kvTable(), Op: op, Rows: []event.Row{{Before: before, After: after}}}
}

func insertKV(t *testing.T, c *chsink.Client, tbl *chsink.Table, batch event.Rows, version uint64) {
	t.Helper()
	rows, err := tbl.Rows(batch, version)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Insert(context.Background(), tbl, rows); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureCreatesTableAndIsIdempotent(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)

	for i := range 2 {
		if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
			t.Fatalf("Ensure() call %d: %v", i+1, err)
		}
	}

	var engine, key string
	if err := ch.DB.QueryRow("SELECT engine, sorting_key FROM system.tables WHERE database = 'replica' AND name = 'kv'").Scan(&engine, &key); err != nil {
		t.Fatal(err)
	}
	// ClickHouse reports a one-column sort key as either "k" or "(k)" depending on the version.
	if engine != "ReplacingMergeTree" || strings.Trim(key, "()") != "k" {
		t.Errorf("engine = %s, sorting key = %s", engine, key)
	}
}

func TestEnsureNeverTouchesAnExistingTable(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)
	if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
		t.Fatal(err)
	}
	insertKV(t, sink, tbl, kvRows(event.Insert, nil, []any{int32(1), "keep me"}), 10)

	if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
		t.Fatal(err)
	}

	rows := readTable(t, ch, "kv", "k")
	if len(rows) != 1 || rows[0]["v"] != "keep me" {
		t.Fatalf("data changed by Ensure: %v", rows)
	}
}

// TestEnsureNeverTakesATableBinwaveKeepsForAnotherJob: a copy a failed rebuild left, or the shadow
// table of a resync, has the shape of a source table of its name, but it is binwave's for that job:
// a source table created later under that name is refused, and the copy keeps its rows. The marker of
// a job for the table itself is the swap of its job, interrupted, and the table is taken, whatever its
// name ends with. The marker of an earlier release names no table, and the name of the table tells.
func TestEnsureNeverTakesATableBinwaveKeepsForAnotherJob(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()
	for _, job := range []struct {
		suffix         string
		marker         func(string) string
		earlierRelease string
	}{
		{"__rebuild", chsink.RebuildMarker, "binwave: rebuild"},
		{"__resync", chsink.ResyncMarker, "binwave: resync shadow"},
	} {
		name := "kv" + job.suffix
		for _, marker := range []string{job.marker("kv"), job.earlierRelease} {
			copied := resolveTable(t, kvTable(), nil)
			copied.Name = name
			if err := sink.Ensure(ctx, copied, additive); err != nil {
				t.Fatal(err)
			}
			insertKV(t, sink, copied, kvRows(event.Insert, nil, []any{int32(1), "copied"}), 10)
			execCH(t, ch, "ALTER TABLE replica."+name+" MODIFY COMMENT '"+marker+"'")

			src := kvTable()
			src.Name = name
			err := sink.Ensure(ctx, resolveTable(t, src, nil), additive)
			if !chsink.IsTableProblem(err) || !strings.Contains(err.Error(), name) {
				t.Errorf("%s on %s: Ensure() = %v, want a table problem that names the table", marker, name, err)
			}
			if rows := readTable(t, ch, name, "k"); len(rows) != 1 || rows[0]["v"] != "copied" {
				t.Errorf("%s on %s: the copy changed: %v", marker, name, rows)
			}
			execCH(t, ch, "DROP TABLE replica."+name)

			own := resolveTable(t, kvTable(), nil)
			if err := sink.Ensure(ctx, own, additive); err != nil {
				t.Fatal(err)
			}
			execCH(t, ch, "ALTER TABLE replica.kv MODIFY COMMENT '"+marker+"'")
			if err := sink.Ensure(ctx, own, additive); err != nil {
				t.Errorf("%s: Ensure() of kv after the swap of its own job = %v, want it taken", marker, err)
			}
			execCH(t, ch, "DROP TABLE replica.kv")
		}

		// A replicated table whose name ends like a copy, after the swap of its own job.
		src := kvTable()
		src.Name = name
		live := resolveTable(t, src, nil)
		if err := sink.Ensure(ctx, live, additive); err != nil {
			t.Fatal(err)
		}
		execCH(t, ch, "ALTER TABLE replica."+name+" MODIFY COMMENT '"+job.marker(name)+"'")
		if err := sink.Ensure(ctx, live, additive); err != nil {
			t.Errorf("Ensure() of %s after the swap of its own job = %v, want it taken", name, err)
		}
		execCH(t, ch, "DROP TABLE replica."+name)
	}

	// The shadow table of a resync is prepared again after a restart, with its own marker.
	shadow := resolveTable(t, kvTable(), nil)
	shadow.Name, shadow.Comment = "kv__resync", chsink.ResyncMarker("kv")
	for i := range 2 {
		if err := sink.Ensure(ctx, shadow, additive); err != nil {
			t.Fatalf("Ensure() of the shadow table, call %d: %v", i+1, err)
		}
	}
	execCH(t, ch, "ALTER TABLE replica.kv__resync MODIFY COMMENT 'binwave: resync shadow'")
	if err := sink.Ensure(ctx, shadow, additive); err != nil {
		t.Errorf("Ensure() of the shadow table an earlier release made: %v", err)
	}
}

// TestEnsureTakesATableOfAJobOnlyWhenItIsTheOneBinwaveMade: a table of the same structure under the name
// of a shadow table, without the marker, or with a marker that names another copy, is not taken for the
// shadow table, whether the job begins or the stream is bound to it: a conflict, and the table is left as
// it was.
func TestEnsureTakesATableOfAJobOnlyWhenItIsTheOneBinwaveMade(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()
	src := kvTable()
	src.Name = "kv__resync"
	byHand := resolveTable(t, src, nil)
	if err := sink.Ensure(ctx, byHand, additive); err != nil {
		t.Fatal(err)
	}
	insertKV(t, sink, byHand, kvRows(event.Insert, nil, []any{int32(1), "by hand"}), 10)
	uuid := tableUUID(t, ch, "replica", "kv__resync")
	comment := func() string {
		var c string
		if err := ch.DB.QueryRow("SELECT comment FROM system.tables WHERE database = 'replica' AND name = 'kv__resync'").Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	shadow := resolveTable(t, kvTable(), nil)
	shadow.Name, shadow.Comment = "kv__resync", chsink.ResyncMarker("kv")
	marker := chsink.ResyncMarker("kv")
	for _, c := range []struct {
		name, comment string
		taken         bool
	}{
		{"no marker", "", false},
		{"a marker naming another copy", chsink.Replacing(marker, "0a000000-0000-0000-0000-00000000000a", "0c000000-0000-0000-0000-00000000000c"), false},
		{"a marker naming the table", chsink.Replacing(marker, "0a000000-0000-0000-0000-00000000000a", uuid), true},
		{"the marker", marker, true},
	} {
		execCH(t, ch, "ALTER TABLE replica.kv__resync MODIFY COMMENT '"+c.comment+"'")
		for _, ddl := range []chsink.DDL{additive, {Mode: additive.Mode, Existing: true}} {
			err := sink.Ensure(ctx, shadow, ddl)
			if c.taken {
				if err != nil {
					t.Errorf("%s, existing %v: Ensure() = %v, want the shadow table taken", c.name, ddl.Existing, err)
				}
				continue
			}
			if !chsink.IsConflict(err) {
				t.Errorf("%s, existing %v: Ensure() = %v, want a conflict", c.name, ddl.Existing, err)
			}
			if got := comment(); got != c.comment {
				t.Errorf("%s, existing %v: the comment is %q, want it left as it was", c.name, ddl.Existing, got)
			}
		}
	}
	if rows := readTable(t, ch, "kv__resync", "k"); len(rows) != 1 || rows[0]["v"] != "by hand" {
		t.Errorf("the table changed: %v", rows)
	}
}

// TestAShadowTableIsKnownByTheUUIDItWasMadeWith: the shadow table of a resync is made with the UUID the
// resync gives it, and only a table of that UUID is it: one with its marker and another UUID is not taken,
// swapped in or dropped. The shadow table in the place of the table without the marker is a swap that was
// finished. ClickHouse refuses a table made with a UUID it holds: one a table has, or had and was dropped.
func TestAShadowTableIsKnownByTheUUIDItWasMadeWith(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()
	const mine, other, later = "0a000000-0000-4000-8000-00000000000a", "0b000000-0000-4000-8000-00000000000b", "0c000000-0000-4000-8000-00000000000c"
	table := resolveTable(t, kvTable(), nil)
	if err := sink.Ensure(ctx, table, additive); err != nil {
		t.Fatal(err)
	}
	insertKV(t, sink, table, kvRows(event.Insert, nil, []any{int32(1), "old"}), 10)
	shadow := resolveTable(t, kvTable(), nil)
	shadow.Name, shadow.Comment, shadow.UUID = "kv__resync", chsink.ResyncMarker("kv"), mine
	if err := sink.Ensure(ctx, shadow, additive); err != nil {
		t.Fatal(err)
	}
	if got := tableUUID(t, ch, "replica", "kv__resync"); got != mine {
		t.Fatalf("the shadow table was made with the UUID %s, want %s", got, mine)
	}
	insertKV(t, sink, shadow, kvRows(event.Insert, nil, []any{int32(1), "new"}), 20)
	value := func(table string) string {
		t.Helper()
		rows := readTable(t, ch, table, "k")
		if len(rows) != 1 {
			t.Fatalf("%s holds %v", table, rows)
		}
		return fmt.Sprint(rows[0]["v"])
	}

	theirs := *shadow
	theirs.UUID = other
	for _, ddl := range []chsink.DDL{additive, {Mode: additive.Mode, Existing: true}} {
		if err := sink.Ensure(ctx, shadow, ddl); err != nil {
			t.Errorf("existing %v: Ensure() with the UUID of the shadow table = %v", ddl.Existing, err)
		}
		if err := sink.Ensure(ctx, &theirs, ddl); !chsink.IsConflict(err) {
			t.Errorf("existing %v: Ensure() with another UUID = %v, want a conflict", ddl.Existing, err)
		}
	}
	req := chsink.SwapRequest{Source: "shop.kv", Database: "replica", Table: "kv", Shadow: "kv__resync", ShadowUUID: other}
	if got, err := sink.SwapMarks(ctx, req); err != nil || got != (chsink.SwapMarks{ShadowExists: true}) {
		t.Errorf("SwapMarks() with another UUID = %+v, %v, want a table there that is not the shadow table", got, err)
	}
	if dropped, err := sink.DropShadow(ctx, req); err != nil || dropped {
		t.Errorf("DropShadow() with another UUID = %v, %v, want nothing dropped", dropped, err)
	}
	if _, err := sink.SwapShadow(ctx, req); !chsink.IsConflict(err) {
		t.Errorf("SwapShadow() with another UUID = %v, want a conflict", err)
	}
	if got := tableUUID(t, ch, "replica", "kv__resync"); got != mine || value("kv") != "old" || value("kv__resync") != "new" {
		t.Fatalf("the tables changed: the shadow table has the UUID %s", got)
	}

	twin := *shadow
	twin.Name, twin.Comment = "kv2__resync", chsink.ResyncMarker("kv2")
	if err := sink.Ensure(ctx, &twin, additive); !errors.Is(err, chsink.ErrUUIDTaken) {
		t.Errorf("Ensure() with the UUID a table has = %v, want ErrUUIDTaken", err)
	}

	// The swap, and its repeat by a restart that knows nothing of it but the UUID.
	req.ShadowUUID = mine
	if got, err := sink.SwapMarks(ctx, req); err != nil || got != (chsink.SwapMarks{Shadow: true, ShadowExists: true}) {
		t.Errorf("SwapMarks() before the swap = %+v, %v", got, err)
	}
	if copied, err := sink.SwapShadow(ctx, req); err != nil || copied != mine {
		t.Fatalf("SwapShadow() = %q, %v, want %s", copied, err, mine)
	}
	for range 2 {
		if got, err := sink.SwapMarks(ctx, req); err != nil || got != (chsink.SwapMarks{Swapped: true}) {
			t.Errorf("SwapMarks() after the swap = %+v, %v, want the swap finished", got, err)
		}
		if copied, err := sink.SwapShadow(ctx, req); err != nil || copied != mine {
			t.Errorf("SwapShadow() again = %q, %v, want the swap finished", copied, err)
		}
	}
	legacy := req
	legacy.ShadowUUID = ""
	var lost *chsink.TableError
	if _, err := sink.SwapShadow(ctx, legacy); !errors.As(err, &lost) {
		t.Errorf("SwapShadow() without the UUID = %v, want the shadow table lost (nothing tells the swap finished)", err)
	}
	if got := tableUUID(t, ch, "replica", "kv"); got != mine || value("kv") != "new" {
		t.Errorf("after the swap the table has the UUID %s and %q, want %s and new", got, value("kv"), mine)
	}

	// Dropped, a shadow table leaves its UUID held.
	again := *shadow
	again.UUID = later
	if err := sink.Ensure(ctx, &again, additive); err != nil {
		t.Fatal(err)
	}
	req.ShadowUUID = later
	if dropped, err := sink.DropShadow(ctx, req); err != nil || !dropped {
		t.Fatalf("DropShadow() = %v, %v, want the shadow table dropped", dropped, err)
	}
	if err := sink.Ensure(ctx, &again, additive); !errors.Is(err, chsink.ErrUUIDTaken) {
		t.Errorf("Ensure() with the UUID of the dropped shadow table = %v, want ErrUUIDTaken", err)
	}
	var n uint64
	if err := ch.DB.QueryRow("SELECT count() FROM system.tables WHERE database = 'replica' AND name LIKE '%resync%'").Scan(&n); err != nil || n != 0 {
		t.Errorf("%d tables under the name of a shadow table (%v), want none", n, err)
	}
}

func TestEnsureReportsAMismatchAndLeavesTheTableAlone(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	execCH(t, ch,
		"CREATE DATABASE replica",
		// Wrong type for k, no v, wrong sort key, wrong engine, no service columns.
		"CREATE TABLE replica.kv (k String, extra String) ENGINE = MergeTree ORDER BY extra",
		"INSERT INTO replica.kv VALUES ('x', 'y')",
	)

	err := sink.Ensure(context.Background(), resolveTable(t, kvTable(), nil), additive)

	var mismatch *chsink.SchemaMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Ensure() = %v, want *SchemaMismatchError", err)
	}
	all := strings.Join(mismatch.Problems, "\n")
	for _, want := range []string{"column k is String, expected Int32", "column v is missing", "column _version is missing", "engine is MergeTree", "sort key is (extra)"} {
		if !strings.Contains(all, want) {
			t.Errorf("problems do not mention %q:\n%s", want, all)
		}
	}

	var n uint64
	if err := ch.DB.QueryRow("SELECT count() FROM replica.kv").Scan(&n); err != nil || n != 1 {
		t.Errorf("the existing table was modified: count = %d, err = %v", n, err)
	}
}

// TestEnsureRefusesAReplacingMergeTreeWithOtherArguments: without _version as the version and
// _is_deleted as the deleted flag, FINAL keeps the wrong version of a row and shows deleted rows.
func TestEnsureRefusesAReplacingMergeTreeWithOtherArguments(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)
	execCH(t, ch, "CREATE DATABASE replica")
	for i, engine := range []string{"ReplacingMergeTree()", "ReplacingMergeTree(_version)", "ReplacingMergeTree(_synced_at, _is_deleted)"} {
		name := fmt.Sprintf("kv%d", i)
		create := strings.Replace(tbl.CreateStatement(), "`kv`", "`"+name+"`", 1)
		create = strings.Replace(create, "ReplacingMergeTree(`_version`, `_is_deleted`)", engine, 1)
		execCH(t, ch, create)
		other := *tbl
		other.Name = name

		err := sink.Ensure(context.Background(), &other, additive)

		var mismatch *chsink.SchemaMismatchError
		if !errors.As(err, &mismatch) || !strings.Contains(strings.Join(mismatch.Problems, "\n"), "expected ReplacingMergeTree(_version, _is_deleted)") {
			t.Errorf("Ensure() of a table with %s = %v, want a mismatch of the engine", engine, err)
		}
	}
	if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
		t.Errorf("Ensure() of the table binwave creates = %v", err)
	}
}

func TestEnsureDDLModes(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)
	ctx := context.Background()

	tableExists := func() bool {
		var n uint64
		if err := ch.DB.QueryRow("SELECT count() FROM system.tables WHERE database = 'replica' AND name = 'kv'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}

	for _, mode := range []config.DDLMode{config.DDLAdditive, config.DDLFull} {
		err := sink.Ensure(ctx, tbl, chsink.DDL{Mode: mode, DryRun: true})
		if err == nil || !strings.Contains(err.Error(), "dry_run") || !chsink.IsMissingTable(err) {
			t.Fatalf("dry run (%s), missing table: Ensure() = %v, want a missing-table error that names ddl.dry_run", mode, err)
		}
	}
	if tableExists() {
		t.Fatal("the table was created although ddl.dry_run is set")
	}

	// Create it, then a dry run accepts a matching table and logs nothing to do.
	if err := sink.Ensure(ctx, tbl, chsink.DDL{Mode: config.DDLFull}); err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []chsink.DDL{{Mode: config.DDLAdditive, DryRun: true}, {Mode: config.DDLFull, DryRun: true}} {
		if err := sink.Ensure(ctx, tbl, ddl); err != nil {
			t.Errorf("Ensure(%+v) on a matching table: %v", ddl, err)
		}
	}
}

// TestReplayAndStaleWritesNeverRegressData is the idempotency contract: replaying events after a
// crash, even out of order relative to newer data, must not change the visible result.
func TestReplayAndStaleWritesNeverRegressData(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)
	if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
		t.Fatal(err)
	}
	visible := func() []map[string]any { return readTable(t, ch, "kv", "k") }

	first := kvRows(event.Insert, nil, []any{int32(1), "a"})
	second := kvRows(event.Update, []any{int32(1), "a"}, []any{int32(1), "b"})
	remove := kvRows(event.Delete, []any{int32(1), "b"}, nil)

	insertKV(t, sink, tbl, first, 10)
	insertKV(t, sink, tbl, second, 20)
	if got := visible(); len(got) != 1 || got[0]["v"] != "b" {
		t.Fatalf("after update: %v", got)
	}

	// A replay of the older event after the newer one was written changes nothing.
	insertKV(t, sink, tbl, first, 10)
	if got := visible(); len(got) != 1 || got[0]["v"] != "b" {
		t.Fatalf("after replaying the insert: %v", got)
	}
	// Replaying the same write twice does not duplicate it.
	insertKV(t, sink, tbl, second, 20)
	if got := visible(); len(got) != 1 {
		t.Fatalf("after replaying the update: %v", got)
	}

	insertKV(t, sink, tbl, remove, 30)
	if got := visible(); len(got) != 0 {
		t.Fatalf("after delete: %v", got)
	}
	// Replaying old writes must not resurrect the deleted row.
	insertKV(t, sink, tbl, first, 10)
	insertKV(t, sink, tbl, second, 20)
	if got := visible(); len(got) != 0 {
		t.Fatalf("the deleted row came back after a replay: %v", got)
	}

	// A genuinely newer insert of the same key revives it.
	insertKV(t, sink, tbl, kvRows(event.Insert, nil, []any{int32(1), "again"}), 40)
	if got := visible(); len(got) != 1 || got[0]["v"] != "again" {
		t.Fatalf("after re-insert: %v", got)
	}
}

func TestPrimaryKeyChangeMovesTheRow(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)
	if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
		t.Fatal(err)
	}

	insertKV(t, sink, tbl, kvRows(event.Insert, nil, []any{int32(1), "a"}), 10)
	insertKV(t, sink, tbl, kvRows(event.Update, []any{int32(1), "a"}, []any{int32(2), "a"}), 20)

	rows := readTable(t, ch, "kv", "k")
	if len(rows) != 1 || rows[0]["k"] != int32(2) {
		t.Fatalf("rows = %v, want only key 2", rows)
	}
}

func TestInsertAddsServiceColumns(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)
	if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
		t.Fatal(err)
	}

	insertKV(t, sink, tbl, kvRows(event.Insert, nil, []any{int32(1), "a"}), 12345)
	insertKV(t, sink, tbl, kvRows(event.Delete, []any{int32(1), "a"}, nil), 12346)

	rows, err := ch.DB.Query("SELECT _version, _is_deleted, _synced_at > now() - INTERVAL 1 HOUR FROM replica.kv ORDER BY _version")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	type svc struct {
		version uint64
		deleted uint8
		recent  bool
	}
	var got []svc
	for rows.Next() {
		var s svc
		if err := rows.Scan(&s.version, &s.deleted, &s.recent); err != nil {
			t.Fatal(err)
		}
		got = append(got, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []svc{{12345, 0, true}, {12346, 1, true}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("service columns = %+v, want %+v", got, want)
	}
}

func TestInsertOfManyRows(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)
	if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
		t.Fatal(err)
	}

	const n = 50_000
	batch := event.Rows{Table: kvTable(), Op: event.Insert}
	for i := range n {
		batch.Rows = append(batch.Rows, event.Row{After: []any{int32(i), "row"}})
	}
	rows, err := tbl.Rows(batch, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(context.Background(), tbl, rows); err != nil {
		t.Fatal(err)
	}

	var count uint64
	if err := ch.DB.QueryRow("SELECT count() FROM replica.kv FINAL").Scan(&count); err != nil || count != n {
		t.Fatalf("count = %d, err = %v, want %d", count, err, n)
	}
}

// TestInsertOfRowsTooWideForOneBlock: ClickHouse refuses a compressed block whose decompressed size is
// about a gigabyte and closes the connection, which the client sees only as a reset. A table of wide
// text rows read in chunks of tens of thousands of rows can make a block over a gigabyte, and its
// resync then cannot proceed at all. An insert whose rows add up to more than one block must go as
// several.
func TestInsertOfRowsTooWideForOneBlock(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	tbl := resolveTable(t, kvTable(), nil)
	if err := sink.Ensure(context.Background(), tbl, additive); err != nil {
		t.Fatal(err)
	}

	// 1 MiB per row, more than three blocks of them: the whole batch is 200+ MiB, which one block
	// cannot be.
	const rows, size = 210, 1 << 20
	body := strings.Repeat("x", size)
	batch := event.Rows{Table: kvTable(), Op: event.Insert}
	for i := range rows {
		batch.Rows = append(batch.Rows, event.Row{After: []any{int32(i), body}})
	}
	converted, err := tbl.Rows(batch, 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := sink.Insert(context.Background(), tbl, converted); err != nil {
		t.Fatalf("Insert() of %d wide rows = %v", rows, err)
	}

	var count uint64
	var bytes uint64
	if err := ch.DB.QueryRow("SELECT count(), sum(length(v)) FROM replica.kv FINAL").Scan(&count, &bytes); err != nil {
		t.Fatal(err)
	}
	if count != rows || bytes != uint64(rows)*size {
		t.Errorf("ClickHouse holds %d rows of %d bytes, want %d rows of %d", count, bytes, rows, rows*size)
	}
}

func TestOpenReportsBadCredentials(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)

	cfg := clickhouseConfig(ch)
	cfg.Password = "wrong"
	_, err := chsink.Open(context.Background(), cfg, chsink.Options{})
	if err == nil || !strings.Contains(err.Error(), "connect to clickhouse") {
		t.Fatalf("Open() = %v", err)
	}
}

// TestTestStandRunsWithDurableInserts guards the recommendation of deploy/clickhouse: the
// stand applies fsync to every MergeTree table, so the integration tests exercise the same
// durability settings production should use.
func TestTestStandRunsWithDurableInserts(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)

	for _, name := range []string{"fsync_after_insert", "fsync_part_directory"} {
		var value string
		if err := ch.DB.QueryRow("SELECT value FROM system.merge_tree_settings WHERE name = ?", name).Scan(&value); err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if value != "1" {
			t.Errorf("%s = %s, want 1", name, value)
		}
	}
}
