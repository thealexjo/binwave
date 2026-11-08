//go:build integration

package integration

import (
	"context"
	"errors"
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
