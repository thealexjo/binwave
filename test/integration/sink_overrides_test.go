//go:build integration

package integration

import (
	"context"
	"reflect"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/typemap"
	"github.com/thealexjo/binwave/test/testenv"
)

// TestTypeOverridesRoundTrip checks the type overrides of tables.overrides against a real
// ClickHouse: Bool, Enum8, String for SET, and the native JSON type.
func TestTypeOverridesRoundTrip(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	// The native JSON type is generally available from ClickHouse 25.3; before that JSON meant
	// the experimental Object type.
	requireClickHouse(t, ch, 25, 3)
	sink := openSink(t, ch)
	ctx := context.Background()

	src := &schema.Table{
		Database: "shop", Name: "settings",
		Columns: []typemap.Column{
			{Name: "id", Kind: typemap.KindInt},
			{Name: "enabled", Kind: typemap.KindTinyInt},
			{Name: "tier", Kind: typemap.KindEnum, Values: []string{"free", "pro"}},
			{Name: "flags", Kind: typemap.KindSet, Values: []string{"a", "b", "c"}},
			{Name: "doc", Kind: typemap.KindJSON},
			{Name: "note", Kind: typemap.KindVarChar, Length: 10, Charset: "utf8mb4", Nullable: true},
		},
		PrimaryKey: []int{0},
	}
	tbl := resolveTable(t, src, map[string]config.TableOverride{"shop.settings": {Columns: map[string]config.ColumnOverride{
		"enabled": {Type: "Bool"},
		"tier":    {Type: "Enum8('free' = 1, 'pro' = 2)"},
		"flags":   {Type: "String"},
		"doc":     {Type: "JSON"},
		"note":    {Type: "LowCardinality(String)"},
	}}})
	if err := sink.Ensure(ctx, tbl, additive); err != nil {
		t.Fatal(err)
	}

	rows, err := tbl.Rows(event.Rows{Table: src, Op: event.Insert, Rows: []event.Row{
		{After: []any{int32(1), int8(1), int64(2), int64(5), `{"a":{"b":[1,2,3]},"c":"d"}`, "x"}},
		{After: []any{int32(2), int8(0), int64(1), int64(0), `{}`, nil}},
	}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Insert(ctx, tbl, rows); err != nil {
		t.Fatal(err)
	}

	var got []map[string]any
	for _, r := range readTable(t, ch, "settings", "id") {
		got = append(got, map[string]any{"id": r["id"], "enabled": r["enabled"], "tier": r["tier"], "flags": r["flags"], "note": r["note"]})
	}
	want := []map[string]any{
		{"id": int32(1), "enabled": true, "tier": "pro", "flags": "a,c", "note": "x"},
		{"id": int32(2), "enabled": false, "tier": "free", "flags": "", "note": nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rows = %v\nwant   %v", got, want)
	}

	// The JSON column holds structured data ClickHouse can address by path.
	var b1 int64
	if err := ch.DB.QueryRow("SELECT doc.a.b[2]::Int64 FROM replica.settings FINAL WHERE id = 1").Scan(&b1); err != nil || b1 != 2 {
		t.Errorf("doc.a.b[2] = %d, err = %v, want 2", b1, err)
	}
}
