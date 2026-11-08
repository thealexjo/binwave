package clickhouse

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/typemap"
)

// accounts(id BIGINT UNSIGNED PK, owner VARCHAR NULL, balance DECIMAL(10,2), opened DATETIME).
func accountsTable(t *testing.T, ov config.TableOverride) *Table {
	t.Helper()
	src := &schema.Table{
		Database: "bank", Name: "accounts",
		Columns: []typemap.Column{
			{Name: "id", Kind: typemap.KindBigInt, Unsigned: true},
			{Name: "owner", Kind: typemap.KindVarChar, Length: 50, Charset: "utf8mb4", Nullable: true},
			{Name: "balance", Kind: typemap.KindDecimal, Precision: 10, Scale: 2},
			{Name: "opened", Kind: typemap.KindDateTime},
		},
		PrimaryKey: []int{0},
	}
	tbl, err := Resolve(src, cfgWith(map[string]config.TableOverride{"bank.accounts": ov}), nil)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func image(id uint64, owner any, balance string) []any {
	return []any{id, owner, decimal.RequireFromString(balance), "2024-01-02 03:04:05"}
}

func TestRowsInsertUpdateDelete(t *testing.T) {
	tbl := accountsTable(t, config.TableOverride{})

	rows, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Insert, Rows: []event.Row{{After: image(1, "ann", "10.50")}}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Deleted || rows[0].Version != 100 {
		t.Fatalf("insert rows = %#v", rows)
	}
	want := []any{uint64(1), "ann", decimal.RequireFromString("10.50"), time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}
	if !valuesEqual(rows[0].Values, want) {
		t.Errorf("insert values = %#v, want %#v", rows[0].Values, want)
	}

	// The same key: an update writes one new image, no tombstone.
	rows, err = tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Update, Rows: []event.Row{
		{Before: image(1, "ann", "10.50"), After: image(1, "ann", "99.99")},
	}}, 101)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Deleted || rows[0].Values[2].(decimal.Decimal).String() != "99.99" {
		t.Fatalf("update rows = %#v", rows)
	}

	// A delete keeps the old image as a tombstone.
	rows, err = tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Delete, Rows: []event.Row{{Before: image(1, nil, "99.99")}}}, 102)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].Deleted || rows[0].Version != 102 || rows[0].Values[1] != nil {
		t.Fatalf("delete rows = %#v", rows)
	}
}

func TestRowsPrimaryKeyChangeWritesTombstoneForOldKey(t *testing.T) {
	tbl := accountsTable(t, config.TableOverride{})

	rows, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Update, Rows: []event.Row{
		{Before: image(1, "ann", "10.00"), After: image(2, "ann", "10.00")},
	}}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want a tombstone and a new row: %#v", len(rows), rows)
	}
	if !rows[0].Deleted || rows[0].Values[0] != uint64(1) {
		t.Errorf("first row = %#v, want a tombstone of key 1", rows[0])
	}
	if rows[1].Deleted || rows[1].Values[0] != uint64(2) {
		t.Errorf("second row = %#v, want the live row of key 2", rows[1])
	}
}

func TestRowsKeyChangeDetectionUsesTheSortKeyOnly(t *testing.T) {
	// Only sort key columns decide whether an old row must be tombstoned.
	tbl := accountsTable(t, config.TableOverride{OrderBy: []string{"id"}})
	rows, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Update, Rows: []event.Row{
		{Before: image(1, "ann", "1.00"), After: image(1, "bob", "1.00")},
	}}, 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("Rows() = %#v, %v; changing a non-key column must not write a tombstone", rows, err)
	}

	tbl = accountsTable(t, config.TableOverride{OrderBy: []string{"id", "balance"}})
	rows, err = tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Update, Rows: []event.Row{
		{Before: image(1, "ann", "1.00"), After: image(1, "ann", "2.00")},
	}}, 1)
	if err != nil || len(rows) != 2 {
		t.Fatalf("Rows() = %#v, %v; changing a sort key column needs a tombstone", rows, err)
	}
}

func TestRowsEqualDecimalKeysAreTheSameKey(t *testing.T) {
	// 1.10 and 1.1 are one value: no spurious tombstone.
	tbl := accountsTable(t, config.TableOverride{OrderBy: []string{"id", "balance"}})
	rows, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Update, Rows: []event.Row{
		{Before: image(1, "a", "1.10"), After: image(1, "b", "1.1")},
	}}, 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("Rows() = %#v, %v", rows, err)
	}
}

func TestRowsUseOnlyReplicatedColumns(t *testing.T) {
	tbl := accountsTable(t, config.TableOverride{ExcludeColumns: []string{"owner"}})
	rows, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Insert, Rows: []event.Row{{After: image(1, "secret", "1.00")}}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows[0].Values) != 3 {
		t.Fatalf("values = %#v, want id, balance, opened", rows[0].Values)
	}
	for _, v := range rows[0].Values {
		if v == "secret" {
			t.Error("an excluded column leaked into the row")
		}
	}
}

func TestRowsConversionErrorNamesTableAndColumn(t *testing.T) {
	tbl := accountsTable(t, config.TableOverride{})
	_, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Insert, Rows: []event.Row{
		{After: []any{uint64(1), "x", "not a decimal", "2024-01-02 03:04:05"}},
	}}, 1)

	var convErr *typemap.ConversionError
	if !errors.As(err, &convErr) {
		t.Fatalf("Rows() = %v, want a typemap.ConversionError", err)
	}
	if convErr.Column != "balance" || !strings.Contains(err.Error(), "bank.accounts") {
		t.Errorf("error = %v", err)
	}
}

func TestRowsRejectsWrongImageWidth(t *testing.T) {
	tbl := accountsTable(t, config.TableOverride{})
	_, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Insert, Rows: []event.Row{{After: []any{uint64(1)}}}}, 1)
	if err == nil || !strings.Contains(err.Error(), "1 values for 4 columns") {
		t.Fatalf("Rows() = %v", err)
	}
}

func TestCollapseKeepsTheFinalStatePerKeyAndVersion(t *testing.T) {
	tbl := accountsTable(t, config.TableOverride{})
	mk := func(op event.Op, r event.Row, version uint64) []Row {
		rows, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: op, Rows: []event.Row{r}}, version)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}

	var rows []Row
	// One transaction (version 5): insert, update, delete of key 1, and an insert of key 2.
	rows = append(rows, mk(event.Insert, event.Row{After: image(1, "a", "1.00")}, 5)...)
	rows = append(rows, mk(event.Update, event.Row{Before: image(1, "a", "1.00"), After: image(1, "b", "2.00")}, 5)...)
	rows = append(rows, mk(event.Insert, event.Row{After: image(2, "c", "3.00")}, 5)...)
	rows = append(rows, mk(event.Delete, event.Row{Before: image(1, "b", "2.00")}, 5)...)
	// The next transaction (version 6) revives key 1.
	rows = append(rows, mk(event.Insert, event.Row{After: image(1, "d", "4.00")}, 6)...)

	got := Collapse(rows)

	type view struct {
		id      uint64
		version uint64
		deleted bool
	}
	var seen []view
	for _, r := range got {
		seen = append(seen, view{r.Values[0].(uint64), r.Version, r.Deleted})
	}
	want := []view{{2, 5, false}, {1, 5, true}, {1, 6, false}}
	if !reflect.DeepEqual(seen, want) {
		t.Errorf("collapsed rows = %v, want %v", seen, want)
	}
}

func TestCollapseWithoutRepeatsReturnsInput(t *testing.T) {
	tbl := accountsTable(t, config.TableOverride{})
	rows, err := tbl.Rows(event.Rows{Table: tbl.Source, Op: event.Insert, Rows: []event.Row{
		{After: image(1, "a", "1.00")}, {After: image(2, "b", "1.00")},
	}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := Collapse(rows); len(got) != 2 {
		t.Errorf("Collapse() dropped rows: %#v", got)
	}
	if got := Collapse(nil); len(got) != 0 {
		t.Errorf("Collapse(nil) = %#v", got)
	}
}

func valuesEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		switch x := a[i].(type) {
		case decimal.Decimal:
			y, ok := b[i].(decimal.Decimal)
			if !ok || !x.Equal(y) {
				return false
			}
		case time.Time:
			y, ok := b[i].(time.Time)
			if !ok || !x.Equal(y) {
				return false
			}
		default:
			if !reflect.DeepEqual(a[i], b[i]) {
				return false
			}
		}
	}
	return true
}

func TestNormalizeKey(t *testing.T) {
	tests := map[string]string{
		"k":              "k",
		"(k)":            "k",
		"a, b":           "a,b",
		"(a, b)":         "a,b",
		"`a`, `b`":       "a,b",
		"\"a\", \"b\"":   "a,b",
		"created_at, id": "created_at,id",
	}
	for in, want := range tests {
		if got := normalizeKey(in); got != want {
			t.Errorf("normalizeKey(%q) = %q, want %q", in, got, want)
		}
	}
}
