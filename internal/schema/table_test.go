package schema

import (
	"reflect"
	"testing"

	"github.com/thealexjo/binwave/internal/typemap"
)

func sample() *Table {
	return &Table{
		Database: "shop",
		Name:     "orders",
		Columns: []typemap.Column{
			{Name: "id", Kind: typemap.KindBigInt, Unsigned: true},
			{Name: "tenant", Kind: typemap.KindInt},
			{Name: "note", Kind: typemap.KindVarChar, Length: 50, Charset: "utf8mb4", Nullable: true},
		},
		PrimaryKey: []int{1, 0},
	}
}

func TestTableAccessors(t *testing.T) {
	tbl := sample()

	if got := tbl.FullName(); got != "shop.orders" {
		t.Errorf("FullName() = %q", got)
	}
	if got := tbl.ColumnIndex("note"); got != 2 {
		t.Errorf("ColumnIndex(note) = %d, want 2", got)
	}
	if got := tbl.ColumnIndex("missing"); got != -1 {
		t.Errorf("ColumnIndex(missing) = %d, want -1", got)
	}
	if got, want := tbl.PrimaryKeyColumns(), []string{"tenant", "id"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PrimaryKeyColumns() = %v, want %v", got, want)
	}
}

func TestNoPrimaryKey(t *testing.T) {
	tbl := &Table{Database: "d", Name: "t", Columns: []typemap.Column{{Name: "a", Kind: typemap.KindInt}}}
	if got := tbl.PrimaryKeyColumns(); len(got) != 0 {
		t.Errorf("PrimaryKeyColumns() = %v, want empty", got)
	}
}

func TestSignatureDetectsSchemaChanges(t *testing.T) {
	base := sample().Signature()
	if base != sample().Signature() {
		t.Fatal("Signature() is not deterministic")
	}

	changes := map[string]func(*Table){
		"column added":      func(t *Table) { t.Columns = append(t.Columns, typemap.Column{Name: "x", Kind: typemap.KindInt}) },
		"type widened":      func(t *Table) { t.Columns[1].Kind = typemap.KindBigInt },
		"nullability":       func(t *Table) { t.Columns[1].Nullable = true },
		"length":            func(t *Table) { t.Columns[2].Length = 100 },
		"renamed":           func(t *Table) { t.Columns[2].Name = "comment" },
		"primary key order": func(t *Table) { t.PrimaryKey = []int{0, 1} },
		"table renamed":     func(t *Table) { t.Name = "orders2" },
	}
	for name, mutate := range changes {
		t.Run(name, func(t *testing.T) {
			tbl := sample()
			mutate(tbl)
			if tbl.Signature() == base {
				t.Error("Signature() did not change")
			}
		})
	}
}
