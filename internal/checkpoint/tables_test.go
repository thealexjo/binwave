package checkpoint

import (
	"reflect"
	"testing"
)

func TestTablesRoundTrip(t *testing.T) {
	tables := map[string]Target{
		"shop.orders": {Database: "shop", Table: "orders"},
		"shop.items":  {Database: "replica", Table: "goods"},
	}
	text := EncodeTables(tables)
	if want := `{"shop.items":{"database":"replica","table":"goods"},"shop.orders":{"database":"shop","table":"orders"}}`; text != want {
		t.Errorf("EncodeTables() = %s, want %s", text, want)
	}
	got, known, err := DecodeTables(text)
	if err != nil || !known || !reflect.DeepEqual(got, tables) {
		t.Fatalf("DecodeTables() = (%v, %v, %v), want %v", got, known, err, tables)
	}
}

func TestNoTablesIsNotAMissingList(t *testing.T) {
	if text := EncodeTables(nil); text != "{}" {
		t.Fatalf("EncodeTables(nil) = %q, want {}", text)
	}
	if got, known, err := DecodeTables("{}"); err != nil || !known || len(got) != 0 {
		t.Errorf(`DecodeTables("{}") = (%v, %v, %v), want an empty known list`, got, known, err)
	}
	if got, known, err := DecodeTables(""); err != nil || known || got == nil || len(got) != 0 {
		t.Errorf(`DecodeTables("") = (%v, %v, %v), want an empty map and no list`, got, known, err)
	}
}

func TestDamagedTablesAreAnError(t *testing.T) {
	if _, _, err := DecodeTables("{not json"); err == nil {
		t.Error("DecodeTables() accepted damaged text")
	}
}
