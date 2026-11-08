package pipeline

import (
	"reflect"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
)

func TestRouterMatch(t *testing.T) {
	r := NewRouter(config.Tables{
		Include: []string{"shop.*", "warehouse.invoices", "logs_?.events"},
		Exclude: []string{"shop.tmp_*", "shop.audit"},
	})

	tests := []struct {
		db, table string
		want      bool
	}{
		{"shop", "orders", true},
		{"shop", "tmp_import", false}, // excluded by a glob
		{"shop", "audit", false},      // excluded by name
		{"shop", "tmp", true},         // the glob needs the underscore
		{"warehouse", "invoices", true},
		{"warehouse", "payments", false}, // not included
		{"logs_a", "events", true},
		{"logs_ab", "events", false},
		{"other", "orders", false},
	}
	for _, tt := range tests {
		if got := r.Match(tt.db, tt.table); got != tt.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tt.db, tt.table, got, tt.want)
		}
	}
}

func TestRouterWithoutIncludeMatchesNothing(t *testing.T) {
	if NewRouter(config.Tables{}).Match("shop", "orders") {
		t.Error("an empty include list matched a table")
	}
}

func TestRouterMalformedPatternMatchesNothing(t *testing.T) {
	r := NewRouter(config.Tables{Include: []string{"shop.[", "shop.ok"}})
	if r.Match("shop", "x") {
		t.Error("a malformed pattern matched")
	}
	if !r.Match("shop", "ok") {
		t.Error("a valid pattern next to a malformed one stopped working")
	}
}

func TestRouterDatabasesAreTheLiterallyNamedOnes(t *testing.T) {
	r := NewRouter(config.Tables{Include: []string{"shop.*", "shop.orders", "warehouse.inv_*", "sh*.x", "?ix.y", "[ab].z", "nodot"}})
	got := r.Databases()
	want := []string{"shop", "warehouse"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Databases() = %v, want %v: patterns with a wildcard in the database part name no database", got, want)
	}
}
