package pipeline

import (
	"testing"

	"github.com/thealexjo/binwave/internal/config"
)

func TestRouterFollowsTheShadowTablesOfOnlineSchemaChanges(t *testing.T) {
	base := config.Tables{Include: []string{"shop.orders", "shop.a_b", "shop._x", "shop.big_*"}, Exclude: []string{"shop.*_skipped"}}
	for _, tt := range []struct {
		table string
		want  bool
	}{
		// pt-online-schema-change: the new table, with an underscore more for every name taken, and the old one.
		{"_orders_new", true},
		{"__orders_new", true},
		{"__________orders_new", true}, // ten underscores
		{"___________orders_new", false},
		{"_orders_old", true},
		// gh-ost: ghost and old table, also with --timestamp-old-table.
		{"_orders_gho", true},
		{"_orders_del", true},
		{"_orders_20260921120000_del", true},
		{"_orders_2026092112_del", false},
		// Names with underscores, and a base that starts with one.
		{"_a_b_new", true},
		{"__x_new", true},
		{"__x_gho", true},
		{"_x_gho", false}, // that is the shadow of x, which is not selected
		{"_big_table_gho", true},
		// What is not a shadow: the service tables of gh-ost, other names, other bases.
		{"_orders_ghc", false},
		{"_orders_ghk", false},
		{"orders_new", false},
		{"orders_old", false},
		{"_orders_bak", false},
		{"_other_new", false},
		{"_new", false},
		{"__new", false},
		// Excluded: the base is not replicated, so neither is its shadow, and a shadow that an
		// exclude pattern names is not either.
		{"_big_skipped_gho", false},
		{"_big_x_skipped_new", false},
	} {
		t.Run(tt.table, func(t *testing.T) {
			r := NewRouter(base)
			if got := r.Match("shop", tt.table); got != tt.want {
				t.Errorf("Match(shop.%s) = %v, want %v", tt.table, got, tt.want)
			}
			if got := r.IsShadow("shop", tt.table); got != tt.want {
				t.Errorf("IsShadow(shop.%s) = %v, want %v", tt.table, got, tt.want)
			}
		})
	}
}

func TestExplicitlySelectedTablesAreNeverShadows(t *testing.T) {
	r := NewRouter(config.Tables{Include: []string{"shop.orders", "shop._orders_old"}})
	if !r.Match("shop", "_orders_old") || r.IsShadow("shop", "_orders_old") {
		t.Error("a table that include names is an ordinary table: its DROP is mirrored like any other")
	}
	if r.Match("other", "_orders_new") {
		t.Error("the shadow of a table of another database matched")
	}
}
