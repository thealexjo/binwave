package clickhouse

import "testing"

func TestMarkedFor(t *testing.T) {
	for _, tc := range []struct {
		comment, table string
		want           string
		marked         bool
	}{
		{"", "kv", "", false},
		{"made by hand", "kv__rebuild", "", false},
		{RebuildMarker("kv"), "kv__rebuild", "kv", true},
		{RebuildMarker("kv"), "kv", "kv", true},
		{RebuildMarker("kv__rebuild"), "kv__rebuild", "kv__rebuild", true},
		{RebuildMarker("kv__rebuild"), "kv__rebuild__rebuild", "kv__rebuild", true},
		{ResyncMarker("kv"), "kv__resync", "kv", true},
		{ResyncMarker("kv__resync"), "kv__resync", "kv__resync", true},
		{ResyncMarker("a table of names"), "x", "a table of names", true},
		// The marker of an earlier release, which names no table.
		{rebuildMarker, "kv__rebuild", "kv", true},
		{rebuildMarker, "kv", "kv", true},
		{resyncMarker, "kv__resync", "kv", true},
		{resyncMarker, "kv", "kv", true},
		{rebuildMarker, "kv__resync", "kv__resync", true},
	} {
		got, marked := markedFor(tc.comment, tc.table)
		if got != tc.want || marked != tc.marked {
			t.Errorf("markedFor(%q, %q) = %q, %v, want %q, %v", tc.comment, tc.table, got, marked, tc.want, tc.marked)
		}
	}
}

func TestIsMarker(t *testing.T) {
	for _, tc := range []struct {
		comment, marker string
		want            bool
	}{
		{RebuildMarker("kv"), RebuildMarker("kv"), true},
		{RebuildMarker("kv"), RebuildMarker("kv__rebuild"), false},
		{ResyncMarker("kv"), RebuildMarker("kv"), false},
		{rebuildMarker, RebuildMarker("kv"), true},
		{resyncMarker, ResyncMarker("kv"), true},
		{resyncMarker, RebuildMarker("kv"), false},
		{"", RebuildMarker("kv"), false},
		{RebuildMarker("kv"), "", false},
		{"made by hand", "", false},
	} {
		if got := isMarker(tc.comment, tc.marker); got != tc.want {
			t.Errorf("isMarker(%q, %q) = %v, want %v", tc.comment, tc.marker, got, tc.want)
		}
	}
}
