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
		// With the UUID of the replaced table.
		{Replacing(ResyncMarker("kv"), "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a", "0c000000-0000-0000-0000-00000000000c"), "kv", "kv", true},
		{Replacing(resyncMarker, "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a", "0c000000-0000-0000-0000-00000000000c"), "kv__resync", "kv", true},
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
		{Replacing(RebuildMarker("kv"), "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a", "0c000000-0000-0000-0000-00000000000c"), RebuildMarker("kv"), true},
		{Replacing(RebuildMarker("kv"), "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a", "0c000000-0000-0000-0000-00000000000c"), RebuildMarker("kv__rebuild"), false},
		{RebuildMarker("kv") + ", replacing something", RebuildMarker("kv"), false},
	} {
		if got := isMarker(tc.comment, tc.marker); got != tc.want {
			t.Errorf("isMarker(%q, %q) = %v, want %v", tc.comment, tc.marker, got, tc.want)
		}
	}
}

func TestReplacedBy(t *testing.T) {
	for _, tc := range []struct {
		comment string
		want    string
		ok      bool
	}{
		{Replacing(ResyncMarker("kv"), "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a", "0c000000-0000-0000-0000-00000000000c"), "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a", true},
		{Replacing(ResyncMarker("a, replacing b"), "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a", "0c000000-0000-0000-0000-00000000000c"), "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a", true},
		{ResyncMarker("kv"), "", false},
		{resyncMarker, "", false},
		{Replacing(ResyncMarker("kv"), zeroUUID, "0c000000-0000-0000-0000-00000000000c"), "", false},
		{Replacing(ResyncMarker("kv"), "not-a-uuid", "0c000000-0000-0000-0000-00000000000c"), "", false},
		{Replacing(ResyncMarker("kv"), "0D6C6A4E-8F1A-4C2E-9B7A-1F2E3D4C5B6A", "0c000000-0000-0000-0000-00000000000c"), "", false},
		{ResyncMarker("kv replacing 0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a"), "", false},
	} {
		if got, ok := replacedBy(tc.comment); got != tc.want || ok != tc.ok {
			t.Errorf("replacedBy(%q) = %q, %v, want %q, %v", tc.comment, got, ok, tc.want, tc.ok)
		}
	}
}

// TestAMarkerReadsBackAnyTableName: the name of the table is the last part of a marker, so a name that
// looks like the other parts is read back whole, with and without the UUID of the replaced table.
func TestAMarkerReadsBackAnyTableName(t *testing.T) {
	const uuid = "0d6c6a4e-8f1a-4c2e-9b7a-1f2e3d4c5b6a"
	for _, name := range []string{
		"orders", "orders, replacing " + uuid, "orders replacing " + uuid, "x replacing " + uuid + " of y",
		"of", " of ", "a of b", "replacing", "", "binwave: resync shadow",
	} {
		for _, m := range []string{ResyncMarker(name), RebuildMarker(name)} {
			for _, comment := range []string{m, Replacing(m, uuid, "0c000000-0000-0000-0000-00000000000c")} {
				if !isMarker(comment, m) {
					t.Errorf("isMarker(%q, %q) = false", comment, m)
				}
				if of, ok := markedFor(comment, "t"); !ok || of != name {
					t.Errorf("markedFor(%q) = %q, %v, want %q", comment, of, ok, name)
				}
				if got, ok := replacedBy(comment); (comment != m) != ok || (ok && got != uuid) {
					t.Errorf("replacedBy(%q) = %q, %v", comment, got, ok)
				}
			}
			if isMarker(m, ResyncMarker(name+"x")) || isMarker(m, RebuildMarker(name+"x")) {
				t.Errorf("%q is taken for the marker of another table", m)
			}
		}
	}
}
