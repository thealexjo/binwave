package clickhouse

import "strings"

// The comment of a table binwave keeps for a job (the copy of a rebuild, the shadow table of a resync)
// is a marker: it says the table is binwave's and names the table the job is for. EXCHANGE TABLES moves
// the comment with the table, so after the swap the marker is on the table that took the place of the
// original, and names it, until the marker is removed.
const (
	rebuildMarker = "binwave: rebuild"
	resyncMarker  = "binwave: resync shadow"
)

// RebuildMarker is the comment of the copy a rebuild of table fills.
func RebuildMarker(table string) string { return rebuildMarker + " of " + table }

// ResyncMarker is the comment of the shadow table of a resync of table.
func ResyncMarker(table string) string { return resyncMarker + " of " + table }

// isMarker reports whether comment is the marker. A marker of an earlier release names no table, and is
// taken for the marker of any.
func isMarker(comment, marker string) bool {
	kind, _, _ := strings.Cut(marker, " of ")
	return comment == marker || comment == kind
}

// markedFor returns the table the marker in the comment of table is for, and false when the comment is
// not a marker. A marker of an earlier release names no table: on a table named as a copy (<t>__rebuild,
// <t>__resync) it is the copy of t, on another table it was carried there by the swap of its own job.
func markedFor(comment, table string) (string, bool) {
	for _, m := range []struct{ kind, suffix string }{{rebuildMarker, rebuildSuffix}, {resyncMarker, resyncSuffix}} {
		if comment == m.kind {
			return strings.TrimSuffix(table, m.suffix), true
		}
		if of, ok := strings.CutPrefix(comment, m.kind+" of "); ok {
			return of, true
		}
	}
	return "", false
}
