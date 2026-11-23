package clickhouse

import "strings"

// The comment of a table binwave keeps for a job (the copy of a rebuild, the shadow table of a resync)
// is a marker: it says the table is binwave's and names the table the job is for. EXCHANGE TABLES moves
// the comment with the table, so after the swap the marker is on the table that took the place of the
// original, and names it, until the marker is removed.
//
// A marker is the kind, then, from right before the exchange, the UUIDs of the table the job replaces
// and of the copy itself, then the name of the table, last, so that any name is read back whole:
//
//	binwave: resync shadow of <table>
//	binwave: resync shadow replacing <uuid> with <uuid> of <table>
//
// A marker of an earlier release is the kind alone.
const (
	rebuildMarker = "binwave: rebuild"
	resyncMarker  = "binwave: resync shadow"

	replacingPart = " replacing "
	withPart      = " with "
	ofPart        = " of "
)

// zeroUUID is the UUID of a table in a database that gives its tables none.
const zeroUUID = "00000000-0000-0000-0000-000000000000"

// RebuildMarker is the comment of the copy a rebuild of table fills.
func RebuildMarker(table string) string { return rebuildMarker + ofPart + table }

// ResyncMarker is the comment of the shadow table of a resync of table.
func ResyncMarker(table string) string { return resyncMarker + ofPart + table }

// Replacing is the marker that also names the UUIDs of the table the job replaces and of the copy. It is
// put on the copy right before the exchange and travels with it: after the exchange, it tells the old
// table, now under the name of the copy, from a table that took that name later, and the copy from a
// table that took its place.
func Replacing(marker, replaced, copied string) string {
	m, ok := parseMarker(marker)
	if !ok {
		return marker
	}
	m.replaced, m.copied = replaced, copied
	return m.String()
}

type parsedMarker struct {
	kind     string
	replaced string // the UUID of the table the job replaces, once named
	copied   string // the UUID of the copy, named with it
	of       string // the table the job is for
	named    bool   // the marker names the table (all but those of an earlier release)
}

func (m parsedMarker) String() string {
	s := m.kind
	if m.replaced != "" {
		s += replacingPart + m.replaced + withPart + m.copied
	}
	if m.named {
		s += ofPart + m.of
	}
	return s
}

// parseMarker reads a comment as a marker.
func parseMarker(comment string) (parsedMarker, bool) {
	for _, kind := range []string{rebuildMarker, resyncMarker} {
		rest, ok := strings.CutPrefix(comment, kind)
		if !ok {
			continue
		}
		m := parsedMarker{kind: kind}
		if after, ok := strings.CutPrefix(rest, replacingPart); ok {
			const n = 36 + len(withPart) + 36
			if len(after) < n || !isUUID(after[:36]) || after[36:36+len(withPart)] != withPart || !isUUID(after[n-36:n]) {
				return parsedMarker{}, false
			}
			m.replaced, m.copied, rest = after[:36], after[n-36:n], after[n:]
		}
		if rest == "" {
			return m, true
		}
		if of, ok := strings.CutPrefix(rest, ofPart); ok {
			m.of, m.named = of, true
			return m, true
		}
		return parsedMarker{}, false
	}
	return parsedMarker{}, false
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, r := range s {
		switch {
		case i == 8 || i == 13 || i == 18 || i == 23:
			if r != '-' {
				return false
			}
		case (r < '0' || r > '9') && (r < 'a' || r > 'f'):
			return false
		}
	}
	return true
}

// replacedBy returns the UUID of the table the marker in comment says its job replaces.
func replacedBy(comment string) (string, bool) {
	m, ok := parseMarker(comment)
	if !ok || m.replaced == "" || m.replaced == zeroUUID {
		return "", false
	}
	return m.replaced, true
}

// ownMarker reports whether a table carries the marker as the copy of the job: a marker that names the
// UUID of another copy was put on the table under the name of the copy while it was another.
func ownMarker(t swapTable, marker string) bool {
	if !t.exists || !isMarker(t.comment, marker) {
		return false
	}
	m, _ := parseMarker(t.comment)
	return m.copied == "" || m.copied == t.uuid && t.uuid != zeroUUID
}

// isCopy reports whether a table is the copy of the job: its own marker, and the UUID the job knows the
// copy by, when it knows one.
func isCopy(t swapTable, marker, uuid string) bool {
	return ownMarker(t, marker) && (uuid == "" || t.uuid == uuid)
}

// isMarker reports whether comment is the marker, with or without the UUID of the replaced table. A
// marker of an earlier release names no table, and is taken for the marker of any.
func isMarker(comment, want string) bool {
	c, ok := parseMarker(comment)
	w, wok := parseMarker(want)
	if !ok || !wok || c.kind != w.kind {
		return false
	}
	return !c.named || (w.named && c.of == w.of)
}

// markedFor returns the table the marker in the comment of table is for, and false when the comment is
// not a marker. A marker of an earlier release names no table: on a table named as a copy (<t>__rebuild,
// <t>__resync) it is the copy of t, on another table it was carried there by the swap of its own job.
func markedFor(comment, table string) (string, bool) {
	m, ok := parseMarker(comment)
	if !ok {
		return "", false
	}
	if m.named {
		return m.of, true
	}
	suffix := rebuildSuffix
	if m.kind == resyncMarker {
		suffix = resyncSuffix
	}
	return strings.TrimSuffix(table, suffix), true
}
