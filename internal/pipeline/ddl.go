package pipeline

import (
	"regexp"
	"strings"

	"github.com/thealexjo/binwave/internal/ddl"
)

// Recognizing statements that destroy data in the source without producing row events: DROP
// TABLE, DROP DATABASE, TRUNCATE, and partition operations that remove rows. The statement is
// parsed (package ddl); a statement the parser rejects falls back to a keyword scan, so an
// unusual dialect can never make a destructive statement invisible. Both err towards "affected":
// when it cannot tell which tables a statement touches, every table might be.

type destructive struct {
	kind string
	// tables are the qualified "database.table" names the statement touches; nil with any set
	// means the recognizer could not tell.
	tables []string
	// database is the database a DROP DATABASE drops.
	database string
	any      bool
}

var (
	reComments  = regexp.MustCompile(`(?s)^\s*(/\*.*?\*/\s*)+`)
	reDropTable = regexp.MustCompile(`(?is)^drop\s+(?:temporary\s+)?tables?\s+(?:if\s+exists\s+)?(.*?)(?:\s+(?:restrict|cascade))?\s*;?\s*$`)
	reDropDB    = regexp.MustCompile(`(?is)^drop\s+(?:database|schema)\s+(?:if\s+exists\s+)?(.*?)\s*;?\s*$`)
	reTruncate  = regexp.MustCompile(`(?is)^truncate\s+(?:table\s+)?(.*?)\s*;?\s*$`)
	rePartition = regexp.MustCompile(`(?is)^alter\s+(?:online\s+|ignore\s+)*table\s+(` + identRe + `)\s+.*\b(?:drop|truncate|exchange)\s+partition\b`)
	reName      = regexp.MustCompile(`^(?:\x60([^\x60]+)\x60|([\w$]+))(?:\s*\.\s*(?:\x60([^\x60]+)\x60|([\w$]+)))?$`)
)

// identRe matches an optionally qualified identifier; \x60 is the backtick.
const identRe = `(?:\x60[^\x60]+\x60|[\w$]+)(?:\s*\.\s*(?:\x60[^\x60]+\x60|[\w$]+))?`

// classify recognizes a destructive statement. defaultDB qualifies names written without a database.
func classify(query, defaultDB string) (destructive, bool) {
	op, err := ddl.Parse(query, defaultDB)
	if err != nil {
		return classifyByKeywords(query, defaultDB)
	}
	switch o := op.(type) {
	case ddl.DropTable:
		d := destructive{kind: "DROP TABLE"}
		for _, t := range o.Tables {
			d.tables = append(d.tables, t.String())
		}
		return d, true
	case ddl.TruncateTable:
		return destructive{kind: "TRUNCATE", tables: []string{o.Table.String()}}, true
	case ddl.DropDatabase:
		// A dropped database takes every table in it; the statement names none of them.
		return destructive{kind: "DROP DATABASE", database: o.Database}, true
	case ddl.AlterTable:
		for _, spec := range o.Specs {
			if p, ok := spec.(ddl.PartitionOp); ok && p.Destructive {
				return destructive{kind: "partition operation", tables: []string{o.Table.String()}}, true
			}
		}
	}
	return destructive{}, false
}

// classifyByKeywords is classify for statements the parser rejects.
func classifyByKeywords(query, defaultDB string) (destructive, bool) {
	q := strings.TrimSpace(reComments.ReplaceAllString(query, ""))

	if m := reDropTable.FindStringSubmatch(q); m != nil {
		return nameList("DROP TABLE", m[1], defaultDB), true
	}
	if m := reDropDB.FindStringSubmatch(q); m != nil {
		name := strings.Trim(strings.TrimSpace(m[1]), "`")
		if name == "" || strings.ContainsAny(name, " ,`") {
			return destructive{kind: "DROP DATABASE", any: true}, true
		}
		return destructive{kind: "DROP DATABASE", database: name}, true
	}
	if m := reTruncate.FindStringSubmatch(q); m != nil {
		return nameList("TRUNCATE", m[1], defaultDB), true
	}
	if m := rePartition.FindStringSubmatch(q); m != nil {
		return nameList("partition operation", m[1], defaultDB), true
	}
	return destructive{}, false
}

// nameList parses "a, b.c" into qualified names; anything unrecognized makes the result "any".
func nameList(kind, list, defaultDB string) destructive {
	d := destructive{kind: kind}
	for _, part := range strings.Split(list, ",") {
		m := reName.FindStringSubmatch(strings.TrimSpace(part))
		if m == nil {
			return destructive{kind: kind, any: true}
		}
		first := m[1] + m[2]
		second := m[3] + m[4]
		if second == "" {
			d.tables = append(d.tables, defaultDB+"."+first)
		} else {
			d.tables = append(d.tables, first+"."+second)
		}
	}
	return d
}
