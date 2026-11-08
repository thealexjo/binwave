package schema

import (
	"fmt"
	"strings"

	"github.com/thealexjo/binwave/internal/typemap"
)

// Table is the source-neutral description of one MySQL table as seen in the binlog.
type Table struct {
	Database string
	Name     string
	Columns  []typemap.Column
	// PrimaryKey holds indexes into Columns in key order; empty when the table has no primary key.
	PrimaryKey []int
	// Partitions are the names of the leaf partitions (a subpartition where there are subpartitions),
	// in the order the table declares them; empty for a table that is not partitioned. Only the
	// definition read from information_schema carries them: the binlog does not describe partitions,
	// and nothing but a chunked read needs them. They are deliberately NOT part of Signature: a
	// partition added to a table (the next month of a table partitioned by date) is not a change of
	// its structure, and treating it as one would throw away a snapshot that is already half read.
	Partitions []string
}

// FullName renders "database.table", the form used by include/exclude patterns and overrides.
func (t *Table) FullName() string { return t.Database + "." + t.Name }

// ColumnIndex returns the index of the column with the given name, or -1.
func (t *Table) ColumnIndex(name string) int {
	for i, c := range t.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// PrimaryKeyColumns returns the names of the primary key columns in key order.
func (t *Table) PrimaryKeyColumns() []string {
	names := make([]string, len(t.PrimaryKey))
	for i, idx := range t.PrimaryKey {
		names[i] = t.Columns[idx].Name
	}
	return names
}

// Signature is a canonical text form of the table definition. Two tables with equal signatures
// map to the same ClickHouse structure, so a changed signature means a schema change.
func (t *Table) Signature() string {
	var b strings.Builder
	b.WriteString(t.FullName())
	for _, c := range t.Columns {
		fmt.Fprintf(&b, "\n%s %s", c.Name, c.SQLType())
	}
	fmt.Fprintf(&b, "\nPRIMARY KEY (%s)", strings.Join(t.PrimaryKeyColumns(), ","))
	return b.String()
}
