// Package ddl parses the DDL statements of the source's binlog into a small model of what they do
// to tables and columns. It answers "what did the statement intend", which the table structure
// in the binlog cannot: renaming a column and dropping one while adding another look the same
// there, but keep the data in one case and lose it in the other.
package ddl

import "fmt"

// Name is a qualified table name.
type Name struct{ Database, Table string }

func (n Name) String() string { return n.Database + "." + n.Table }

// Op is what a statement does. It is one of the types below.
type Op interface{ isOp() }

// CreateTable is CREATE TABLE. Like is set for CREATE TABLE ... LIKE, which copies the structure
// of another table (pt-online-schema-change and gh-ost create their shadow tables this way).
type CreateTable struct {
	Table       Name
	Like        *Name
	IfNotExists bool
	Temporary   bool
}

// DropTable is DROP TABLE.
type DropTable struct {
	Tables    []Name
	IfExists  bool
	Temporary bool
}

// TruncateTable is TRUNCATE TABLE.
type TruncateTable struct{ Table Name }

// DropDatabase is DROP DATABASE.
type DropDatabase struct {
	Database string
	IfExists bool
}

// Rename is one pair of a RENAME TABLE statement.
type Rename struct{ From, To Name }

// RenameTable is RENAME TABLE a TO b [, c TO d ...]. The pairs are applied in order, so a swap
// (a to old, new to a) is two pairs.
type RenameTable struct{ Pairs []Rename }

// AlterTable is ALTER TABLE with its specifications in the order written.
type AlterTable struct {
	Table Name
	Specs []Spec
}

// Other is a statement that does not change a table's columns or existence: an index, a view, a
// trigger, a user, a session setting.
type Other struct{ Kind string }

func (CreateTable) isOp()   {}
func (DropTable) isOp()     {}
func (TruncateTable) isOp() {}
func (DropDatabase) isOp()  {}
func (RenameTable) isOp()   {}
func (AlterTable) isOp()    {}
func (Other) isOp()         {}

// Spec is one specification of an ALTER TABLE.
type Spec interface{ isSpec() }

// Position says where a new or modified column goes: at the end (the zero value), first, or after
// another column.
type Position struct {
	First bool
	After string
}

// DefaultKind classifies a column default.
type DefaultKind int

// Default kinds.
const (
	// DefaultNone means the column has no DEFAULT clause.
	DefaultNone DefaultKind = iota
	// DefaultNull is DEFAULT NULL.
	DefaultNull
	// DefaultLiteral is a constant; Text holds its value without quotes.
	DefaultLiteral
	// DefaultExpression is anything else, such as CURRENT_TIMESTAMP or (expr); Text holds its SQL.
	DefaultExpression
)

// Default is the DEFAULT clause of a column.
type Default struct {
	Kind DefaultKind
	Text string
}

// DataType is the type of a column as the statement declares it.
type DataType struct {
	// Kind is the MySQL type: TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL,
	// YEAR, DATE, DATETIME, TIMESTAMP, TIME, CHAR, VARCHAR, TEXT, BINARY, VARBINARY, BLOB, ENUM,
	// SET, BIT, JSON, GEOMETRY. Empty when the parser reports a type this package does not know.
	Kind     string
	Unsigned bool
	// Length is the declared length: characters for CHAR and VARCHAR, bytes for BINARY and
	// VARBINARY, bits for BIT; -1 when the statement does not say.
	Length int
	// Precision and Scale apply to DECIMAL; Fsp is the fractional seconds precision of DATETIME,
	// TIMESTAMP and TIME. -1 when the statement does not say.
	Precision, Scale, Fsp int
	// Members are the ENUM or SET members in declaration order.
	Members []string
	Charset string
}

// Column is a column definition, as far as the replication needs it: what the type is, and what
// existing rows receive when the column is added, which only the statement knows.
type Column struct {
	Name string
	// Type is the type as the parser renders it, for messages; DataType is the same, structured.
	Type          string
	DataType      DataType
	Nullable      bool
	Default       Default
	AutoIncrement bool
	Generated     bool
	// PrimaryKey is set for an inline PRIMARY KEY.
	PrimaryKey bool
}

// AddColumns is ADD COLUMN, one or several columns.
type AddColumns struct {
	Columns  []Column
	Position Position
}

// DropColumn is DROP COLUMN.
type DropColumn struct{ Name string }

// ModifyColumn is MODIFY COLUMN: the column keeps its name.
type ModifyColumn struct {
	Column   Column
	Position Position
}

// ChangeColumn is CHANGE COLUMN old new definition: a rename when the names differ.
type ChangeColumn struct {
	Old      string
	Column   Column
	Position Position
}

// RenameColumn is RENAME COLUMN old TO new.
type RenameColumn struct{ Old, New string }

// RenameTo is ALTER TABLE ... RENAME TO name.
type RenameTo struct{ Table Name }

// DropPrimaryKey is DROP PRIMARY KEY.
type DropPrimaryKey struct{}

// AddPrimaryKey is ADD PRIMARY KEY (columns), also as a constraint.
type AddPrimaryKey struct{ Columns []string }

// PartitionOp is an operation on partitions. Destructive ones remove rows without row events.
type PartitionOp struct {
	Kind        string
	Destructive bool
}

// Ignored is a specification that changes nothing replication cares about: indexes, foreign
// keys, table options, defaults of future rows, algorithm and lock hints.
type Ignored struct{ What string }

// Unsupported is a specification this package does not understand. Callers must not assume it is
// harmless.
type Unsupported struct{ What string }

func (AddColumns) isSpec()     {}
func (DropColumn) isSpec()     {}
func (ModifyColumn) isSpec()   {}
func (ChangeColumn) isSpec()   {}
func (RenameColumn) isSpec()   {}
func (RenameTo) isSpec()       {}
func (DropPrimaryKey) isSpec() {}
func (AddPrimaryKey) isSpec()  {}
func (PartitionOp) isSpec()    {}
func (Ignored) isSpec()        {}
func (Unsupported) isSpec()    {}

// ParseError is returned for a statement the parser rejects.
type ParseError struct {
	Query string
	Err   error
}

func (e *ParseError) Error() string { return fmt.Sprintf("cannot parse DDL statement: %v", e.Err) }
func (e *ParseError) Unwrap() error { return e.Err }
