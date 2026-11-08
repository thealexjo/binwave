package clickhouse

import (
	"errors"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// TableError is a problem of one table that no retry and no other table's success can change: it
// has no usable key, a column the client cannot insert, a structure that does not match the
// ClickHouse table, a table or column that is gone. The pipeline sets such a table aside (or
// stops, as tables.on_replication_error says) instead of treating it as a failure of the
// service. Err is the message for the operator.
type TableError struct {
	Table string
	Err   error
	// Missing marks a table or column that does not exist in ClickHouse and that binwave did not
	// create.
	// ddl.on_missing_structure decides what happens to it.
	Missing bool
	// Recoverable marks a problem that resynchronizing the table fixes on its own: a value that
	// existing rows should get for a new column and that a statement's own reasoning would have
	// given them (a generated column, an expression default, AUTO_INCREMENT), or a type change
	// MySQL itself recalculates by rules binwave does not reproduce. A resync needs no guessing for
	// either: it rereads the table from the structure it has now. tables.autoresync acts on this.
	Recoverable bool
}

func (e *TableError) Error() string { return e.Err.Error() }
func (e *TableError) Unwrap() error { return e.Err }

// IsMissingTable reports whether err says that the ClickHouse table is missing.
func IsMissingTable(err error) bool {
	var te *TableError
	return errors.As(err, &te) && te.Missing
}

// IsTableProblem reports whether err is a problem of a single table: a *TableError, or a
// *SchemaMismatchError.
func IsTableProblem(err error) bool {
	var te *TableError
	var sm *SchemaMismatchError
	return errors.As(err, &te) || errors.As(err, &sm)
}

// Recoverable reports whether resynchronizing the table (binwave tables resync) fixes the problem
// err describes on its own, without an operator deciding anything: a *SchemaMismatchError always
// does (a resync rereads the structure the table has now, whatever changed it), and a *TableError
// does when it says so (TableError.Recoverable). tables.autoresync acts on this.
func Recoverable(err error) bool {
	var sm *SchemaMismatchError
	if errors.As(err, &sm) {
		return true
	}
	var te *TableError
	return errors.As(err, &te) && te.Recoverable
}

// missingTableCode is the exception for a table that does not exist.
const missingTableCode = 60

// tableCodes are the server exceptions that say a table or its column is not what the insert
// expected. They come from preparing an insert; an outage or an overload looks different.
var tableCodes = map[int32]bool{
	16: true, // NO_SUCH_COLUMN_IN_TABLE
	47: true, // UNKNOWN_IDENTIFIER
	60: true, // UNKNOWN_TABLE
}

func isMissingTableException(err error) bool {
	var exception *ch.Exception
	return errors.As(err, &exception) && exception.Code == missingTableCode
}

func isTableException(err error) bool {
	var exception *ch.Exception
	return errors.As(err, &exception) && tableCodes[exception.Code]
}
