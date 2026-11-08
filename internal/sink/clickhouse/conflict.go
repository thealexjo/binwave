package clickhouse

import "errors"

// ConflictError is a state of ClickHouse that must not exist when the source does what it did:
// a table that a rename would overwrite, a column that a rename would duplicate, a table that
// the source replaced with one binwave never saw. binwave does not overwrite, drop or guess, and
// setting the table aside would hide that ClickHouse and the source disagree, so the service
// stops, and restarting hits the same statement: the operator has to change ClickHouse or the
// configuration first.
type ConflictError struct {
	Err error
	// Detail is what ClickHouse said, which can hold a value of a row: personal data, so it is for the
	// operator's status command and never for a log.
	Detail string
}

func (e *ConflictError) Error() string { return e.Err.Error() }
func (e *ConflictError) Unwrap() error { return e.Err }

// IsConflict reports whether err is a *ConflictError.
func IsConflict(err error) bool {
	var ce *ConflictError
	return errors.As(err, &ce)
}
