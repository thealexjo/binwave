// Package failure describes a replication failure for the operator: which transaction, which
// statement or row, and why. It is the one type that the pipeline fills, the status of the service
// carries, and the commands of `binwave replication` print, so that a failure can be debugged and,
// when the operator decides so, skipped.
//
// The values of a row can be personal data. Logs, /status on the metrics port and error messages
// carry the Redacted form, which names the key columns and no value; the whole form is only
// served on the control socket, which belongs to the service's user, to `binwave replication status`.
package failure

import "time"

// Kinds of failure.
const (
	// KindRows is a change of rows that could not be applied.
	KindRows = "rows"
	// KindDDL is a schema statement that could not be applied.
	KindDDL = "ddl"
	// KindWrite is a write of buffered rows that ClickHouse refused. The rows come from several
	// transactions, so it is tied to none of them: it cannot be skipped, only fixed.
	KindWrite = "write"
)

// Failure is what could not be replicated.
type Failure struct {
	Time time.Time `json:"time"`
	// GTID is the transaction that failed; empty for KindWrite.
	GTID string `json:"gtid,omitempty"`
	Kind string `json:"kind"`
	// Code identifies the kind of the problem (docs/reference/error-codes.md); empty when the
	// underlying error is not one of the causes errcode recognizes yet.
	Code string `json:"code,omitempty"`
	// Table is the source table, "database.table".
	Table string `json:"table,omitempty"`
	// Op is insert, update or delete for KindRows.
	Op string `json:"op,omitempty"`
	// Rows is the size of the batch of rows the failure is in.
	Rows int `json:"rows,omitempty"`
	// Statement is the DDL statement for KindDDL.
	Statement string `json:"statement,omitempty"`
	// Key names what identifies the row: "PRIMARY KEY (id)", or "no primary key".
	Key string `json:"key,omitempty"`
	// KeyValues are the values of that key in the failing row. Personal data: see Redacted.
	KeyValues []string `json:"key_values,omitempty"`
	// Message is why, without any value.
	Message string `json:"message"`
	// Detail is the reason with the values involved, when there are any. Personal data.
	Detail string `json:"detail,omitempty"`
}

// Redacted returns the failure without the values of a row: safe for logs and for the metrics port.
func (f Failure) Redacted() Failure {
	f.KeyValues = nil
	f.Detail = ""
	return f
}

// Skippable reports whether the failure is tied to a transaction that can be skipped.
func (f Failure) Skippable() bool { return f.GTID != "" }
