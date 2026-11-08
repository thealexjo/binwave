// Package event defines the change events that flow from a source into the pipeline.
// A source emits, per transaction: Begin, zero or more Rows or DDL, then Commit. A DDL statement
// is a transaction of its own: Begin, DDL, Commit.
package event

import (
	"time"

	"github.com/thealexjo/binwave/internal/schema"
)

// Event is one item of the change stream.
type Event interface{ isEvent() }

// Begin opens a transaction.
type Begin struct {
	// GTID identifies the transaction as "source_uuid:sequence".
	GTID string
	// CommitTime is when the transaction was committed on the origin server.
	CommitTime time.Time
}

// Op is the kind of a row change.
type Op int

// Row operations.
const (
	Insert Op = iota + 1
	Update
	Delete
)

func (o Op) String() string {
	switch o {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	default:
		return "unknown"
	}
}

// Row is one changed row. Values follow the table's column order and are raw source values,
// in the forms documented on typemap.Mapping.Convert.
type Row struct {
	// Before is the row image before an Update or Delete; nil for Insert.
	Before []any
	// After is the row image after an Insert or Update; nil for Delete.
	After []any
}

// Rows is a batch of changes of one kind to one table within a transaction.
type Rows struct {
	Table *schema.Table
	Op    Op
	Rows  []Row
}

// DDL is a schema statement seen in the stream.
type DDL struct {
	Database string
	Query    string
	// Time is when the source began to execute the statement, to the second. It is what
	// CURRENT_TIMESTAMP evaluates to in the statement.
	Time time.Time
}

// Commit closes a transaction. GTIDSet is the executed GTID set including it, which is the
// position to persist once everything up to here is durably written.
type Commit struct {
	GTIDSet string
}

// Heartbeat proves the source is alive while nothing changes.
type Heartbeat struct{}

func (Begin) isEvent()     {}
func (Rows) isEvent()      {}
func (DDL) isEvent()       {}
func (Commit) isEvent()    {}
func (Heartbeat) isEvent() {}
