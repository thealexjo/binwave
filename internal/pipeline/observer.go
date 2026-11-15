package pipeline

import (
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/snapshot"
)

// Observer receives what happens in the pipeline, for metrics and status. It is called on the
// pipeline's goroutine and must not block. Table names are passed even where the current
// implementation only counts, so that per-table statistics can be added without touching the
// pipeline.
type Observer interface {
	// TransactionBegan is called when a transaction starts; commit is when the source committed it.
	TransactionBegan(commit time.Time)
	// TransactionCommitted is called when the end of a transaction has been read.
	TransactionCommitted()
	// RebuildStarted and RebuildFinished bracket the rebuild of a table under a new sort key, while
	// which no table is replicated.
	RebuildStarted(table string)
	RebuildFinished(table string, ok bool)
	// TransactionSkipped is called for a transaction dropped on the operator's request.
	TransactionSkipped()
	// RowsRead is called with the number of ClickHouse rows a change of table produced.
	RowsRead(table string, rows int)
	// BufferChanged reports how much is waiting in memory to be written.
	BufferChanged(rows, bytes int)
	// Inserted is called after ClickHouse accepted a batch of table.
	Inserted(table string, rows int, took time.Duration)
	// PositionSaved is called after the position was saved. applied is when the source committed
	// the last transaction the position covers; open is when it committed the transaction that is
	// still being read (zero if none), whose rows may already be written but not covered.
	PositionSaved(pos checkpoint.Position, applied, open time.Time)
	// PositionLoaded is called with the position replication starts from, saved before or decided now.
	PositionLoaded(pos checkpoint.Position)
	// Idle is called when the source had nothing to send and everything read is written and saved.
	Idle()
	// Retry is called when a ClickHouse request failed and is going to be retried. reason is
	// ReasonOverload or ReasonTransient.
	Retry(operation, reason string, err error)
	// SourceRetry is called when a read of the source for a resync or the initial load failed and is
	// going to be retried. Unlike the other methods it may be called from the goroutine reading a table.
	SourceRetry(operation string, err error)
	// Recovered is called when a request that had been retried finally succeeded. Without it a run of
	// failures has no end: the status would keep showing the last one as if it were still happening.
	Recovered()
	// TablesSelected reports how many source tables the configuration selects (see sourceTables).
	// Unlike the other methods it may be called from the goroutine of the check after the start.
	TablesSelected(n int)
	// RowsSkipped is called with the rows of a change that was dropped because its table is
	// quarantined.
	RowsSkipped(table string, rows int)
	// QuarantineChanged reports the tables that are quarantined, after every change and at start.
	QuarantineChanged(entries []checkpoint.Quarantined)
	// ResyncChanged reports the resyncs that are running, after every chunk written and every change of the set.
	ResyncChanged(statuses []snapshot.Status)
	// ResyncRowsRead is called with the rows of every chunk a resync or the initial load read and
	// buffered for its shadow table.
	ResyncRowsRead(table string, rows int)
	// ResyncFinished is called when a resync ended: ok when the table was replaced by its copy, otherwise
	// the resync was given up or discarded.
	ResyncFinished(table string, ok bool)
	// AutoresyncResolved is called when a resync tables.autoresync began on its own (without an
	// operator) ends: ok when it finished and the table replicates again, false when it failed
	// (attempts counts how many times, including this one, autoresync has tried this table). A
	// table tables.autoresync fixed is otherwise indistinguishable from one that always replicated
	// fine, so this is the only way to know it happened.
	AutoresyncResolved(table string, ok bool, attempts int)
	// DestructiveDDLSkipped is called when the source dropped or truncated replicated data and
	// ddl.mode additive, or a partition operation, left ClickHouse alone.
	DestructiveDDLSkipped()
}

// Retry reasons.
const (
	ReasonOverload  = "overload"
	ReasonTransient = "transient"
)

type nopObserver struct{}

func (nopObserver) TransactionBegan(time.Time)                              {}
func (nopObserver) TransactionCommitted()                                   {}
func (nopObserver) TransactionSkipped()                                     {}
func (nopObserver) RebuildStarted(string)                                   {}
func (nopObserver) RebuildFinished(string, bool)                            {}
func (nopObserver) RowsRead(string, int)                                    {}
func (nopObserver) BufferChanged(int, int)                                  {}
func (nopObserver) Inserted(string, int, time.Duration)                     {}
func (nopObserver) PositionSaved(checkpoint.Position, time.Time, time.Time) {}
func (nopObserver) PositionLoaded(checkpoint.Position)                      {}
func (nopObserver) Idle()                                                   {}
func (nopObserver) Retry(string, string, error)                             {}
func (nopObserver) SourceRetry(string, error)                               {}
func (nopObserver) Recovered()                                              {}
func (nopObserver) TablesSelected(int)                                      {}
func (nopObserver) ResyncChanged([]snapshot.Status)                         {}
func (nopObserver) ResyncRowsRead(string, int)                              {}
func (nopObserver) ResyncFinished(string, bool)                             {}
func (nopObserver) AutoresyncResolved(string, bool, int)                    {}
func (nopObserver) DestructiveDDLSkipped()                                  {}
func (nopObserver) RowsSkipped(string, int)                                 {}
func (nopObserver) QuarantineChanged([]checkpoint.Quarantined)              {}
