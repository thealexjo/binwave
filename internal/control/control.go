// Package control is the channel an operator uses to talk to a running service: pause and
// resume replication, ask for the status, list and release quarantined tables. It is HTTP with
// JSON over a Unix socket that only the service's user can use, so nothing is exposed to the
// network and no authentication is needed. The commands of `binwave replication` and
// `binwave tables` are its client.
package control

import (
	"context"
	"errors"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/observability"
)

// ErrStarting is what the backend answers while the service has not finished starting: there is
// nothing to pause yet.
var ErrStarting = errors.New("the service is still starting")

// Backend is what the service offers to the control channel.
type Backend interface {
	// Pause stops replication and waits until it has stopped; the service goes on running.
	Pause(ctx context.Context) error
	// Resume starts replication again from the saved position, dropping the transactions the
	// request names.
	Resume(ctx context.Context, req ResumeRequest) error
	// Status describes the service.
	Status() (observability.Status, error)
	// Quarantined lists the tables that are set aside.
	Quarantined(ctx context.Context) ([]checkpoint.Quarantined, error)
	// Release takes tables out of quarantine. unknown are those that were not in it.
	Release(ctx context.Context, tables []string) (released, unknown []string, err error)
	// Resync reads tables again from the source and replaces their ClickHouse tables with the copy.
	// It returns when each resync has begun, not when it has ended; `status` shows how it goes.
	Resync(ctx context.Context, tables []string) (results []ResyncResult, err error)
	// SnapshotRate changes the combined speed limit of the initial load while it runs; 0 removes it.
	// It is not saved: after a restart the limit is the configured replication.snapshot.max_rows_per_second again.
	SnapshotRate(ctx context.Context, rowsPerSecond int) error
	// ChunkSize fixes how many rows a chunk of a snapshot read asks for, while the reads run; 0 or
	// less returns to adaptive sizing. Unlike SnapshotRate it applies to every snapshot read, a
	// `tables resync` included, and it is not saved either.
	ChunkSize(ctx context.Context, rows int) error
}

// snapshotRateRequest is the body of POST /v1/replication/snapshot-rate.
type snapshotRateRequest struct {
	RowsPerSecond int `json:"rows_per_second"`
}

// chunkSizeRequest is the body of POST /v1/replication/chunk-size. rows of 0 asks for adaptive sizing.
type chunkSizeRequest struct {
	Rows int `json:"rows"`
}

// ResyncResult is the outcome of the request to resynchronize one table.
type ResyncResult struct {
	Table string `json:"table"`
	// Error is why the resync could not begin; empty when it began.
	Error string `json:"error,omitempty"`
}

// resyncRequest and resyncResponse are the body of POST /v1/tables/resync.
type resyncRequest struct {
	Tables []string `json:"tables"`
}

type resyncResponse struct {
	Results []ResyncResult `json:"results"`
}

// ResumeRequest is the body of POST /v1/resume. Both fields are optional: without them replication
// starts again and tries the failed transaction once more.
type ResumeRequest struct {
	// SkipTransactions drops that many transactions, starting with the one that failed, like
	// MySQL's sql_replica_skip_counter.
	SkipTransactions int `json:"skip_transactions,omitempty"`
	// GTIDNext drops the transaction with that GTID, like the empty transaction injected with
	// SET GTID_NEXT in MySQL.
	GTIDNext string `json:"gtid_next,omitempty"`
}

// Skips reports whether the request drops anything.
func (r ResumeRequest) Skips() bool { return r.SkipTransactions > 0 || r.GTIDNext != "" }

// releaseRequest and releaseResponse are the body of POST /v1/tables/release.
type releaseRequest struct {
	Tables []string `json:"tables"`
	// AcceptDataGap says the caller knows that the changes made while the tables were
	// quarantined are missing in ClickHouse and are not brought back by releasing them.
	AcceptDataGap bool `json:"accept_data_gap"`
}

// ErrDataGapNotAccepted is answered to a release that does not acknowledge the data gap.
var ErrDataGapNotAccepted = errors.New("releasing a table does not bring back the changes made while it was quarantined; " +
	"they stay missing in ClickHouse. Acknowledge it with --accept-data-gap")

type releaseResponse struct {
	Released []string `json:"released"`
	Unknown  []string `json:"unknown"`
}

// errorBody is what a failed request answers.
type errorBody struct {
	Error string `json:"error"`
}
