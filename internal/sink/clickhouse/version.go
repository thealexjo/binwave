package clickhouse

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// timestampShift is how far the commit time in microseconds is shifted left. The low bits leave
// room for the strictly increasing steps taken while the source clock is behind the chain.
const timestampShift = 12

// Versioner hands out the ReplacingMergeTree version of each transaction, in binlog order.
//
// Binlog order is commit order, and transactions that touch the same row cannot commit
// concurrently, so a version that rises with binlog order always keeps the newest change. The
// commit time supplies the value while the source clock behaves; when it steps back, or two
// transactions share a microsecond, the version still rises by one. The clock alone cannot
// promise that: a step back would give a newer change a lower version, and ClickHouse would
// silently keep the older row.
//
// The result is a pure function of the stream from the saved starting point and that point's
// version, so replaying after a crash reproduces the same versions. The version saved with the
// position (checkpoint.Position.Version) is the starting point. A Versioner must be fed every
// transaction of the stream, including those of tables that are not replicated, so the chain
// does not depend on which tables are configured.
type Versioner struct {
	last uint64
}

// NewVersioner starts a chain after the given version, the one saved with the resume position;
// zero starts a fresh chain.
func NewVersioner(last uint64) *Versioner { return &Versioner{last: last} }

// Last returns the version of the most recent transaction.
func (v *Versioner) Last() uint64 { return v.last }

// Next returns the version of the next transaction, committed at the given time.
func (v *Versioner) Next(commit time.Time) (uint64, error) {
	if commit.IsZero() {
		return 0, errors.New("the transaction has no commit timestamp; MySQL 8.0.1 or newer is required")
	}
	micros := commit.UnixMicro()
	if micros < 0 || micros >= 1<<(64-timestampShift) {
		return 0, fmt.Errorf("commit time %s is outside the supported range", commit.UTC().Format(time.RFC3339))
	}

	next := uint64(micros) << timestampShift
	if next <= v.last {
		if v.last == math.MaxUint64 {
			return 0, errors.New("the version space is exhausted")
		}
		next = v.last + 1
	}
	v.last = next
	return next, nil
}
