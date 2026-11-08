package pipeline

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/gtid"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/typemap"
)

// FailedError is a failure of replication that restarting does not fix and that the operator has
// to decide about: a change that ClickHouse cannot take, a state that contradicts the source. The
// engine does not exit on it: the service stays up in the state failed, `binwave replication
// status` describes the failure, and `binwave replication resume` tries again, or skips the
// transaction with --skip-transactions or --gtid-next. Failures of the infrastructure (an
// unreachable database, an overloaded ClickHouse) are not FailedError: they are retried, and
// what cannot be retried ends the process, which the service manager restarts.
type FailedError struct {
	Failure failure.Failure
	Err     error
}

func (e *FailedError) Error() string {
	if e.Failure.GTID != "" {
		return fmt.Sprintf("transaction %s: %v", e.Failure.GTID, e.Err)
	}
	return e.Err.Error()
}

func (e *FailedError) Unwrap() error { return e.Err }

// WriteError is the refusal of a write of buffered rows by ClickHouse for a reason of the table
// (it is gone, a column is missing). The rows come from several transactions.
type WriteError struct {
	Table string
	Rows  int
	Err   error
}

func (e *WriteError) Error() string {
	return fmt.Sprintf("write %d rows to %s: %v", e.Rows, e.Table, e.Err)
}

func (e *WriteError) Unwrap() error { return e.Err }

// isDataFailure reports whether err is a failure that belongs to the data path, as opposed to the
// infrastructure. A *preflight.Error is deliberately not one of these: it is what the checks after
// start use, and what the lazy check of a table use (bind, for a table that appeared after them)
// use for a problem of tables.on_replication_error: stop specifically — a property of the table
// itself (no key, a cascading foreign key), not of one transaction, so skipping a transaction (the
// remedy the failed state offers) would not help: every future row of the table hits it again. It
// must reach engine.classify as the config problem it is and end the service, the same as the
// checks after start already do for a table they knew about then.
func isDataFailure(err error) bool {
	var (
		conflict   *chsink.ConflictError
		table      *chsink.TableError
		mismatch   *chsink.SchemaMismatchError
		conversion *typemap.ConversionError
		write      *WriteError
	)
	return errors.As(err, &conflict) || errors.As(err, &table) || errors.As(err, &mismatch) ||
		errors.As(err, &conversion) || errors.As(err, &write)
}

// annotate turns a failure of the data path into a FailedError that names what failed. Other errors
// pass through.
func (p *Pipeline) annotate(ev event.Event, err error) error {
	var already *FailedError
	if err == nil || errors.As(err, &already) || !isDataFailure(err) {
		return err
	}
	f := failure.Failure{Time: p.d.Now(), Message: err.Error(), Code: string(causeCode(err))}
	var conversion *typemap.ConversionError
	var conflict *chsink.ConflictError
	switch {
	case errors.As(err, &conversion):
		f.Detail = conversion.Detail()
	case errors.As(err, &conflict):
		f.Detail = conflict.Detail
	}

	var write *WriteError
	switch e := ev.(type) {
	case event.Rows:
		if errors.As(err, &write) {
			f.Kind, f.Table, f.Rows = failure.KindWrite, write.Table, write.Rows
			break
		}
		f.Kind, f.Table, f.Op, f.Rows = failure.KindRows, e.Table.FullName(), e.Op.String(), len(e.Rows)
		f.Key, f.KeyValues = rowKey(e, err)
	case event.DDL:
		if errors.As(err, &write) {
			f.Kind, f.Table, f.Rows = failure.KindWrite, write.Table, write.Rows
			break
		}
		f.Kind, f.Statement = failure.KindDDL, truncate(e.Query, 2000)
	default:
		f.Kind = failure.KindWrite
		if errors.As(err, &write) {
			f.Table, f.Rows = write.Table, write.Rows
		}
	}
	if f.Kind != failure.KindWrite && p.tx != nil {
		f.GTID = p.tx.gtid
	}
	return &FailedError{Failure: f, Err: err}
}

// rowKey names the key of the table and, when the error says which row failed, its values.
func rowKey(e event.Rows, err error) (name string, values []string) {
	t := e.Table
	if len(t.PrimaryKey) == 0 {
		name = "no primary key"
	} else {
		cols := make([]string, len(t.PrimaryKey))
		for i, idx := range t.PrimaryKey {
			cols[i] = t.Columns[idx].Name
		}
		name = "PRIMARY KEY (" + strings.Join(cols, ", ") + ")"
	}
	var re *chsink.RowError
	if !errors.As(err, &re) || re.Index >= len(e.Rows) || len(t.PrimaryKey) == 0 {
		return name, nil
	}
	image := e.Rows[re.Index].Before
	if re.After {
		image = e.Rows[re.Index].After
	}
	for _, idx := range t.PrimaryKey {
		if idx < len(image) {
			values = append(values, formatValue(image[idx]))
		}
	}
	return name, values
}

// formatValue renders a key value for the operator: text as text, bytes as a quoted string when
// they are text and as hex otherwise, long values cut.
func formatValue(v any) string {
	var s string
	switch x := v.(type) {
	case nil:
		s = "NULL"
	case []byte:
		if utf8.Valid(x) {
			s = strconv.Quote(string(x))
		} else {
			s = fmt.Sprintf("0x%x", x)
		}
	case string:
		s = strconv.Quote(x)
	default:
		s = fmt.Sprint(x)
	}
	return truncate(s, 100)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s + "…"
}

// Skip tells the pipeline to drop transactions instead of replicating them, like MySQL's
// SET sql_replica_skip_counter (Count) and the empty transaction that is injected with
// SET GTID_NEXT (GTIDs): the position moves past them, and their changes are not applied.
// What a skipped transaction wrote to ClickHouse before it failed stays there.
type Skip struct {
	// Count is how many of the next transactions are dropped.
	Count int
	// GTIDs are dropped whenever they come.
	GTIDs *gtid.Set
}

// SetSkip sets the transactions to drop. It must not be called while the pipeline runs.
func (p *Pipeline) SetSkip(s Skip) {
	p.skip = s
}

// skipping decides at the beginning of a transaction whether it is dropped.
func (p *Pipeline) skipping(id string) (bool, error) {
	if p.skip.GTIDs != nil {
		ok, err := p.skip.GTIDs.Contains(id)
		if err != nil {
			return false, fmt.Errorf("check transaction %s against the GTIDs to skip: %w", id, err)
		}
		if ok {
			return true, nil
		}
	}
	if p.skip.Count > 0 {
		p.skip.Count--
		return true, nil
	}
	return false, nil
}

// dropTransactionRows removes from the buffers the rows of the transaction that failed, so that the
// final flush does not write half of it: a transaction the operator skips is then not in
// ClickHouse at all, unless a full buffer made part of it go out earlier (a transaction bigger
// than the buffer cannot be held whole), and one the operator does not skip is read again whole
// from the source.
func (p *Pipeline) dropTransactionRows(version uint64) {
	kept := p.order[:0]
	for _, t := range p.order {
		b := p.buffers[t]
		rows := b.rows[:0]
		bytes := 0
		for _, r := range b.rows {
			if r.Version == version {
				continue
			}
			rows = append(rows, r)
			bytes += chsink.RowBytes(r)
		}
		p.memBytes -= b.bytes - bytes
		p.memRows -= len(b.rows) - len(rows)
		b.rows, b.bytes = rows, bytes
		if len(rows) == 0 {
			delete(p.buffers, t)
			continue
		}
		kept = append(kept, t)
	}
	p.order = kept
	p.d.Observer.BufferChanged(p.memRows, p.memBytes)
}
