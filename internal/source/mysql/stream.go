package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
)

// Handler receives the change events. Returning an error stops the stream; blocking applies
// backpressure, because the next binlog event is only read after Handler returns.
type Handler = func(ctx context.Context, ev event.Event) error

// maxCachedTables bounds the table definition cache.
const maxCachedTables = 4096

// stream turns raw binlog events into the neutral event stream and tracks the executed GTID set.
type stream struct {
	handle  Handler
	include func(database, table string) bool
	tables  *tableCache
	log     *slog.Logger

	executed gomysql.GTIDSet
	// pending is the GTID of the open transaction; empty when none is open.
	pending string
}

func newStream(from string, cs collations, include func(string, string) bool, handle Handler, log *slog.Logger) (*stream, error) {
	executed, err := gomysql.ParseMysqlGTIDSet(from)
	if err != nil {
		return nil, fmt.Errorf("parse GTID set %q: %w", from, err)
	}
	if include == nil {
		include = func(string, string) bool { return true }
	}
	return &stream{
		handle:   handle,
		include:  include,
		tables:   &tableCache{cs: cs, byName: make(map[string]cachedTable)},
		log:      log,
		executed: executed,
	}, nil
}

// process handles one binlog event.
func (s *stream) process(ctx context.Context, be *replication.BinlogEvent) error {
	switch e := be.Event.(type) {
	case *replication.GTIDEvent:
		return s.begin(ctx, e)
	case *replication.GtidTaggedLogEvent:
		return s.begin(ctx, &e.GTIDEvent)
	case *replication.XIDEvent:
		return s.commit(ctx)
	case *replication.QueryEvent:
		return s.query(ctx, e, time.Unix(int64(be.Header.Timestamp), 0).UTC())
	case *replication.RowsEvent:
		return s.rows(ctx, be.Header.EventType, e)
	case *replication.TransactionPayloadEvent:
		// binlog_transaction_compression wraps a whole transaction in one event.
		for _, inner := range e.Events {
			if err := s.process(ctx, inner); err != nil {
				return err
			}
		}
		return nil
	case *replication.HeartbeatEvent:
		return s.handle(ctx, event.Heartbeat{})
	default:
		// FORMAT_DESCRIPTION, ROTATE, TABLE_MAP (resolved lazily with the rows), PREVIOUS_GTIDS, ...
		return nil
	}
}

func (s *stream) begin(ctx context.Context, e *replication.GTIDEvent) error {
	next, err := e.GTIDNext()
	if err != nil {
		return fmt.Errorf("decode GTID event: %w", err)
	}
	if s.pending != "" && s.pending != next.String() {
		return fmt.Errorf("transaction %s started before %s ended", next, s.pending)
	}
	// The same GTID while it is still open is a redelivery: after a reconnect in the middle of a
	// transaction the library sends it again from its first event. The transaction starts over.
	s.pending = next.String()

	commitTime := e.OriginalCommitTime()
	if commitTime.IsZero() {
		commitTime = e.ImmediateCommitTime()
	}
	return s.handle(ctx, event.Begin{GTID: s.pending, CommitTime: commitTime})
}

func (s *stream) commit(ctx context.Context) error {
	if s.pending == "" {
		return errors.New("transaction end without a GTID: is gtid_mode ON?")
	}
	if err := s.executed.Update(s.pending); err != nil {
		return fmt.Errorf("add %s to the executed GTID set: %w", s.pending, err)
	}
	s.pending = ""
	return s.handle(ctx, event.Commit{GTIDSet: s.executed.String()})
}

func (s *stream) query(ctx context.Context, e *replication.QueryEvent, at time.Time) error {
	q := strings.TrimSpace(string(e.Query))
	switch word := strings.ToUpper(firstWord(q)); word {
	case "BEGIN", "SAVEPOINT", "RELEASE":
		return nil
	case "COMMIT", "ROLLBACK":
		// A rollback is logged only when non-transactional changes stay; the GTID is consumed either way.
		if strings.Contains(strings.ToUpper(q), " TO ") {
			return nil // ROLLBACK TO SAVEPOINT does not end the transaction
		}
		return s.commit(ctx)
	case "XA":
		return fmt.Errorf("XA transactions are not supported (transaction %s)", s.pending)
	}

	if s.pending == "" {
		return errors.New("statement outside a transaction: is gtid_mode ON?")
	}
	if err := s.handle(ctx, event.DDL{Database: string(e.Schema), Query: q, Time: at}); err != nil {
		return err
	}
	// DDL commits implicitly and the binlog carries no XID for it.
	return s.commit(ctx)
}

func (s *stream) rows(ctx context.Context, typ replication.EventType, e *replication.RowsEvent) error {
	op, err := rowOp(typ)
	if err != nil {
		return err
	}
	if s.pending == "" {
		return errors.New("row event outside a transaction: is gtid_mode ON?")
	}
	if !s.include(string(e.Table.Schema), string(e.Table.Table)) {
		return nil
	}

	tbl, err := s.tables.get(e.Table)
	if err != nil {
		return err
	}
	rows, err := splitRows(op, tbl, e)
	if err != nil {
		return err
	}
	return s.handle(ctx, event.Rows{Table: tbl, Op: op, Rows: rows})
}

func rowOp(typ replication.EventType) (event.Op, error) {
	switch typ {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return event.Insert, nil
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return event.Update, nil
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return event.Delete, nil
	case replication.PARTIAL_UPDATE_ROWS_EVENT:
		return 0, errors.New("partial JSON updates are not supported: binlog_row_value_options must not contain PARTIAL_JSON")
	default:
		return 0, fmt.Errorf("unsupported rows event type %s", typ)
	}
}

// splitRows pairs the flat row list of a rows event into changes: updates carry before and
// after images alternately.
func splitRows(op event.Op, tbl *schema.Table, e *replication.RowsEvent) ([]event.Row, error) {
	for _, r := range e.Rows {
		if len(r) != len(tbl.Columns) {
			return nil, fmt.Errorf("table %s: row has %d values for %d columns; binlog_row_image must be FULL",
				tbl.FullName(), len(r), len(tbl.Columns))
		}
	}

	switch op {
	case event.Insert:
		out := make([]event.Row, len(e.Rows))
		for i, r := range e.Rows {
			out[i] = event.Row{After: r}
		}
		return out, nil
	case event.Delete:
		out := make([]event.Row, len(e.Rows))
		for i, r := range e.Rows {
			out[i] = event.Row{Before: r}
		}
		return out, nil
	default:
		if len(e.Rows)%2 != 0 {
			return nil, fmt.Errorf("table %s: update event has an odd number of row images (%d)", tbl.FullName(), len(e.Rows))
		}
		out := make([]event.Row, len(e.Rows)/2)
		for i := range out {
			out[i] = event.Row{Before: e.Rows[2*i], After: e.Rows[2*i+1]}
		}
		return out, nil
	}
}

func firstWord(s string) string {
	if i := strings.IndexAny(s, " \t\r\n"); i >= 0 {
		return s[:i]
	}
	return s
}

// tableCache resolves TABLE_MAP events to table definitions.
//
// Every transaction brings a fresh TABLE_MAP event for each table it touches, describing the
// structure at that moment. The cache remembers the last definition per table name and hands
// back the same *schema.Table for as long as the structure is unchanged, so a changed pointer
// means a changed structure. Nothing here depends on seeing the DDL statement: a change made
// without one (sql_log_bin=0, online schema-change tools) is noticed all the same.
type tableCache struct {
	cs     collations
	byName map[string]cachedTable
}

type cachedTable struct {
	ev    *replication.TableMapEvent
	table *schema.Table
}

func (c *tableCache) get(ev *replication.TableMapEvent) (*schema.Table, error) {
	key := string(ev.Schema) + "\x00" + string(ev.Table)
	if cur, ok := c.byName[key]; ok && sameDefinition(cur.ev, ev) {
		return cur.table, nil
	}
	tbl, err := tableFromMap(ev, c.cs)
	if err != nil {
		return nil, err
	}
	if len(c.byName) >= maxCachedTables {
		clear(c.byName)
	}
	c.byName[key] = cachedTable{ev: ev, table: tbl}
	return tbl, nil
}
