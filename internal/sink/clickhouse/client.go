package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"syscall"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/tlsutil"
)

// Options tune a Client.
type Options struct {
	Logger *slog.Logger
	// Now supplies the _synced_at timestamp; it defaults to time.Now.
	Now func() time.Time
	// Writers is how many inserts the caller runs at the same time (buffer.insert_parallelism). The
	// connection pool is sized from it, with room for what else shares this connection while a flush
	// runs: the statements of auto-DDL, the swap of a resynchronized table, the saved position and the
	// status queries. 0 or less asks for the default.
	Writers int
}

// maxBlockBytes is how much one block of an insert may carry, as RowBytes estimates it. An insert of
// many rows is sent as several blocks, and none of them is allowed to grow without a bound.
//
// ClickHouse refuses a compressed block whose decompressed size is about a gigabyte: it answers
// "Too large size_decompressed: ... Most likely corrupted data" (TOO_LARGE_SIZE_COMPRESSED) and closes
// the connection, which the client only sees as "connection reset by peer" — the exception never
// arrives, because there is nothing left to read it from. That is not a hypothetical: a table of text
// rows tens of kilobytes wide, read in chunks of tens of thousands of rows, makes a block over a
// gigabyte, and the resync of such a table cannot proceed at all. The bound on the size of a chunk
// (replication.snapshot.chunk_max_bytes) does not prevent it, because it is a moving average of what
// the rows of the previous chunks weighed, and the chunk where a table's rows first turn out to be
// wide is exactly the one it does not know about yet.
//
// 64 MiB leaves a wide margin under the limit of the server and is a reasonable size for a part.
const maxBlockBytes = 64 << 20

// connectionHeadroom is how many connections are kept on top of the ones the inserts of a flush use:
// the saved position, the status queries, auto-DDL and the swap of a resynchronized table share the
// same connection, and none of them should wait for a flush to end.
const connectionHeadroom = 4

// Client writes replicated rows into ClickHouse over the native protocol.
type Client struct {
	conn driver.Conn
	log  *slog.Logger
	now  func() time.Time
}

// Open connects to ClickHouse and checks the connection.
//
// The connection has no default database: the target database may not exist yet, and every
// statement names its database explicitly.
func Open(ctx context.Context, cfg config.ClickHouse, opts Options) (*Client, error) {
	host, _, err := net.SplitHostPort(cfg.Addresses[0])
	if err != nil {
		return nil, fmt.Errorf("clickhouse address %q: %w", cfg.Addresses[0], err)
	}
	tlsCfg, err := tlsutil.Build(cfg.TLS, host)
	if err != nil {
		return nil, fmt.Errorf("clickhouse TLS: %w", err)
	}

	conns := max(opts.Writers, 1) + connectionHeadroom
	conn, err := ch.Open(&ch.Options{
		Addr: cfg.Addresses,
		Auth: ch.Auth{Username: cfg.User, Password: cfg.Password},
		TLS:  tlsCfg,
		// Compression keeps large batches cheap on the wire.
		Compression:  &ch.Compression{Method: ch.CompressionLZ4},
		DialTimeout:  10 * time.Second,
		MaxOpenConns: conns,
		MaxIdleConns: conns,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("connect to clickhouse %s: %w", strings.Join(cfg.Addresses, ","), err)
	}

	c := &Client{conn: conn, log: opts.Logger, now: opts.Now}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c, nil
}

// Conn exposes the connection for components that share it, such as the checkpoint store.
func (c *Client) Conn() driver.Conn { return c.conn }

// Close releases the connection.
func (c *Client) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close clickhouse connection: %w", err)
	}
	return nil
}

// DDL controls what Ensure may do to the schema.
type DDL struct {
	Mode   config.DDLMode
	DryRun bool
}

// SchemaMismatchError reports an existing ClickHouse table that does not match the source table.
// Changing existing tables is the job of auto-DDL (M5); until then a mismatch stops the service.
type SchemaMismatchError struct {
	Table    string
	Problems []string
}

func (e *SchemaMismatchError) Error() string {
	return fmt.Sprintf("clickhouse table %s does not match the source:\n  - %s", e.Table, strings.Join(e.Problems, "\n  - "))
}

// Ensure makes the ClickHouse table ready for inserts: it creates the database and table when
// they do not exist, then verifies that the table matches. It never drops, truncates or
// recreates anything on its own. With ddl.dry_run nothing is created: what would be executed
// is logged, and the missing table is reported, so that the DBA can create it by hand.
func (c *Client) Ensure(ctx context.Context, t *Table, ddl DDL) error {
	actual, err := c.columns(ctx, t.Database, t.Name)
	if err != nil {
		return err
	}
	if len(actual) == 0 {
		createDB, createTable := "CREATE DATABASE IF NOT EXISTS "+quote(t.Database), t.CreateStatement()
		if ddl.DryRun {
			for _, s := range []string{createDB, createTable} {
				c.log.Info("dry run: statement not executed", "statement", s)
			}
			return &TableError{Table: t.Source.FullName(), Missing: true, Err: fmt.Errorf(
				"clickhouse table %s does not exist and ddl.dry_run is set, so binwave did not create it (the statements are in the log); "+
					"a dry run cannot replicate a table that does not exist yet. "+
					"ddl.on_missing_structure: quarantine would set the table aside and go on, at the price of the changes made meanwhile", t.FullName())}
		}
		err := c.conn.Exec(ctx, createTable)
		if isUnknownDatabase(err) {
			// Only a database that is not there is created: CREATE DATABASE needs its privilege even when it exists.
			if err := c.conn.Exec(ctx, createDB); err != nil {
				return fmt.Errorf("create table %s: %w", t.FullName(), err)
			}
			err = c.conn.Exec(ctx, createTable)
		}
		if err != nil {
			return fmt.Errorf("create table %s: %w", t.FullName(), err)
		}
	} else if err := c.notAnotherJobs(ctx, t); err != nil {
		return err
	}
	if err := c.verify(ctx, t); err != nil {
		return err
	}
	if !ddl.DryRun {
		return c.recordTypes(ctx, t)
	}
	return nil
}

// recordTypes puts the comment that says what a column is in the source on every column that has no
// comment yet: a table binwave made before it wrote them, or made by hand. A comment somebody wrote
// is left as it is. It changes metadata only, and it is what lets a later change of the type know
// what the column was.
func (c *Client) recordTypes(ctx context.Context, t *Table) error {
	defs, err := c.columnDefs(ctx, t.Database, t.Name)
	if err != nil {
		return err
	}
	have := map[string]string{}
	for _, d := range defs {
		have[d.name] = d.comment
	}
	var clauses []string
	for _, col := range t.Columns {
		if comment, ok := have[col.Name]; ok && comment == "" {
			clauses = append(clauses, "COMMENT COLUMN "+quote(col.Name)+strings.TrimPrefix(commentClause(col.Mapping.Column), " COMMENT"))
		}
	}
	if len(clauses) == 0 {
		return nil
	}
	stmt := "ALTER TABLE " + t.FullName() + " " + strings.Join(clauses, ", ")
	if err := c.conn.Exec(ctx, stmt); err != nil {
		if IsTransient(err) {
			return fmt.Errorf("record the source types of the columns of %s: %w", t.FullName(), err)
		}
		// Only a later change of the type is harder to judge without them: not a reason to stop.
		c.log.Warn("source types not saved in column comments",
			"table", t.FullName(), "error", err)
	}
	return nil
}

// notAnotherJobs refuses an existing table that has the marker of a table binwave keeps for a job (the
// copy of a rebuild, the shadow table of a resync) when it is not prepared for that job: its shape is
// the shape of a source table of its name, and rows written to it would mix with the copy.
func (c *Client) notAnotherJobs(ctx context.Context, t *Table) error {
	comment, _, err := c.tableComment(ctx, t.Database, t.Name)
	if err != nil {
		return err
	}
	if (comment == RebuildMarker || comment == ResyncMarker) && comment != t.Comment {
		return &TableError{Table: t.Source.FullName(), Err: fmt.Errorf(
			"clickhouse table %s has the comment %q: it is a table binwave keeps for another job, and %s is not written to it; "+
				"drop it in ClickHouse if that job is over, or give the table another name with tables.overrides.<database.table>.target_table",
			t.FullName(), comment, t.Source.FullName())}
	}
	return nil
}

// verify compares the existing table with the expected structure.
func (c *Client) verify(ctx context.Context, t *Table) error {
	actual, err := c.columns(ctx, t.Database, t.Name)
	if err != nil {
		return err
	}
	if len(actual) == 0 {
		return fmt.Errorf("clickhouse table %s does not exist after CREATE TABLE", t.FullName())
	}

	var problems []string
	for _, want := range t.expectedColumns() {
		got, ok := actual[want[0]]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("column %s is missing (expected %s)", want[0], want[1]))
		case got != want[1]:
			problems = append(problems, fmt.Sprintf("column %s is %s, expected %s", want[0], got, want[1]))
		}
	}

	engine, sortingKey, err := c.tableInfo(ctx, t.Database, t.Name)
	if err != nil {
		return err
	}
	if !isBinwaveEngine(engine) {
		problems = append(problems, fmt.Sprintf("engine is %s, expected %s", engineCall(engine), binwaveEngine))
	}
	if want := strings.Join(t.OrderBy, ","); normalizeKey(sortingKey) != want {
		problems = append(problems, fmt.Sprintf("sort key is (%s), expected (%s)", sortingKey, want))
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		return &SchemaMismatchError{Table: t.FullName(), Problems: problems}
	}
	return nil
}

// normalizeKey brings a sorting key as reported by ClickHouse ("k", "(k)", "a, b", "`a`, `b`")
// to the form "a,b" used for comparison.
func normalizeKey(key string) string {
	key = strings.NewReplacer("`", "", "\"", "", " ", "").Replace(key)
	if strings.HasPrefix(key, "(") && strings.HasSuffix(key, ")") {
		key = key[1 : len(key)-1]
	}
	return key
}

func (c *Client) columns(ctx context.Context, database, table string) (map[string]string, error) {
	rows, err := c.conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = ? AND table = ?", database, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of %s.%s: %w", database, table, err)
	}
	defer func() { _ = rows.Close() }()

	cols := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, fmt.Errorf("read columns of %s.%s: %w", database, table, err)
		}
		cols[name] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read columns of %s.%s: %w", database, table, err)
	}
	return cols, nil
}

// isUnknownDatabase reports whether ClickHouse refused a statement because its database does not exist.
func isUnknownDatabase(err error) bool {
	var exception *ch.Exception
	return errors.As(err, &exception) && exception.Code == 81 // UNKNOWN_DATABASE
}

// binwaveEngine is the engine of every table binwave writes, with the arguments FINAL depends on.
var binwaveEngine = "ReplacingMergeTree(" + ColVersion + ", " + ColIsDeleted + ")"

// engineCall returns the engine and its arguments from engine_full, without the clauses after them.
func engineCall(engineFull string) string {
	full := strings.TrimSpace(engineFull)
	open := strings.IndexByte(full, '(')
	if space := strings.IndexByte(full, ' '); open < 0 || (space >= 0 && space < open) {
		if space < 0 {
			return full
		}
		return full[:space]
	}
	if end := strings.IndexByte(full[open:], ')'); end >= 0 {
		return full[:open+end+1]
	}
	return full
}

// isBinwaveEngine reports whether engine_full is ReplacingMergeTree(_version, _is_deleted).
func isBinwaveEngine(engineFull string) bool {
	plain := strings.NewReplacer("`", "", "\"", "", " ", "").Replace(engineCall(engineFull))
	return plain == strings.ReplaceAll(binwaveEngine, " ", "")
}

// tableInfo returns engine_full and the sorting key of a table.
func (c *Client) tableInfo(ctx context.Context, database, table string) (engine, sortingKey string, err error) {
	row := c.conn.QueryRow(ctx, "SELECT engine_full, sorting_key FROM system.tables WHERE database = ? AND name = ?", database, table)
	if err := row.Scan(&engine, &sortingKey); err != nil {
		return "", "", fmt.Errorf("read table info of %s.%s: %w", database, table, err)
	}
	return engine, sortingKey, nil
}

// Insert writes rows in one batch. The service columns are appended to every row: _version and
// _is_deleted from the row, _synced_at from the client clock. It is safe to repeat after a
// failure, because equal versions collapse.
func (c *Client) Insert(ctx context.Context, t *Table, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	cols := make([]string, 0, len(t.Columns)+3)
	for _, col := range t.Columns {
		cols = append(cols, quote(col.Name))
	}
	cols = append(cols, quote(ColVersion), quote(ColIsDeleted), quote(ColSyncedAt))

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+t.FullName()+" ("+strings.Join(cols, ", ")+")")
	if err != nil {
		err = fmt.Errorf("prepare insert into %s: %w", t.FullName(), err)
		if isTableException(err) {
			return &TableError{Table: t.Source.FullName(), Err: err, Missing: isMissingTableException(err)}
		}
		return err
	}

	syncedAt := c.now().UTC()
	pending := 0
	for _, r := range rows {
		// The block ends before the row that would take it past the bound, not after: a block is never
		// larger than the bound, unless one row alone is (then it is a block of its own, and nothing
		// binwave does can make it smaller).
		if size := RowBytes(r); pending > 0 && pending+size > maxBlockBytes {
			if err := batch.Flush(); err != nil {
				return fmt.Errorf("insert %d rows into %s: %w", len(rows), t.FullName(), err)
			}
			pending = 0
		}
		values := make([]any, 0, len(r.Values)+3)
		values = append(values, r.Values...)
		var deleted uint8
		if r.Deleted {
			deleted = 1
		}
		values = append(values, r.Version, deleted, syncedAt)

		if err := batch.Append(values...); err != nil {
			_ = batch.Abort()
			// The client cannot convert the value to the column: the column is not what the
			// table was resolved for, which retrying cannot change.
			return &TableError{Table: t.Source.FullName(), Err: fmt.Errorf("append to insert into %s: %w", t.FullName(), err)}
		}
		pending += RowBytes(r)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert %d rows into %s: %w", len(rows), t.FullName(), err)
	}
	return nil
}

// transientCodes are ClickHouse server exception codes that clear up by themselves.
var transientCodes = map[int32]bool{
	159: true, // TIMEOUT_EXCEEDED
	164: true, // READONLY: a replica catching up or in maintenance
	202: true, // TOO_MANY_SIMULTANEOUS_QUERIES
	209: true, // SOCKET_TIMEOUT
	210: true, // NETWORK_ERROR
	236: true, // ABORTED: a server shutting down aborts the inserts in flight
	241: true, // MEMORY_LIMIT_EXCEEDED
	252: true, // TOO_MANY_PARTS: the server asks inserts to slow down
	319: true, // UNKNOWN_STATUS_OF_INSERT: the insert may have happened; repeating is safe
	394: true, // QUERY_WAS_CANCELLED: a server shutting down cancels the queries in flight
	425: true, // SYSTEM_ERROR
	999: true, // KEEPER_EXCEPTION
}

// IsTransient reports whether an error from ClickHouse is worth retrying: network trouble and
// overload clear up, while a missing table, a type mismatch or a rejected login fail the same
// way every time. Repeating an insert is always safe, so the doubt is only about wasted time.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var exception *ch.Exception
	if errors.As(err, &exception) {
		return transientCodes[exception.Code]
	}
	switch {
	case errors.Is(err, context.Canceled):
		return false
	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, ch.ErrAcquireConnTimeout),
		errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNREFUSED):
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// overloadCodes are the server exception codes that mean "too busy right now": the server asks
// for less load and recovers on its own, but only if given time.
var overloadCodes = map[int32]bool{
	159: true, // TIMEOUT_EXCEEDED
	202: true, // TOO_MANY_SIMULTANEOUS_QUERIES
	241: true, // MEMORY_LIMIT_EXCEEDED
	252: true, // TOO_MANY_PARTS
}

// IsOverload reports whether a ClickHouse error means the server is overloaded. Such errors are
// retried like other transient ones, but only for a limited time (retry.overload_budget): unlike
// a network outage, an overload that lasts needs an operator, and retrying it endlessly would
// hold back replication of every table while hiding the problem.
func IsOverload(err error) bool {
	var exception *ch.Exception
	return errors.As(err, &exception) && overloadCodes[exception.Code]
}
