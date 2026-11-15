package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Conn is the part of a ClickHouse connection the store needs.
type Conn interface {
	Exec(ctx context.Context, query string, args ...any) error
	QueryRow(ctx context.Context, query string, args ...any) driver.Row
}

// positionID is the key of the position row: one table serves one binwave instance. Instances that
// share a ClickHouse database use different checkpoint.table values.
const positionID = "position"

// tablesID is the key of the row with the list of tables, which changes rarely and can be long, so it
// is written only when it changes and not with every position.
const tablesID = "tables"

// ClickHouse stores the position in a ClickHouse table. Every save inserts a row with a higher
// sequence number; ReplacingMergeTree collapses old rows in the background, and Load reads the
// row with the highest sequence, so it is correct even before merges happen.
type ClickHouse struct {
	conn     Conn
	database string
	table    string
	now      func() time.Time

	mu     sync.Mutex
	seq    uint64
	tables string // the list in the tables row
}

// NewClickHouse creates a store on the given table of the given database. now may be nil.
func NewClickHouse(conn Conn, database, table string, now func() time.Time) *ClickHouse {
	if now == nil {
		now = time.Now
	}
	return &ClickHouse{conn: conn, database: quote(database), table: quote(database) + "." + quote(table), now: now}
}

// stateTable is the definition of the state table, after its name.
const stateTable = ` (
    id         String,
    seq        UInt64,
    gtid_set   String,
    version    UInt64,
    updated_at DateTime64(3, 'UTC'),
    quarantine String DEFAULT '',
    resync     String DEFAULT '',
    load_total UInt32 DEFAULT 0,
    tables     String DEFAULT ''
)
ENGINE = ReplacingMergeTree(seq)
ORDER BY id`

// Init creates the state database and table if they do not exist. It never touches an existing table.
func (s *ClickHouse) Init(ctx context.Context) error {
	create := func() error { return s.conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+s.table+stateTable) }
	err := create()
	if isUnknownDatabase(err) {
		// Only a database that is not there is created: CREATE DATABASE needs its privilege even when it exists.
		if err := s.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+s.database); err != nil {
			return fmt.Errorf("create checkpoint database %s: %w", s.database, err)
		}
		err = create()
	}
	if err != nil {
		return fmt.Errorf("create checkpoint table %s: %w", s.table, err)
	}
	// A table made by an earlier version has no quarantine column. The state table is binwave's
	// own, so adding a column to it is safe; the statement changes nothing when the column exists.
	if err := s.conn.Exec(ctx, "ALTER TABLE "+s.table+" ADD COLUMN IF NOT EXISTS quarantine String DEFAULT ''"); err != nil {
		return fmt.Errorf("add the quarantine column to checkpoint table %s: %w", s.table, err)
	}
	if err := s.conn.Exec(ctx, "ALTER TABLE "+s.table+" ADD COLUMN IF NOT EXISTS resync String DEFAULT ''"); err != nil {
		return fmt.Errorf("add the resync column to checkpoint table %s: %w", s.table, err)
	}
	if err := s.conn.Exec(ctx, "ALTER TABLE "+s.table+" ADD COLUMN IF NOT EXISTS load_total UInt32 DEFAULT 0"); err != nil {
		return fmt.Errorf("add the load_total column to checkpoint table %s: %w", s.table, err)
	}
	if err := s.conn.Exec(ctx, "ALTER TABLE "+s.table+" ADD COLUMN IF NOT EXISTS tables String DEFAULT ''"); err != nil {
		return fmt.Errorf("add the tables column to checkpoint table %s: %w", s.table, err)
	}
	return nil
}

// Load returns the saved position; found is false when nothing has been saved yet.
func (s *ClickHouse) Load(ctx context.Context) (pos Position, found bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var seq uint64
	var loadTotal uint32
	row := s.conn.QueryRow(ctx, "SELECT gtid_set, version, quarantine, resync, load_total, seq FROM "+s.table+" WHERE id = ? ORDER BY seq DESC LIMIT 1", positionID)
	if err := row.Scan(&pos.GTIDSet, &pos.Version, &pos.Quarantine, &pos.Resync, &loadTotal, &seq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Position{}, false, nil
		}
		return Position{}, false, fmt.Errorf("read checkpoint from %s: %w", s.table, err)
	}
	pos.LoadTotal = int(loadTotal)
	s.seq = seq

	var tablesSeq uint64
	row = s.conn.QueryRow(ctx, "SELECT tables, seq FROM "+s.table+" WHERE id = ? ORDER BY seq DESC LIMIT 1", tablesID)
	if err := row.Scan(&pos.Tables, &tablesSeq); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Position{}, false, fmt.Errorf("read the list of tables from %s: %w", s.table, err)
	}
	s.seq = max(s.seq, tablesSeq)
	s.tables = pos.Tables
	return pos, true, nil
}

// Save durably records the position. Load must be called first so the sequence continues from
// what is stored. A list of tables that changed is written first, so it is never older than the
// position.
func (s *ClickHouse) Save(ctx context.Context, pos Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if pos.Tables != s.tables {
		next := s.seq + 1
		err := s.conn.Exec(ctx, "INSERT INTO "+s.table+" (id, seq, gtid_set, version, tables, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
			tablesID, next, "", uint64(0), pos.Tables, s.now().UTC())
		if err != nil {
			return fmt.Errorf("save the list of tables to %s: %w", s.table, err)
		}
		s.seq, s.tables = next, pos.Tables
	}
	next := s.seq + 1
	err := s.conn.Exec(ctx, "INSERT INTO "+s.table+" (id, seq, gtid_set, version, quarantine, resync, load_total, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		positionID, next, pos.GTIDSet, pos.Version, pos.Quarantine, pos.Resync, uint32(pos.LoadTotal), s.now().UTC())
	if err != nil {
		return fmt.Errorf("save checkpoint to %s: %w", s.table, err)
	}
	s.seq = next
	return nil
}

func quote(name string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(name, `\`, `\\`), "`", "\\`") + "`"
}

// isUnknownDatabase reports whether ClickHouse refused a statement because its database does not exist.
func isUnknownDatabase(err error) bool {
	var exception *clickhouse.Exception
	return errors.As(err, &exception) && exception.Code == 81 // UNKNOWN_DATABASE
}
