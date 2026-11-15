package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeRow is a driver.Row with a canned result.
type fakeRow struct {
	set        string
	version    uint64
	quarantine string
	resync     string
	seq        uint64
	loadTotal  uint32
	err        error
}

func (r fakeRow) Err() error           { return r.err }
func (r fakeRow) ScanStruct(any) error { return errors.New("not used") }
func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*string) = r.set
	*dest[1].(*uint64) = r.version
	*dest[2].(*string) = r.quarantine
	*dest[3].(*string) = r.resync
	*dest[4].(*uint32) = r.loadTotal
	*dest[5].(*uint64) = r.seq
	return nil
}

// fakeTablesRow is the row with the list of tables; without one there is none.
type fakeTablesRow struct {
	tables string
	seq    uint64
}

func (r *fakeTablesRow) Err() error           { return nil }
func (r *fakeTablesRow) ScanStruct(any) error { return errors.New("not used") }
func (r *fakeTablesRow) Scan(dest ...any) error {
	if r == nil {
		return sql.ErrNoRows
	}
	*dest[0].(*string) = r.tables
	*dest[1].(*uint64) = r.seq
	return nil
}

type fakeConn struct {
	execs     []string
	args      [][]any
	row       fakeRow
	tablesRow *fakeTablesRow
	execErr   error
	// execErrs fails the nth Exec (1-based).
	execErrs map[int]error
}

func (c *fakeConn) Exec(_ context.Context, query string, args ...any) error {
	c.execs = append(c.execs, query)
	c.args = append(c.args, args)
	if err := c.execErrs[len(c.execs)]; err != nil {
		return err
	}
	return c.execErr
}

func (c *fakeConn) QueryRow(_ context.Context, _ string, args ...any) driver.Row {
	if len(args) > 0 && args[0] == tablesID {
		return c.tablesRow
	}
	return c.row
}

func TestClickHouseLoadWithoutRows(t *testing.T) {
	store := NewClickHouse(&fakeConn{row: fakeRow{err: sql.ErrNoRows}}, "db", "state", nil)

	pos, found, err := store.Load(context.Background())
	if err != nil || found || pos != (Position{}) {
		t.Fatalf("Load() = (%+v, %v, %v), want nothing found", pos, found, err)
	}
}

func TestClickHouseLoadError(t *testing.T) {
	boom := errors.New("connection reset")
	store := NewClickHouse(&fakeConn{row: fakeRow{err: boom}}, "db", "state", nil)

	if _, _, err := store.Load(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Load() = %v, want %v", err, boom)
	}
}

func TestClickHouseSaveContinuesTheStoredSequence(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	conn := &fakeConn{row: fakeRow{set: "u:1-9", version: 900, seq: 41}}
	store := NewClickHouse(conn, "db", "state", func() time.Time { return at })

	pos, found, err := store.Load(ctx)
	if err != nil || !found || pos != (Position{GTIDSet: "u:1-9", Version: 900}) {
		t.Fatalf("Load() = (%+v, %v, %v)", pos, found, err)
	}
	for i, next := range []string{"u:1-10", "u:1-11"} {
		if err := store.Save(ctx, Position{GTIDSet: next, Version: uint64(1000 + i)}); err != nil {
			t.Fatal(err)
		}
	}

	if got := conn.args[0][1]; got != uint64(42) {
		t.Errorf("first save used seq %v, want 42", got)
	}
	if got := conn.args[1][1]; got != uint64(43) {
		t.Errorf("second save used seq %v, want 43", got)
	}
	if got := conn.args[1][2]; got != "u:1-11" {
		t.Errorf("saved set = %v", got)
	}
	if got := conn.args[1][3]; got != uint64(1001) {
		t.Errorf("saved version = %v, want 1001", got)
	}
	if got := conn.args[0][6]; got != uint32(0) {
		t.Errorf("saved load_total = %v, want 0 without an initial load", got)
	}
	if got := conn.args[0][7]; got != at {
		t.Errorf("updated_at = %v, want the injected clock", got)
	}
}

func TestClickHouseFailedSaveDoesNotAdvanceTheSequence(t *testing.T) {
	ctx := context.Background()
	conn := &fakeConn{execErr: errors.New("server busy")}
	store := NewClickHouse(conn, "db", "state", nil)

	if err := store.Save(ctx, Position{GTIDSet: "u:1"}); err == nil {
		t.Fatal("Save() succeeded although the insert failed")
	}
	conn.execErr = nil
	if err := store.Save(ctx, Position{GTIDSet: "u:1"}); err != nil {
		t.Fatal(err)
	}
	if got := conn.args[1][1]; got != uint64(1) {
		t.Errorf("retry used seq %v, want 1", got)
	}
}

func TestClickHouseInitCreatesOnlyADatabaseThatIsNotThere(t *testing.T) {
	conn := &fakeConn{execErrs: map[int]error{1: &clickhouse.Exception{Code: 81, Message: "Database `db` does not exist"}}}
	if err := NewClickHouse(conn, "db", "state", nil).Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(conn.execs) != 7 || !strings.HasPrefix(conn.execs[1], "CREATE DATABASE IF NOT EXISTS `db`") || !strings.HasPrefix(conn.execs[2], "CREATE TABLE IF NOT EXISTS `db`.`state`") {
		t.Fatalf("statements = %v, want the table, the database, the table again and four ADD COLUMN", conn.execs)
	}
}

func TestClickHouseInitNeverDropsAnything(t *testing.T) {
	conn := &fakeConn{}
	if err := NewClickHouse(conn, "we`ird", "state", nil).Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The database exists: CREATE DATABASE, which needs its privilege even then, is not run.
	if len(conn.execs) != 5 {
		t.Fatalf("Init ran %d statements, want CREATE TABLE and four ADD COLUMN: %v", len(conn.execs), conn.execs)
	}
	if !strings.HasPrefix(conn.execs[0], "CREATE TABLE IF NOT EXISTS `we\\`ird`.`state`") {
		t.Errorf("first statement = %s", conn.execs[0])
	}
	if !strings.HasPrefix(conn.execs[1], "ALTER TABLE `we\\`ird`.`state` ADD COLUMN IF NOT EXISTS quarantine") {
		t.Errorf("second statement = %s", conn.execs[1])
	}
	for _, stmt := range conn.execs {
		for _, word := range []string{"DROP", "TRUNCATE"} {
			if strings.Contains(strings.ToUpper(stmt), word) {
				t.Errorf("statement %q contains %s", stmt, word)
			}
		}
	}
}

func TestClickHouseStoresAndLoadsTheQuarantine(t *testing.T) {
	ctx := context.Background()
	enc := EncodeQuarantine([]Quarantined{{Table: "shop.orders", Since: "u:1-5", Reason: "no key"}})
	conn := &fakeConn{row: fakeRow{set: "u:1-9", version: 9, quarantine: enc, seq: 3}}
	store := NewClickHouse(conn, "db", "state", nil)

	pos, _, err := store.Load(ctx)
	if err != nil || pos.Quarantine != enc {
		t.Fatalf("Load() = (%+v, %v), want the quarantine list back", pos, err)
	}
	if err := store.Save(ctx, Position{GTIDSet: "u:1-10", Version: 10, Quarantine: enc}); err != nil {
		t.Fatal(err)
	}
	if got := conn.args[0][4]; got != enc {
		t.Errorf("the quarantine saved = %v, want %v", got, enc)
	}
}

func TestClickHouseStoresAndLoadsTheResyncs(t *testing.T) {
	ctx := context.Background()
	enc := EncodeResyncs([]Resync{{Table: "shop.orders", Shadow: "orders__resync", Version: 7}})
	conn := &fakeConn{row: fakeRow{set: "u:1-9", version: 9, resync: enc, seq: 3}}
	store := NewClickHouse(conn, "db", "state", nil)

	pos, _, err := store.Load(ctx)
	if err != nil || pos.Resync != enc {
		t.Fatalf("Load() = (%+v, %v), want the resync list back", pos, err)
	}
	if err := store.Save(ctx, Position{GTIDSet: "u:1-10", Version: 10, Resync: enc}); err != nil {
		t.Fatal(err)
	}
	if got := conn.args[0][5]; got != enc {
		t.Errorf("the resync list saved = %v, want %v", got, enc)
	}
}

func TestClickHouseWritesTheListOfTablesOnlyWhenItChanges(t *testing.T) {
	ctx := context.Background()
	enc := EncodeTables(map[string]Target{"shop.orders": {Database: "shop", Table: "orders"}})
	conn := &fakeConn{row: fakeRow{set: "u:1-9", version: 9, seq: 41}, tablesRow: &fakeTablesRow{tables: enc, seq: 50}}
	store := NewClickHouse(conn, "db", "state", nil)

	pos, _, err := store.Load(ctx)
	if err != nil || pos.Tables != enc {
		t.Fatalf("Load() = (%+v, %v), want the list of tables back", pos, err)
	}
	if err := store.Save(ctx, Position{GTIDSet: "u:1-10", Tables: enc}); err != nil {
		t.Fatal(err)
	}
	if len(conn.execs) != 1 || conn.args[0][0] != positionID || conn.args[0][1] != uint64(51) {
		t.Fatalf("an unchanged list was written again, or the sequence went back: %v %v", conn.execs, conn.args)
	}

	changed := EncodeTables(map[string]Target{"shop.orders": {Database: "shop", Table: "orders"}, "shop.items": {Database: "shop", Table: "items"}})
	if err := store.Save(ctx, Position{GTIDSet: "u:1-11", Tables: changed}); err != nil {
		t.Fatal(err)
	}
	if len(conn.execs) != 3 || conn.args[1][0] != tablesID || conn.args[1][4] != changed || conn.args[2][0] != positionID {
		t.Fatalf("statements %v %v, want the list and then the position", conn.execs, conn.args)
	}
	if conn.args[1][1] != uint64(52) || conn.args[2][1] != uint64(53) {
		t.Errorf("sequences %v and %v, want 52 and 53", conn.args[1][1], conn.args[2][1])
	}
}

func TestClickHouseLoadsAPositionWithoutAListOfTables(t *testing.T) {
	store := NewClickHouse(&fakeConn{row: fakeRow{set: "u:1-9", version: 9, seq: 3}}, "db", "state", nil)

	pos, found, err := store.Load(context.Background())
	if err != nil || !found || pos.Tables != "" {
		t.Fatalf("Load() = (%+v, %v, %v), want the position and no list", pos, found, err)
	}
}
