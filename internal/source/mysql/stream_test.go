package mysql

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/thealexjo/binwave/internal/event"
)

const testUUID = "6d4d8f60-1111-2222-3333-444455556666"

func sid() []byte {
	// The UUID above, as the 16 raw bytes a GTID event carries.
	b := []byte{0x6d, 0x4d, 0x8f, 0x60, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x66, 0x66}
	return b
}

func gtidEv(gno int64) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
		Event: &replication.GTIDEvent{
			SID: sid(), GNO: gno,
			OriginalCommitTimestamp: 1_700_000_000_123_456,
		},
	}
}

func xidEv() *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.XID_EVENT}, Event: &replication.XIDEvent{}}
}

func queryEv(q string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
		Event:  &replication.QueryEvent{Schema: []byte("shop"), Query: []byte(q)},
	}
}

func rowsEv(typ replication.EventType, tm *replication.TableMapEvent, rows ...[]any) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: typ},
		Event:  &replication.RowsEvent{Table: tm, TableID: tm.TableID, Rows: rows},
	}
}

// ordersMap is a TABLE_MAP for shop.t(id BIGINT UNSIGNED PK, name VARCHAR(40) NULL).
func ordersMap(tableID uint64) *replication.TableMapEvent {
	tm := buildMap([]uint64{0},
		mcol{name: "id", typ: gomysql.MYSQL_TYPE_LONGLONG, unsigned: true},
		mcol{name: "name", typ: gomysql.MYSQL_TYPE_VARCHAR, meta: 160, collation: 45, nullable: true},
	)
	tm.TableID = tableID
	return tm
}

// collect runs events through a stream and returns what the handler saw.
func collect(t *testing.T, from string, include func(string, string) bool, events ...*replication.BinlogEvent) ([]event.Event, error) {
	t.Helper()
	var got []event.Event
	st, err := newStream(from, testCollations, include, func(_ context.Context, ev event.Event) error {
		got = append(got, ev)
		return nil
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	for _, be := range events {
		if err := st.process(context.Background(), be); err != nil {
			return got, err
		}
	}
	return got, nil
}

func TestTransactionLifecycle(t *testing.T) {
	tm := ordersMap(10)
	got, err := collect(t, "", nil,
		gtidEv(1),
		queryEv("BEGIN"),
		rowsEv(replication.WRITE_ROWS_EVENTv2, tm, []any{uint64(1), "a"}, []any{uint64(2), nil}),
		rowsEv(replication.UPDATE_ROWS_EVENTv2, tm, []any{uint64(1), "a"}, []any{uint64(1), "b"}),
		rowsEv(replication.DELETE_ROWS_EVENTv2, tm, []any{uint64(2), nil}),
		xidEv(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d events, want 5: %#v", len(got), got)
	}

	begin, ok := got[0].(event.Begin)
	if !ok || begin.GTID != testUUID+":1" {
		t.Fatalf("event 0 = %#v, want Begin %s:1", got[0], testUUID)
	}
	if want := int64(1_700_000_000); begin.CommitTime.Unix() != want {
		t.Errorf("CommitTime = %v", begin.CommitTime)
	}

	ins := got[1].(event.Rows)
	if ins.Op != event.Insert || len(ins.Rows) != 2 || ins.Table.FullName() != "shop.t" {
		t.Fatalf("insert = %#v", ins)
	}
	if !reflect.DeepEqual(ins.Rows[0].After, []any{uint64(1), "a"}) || ins.Rows[0].Before != nil {
		t.Errorf("insert row = %#v", ins.Rows[0])
	}
	if ins.Rows[1].After[1] != nil {
		t.Errorf("NULL was not preserved: %#v", ins.Rows[1])
	}

	upd := got[2].(event.Rows)
	if upd.Op != event.Update || len(upd.Rows) != 1 ||
		!reflect.DeepEqual(upd.Rows[0].Before, []any{uint64(1), "a"}) || !reflect.DeepEqual(upd.Rows[0].After, []any{uint64(1), "b"}) {
		t.Errorf("update = %#v", upd)
	}

	del := got[3].(event.Rows)
	if del.Op != event.Delete || !reflect.DeepEqual(del.Rows[0].Before, []any{uint64(2), nil}) || del.Rows[0].After != nil {
		t.Errorf("delete = %#v", del)
	}

	if commit := got[4].(event.Commit); commit.GTIDSet != testUUID+":1" {
		t.Errorf("Commit.GTIDSet = %q", commit.GTIDSet)
	}
}

func TestCommitCarriesTheWholeExecutedSet(t *testing.T) {
	got, err := collect(t, testUUID+":1-4", nil,
		gtidEv(5), xidEv(),
		gtidEv(6), xidEv(),
	)
	if err != nil {
		t.Fatal(err)
	}
	var sets []string
	for _, ev := range got {
		if c, ok := ev.(event.Commit); ok {
			sets = append(sets, c.GTIDSet)
		}
	}
	if want := []string{testUUID + ":1-5", testUUID + ":1-6"}; !reflect.DeepEqual(sets, want) {
		t.Errorf("committed sets = %v, want %v", sets, want)
	}
}

func TestPositionAdvancesOnlyAtTransactionEnd(t *testing.T) {
	got, err := collect(t, "", nil, gtidEv(1), rowsEv(replication.WRITE_ROWS_EVENTv2, ordersMap(1), []any{uint64(1), "x"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range got {
		if _, ok := ev.(event.Commit); ok {
			t.Fatal("Commit emitted before the transaction ended")
		}
	}
}

func TestDDLIsATransactionOfItsOwn(t *testing.T) {
	got, err := collect(t, "", nil,
		gtidEv(1),
		queryEv("ALTER TABLE t ADD COLUMN x INT"),
		gtidEv(2),
		queryEv("BEGIN"), xidEv(),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Begin", "DDL", "Commit", "Begin", "Commit"}
	var kinds []string
	for _, ev := range got {
		kinds = append(kinds, reflect.TypeOf(ev).Name())
	}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("events = %v, want %v", kinds, want)
	}
	ddl := got[1].(event.DDL)
	if ddl.Database != "shop" || ddl.Query != "ALTER TABLE t ADD COLUMN x INT" {
		t.Errorf("DDL = %#v", ddl)
	}
	if c := got[2].(event.Commit); c.GTIDSet != testUUID+":1" {
		t.Errorf("DDL commit set = %q", c.GTIDSet)
	}
}

// TestCreateTableSelectIsOneTransactionWithItsRows: with row-based logging MySQL writes
// CREATE TABLE ... SELECT as BEGIN, the CREATE TABLE (START TRANSACTION) statement, the rows, and
// XID; the statement does not commit, the XID does.
func TestCreateTableSelectIsOneTransactionWithItsRows(t *testing.T) {
	tm := ordersMap(42)
	got, err := collect(t, "", nil,
		gtidEv(1),
		queryEv("BEGIN"),
		queryEv("CREATE TABLE `shop`.`t` (`id` bigint unsigned NOT NULL, `name` varchar(40) DEFAULT NULL, PRIMARY KEY (`id`)) START TRANSACTION"),
		rowsEv(replication.WRITE_ROWS_EVENTv2, tm, []any{uint64(1), "a"}, []any{uint64(2), "b"}),
		xidEv(),
		gtidEv(2),
		queryEv("ALTER TABLE t ADD COLUMN x INT"),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Begin", "DDL", "Rows", "Commit", "Begin", "DDL", "Commit"}
	var kinds []string
	for _, ev := range got {
		kinds = append(kinds, reflect.TypeOf(ev).Name())
	}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("events = %v, want %v", kinds, want)
	}
	if c := got[3].(event.Commit); c.GTIDSet != testUUID+":1" {
		t.Errorf("commit set = %q", c.GTIDSet)
	}
}

func TestQueryCommitAndRollbackEndTheTransaction(t *testing.T) {
	// Non-transactional engines end with a COMMIT statement instead of an XID; a rolled-back
	// transaction that touched them ends with ROLLBACK. Both consume the GTID.
	for _, end := range []string{"COMMIT", "ROLLBACK", "commit"} {
		t.Run(end, func(t *testing.T) {
			got, err := collect(t, "", nil, gtidEv(1), queryEv(end))
			if err != nil {
				t.Fatal(err)
			}
			if c, ok := got[len(got)-1].(event.Commit); !ok || c.GTIDSet != testUUID+":1" {
				t.Errorf("last event = %#v, want Commit", got[len(got)-1])
			}
		})
	}
}

func TestRollbackToSavepointDoesNotEndTheTransaction(t *testing.T) {
	got, err := collect(t, "", nil, gtidEv(1), queryEv("ROLLBACK TO SAVEPOINT s1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("events = %#v, want only Begin", got)
	}
}

func TestCompressedTransactionIsUnwrapped(t *testing.T) {
	payload := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.TRANSACTION_PAYLOAD_EVENT},
		Event: &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{
			queryEv("BEGIN"),
			rowsEv(replication.WRITE_ROWS_EVENTv2, ordersMap(3), []any{uint64(7), "z"}),
			xidEv(),
		}},
	}
	got, err := collect(t, "", nil, gtidEv(1), payload)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Begin", "Rows", "Commit"}
	var kinds []string
	for _, ev := range got {
		kinds = append(kinds, reflect.TypeOf(ev).Name())
	}
	if !reflect.DeepEqual(kinds, want) {
		t.Errorf("events = %v, want %v", kinds, want)
	}
}

func TestExcludedTablesAreSkippedButPositionStillAdvances(t *testing.T) {
	skipAll := func(string, string) bool { return false }
	got, err := collect(t, "", skipAll,
		gtidEv(1),
		// An unsupported column type would fail if the table were resolved.
		rowsEv(replication.WRITE_ROWS_EVENTv2, buildMap(nil, mcol{name: "v", typ: gomysql.MYSQL_TYPE_VECTOR}), []any{1}),
		xidEv(),
	)
	if err != nil {
		t.Fatalf("skipped table was resolved: %v", err)
	}
	var kinds []string
	for _, ev := range got {
		kinds = append(kinds, reflect.TypeOf(ev).Name())
	}
	if want := []string{"Begin", "Commit"}; !reflect.DeepEqual(kinds, want) {
		t.Errorf("events = %v, want %v", kinds, want)
	}
}

// TestTableDefinitionIsSharedWhileTheStructureIsUnchanged: every transaction brings a fresh
// TABLE_MAP event, yet consumers get the same *schema.Table until the structure changes.
func TestTableDefinitionIsSharedWhileTheStructureIsUnchanged(t *testing.T) {
	got, err := collect(t, "", nil,
		gtidEv(1), rowsEv(replication.WRITE_ROWS_EVENTv2, ordersMap(9), []any{uint64(1), "a"}), xidEv(),
		gtidEv(2), rowsEv(replication.WRITE_ROWS_EVENTv2, ordersMap(9), []any{uint64(2), "b"}), xidEv(),
		// The table id changes when the table is reopened; the structure is what counts.
		gtidEv(3), rowsEv(replication.WRITE_ROWS_EVENTv2, ordersMap(77), []any{uint64(3), "c"}), xidEv(),
	)
	if err != nil {
		t.Fatal(err)
	}
	batches := rowsBatches(got)
	if len(batches) != 3 {
		t.Fatalf("got %d batches", len(batches))
	}
	if batches[0].Table != batches[1].Table || batches[1].Table != batches[2].Table {
		t.Error("the table definition was rebuilt although the structure did not change")
	}
}

func TestTableDefinitionIsReplacedWhenTheStructureChanges(t *testing.T) {
	widened := ordersMap(9)
	widened.ColumnMeta = append([]uint16{}, widened.ColumnMeta...)
	widened.ColumnMeta[1] = 400 // VARCHAR(40) became VARCHAR(100)

	got, err := collect(t, "", nil,
		gtidEv(1), rowsEv(replication.WRITE_ROWS_EVENTv2, ordersMap(9), []any{uint64(1), "a"}), xidEv(),
		gtidEv(2), rowsEv(replication.WRITE_ROWS_EVENTv2, widened, []any{uint64(2), "b"}), xidEv(),
		gtidEv(3), rowsEv(replication.WRITE_ROWS_EVENTv2, widened, []any{uint64(3), "c"}), xidEv(),
	)
	if err != nil {
		t.Fatal(err)
	}
	batches := rowsBatches(got)
	if batches[0].Table == batches[1].Table {
		t.Fatal("a structure change kept the old definition")
	}
	if batches[0].Table.Signature() == batches[1].Table.Signature() {
		t.Error("the new definition has the old signature")
	}
	if batches[1].Table != batches[2].Table {
		t.Error("the new definition was not reused afterwards")
	}
}

func rowsBatches(events []event.Event) []event.Rows {
	var out []event.Rows
	for _, ev := range events {
		if r, ok := ev.(event.Rows); ok {
			out = append(out, r)
		}
	}
	return out
}

func TestHeartbeatEventIsForwarded(t *testing.T) {
	got, err := collect(t, "", nil, &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT},
		Event:  &replication.HeartbeatEvent{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != (event.Heartbeat{}) {
		t.Errorf("events = %#v", got)
	}
}

func TestIrrelevantEventsAreIgnored(t *testing.T) {
	got, err := collect(t, "", nil,
		&replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.ROTATE_EVENT}, Event: &replication.RotateEvent{}},
		&replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.FORMAT_DESCRIPTION_EVENT}, Event: &replication.FormatDescriptionEvent{}},
		&replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.TABLE_MAP_EVENT}, Event: ordersMap(1)},
	)
	if err != nil || len(got) != 0 {
		t.Errorf("events = %#v, err = %v", got, err)
	}
}

func TestStreamErrors(t *testing.T) {
	tm := ordersMap(1)
	tests := []struct {
		name    string
		events  []*replication.BinlogEvent
		wantMsg string
	}{
		{"rows outside a transaction", []*replication.BinlogEvent{rowsEv(replication.WRITE_ROWS_EVENTv2, tm, []any{uint64(1), "a"})}, "outside a transaction"},
		{"commit without gtid", []*replication.BinlogEvent{xidEv()}, "without a GTID"},
		{"statement without gtid", []*replication.BinlogEvent{queryEv("CREATE TABLE t (a INT)")}, "outside a transaction"},
		{"a different transaction inside an open one", []*replication.BinlogEvent{gtidEv(1), gtidEv(2)}, "started before"},
		{"xa", []*replication.BinlogEvent{gtidEv(1), queryEv("XA START 'x'")}, "XA transactions are not supported"},
		{"partial json update", []*replication.BinlogEvent{gtidEv(1), rowsEv(replication.PARTIAL_UPDATE_ROWS_EVENT, tm, []any{uint64(1), "a"})}, "PARTIAL_JSON"},
		{"partial row image", []*replication.BinlogEvent{gtidEv(1), rowsEv(replication.WRITE_ROWS_EVENTv2, tm, []any{uint64(1)})}, "binlog_row_image must be FULL"},
		{"odd update images", []*replication.BinlogEvent{gtidEv(1), rowsEv(replication.UPDATE_ROWS_EVENTv2, tm, []any{uint64(1), "a"})}, "odd number"},
		{"unresolvable table", []*replication.BinlogEvent{gtidEv(1), rowsEv(replication.WRITE_ROWS_EVENTv2, buildMap(nil, mcol{name: "v", typ: gomysql.MYSQL_TYPE_VECTOR}), []any{1})}, "unsupported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := collect(t, "", nil, tt.events...)
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantMsg)
			}
		})
	}
}

// TestRedeliveredTransactionRestartsInsteadOfFailing: after a reconnect in the middle of a
// transaction, the library sends that transaction again from its first event. That is normal
// at-least-once delivery, not a protocol violation: the transaction simply starts over.
func TestRedeliveredTransactionRestartsInsteadOfFailing(t *testing.T) {
	tm := ordersMap(1)
	got, err := collect(t, "", nil,
		gtidEv(1), rowsEv(replication.WRITE_ROWS_EVENTv2, tm, []any{uint64(1), "a"}),
		// the connection dropped here; the same transaction arrives again from the start
		gtidEv(1), rowsEv(replication.WRITE_ROWS_EVENTv2, tm, []any{uint64(1), "a"}), rowsEv(replication.WRITE_ROWS_EVENTv2, tm, []any{uint64(2), "b"}), xidEv(),
	)
	if err != nil {
		t.Fatalf("a redelivered transaction was rejected: %v", err)
	}
	var kinds []string
	for _, ev := range got {
		kinds = append(kinds, reflect.TypeOf(ev).Name())
	}
	want := []string{"Begin", "Rows", "Begin", "Rows", "Rows", "Commit"}
	if !reflect.DeepEqual(kinds, want) {
		t.Errorf("events = %v, want %v: the consumer sees the restart as a second Begin", kinds, want)
	}
	if c := got[len(got)-1].(event.Commit); c.GTIDSet != testUUID+":1" {
		t.Errorf("the commit carries %q", c.GTIDSet)
	}
}

func TestHandlerErrorStopsTheStream(t *testing.T) {
	boom := errors.New("sink is down")
	st, err := newStream("", testCollations, nil, func(context.Context, event.Event) error { return boom }, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.process(context.Background(), gtidEv(1)); !errors.Is(err, boom) {
		t.Errorf("process() = %v, want %v", err, boom)
	}
}

func TestInvalidStartingSet(t *testing.T) {
	if _, err := newStream("not-a-gtid", testCollations, nil, nil, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("newStream() accepted a malformed GTID set")
	}
}
