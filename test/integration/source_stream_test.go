//go:build integration

package integration

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/test/testenv"
)

func mustExec(t *testing.T, m *testenv.MySQL, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := m.DB.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("%s\n%v", s, err)
		}
	}
}

func kinds(events []event.Event) []string {
	var out []string
	for _, ev := range events {
		switch ev.(type) {
		case event.Begin:
			out = append(out, "Begin")
		case event.Rows:
			out = append(out, "Rows")
		case event.DDL:
			out = append(out, "DDL")
		case event.Commit:
			out = append(out, "Commit")
		}
	}
	return out
}

func TestOpenRejectsUnmetRequirements(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t, testenv.WithMySQLArgs("--binlog-format=MIXED", "--binlog-row-metadata=MINIMAL"))

	src := mysqlsrc.New(sourceConfig(my, config.TLSPreferred), mysqlsrc.Options{}, quietLogger())
	err := src.Open(context.Background())

	var reqErr *mysqlsrc.RequirementsError
	if !errors.As(err, &reqErr) {
		t.Fatalf("Open() = %v, want *RequirementsError", err)
	}
	if got := len(reqErr.Problems); got != 2 {
		t.Errorf("reported %d problems, want 2: %v", got, err)
	}
	for _, want := range []string{"binlog_format must be ROW", "binlog_row_metadata must be FULL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}

func TestOpenAcceptsAProperlyConfiguredSource(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	src := openSource(t, my, mysqlsrc.Options{})

	set, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatalf("ExecutedGTIDSet() = %v", err)
	}
	if strings.ContainsAny(set, " \n") {
		t.Errorf("GTID set %q contains whitespace", set)
	}
}

func TestOpenFailsClearlyOnBadCredentials(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)

	cfg := sourceConfig(my, config.TLSPreferred)
	cfg.Password = "wrong"
	err := mysqlsrc.New(cfg, mysqlsrc.Options{}, quietLogger()).Open(context.Background())

	if err == nil || !strings.Contains(err.Error(), "connect to source") {
		t.Fatalf("Open() = %v", err)
	}
	var reqErr *mysqlsrc.RequirementsError
	if errors.As(err, &reqErr) {
		t.Error("a connection failure was reported as an unmet requirement")
	}
}

// TestTransactionsAreDeliveredWhole covers the ordering contract: Begin, changes, Commit, and a
// position that only advances at Commit. It also checks update and delete images and rollbacks.
func TestTransactionsAreDeliveredWhole(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.items (id INT PRIMARY KEY, name VARCHAR(20), qty INT UNSIGNED)")
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tx, err := my.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{
		"INSERT INTO binwave.items VALUES (1, 'a', 10), (2, 'b', 20)",
		"UPDATE binwave.items SET name = 'a2', qty = 11 WHERE id = 1",
		"DELETE FROM binwave.items WHERE id = 2",
	} {
		if _, err := tx.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// A rolled-back transaction leaves nothing in the binlog.
	rolledBack, err := my.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rolledBack.Exec("INSERT INTO binwave.items VALUES (99, 'ghost', 1)"); err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.Rollback(); err != nil {
		t.Fatal(err)
	}
	mustExec(t, my, "INSERT INTO binwave.items VALUES (3, 'c', 30)")

	events := streamCommits(t, src, start, 2)

	if got, want := kinds(events), []string{"Begin", "Rows", "Rows", "Rows", "Commit", "Begin", "Rows", "Commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("event order = %v, want %v", got, want)
	}

	batches := rowsOf(events, "binwave.items")
	if batches[0].Op != event.Insert || len(batches[0].Rows) != 2 {
		t.Errorf("first batch = %s with %d rows", batches[0].Op, len(batches[0].Rows))
	}
	upd := batches[1]
	if upd.Op != event.Update || len(upd.Rows) != 1 {
		t.Fatalf("second batch = %s with %d rows", upd.Op, len(upd.Rows))
	}
	if want := []any{int32(1), "a", uint32(10)}; !reflect.DeepEqual(upd.Rows[0].Before, want) {
		t.Errorf("update before = %#v, want %#v", upd.Rows[0].Before, want)
	}
	if want := []any{int32(1), "a2", uint32(11)}; !reflect.DeepEqual(upd.Rows[0].After, want) {
		t.Errorf("update after = %#v, want %#v", upd.Rows[0].After, want)
	}
	del := batches[2]
	if del.Op != event.Delete || !reflect.DeepEqual(del.Rows[0].Before, []any{int32(2), "b", uint32(20)}) {
		t.Errorf("delete = %#v", del)
	}
	if got := batches[0].Table.PrimaryKeyColumns(); !reflect.DeepEqual(got, []string{"id"}) {
		t.Errorf("primary key = %v", got)
	}
}

// TestRestartFromACommittedPositionContinuesWithoutRepeats is the at-least-once contract from
// the source's side: resuming from a Commit's GTID set replays nothing already committed.
func TestRestartFromACommittedPositionContinuesWithoutRepeats(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.log (id INT PRIMARY KEY)")
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, my, "INSERT INTO binwave.log VALUES (1)", "INSERT INTO binwave.log VALUES (2)", "INSERT INTO binwave.log VALUES (3)")

	first := streamCommits(t, src, start, 2)
	var resumeFrom string
	for _, ev := range first {
		if c, ok := ev.(event.Commit); ok {
			resumeFrom = c.GTIDSet
		}
	}

	second := streamCommits(t, src, resumeFrom, 1)
	batches := rowsOf(second, "binwave.log")
	if len(batches) != 1 || !reflect.DeepEqual(batches[0].Rows[0].After, []any{int32(3)}) {
		t.Fatalf("resumed stream delivered %#v, want only id 3", batches)
	}
}

func TestStartingFromNowSkipsHistory(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.log (id INT PRIMARY KEY)", "INSERT INTO binwave.log VALUES (1)")
	src := openSource(t, my, mysqlsrc.Options{})

	now, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, my, "INSERT INTO binwave.log VALUES (2)")

	batches := rowsOf(streamCommits(t, src, now, 1), "binwave.log")
	if len(batches) != 1 || !reflect.DeepEqual(batches[0].Rows[0].After, []any{int32(2)}) {
		t.Fatalf("delivered %#v, want only id 2", batches)
	}
}

// TestSchemaChangeIsVisible checks that DDL arrives as its own transaction and that rows written
// afterwards carry the new table definition.
func TestSchemaChangeIsVisible(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.evolving (id INT PRIMARY KEY, a INT)")
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, my,
		"INSERT INTO binwave.evolving VALUES (1, 10)",
		"ALTER TABLE binwave.evolving ADD COLUMN b VARCHAR(10) NULL",
		"INSERT INTO binwave.evolving VALUES (2, 20, 'x')",
	)

	events := streamCommits(t, src, start, 3)
	if got, want := kinds(events), []string{"Begin", "Rows", "Commit", "Begin", "DDL", "Commit", "Begin", "Rows", "Commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("event order = %v, want %v", got, want)
	}

	var ddl event.DDL
	for _, ev := range events {
		if d, ok := ev.(event.DDL); ok {
			ddl = d
		}
	}
	if ddl.Database != "binwave" || !strings.Contains(strings.ToUpper(ddl.Query), "ADD COLUMN B") {
		t.Errorf("DDL = %#v", ddl)
	}

	batches := rowsOf(events, "binwave.evolving")
	before, after := batches[0].Table, batches[1].Table
	if len(before.Columns) != 2 || len(after.Columns) != 3 {
		t.Fatalf("columns before/after = %d/%d, want 2/3", len(before.Columns), len(after.Columns))
	}
	if before.Signature() == after.Signature() {
		t.Error("Signature() did not change after ALTER TABLE")
	}
}

func TestIncludeFilterSkipsTablesButKeepsPositionMoving(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.wanted (id INT PRIMARY KEY)", "CREATE TABLE binwave.ignored (id INT PRIMARY KEY)")
	src := openSource(t, my, mysqlsrc.Options{Include: func(_, table string) bool { return table == "wanted" }})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, my, "INSERT INTO binwave.ignored VALUES (1)", "INSERT INTO binwave.wanted VALUES (1)")

	events := streamCommits(t, src, start, 2)
	if got := len(rowsOf(events, "binwave.ignored")); got != 0 {
		t.Errorf("ignored table produced %d batches", got)
	}
	if got := len(rowsOf(events, "binwave.wanted")); got != 1 {
		t.Errorf("wanted table produced %d batches, want 1", got)
	}
	// Both transactions are reported, so the position covers the filtered one too.
	if got, want := kinds(events), []string{"Begin", "Commit", "Begin", "Rows", "Commit"}; !reflect.DeepEqual(got, want) {
		t.Errorf("event order = %v, want %v", got, want)
	}
}

// TestCompressedTransactionsAreDecoded guards a production hazard: with
// binlog_transaction_compression the whole transaction arrives as one wrapped event.
func TestCompressedTransactionsAreDecoded(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t, testenv.WithMySQLArgs("--binlog-transaction-compression=ON"))
	mustExec(t, my, "CREATE TABLE binwave.zip (id INT PRIMARY KEY, body TEXT)")
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	body := strings.Repeat("compress me ", 500)
	mustExec(t, my, "INSERT INTO binwave.zip VALUES (1, '"+body+"')")

	batches := rowsOf(streamCommits(t, src, start, 1), "binwave.zip")
	if len(batches) != 1 {
		t.Fatalf("compressed transaction produced %d batches, want 1", len(batches))
	}
	// TEXT arrives as raw bytes in the column character set.
	if got, ok := batches[0].Rows[0].After[1].([]byte); !ok || string(got) != body {
		t.Fatalf("compressed row value = %T, want the original %d bytes", batches[0].Rows[0].After[1], len(body))
	}
}

// TestEveryTLSModeStreams checks that the SQL connection and the binlog connection agree on TLS.
func TestEveryTLSModeStreams(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.tls_probe (id INT PRIMARY KEY)")

	for i, mode := range []config.TLSMode{config.TLSDisabled, config.TLSPreferred, config.TLSRequired} {
		t.Run(string(mode), func(t *testing.T) {
			src := mysqlsrc.New(sourceConfig(my, mode), mysqlsrc.Options{}, quietLogger())
			if err := src.Open(context.Background()); err != nil {
				t.Fatalf("Open(%s): %v", mode, err)
			}
			t.Cleanup(func() { _ = src.Close() })

			start, err := src.ExecutedGTIDSet(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			mustExec(t, my, "INSERT INTO binwave.tls_probe VALUES ("+string(rune('1'+i))+")")

			if got := len(rowsOf(streamCommits(t, src, start, 1), "binwave.tls_probe")); got != 1 {
				t.Errorf("mode %s delivered %d batches, want 1", mode, got)
			}
		})
	}
}
