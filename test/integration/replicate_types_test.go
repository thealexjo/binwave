//go:build integration

package integration

import (
	"context"
	"testing"

	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/test/testenv"
)

// TestEveryTypeRoundTripsFromMySQLToClickHouse walks edge values of every MySQL type through the
// whole data path: MySQL, binlog, source, typemap, ClickHouse. What ClickHouse returns must equal
// what typemap is documented to produce.
func TestEveryTypeRoundTripsFromMySQLToClickHouse(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	ctx := context.Background()

	if _, err := my.DB.ExecContext(ctx, typesDDL); err != nil {
		t.Fatal(err)
	}
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	insertTypeRows(t, my)
	events := streamCommits(t, src, start, 4)

	batches := rowsOf(events, "binwave.types")
	tbl := resolveTable(t, batches[0].Table, nil)
	sink := openSink(t, ch)
	if err := sink.Ensure(ctx, tbl, additive); err != nil {
		t.Fatal(err)
	}
	applyEvents(t, sink, map[string]*chsink.Table{"binwave.types": tbl}, events)

	got := readTable(t, ch, "types", "id")
	want := expectedTypeRows()
	if len(got) != len(want) {
		t.Fatalf("ClickHouse holds %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		for _, col := range typeColumns {
			if !sameValue(got[i][col], want[i][col]) {
				t.Errorf("row %d, %s: ClickHouse has %#v, want %#v", i+1, col, got[i][col], want[i][col])
			}
		}
	}
}

// TestChangesConvergeFromMySQLToClickHouse replays inserts, updates, a key change and deletes
// and compares the visible ClickHouse state with the MySQL table.
func TestChangesConvergeFromMySQLToClickHouse(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	ctx := context.Background()

	mustExec(t, my, "CREATE TABLE binwave.acct (id INT UNSIGNED PRIMARY KEY, owner VARCHAR(20), balance DECIMAL(10,2) NOT NULL)")
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, my,
		"INSERT INTO binwave.acct VALUES (1, 'ann', 10.00), (2, 'bob', 20.00), (3, 'cy', 30.00), (4, 'di', 40.00)",
		"UPDATE binwave.acct SET balance = balance + 5 WHERE id IN (1, 2)",
		"UPDATE binwave.acct SET id = 30 WHERE id = 3", // primary key change
		"DELETE FROM binwave.acct WHERE id = 2",
		"UPDATE binwave.acct SET owner = 'anna' WHERE id = 1",
		"INSERT INTO binwave.acct VALUES (2, 'bob again', 1.00)", // a deleted key comes back
	)

	events := streamCommits(t, src, start, 6)
	tbl := resolveTable(t, rowsOf(events, "binwave.acct")[0].Table, nil)
	sink := openSink(t, ch)
	if err := sink.Ensure(ctx, tbl, additive); err != nil {
		t.Fatal(err)
	}
	applyEvents(t, sink, map[string]*chsink.Table{"binwave.acct": tbl}, events)

	// The expected state is whatever MySQL holds.
	type acct struct {
		id      uint32
		owner   string
		balance string
	}
	var want []acct
	rows, err := my.DB.Query("SELECT id, owner, CAST(balance AS CHAR) FROM binwave.acct ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var a acct
		if err := rows.Scan(&a.id, &a.owner, &a.balance); err != nil {
			t.Fatal(err)
		}
		want = append(want, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	got := readTable(t, ch, "acct", "id")
	if len(got) != len(want) {
		t.Fatalf("ClickHouse has %d rows, MySQL has %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		balance, ok := g["balance"].(interface{ String() string })
		if g["id"] != w.id || g["owner"] != w.owner || !ok {
			t.Errorf("row %d: ClickHouse %v, MySQL %+v", i, g, w)
			continue
		}
		if balance.String() != trimZeros(w.balance) {
			t.Errorf("row %d balance: ClickHouse %s, MySQL %s", i, balance, w.balance)
		}
	}
}

// trimZeros renders a fixed-scale decimal the way decimal.Decimal.String does.
func trimZeros(s string) string {
	for len(s) > 0 && containsDot(s) && (s[len(s)-1] == '0') {
		s = s[:len(s)-1]
	}
	if len(s) > 0 && s[len(s)-1] == '.' {
		s = s[:len(s)-1]
	}
	return s
}

func containsDot(s string) bool {
	for i := range len(s) {
		if s[i] == '.' {
			return true
		}
	}
	return false
}
