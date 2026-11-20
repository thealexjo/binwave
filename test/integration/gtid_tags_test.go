//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/observability"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/test/testenv"
)

// Tagged GTIDs (MySQL 8.4): a session that sets gtid_next to 'AUTOMATIC:<tag>' writes transactions
// named uuid:tag:sequence, next to the untagged uuid:sequence of everything else, and gtid_executed
// holds both.

const gtidTag = "batch"

// requireGTIDTags skips on a server without tagged GTIDs (MySQL 8.0 of the version matrix) and
// returns a connection of an application allowed to write them; binwave keeps the rights of a
// production user, which tagged GTIDs do not change.
func requireGTIDTags(t *testing.T, my *testenv.MySQL) *sql.DB {
	t.Helper()
	var version string
	if err := my.DB.QueryRow("SELECT @@version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	var major, minor int
	if _, err := fmt.Sscanf(version, "%d.%d", &major, &minor); err != nil {
		t.Fatalf("read the server version %q: %v", version, err)
	}
	if major == 8 && minor < 4 {
		t.Skipf("MySQL %s has no tagged GTIDs", version)
	}
	return my.Writer(t, "TRANSACTION_GTID_TAG, SESSION_VARIABLES_ADMIN")
}

// execTagged runs every statement as a transaction of its own with a tagged GTID, on one
// connection of the writer, and sets the connection back to untagged GTIDs.
func execTagged(t *testing.T, w *sql.DB, stmts ...string) {
	t.Helper()
	ctx := context.Background()
	conn, err := w.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = conn.ExecContext(ctx, "SET gtid_next = 'AUTOMATIC'")
		_ = conn.Close()
	}()
	for _, stmt := range append([]string{"SET gtid_next = 'AUTOMATIC:" + gtidTag + "'"}, stmts...) {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

// sameGTIDSets asks MySQL whether two sets hold the same transactions: the text of a set with tags
// may list its parts in another order.
func sameGTIDSets(t *testing.T, my *testenv.MySQL, a, b string) bool {
	t.Helper()
	var same int
	if err := my.DB.QueryRow("SELECT GTID_SUBSET(?, ?) AND GTID_SUBSET(?, ?)", a, b, b, a).Scan(&same); err != nil {
		t.Fatalf("compare GTID sets %q and %q: %v", a, b, err)
	}
	return same == 1
}

// TestTaggedTransactionsAreStreamedAndAPositionWithTagsResumesWithoutRepeats: a tagged transaction
// starts with its tagged GTID, and a stream resumed from a set with tags (sent to the server in the
// tagged encoding) replays nothing already committed, tagged or not.
func TestTaggedTransactionsAreStreamedAndAPositionWithTagsResumesWithoutRepeats(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	w := requireGTIDTags(t, my)
	mustExec(t, my, "CREATE TABLE binwave.log (id INT PRIMARY KEY)")
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	execTagged(t, w, "INSERT INTO binwave.log VALUES (1)", "INSERT INTO binwave.log VALUES (2)")
	mustExec(t, my, "INSERT INTO binwave.log VALUES (3)")
	execTagged(t, w, "INSERT INTO binwave.log VALUES (4)")

	first := streamCommits(t, src, start, 2)
	var resumeFrom string
	for _, ev := range first {
		switch e := ev.(type) {
		case event.Begin:
			if !strings.Contains(e.GTID, ":"+gtidTag+":") {
				t.Errorf("Begin GTID %q has no tag", e.GTID)
			}
		case event.Commit:
			resumeFrom = e.GTIDSet
		}
	}
	if !strings.Contains(resumeFrom, ":"+gtidTag+":") {
		t.Fatalf("the position after two tagged transactions has no tag: %q", resumeFrom)
	}

	second := streamCommits(t, src, resumeFrom, 2)
	var ids []any
	for _, b := range rowsOf(second, "binwave.log") {
		for _, r := range b.Rows {
			ids = append(ids, r.After[0])
		}
	}
	if want := []any{int32(3), int32(4)}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("resumed stream delivered ids %v, want %v", ids, want)
	}
}

// TestTaggedTransactionsReplicateAndTheirPositionSurvivesARestart: tagged and untagged
// transactions mixed arrive in ClickHouse, the saved position holds the tags and equals
// gtid_executed, and changes made while binwave is down arrive after a restart.
func TestTaggedTransactionsReplicateAndTheirPositionSurvivesARestart(t *testing.T) {
	for _, store := range []string{"clickhouse", "file"} {
		t.Run(store, func(t *testing.T) {
			t.Parallel()
			my := testenv.StartMySQL(t)
			w := requireGTIDTags(t, my)
			ch := testenv.StartClickHouse(t)
			mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
			cfg := engineConfig(t, my, ch, store)

			first := startEngine(t, cfg)
			execTagged(t, w, "INSERT INTO binwave.acct VALUES (1, 'ann', 10), (2, 'bob', 20)")
			mustExec(t, my, "INSERT INTO binwave.acct VALUES (3, 'cy', 30)")
			execTagged(t, w,
				"UPDATE binwave.acct SET qty = qty + 1 WHERE id = 1",
				"DELETE FROM binwave.acct WHERE id = 2",
			)
			waitForConvergence(t, my, ch)
			if err := first.stop(t); err != nil {
				t.Fatalf("graceful stop returned %v", err)
			}
			pos := loadPosition(t, ch, cfg).GTIDSet
			if !strings.Contains(pos, ":"+gtidTag+":") {
				t.Errorf("saved position %q has no tag", pos)
			}
			if executed := executedGTIDs(t, my); !sameGTIDSets(t, my, pos, executed) {
				t.Errorf("saved position %q, MySQL has executed %q", pos, executed)
			}

			// Changes while binwave is down, tagged and not.
			execTagged(t, w, "INSERT INTO binwave.acct VALUES (4, 'di', 40)")
			mustExec(t, my, "UPDATE binwave.acct SET owner = 'cyd' WHERE id = 3")

			second := startEngine(t, cfg)
			execTagged(t, w, "INSERT INTO binwave.acct VALUES (5, 'ed', 50)")
			waitForConvergence(t, my, ch)
			if err := second.stop(t); err != nil {
				t.Fatalf("second stop returned %v", err)
			}
			if pos, executed := loadPosition(t, ch, cfg).GTIDSet, executedGTIDs(t, my); !sameGTIDSets(t, my, pos, executed) {
				t.Errorf("final position %q, MySQL has executed %q", pos, executed)
			}
		})
	}
}

// TestATaggedFailedTransactionIsShownWithItsTagAndSkippedByItsGTID: the failure names the
// transaction as MySQL does, uuid:tag:sequence, and resume --gtid-next with that GTID drops exactly
// it.
func TestATaggedFailedTransactionIsShownWithItsTagAndSkippedByItsGTID(t *testing.T) {
	t.Parallel()
	my, ch, e, c := startFailingScenario(t)
	w := requireGTIDTags(t, my)

	execTagged(t, w, failingStatement)
	mustExec(t, my, "INSERT INTO binwave.a VALUES (2, 'after')")
	st := waitFailed(t, c)

	f := st.Failure
	if !strings.Contains(f.GTID, ":"+gtidTag+":") {
		t.Errorf("the failed transaction is shown as %q, without its tag", f.GTID)
	}
	var contained int
	if err := my.DB.QueryRow("SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed)", f.GTID).Scan(&contained); err != nil || contained != 1 {
		t.Errorf("GTID_SUBSET(%s, gtid_executed) = %d (%v): the GTID shown is not one of the source's", f.GTID, contained, err)
	}

	if err := c.Resume(context.Background(), control.ResumeRequest{GTIDNext: f.GTID}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the row after the skipped transaction", 60*time.Second, func() bool { return chRowCount(t, ch, "a") == 2 })
	after, err := c.Status(context.Background())
	if err != nil || after.State != observability.StateReady || after.Failure != nil {
		t.Errorf("status after the skip: %+v (%v)", after, err)
	}
	e.stillRunning(t)
}
