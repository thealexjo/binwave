//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/test/testenv"
)

// controlSocket gives cfg a control socket with a path that is short enough for a Unix socket.
func controlSocket(t *testing.T, cfg *config.Config) control.Client {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "bwc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	cfg.Control.Socket = filepath.Join(dir, "c.sock")
	return control.Client{Socket: cfg.Control.Socket}
}

// waitFailed waits until the service is in the failed state and returns what the operator sees.
func waitFailed(t *testing.T, c control.Client) observability.Status {
	t.Helper()
	var st observability.Status
	waitUntil(t, "the failed state", 60*time.Second, func() bool {
		var err error
		st, err = c.Status(context.Background())
		return err == nil && st.State == observability.StateFailed && st.Failure != nil
	})
	return st
}

// failingStatement is a statement ClickHouse cannot follow: a table that binwave never saw is
// renamed onto a replicated one.
const failingStatement = "RENAME TABLE binwave.a TO binwave.a_old, binwave.tmp TO binwave.a"

func startFailingScenario(t *testing.T) (*testenv.MySQL, *testenv.ClickHouse, *hubEngine, control.Client) {
	t.Helper()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.a (id INT PRIMARY KEY, v VARCHAR(10) NOT NULL)", "CREATE TABLE binwave.tmp (id INT PRIMARY KEY, v VARCHAR(10) NOT NULL)")
	cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.a"]}`)
	cfg.DDL.Mode = config.DDLFull
	c := controlSocket(t, cfg)
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.a VALUES (1, 'a')")
	waitUntil(t, "the first row", 60*time.Second, func() bool { return chRowCount(t, ch, "a") == 1 })
	return my, ch, e, c
}

// TestAFailedTransactionStopsReplicationNotTheServiceAndIsShownWithItsGTID: the service stays up,
// the status names the transaction the way MySQL does (server_uuid:sequence, the one that is in
// gtid_executed), and the failure is on the metrics port without a key value.
func TestAFailedTransactionStopsReplicationNotTheServiceAndIsShownWithItsGTID(t *testing.T) {
	t.Parallel()
	my, _, e, c := startFailingScenario(t)
	var uuid string
	if err := my.DB.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}

	mustExec(t, my, failingStatement)
	st := waitFailed(t, c)

	f := st.Failure
	if f.Kind != failure.KindDDL || !strings.HasPrefix(f.GTID, uuid+":") || !strings.Contains(f.Statement, "RENAME TABLE") || f.Message == "" {
		t.Errorf("failure = %+v", f)
	}
	if ok, _ := e.hub.Healthy(); !ok {
		t.Error("the service is not healthy: a restart would only meet the same failure")
	}
	e.stillRunning(t)
	// The GTID is a transaction the source has executed.
	var contained int
	if err := my.DB.QueryRow("SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed)", f.GTID).Scan(&contained); err != nil || contained != 1 {
		t.Errorf("GTID_SUBSET(%s, gtid_executed) = %d (%v): the GTID shown is not one of the source's", f.GTID, contained, err)
	}
}

// TestAFailedTransactionCanBeRetriedAfterTheCauseIsFixed: resume without a skip tries the transaction
// again. The cause here is a leftover table in ClickHouse; the operator removes it.
func TestAFailedTransactionCanBeRetriedAfterTheCauseIsFixed(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.a (id INT PRIMARY KEY, v VARCHAR(10) NOT NULL)")
	cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.*"]}`)
	cfg.DDL.Mode = config.DDLFull
	c := controlSocket(t, cfg)
	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.a VALUES (1, 'a')")
	waitUntil(t, "the first row", 60*time.Second, func() bool { return chRowCount(t, ch, "a") == 1 })
	if _, err := ch.DB.Exec("CREATE TABLE replica.b (id Int32) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}

	mustExec(t, my, "RENAME TABLE binwave.a TO binwave.b")
	st := waitFailed(t, c)
	if !strings.Contains(st.Failure.Message, "replica") {
		t.Errorf("the message does not name the leftover: %s", st.Failure.Message)
	}

	if _, err := ch.DB.Exec("DROP TABLE replica.b"); err != nil {
		t.Fatal(err)
	}
	if err := c.Resume(context.Background(), control.ResumeRequest{}); err != nil {
		t.Fatal(err)
	}

	waitUntil(t, "the rename to be applied", 60*time.Second, func() bool { return chRowCount(t, ch, "b") == 1 && chRowCount(t, ch, "a") == -1 })
	mustExec(t, my, "INSERT INTO binwave.b VALUES (2, 'b')")
	waitSame(t, my, ch, pair{"after", "SELECT id, v FROM binwave.b ORDER BY id", "SELECT id, v FROM replica.b FINAL ORDER BY id"})
	e.stillRunning(t)
}

// TestAFailedTransactionCanBeSkippedByCountOrByGTID: the transaction is dropped, the position moves past
// it, and replication goes on with what follows. Both forms are tried.
func TestAFailedTransactionCanBeSkippedByCountOrByGTID(t *testing.T) {
	t.Parallel()
	for _, form := range []string{"count", "gtid"} {
		t.Run(form, func(t *testing.T) {
			t.Parallel()
			my, ch, e, c := startFailingScenario(t)
			mustExec(t, my, failingStatement, "INSERT INTO binwave.a VALUES (2, 'after')")
			st := waitFailed(t, c)

			req := control.ResumeRequest{SkipTransactions: 1}
			if form == "gtid" {
				req = control.ResumeRequest{GTIDNext: st.Failure.GTID}
			}
			if err := c.Resume(context.Background(), req); err != nil {
				t.Fatal(err)
			}

			waitUntil(t, "the row after the skipped transaction", 60*time.Second, func() bool { return chRowCount(t, ch, "a") == 2 })
			after, err := c.Status(context.Background())
			if err != nil || after.State != observability.StateReady || after.Failure != nil {
				t.Errorf("status after the skip: %+v (%v)", after, err)
			}
			e.stillRunning(t)
			if got := e.hub.Status().Failure; got != nil {
				t.Errorf("the failure is still there: %+v", got)
			}
		})
	}
}

// TestSkippingIsRefusedWhileReplicationRuns: like MySQL, only a stopped replica skips.
func TestSkippingIsRefusedWhileReplicationRuns(t *testing.T) {
	t.Parallel()
	_, _, e, c := startFailingScenario(t)

	err := c.Resume(context.Background(), control.ResumeRequest{SkipTransactions: 1})

	if err == nil || !strings.Contains(err.Error(), "replication is running") {
		t.Errorf("Resume() = %v", err)
	}
	e.stillRunning(t)
}

// waitFailedHub waits until the engine is in the failed state and returns the failure.
func waitFailedHub(t *testing.T, e *hubEngine) failure.Failure {
	t.Helper()
	var f *failure.Failure
	waitUntil(t, "the failed state", 60*time.Second, func() bool {
		st := e.hub.Status()
		f = st.Failure
		return st.State == observability.StateFailed && f != nil
	})
	select {
	case err := <-e.done:
		t.Fatalf("the engine stopped instead of staying up in the failed state: %v", err)
	default:
	}
	return *f
}
