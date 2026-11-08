//go:build integration

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/test/testenv"
)

// TestInitialLoadReadsExistingRowsWhileTheSourceIsWritten: rows already in the source before binwave
// ever starts are read by the initial load, while the source keeps changing; when the writing stops
// ClickHouse holds exactly what MySQL holds, and nothing of the load is left in the saved state.
func TestInitialLoadReadsExistingRowsWhileTheSourceIsWritten(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")

	const rows = 20000
	for from := 1; from <= rows; from += 1000 {
		var values []string
		for n := from; n < from+1000 && n <= rows; n++ {
			values = append(values, fmt.Sprintf("(%d, 'o%d', %d)", n, n, n))
		}
		mustExec(t, my, "INSERT INTO binwave.acct VALUES "+strings.Join(values, ","))
	}

	cfg := engineConfig(t, my, ch, "clickhouse")
	cfg.Replication.InitialPosition = config.StartFromSnapshot
	cfg.Replication.Snapshot.Parallelism = 2
	cfg.Replication.Snapshot.MaxRowsPerSecond = 10000 // a load of two seconds, long enough to be seen in the metrics

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			id := 1 + (i*7919)%rows
			_, _ = my.DB.Exec("UPDATE binwave.acct SET qty = qty + 1 WHERE id = ?", id)
			if i%4 == 0 {
				_, _ = my.DB.Exec("INSERT IGNORE INTO binwave.acct VALUES (?, 'new', 1)", rows+1+i)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	e := startHubEngine(t, cfg)
	var sawTheLoad bool // the metrics of the initial load counted the table while it was loaded
	waitUntil(t, "the initial load to finish", 120*time.Second, func() bool {
		if metricValue(t, e.hub, "binwave_initial_load_covered_tables") == 1 &&
			metricValue(t, e.hub, `binwave_initial_load_tables{state="loaded"}`) == 0 {
			sawTheLoad = true
		}
		if metricValue(t, e.hub, "binwave_resyncing_tables") != 0 {
			t.Errorf("the initial load is counted among the resyncs an operator asked for")
		}
		return metricValue(t, e.hub, "binwave_table_resyncs_total") == 1
	})
	close(stop)
	<-writerDone

	waitForConvergence(t, my, ch)
	if !sawTheLoad {
		t.Error("binwave_initial_load_covered_tables never counted the table being loaded")
	}
	if got := metricValue(t, e.hub, "binwave_initial_load_covered_tables"); got != 0 {
		t.Errorf("initial load covered tables = %v after the load finished", got)
	}
	if got := metricValue(t, e.hub, "binwave_resyncing_tables"); got != 0 {
		t.Errorf("resyncing tables = %v after the initial load", got)
	}
	// Every row the load read is counted, the ones read since the last publication of the statuses too;
	// the writer adds rows during the load, so there can be more.
	if got := metricValue(t, e.hub, "binwave_resync_rows_read_total"); got < rows {
		t.Errorf("rows read by the initial load = %v, want at least the %d the table had", got, rows)
	}
	if pos := loadPosition(t, ch, cfg); pos.Resync != "" {
		t.Errorf("the state of the initial load is still saved: %s", pos.Resync)
	}

	// The stream goes on after the load finished: a row written now arrives.
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (900000, 'after', 1)")
	waitForConvergence(t, my, ch)

	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}

// TestInitialLoadQuarantinesAKeylessTable: a table with no key at all is quarantined the way it
// always would be, and the tables that can be loaded are not held up by it.
func TestInitialLoadQuarantinesAKeylessTable(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)",
		"CREATE TABLE binwave.keyless (a INT)",
		"INSERT INTO binwave.acct VALUES (1, 'a', 1), (2, 'b', 2)",
	)
	cfg := engineConfig(t, my, ch, "clickhouse")
	cfg.Replication.InitialPosition = config.StartFromSnapshot

	e := startHubEngine(t, cfg)
	waitForConvergence(t, my, ch)
	waitUntil(t, "the keyless table to be quarantined", 60*time.Second, func() bool {
		return len(e.hub.Status().Quarantined) == 1
	})
	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}

// TestInitialLoadQuarantinesATableWithAnUnchunkableKey: a table with a real primary key, one that
// replicates from the stream just fine, but that a chunk cannot resume from (here, a FLOAT key) is
// quarantined right at the first start rather than silently missing its entire history forever, which
// is a silent distortion of data. A table that can be loaded is not held up by it.
func TestInitialLoadQuarantinesATableWithAnUnchunkableKey(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)",
		"CREATE TABLE binwave.floatkeyed (id FLOAT PRIMARY KEY, note VARCHAR(20) NOT NULL)",
		"INSERT INTO binwave.acct VALUES (1, 'a', 1), (2, 'b', 2)",
		"INSERT INTO binwave.floatkeyed VALUES (1.5, 'x')",
	)
	cfg := engineConfig(t, my, ch, "clickhouse")
	cfg.Replication.InitialPosition = config.StartFromSnapshot

	e := startHubEngine(t, cfg)
	waitForConvergence(t, my, ch)
	waitUntil(t, "the unchunkable table to be quarantined", 60*time.Second, func() bool {
		st := e.hub.Status()
		for _, q := range st.Quarantined {
			if q.Table == "binwave.floatkeyed" {
				return true
			}
		}
		return false
	})
	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}
