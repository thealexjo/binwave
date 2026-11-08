//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/thealexjo/binwave/test/testenv"
)

// TestAutoresyncFixesAGeneratedColumnWithoutAnOperator: ADD COLUMN ... GENERATED quarantines the
// table (binwave cannot reproduce the computed value of existing rows), and with tables.autoresync
// enabled the service resynchronizes it on its own; the table converges and the quarantine clears
// without any command from an operator.
func TestAutoresyncFixesAGeneratedColumnWithoutAnOperator(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "clickhouse")
	cfg.Tables.Autoresync = true

	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'a', 1), (2, 'b', 2)")
	waitForConvergence(t, my, ch)

	mustExec(t, my, "ALTER TABLE binwave.acct ADD COLUMN total INT GENERATED ALWAYS AS (qty * 2) STORED")
	mustExec(t, my, "INSERT INTO binwave.acct (id, owner, qty) VALUES (3, 'c', 3)")

	// tables.autoresync can fix this so fast that the quarantine may never be observed in a poll
	// (it can come and go between two checks): what must be true afterward is the metric (a counter,
	// so it survives) and the table itself, not that a moment of quarantine was ever caught live.
	waitUntil(t, "autoresync to resynchronize the table on its own", 60*time.Second, func() bool {
		return metricValue(t, e.hub, "binwave_autoresync_total") == 1
	})
	waitUntil(t, "the quarantine to clear", 30*time.Second, func() bool { return len(e.hub.Status().Quarantined) == 0 })
	waitForConvergence(t, my, ch)

	// The stream goes on normally afterward.
	mustExec(t, my, "INSERT INTO binwave.acct (id, owner, qty) VALUES (4, 'd', 4)")
	waitForConvergence(t, my, ch)

	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}

// TestAutoresyncOffLeavesTheTableQuarantined: the default (off) behaviour is unchanged — a
// recoverable quarantine stays quarantined until an operator resynchronizes it.
func TestAutoresyncOffLeavesTheTableQuarantined(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "clickhouse")

	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'a', 1)")
	waitForConvergence(t, my, ch)

	mustExec(t, my, "ALTER TABLE binwave.acct ADD COLUMN total INT GENERATED ALWAYS AS (qty * 2) STORED")
	waitUntil(t, "the table to be quarantined for the generated column", 30*time.Second, func() bool {
		return len(e.hub.Status().Quarantined) == 1
	})

	time.Sleep(3 * time.Second)
	if got := len(e.hub.Status().Quarantined); got != 1 {
		t.Errorf("quarantined = %d, want it to stay quarantined without tables.autoresync", got)
	}
	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}
