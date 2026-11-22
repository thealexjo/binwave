//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/control"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/test/testenv"
)

// TestResyncBringsATableBackWhileTheSourceIsWritten: a table that diverged from its source (a row the
// source does not have, a row it lost) is read again and replaced by the copy while the source goes on
// changing, and when the writing stops ClickHouse holds what MySQL holds. Nothing of the resync is left.
func TestResyncBringsATableBackWhileTheSourceIsWritten(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "clickhouse")
	cfg.Control.Socket = shortSocket(t)
	c := control.Client{Socket: cfg.Control.Socket}
	ctx := context.Background()

	const rows = 30000
	e := startHubEngine(t, cfg)
	for from := 1; from <= rows; from += 1000 {
		var values []string
		for n := from; n < from+1000 && n <= rows; n++ {
			values = append(values, fmt.Sprintf("(%d, 'o%d', %d)", n, n, n))
		}
		mustExec(t, my, "INSERT INTO binwave.acct VALUES "+strings.Join(values, ","))
	}
	waitForConvergence(t, my, ch)

	// The table diverges: a row that the source never had, with a version above any the stream will use.
	execCH(t, ch, "INSERT INTO replica.acct (id, owner, qty, _version, _is_deleted) VALUES (999999, 'ghost', 1, 18446744073709551000, 0)")
	if _, ok := clickhouseAccounts(t, ch); !ok {
		t.Fatal("the table cannot be read")
	}

	// The source keeps changing while the resync runs: updates, deletes and inserts.
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			id := 1 + (i*7919)%rows
			_, _ = my.DB.Exec("UPDATE binwave.acct SET qty = qty + 1 WHERE id = ?", id)
			if i%5 == 0 {
				_, _ = my.DB.Exec("DELETE FROM binwave.acct WHERE id = ?", 1+(i*104729)%rows)
			}
			if i%3 == 0 {
				_, _ = my.DB.Exec("INSERT IGNORE INTO binwave.acct VALUES (?, 'new', 1)", rows+1+i)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	results, err := c.Resync(ctx, []string{"binwave.acct"})
	if err != nil || len(results) != 1 || results[0].Error != "" {
		close(stop)
		writer.Wait()
		t.Fatalf("Resync() = %+v, %v", results, err)
	}
	waitUntil(t, "the resync to finish", 120*time.Second, func() bool {
		return metricValue(t, e.hub, "binwave_table_resyncs_total") == 1
	})
	close(stop)
	writer.Wait()

	waitForConvergence(t, my, ch) // includes the ghost row: it is gone
	if got := metricValue(t, e.hub, "binwave_resyncing_tables"); got != 0 {
		t.Errorf("resyncing tables = %v after the resync", got)
	}
	var shadows uint64
	if err := ch.DB.QueryRow("SELECT count() FROM system.tables WHERE database = 'replica' AND name LIKE '%__resync'").Scan(&shadows); err != nil || shadows != 0 {
		t.Errorf("shadow tables left = %d (%v)", shadows, err)
	}
	var comment string
	if err := ch.DB.QueryRow("SELECT comment FROM system.tables WHERE database = 'replica' AND name = 'acct'").Scan(&comment); err != nil || strings.Contains(comment, "binwave") {
		t.Errorf("the table still carries the resync marker %q (%v)", comment, err)
	}
	if pos := loadPosition(t, ch, cfg); pos.Resync != "" {
		t.Errorf("the state of the resync is still saved: %s", pos.Resync)
	}
	// The stream goes on after the swap: a row written now arrives.
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (900000, 'after', 1)")
	waitForConvergence(t, my, ch)

	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}

// TestResyncRefusesWhatItCannotDo: a table that is not replicated, a service that is paused, and a
// shadow table left by an earlier resync are refused with a reason, and nothing is changed.
func TestResyncRefusesWhatItCannotDo(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
	cfg := engineConfig(t, my, ch, "clickhouse")
	cfg.Control.Socket = shortSocket(t)
	c := control.Client{Socket: cfg.Control.Socket}
	ctx := context.Background()

	e := startHubEngine(t, cfg)
	mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'a', 1)")
	waitForConvergence(t, my, ch)

	results, err := c.Resync(ctx, []string{"other.acct"})
	if err != nil || len(results) != 1 || !strings.Contains(results[0].Error, "not a replicated table") {
		t.Errorf("Resync(other.acct) = %+v, %v", results, err)
	}

	execCH(t, ch, "CREATE TABLE replica.acct__resync (id Int32) ENGINE = MergeTree ORDER BY id")
	results, err = c.Resync(ctx, []string{"binwave.acct"})
	if err != nil || len(results) != 1 || !strings.Contains(results[0].Error, "acct__resync") {
		t.Errorf("Resync with a leftover shadow table = %+v, %v", results, err)
	}
	var n uint64
	if err := ch.DB.QueryRow("SELECT count() FROM system.tables WHERE database = 'replica' AND name = 'acct__resync'").Scan(&n); err != nil || n != 1 {
		t.Errorf("the leftover table was dropped (%d, %v): binwave must not touch what it did not just create", n, err)
	}
	execCH(t, ch, "DROP TABLE replica.acct__resync")

	if err := c.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	results, err = c.Resync(ctx, []string{"binwave.acct"})
	if err != nil || len(results) != 1 || !strings.Contains(results[0].Error, "not running") {
		t.Errorf("Resync while paused = %+v, %v", results, err)
	}
	if err := e.stop(); err != nil {
		t.Errorf("graceful stop returned %v", err)
	}
}

// TestSwapShadowIsRepeatableAtEveryStep: the swap is one EXCHANGE and two cleanups, and a crash may end it
// anywhere. Repeating it from any of the states finishes the work, also for a table whose own name ends
// like a shadow table and for a shadow table an earlier release marked, and a table that has no shadow
// table and is not marked is left alone.
func TestSwapShadowIsRepeatableAtEveryStep(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()
	execCH(t, ch, "CREATE DATABASE IF NOT EXISTS swapdb")

	for _, c := range []struct{ table, marker string }{
		{"t", chsink.ResyncMarker("t")},
		{"t__resync", chsink.ResyncMarker("t__resync")},
		{"t", "binwave: resync shadow"},
	} {
		shadow := c.table + "__resync"
		req := chsink.SwapRequest{Source: "shop." + c.table, Database: "swapdb", Table: c.table, Shadow: shadow}
		table, shadowTable := "swapdb.`"+c.table+"`", "swapdb.`"+shadow+"`"
		done := c.table + ":"

		names := func() string {
			rows, err := ch.DB.Query("SELECT name || ':' || comment FROM system.tables WHERE database = 'swapdb' ORDER BY name")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			var out []string
			for rows.Next() {
				var s string
				if err := rows.Scan(&s); err != nil {
					t.Fatal(err)
				}
				out = append(out, s)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			return strings.Join(out, ",")
		}
		setup := func() {
			execCH(t, ch, "DROP TABLE IF EXISTS "+table, "DROP TABLE IF EXISTS "+shadowTable,
				"CREATE TABLE "+table+" (v String) ENGINE = MergeTree ORDER BY v",
				"CREATE TABLE "+shadowTable+" (v String) ENGINE = MergeTree ORDER BY v COMMENT '"+c.marker+"'",
				"INSERT INTO "+table+" VALUES ('old')", "INSERT INTO "+shadowTable+" VALUES ('new')")
		}
		value := func() string {
			var v string
			if err := ch.DB.QueryRow("SELECT v FROM " + table).Scan(&v); err != nil {
				t.Fatal(err)
			}
			return v
		}

		marks := func(table, shadow, shadowExists bool, when string) {
			t.Helper()
			want := chsink.SwapMarks{Table: table, Shadow: shadow, ShadowExists: shadowExists}
			if got, err := sink.SwapMarks(ctx, req); err != nil || got != want {
				t.Errorf("%s: SwapMarks() %s = %+v, %v, want %+v", c.marker, when, got, err, want)
			}
		}

		setup()
		marks(false, true, true, "before the swap")
		if sent, err := sink.SwapShadow(ctx, req); err != nil || !sent {
			t.Fatalf("%s: SwapShadow() = %v, %v, want the exchange sent", c.marker, sent, err)
		}
		marks(false, false, false, "after the swap")
		if got := names(); got != done || value() != "new" {
			t.Errorf("%s: after the swap: tables %q value %q, want %q and new", c.marker, got, value(), done)
		}
		// Repeated because the answer was lost: finished, it changes nothing.
		repeat := req
		repeat.Exchanged = true
		if _, err := sink.SwapShadow(ctx, repeat); err != nil || value() != "new" {
			t.Errorf("%s: repeated swap = %v, value %q: it must change nothing", c.marker, err, value())
		}

		// A crash right after the EXCHANGE: the table has the marker, the shadow name holds the old table.
		setup()
		execCH(t, ch, "EXCHANGE TABLES "+table+" AND "+shadowTable)
		marks(true, false, true, "after the exchange")
		if _, err := sink.SwapShadow(ctx, req); err != nil {
			t.Fatalf("%s: SwapShadow() after the exchange = %v", c.marker, err)
		}
		if got := names(); got != done || value() != "new" {
			t.Errorf("%s: after finishing an exchange: tables %q value %q", c.marker, got, value())
		}

		// A crash after the old table was dropped, before the marker was removed.
		setup()
		execCH(t, ch, "EXCHANGE TABLES "+table+" AND "+shadowTable, "DROP TABLE "+shadowTable)
		marks(true, false, false, "after the old table was dropped")
		if _, err := sink.SwapShadow(ctx, req); err != nil {
			t.Fatalf("%s: SwapShadow() before the marker was removed = %v", c.marker, err)
		}
		if got := names(); got != done || value() != "new" {
			t.Errorf("%s: after removing the marker: tables %q value %q", c.marker, got, value())
		}

		// An exchange sent before whose answer was lost, and that is not made yet (it may still be running
		// on the server): it is not sent again, which could exchange the tables back.
		setup()
		repeat = req
		repeat.Exchanged = true
		if sent, err := sink.SwapShadow(ctx, repeat); !sent || err == nil {
			t.Errorf("%s: a repeat of an exchange not made yet: %v, %v, want an error and the exchange counted as sent", c.marker, sent, err)
		}
		if value() != "old" {
			t.Errorf("%s: a repeat of an exchange not made yet changed the table", c.marker)
		}

		// A shadow table lost before the swap: no swap was made, and nothing takes the table's place. Without
		// Exchanged this is not taken for a swap finished before.
		setup()
		execCH(t, ch, "DROP TABLE "+shadowTable)
		if sent, err := sink.SwapShadow(ctx, req); sent || !chsink.IsTableProblem(err) {
			t.Errorf("%s: a lost shadow table: %v, %v, want an error of the table and nothing sent", c.marker, sent, err)
		}
		if value() != "old" {
			t.Errorf("%s: a lost shadow table changed the table", c.marker)
		}

		// A shadow table that is not the one binwave made is not put in the place of anything.
		setup()
		execCH(t, ch, "ALTER TABLE "+shadowTable+" MODIFY COMMENT 'made by hand'")
		marks(false, false, true, "with a shadow table without the marker")
		_, err := sink.SwapShadow(ctx, req)
		if !chsink.IsConflict(err) {
			t.Errorf("%s: a shadow table without the marker: %v, want a conflict", c.marker, err)
		}
		if value() != "old" {
			t.Errorf("%s: a table was replaced by a shadow table binwave did not make", c.marker)
		}
		execCH(t, ch, "DROP TABLE IF EXISTS "+table, "DROP TABLE IF EXISTS "+shadowTable)
	}
}

// TestARestartInTheMiddleOfTheSwapOfAResyncLosesNothing: the swap of a resync is EXCHANGE TABLES, the
// drop of the old table and the removal of the marker, and the position without the resync is saved
// only after them. A crash between them leaves the resync saved, and the transactions after the saved
// position come again after the restart, before the swap is repeated: their rows must end up in the
// table that stays.
func TestARestartInTheMiddleOfTheSwapOfAResyncLosesNothing(t *testing.T) {
	t.Parallel()
	crashes := []struct {
		name  string
		steps []string
	}{
		{"after the exchange", []string{"EXCHANGE TABLES replica.acct AND replica.acct__resync"}},
		{"after the old table was dropped", []string{"EXCHANGE TABLES replica.acct AND replica.acct__resync", "DROP TABLE replica.acct__resync"}},
		{"after the marker was removed", []string{"EXCHANGE TABLES replica.acct AND replica.acct__resync", "DROP TABLE replica.acct__resync", "ALTER TABLE replica.acct MODIFY COMMENT ''"}},
	}
	// The entry as it was saved: read in full, or, by an earlier release that could make the exchange
	// before a position said so, still reading at the start of its last partition, with the marker of that
	// release. Where the shadow table is gone, the read begins again, with every partition.
	saves := []struct {
		name   string
		done   bool
		marker string
	}{{"read in full", true, chsink.ResyncMarker("acct")}, {"still reading by an earlier release", false, "binwave: resync shadow"}}
	for _, saved := range saves {
		for _, crash := range crashes {
			t.Run(saved.name+"/"+crash.name, func(t *testing.T) {
				t.Parallel()
				my := testenv.StartMySQL(t)
				ch := testenv.StartClickHouse(t)
				mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL) "+
					"PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (3), PARTITION p1 VALUES LESS THAN MAXVALUE)")
				cfg := engineConfig(t, my, ch, "file")
				w := my.Writer(t, "RELOAD")

				e := startHubEngine(t, cfg)
				mustExec(t, my, "INSERT INTO binwave.acct VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")
				waitForConvergence(t, my, ch)
				if err := e.stop(); err != nil {
					t.Fatal(err)
				}

				// The resync was read to the end and the stream has passed its last watermark: the swap was
				// begun. The copy holds what the source holds; the old table has a row it does not.
				pos := loadPosition(t, ch, cfg)
				entry := checkpoint.Resync{
					Table: "binwave.acct", Shadow: "acct__resync", Version: pos.Version, Since: pos.GTIDSet,
					Rows: 3, Done: true, Final: pos.GTIDSet, Started: time.Now(),
				}
				if !saved.done {
					entry.Done, entry.Final, entry.Strategy, entry.Partition, entry.Rows = false, "", "partitioned", "p1", 2
				}
				pos.Resync = checkpoint.EncodeResyncs([]checkpoint.Resync{entry})
				if err := checkpoint.NewFile(cfg.Checkpoint.FilePath).Save(context.Background(), pos); err != nil {
					t.Fatal(err)
				}
				execCH(t, ch,
					"CREATE TABLE replica.acct__resync AS replica.acct",
					"INSERT INTO replica.acct__resync SELECT * FROM replica.acct",
					"ALTER TABLE replica.acct__resync MODIFY COMMENT '"+saved.marker+"'",
					"INSERT INTO replica.acct (id, owner, qty, _version, _is_deleted) VALUES (999999, 'ghost', 1, 1, 0)")
				execCH(t, ch, crash.steps...)

				// What the source wrote after the saved position, which comes again after the restart. It is in
				// a binary log of its own, as after a rotation: the stream begins with it, with no transaction
				// before it that the source skips and reports with a heartbeat.
				if _, err := w.Exec("FLUSH BINARY LOGS"); err != nil {
					t.Fatal(err)
				}
				mustExec(t, my, "INSERT INTO binwave.acct VALUES (4, 'd', 4)")
				mustExec(t, my, "UPDATE binwave.acct SET qty = 10 WHERE id = 1")

				e = startHubEngine(t, cfg)
				waitUntil(t, "the resync to be finished", 60*time.Second, func() bool { return loadPosition(t, ch, cfg).Resync == "" })
				want := mysqlAccounts(t, my)
				var got map[int32]acct
				deadline := time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) {
					if got, _ = clickhouseAccounts(t, ch); sameAccounts(got, want) {
						break
					}
					time.Sleep(200 * time.Millisecond)
				}
				if !sameAccounts(got, want) {
					t.Errorf("ClickHouse holds %v, want what MySQL holds: %v", got, want)
				}
				if tables := chTables(t, ch); strings.Join(tables, ",") != "acct" {
					t.Errorf("tables %v, want only acct", tables)
				}
				if err := e.stop(); err != nil {
					t.Errorf("graceful stop returned %v", err)
				}
			})
		}
	}
}
