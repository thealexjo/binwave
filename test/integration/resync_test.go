//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/control"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/test/testenv"
)

// tableUUID is the UUID of a ClickHouse table.
func tableUUID(t *testing.T, ch *testenv.ClickHouse, database, table string) string {
	t.Helper()
	var uuid string
	if err := ch.DB.QueryRow("SELECT toString(uuid) FROM system.tables WHERE database = ? AND name = ?", database, table).Scan(&uuid); err != nil {
		t.Fatalf("the UUID of %s.%s: %v", database, table, err)
	}
	return uuid
}

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
	execCH(t, ch, "CREATE DATABASE IF NOT EXISTS warehouse")

	for _, c := range []struct{ table, marker string }{
		{"t", chsink.ResyncMarker("t")},
		{"t__resync", chsink.ResyncMarker("t__resync")},
		{"t", "binwave: resync shadow"},
	} {
		shadow := c.table + "__resync"
		req := chsink.SwapRequest{Source: "shop." + c.table, Database: "warehouse", Table: c.table, Shadow: shadow}
		table, shadowTable := "warehouse.`"+c.table+"`", "warehouse.`"+shadow+"`"
		done := c.table + ":"

		names := func() string {
			rows, err := ch.DB.Query("SELECT name || ':' || comment FROM system.tables WHERE database = 'warehouse' ORDER BY name")
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
		copyUUID := tableUUID(t, ch, "warehouse", shadow)
		if copied, err := sink.SwapShadow(ctx, req); err != nil || copied != copyUUID {
			t.Fatalf("%s: SwapShadow() = %q, %v, want the UUID of the copy %s", c.marker, copied, err, copyUUID)
		}
		marks(false, false, false, "after the swap")
		if got := names(); got != done || value() != "new" {
			t.Errorf("%s: after the swap: tables %q value %q, want %q and new", c.marker, got, value(), done)
		}
		// Repeated because the answer was lost: the table is the copy, by its UUID, and nothing changes.
		repeat := req
		repeat.Copy = copyUUID
		if _, err := sink.SwapShadow(ctx, repeat); err != nil || value() != "new" {
			t.Errorf("%s: repeated swap = %v, value %q: it must change nothing", c.marker, err, value())
		}
		// Repeated with the UUID of another copy: what is in the place of the table is not the copy.
		repeat.Copy = "0c000000-0000-0000-0000-0000000000cc"
		if _, err := sink.SwapShadow(ctx, repeat); !chsink.IsConflict(err) || value() != "new" {
			t.Errorf("%s: a swap that finds another table than its copy = %v, value %q: want a conflict, nothing changed", c.marker, err, value())
		}

		// What binwave does right before the exchange: the marker names the UUID of the table it replaces.
		exchange := func() {
			marker := chsink.Replacing(c.marker, tableUUID(t, ch, "warehouse", c.table), tableUUID(t, ch, "warehouse", shadow))
			execCH(t, ch, "ALTER TABLE "+shadowTable+" MODIFY COMMENT '"+marker+"'", "EXCHANGE TABLES "+table+" AND "+shadowTable)
		}

		// A crash right after the EXCHANGE: the table has the marker, the shadow name holds the old table.
		setup()
		exchange()
		marks(true, false, true, "after the exchange")
		if _, err := sink.SwapShadow(ctx, req); err != nil {
			t.Fatalf("%s: SwapShadow() after the exchange = %v", c.marker, err)
		}
		if got := names(); got != done || value() != "new" {
			t.Errorf("%s: after finishing an exchange: tables %q value %q", c.marker, got, value())
		}

		// The same with a marker that names no table replaced, as written before markers did: which table
		// is under the shadow name cannot be told, and it is left.
		setup()
		execCH(t, ch, "EXCHANGE TABLES "+table+" AND "+shadowTable)
		if _, err := sink.SwapShadow(ctx, req); err != nil {
			t.Fatalf("%s: SwapShadow() after an exchange with a marker naming no table = %v", c.marker, err)
		}
		if got, want := names(), c.table+":,"+shadow+":"; got != want || value() != "new" {
			t.Errorf("%s: after an exchange with a marker naming no table: tables %q value %q, want %q and new", c.marker, got, value(), want)
		}

		// A crash after the old table was dropped, before the marker was removed.
		setup()
		exchange()
		execCH(t, ch, "DROP TABLE "+shadowTable)
		marks(true, false, false, "after the old table was dropped")
		if _, err := sink.SwapShadow(ctx, req); err != nil {
			t.Fatalf("%s: SwapShadow() before the marker was removed = %v", c.marker, err)
		}
		if got := names(); got != done || value() != "new" {
			t.Errorf("%s: after removing the marker: tables %q value %q", c.marker, got, value())
		}

		// The same, and another table took the name of the shadow table while the service was down: it is
		// not the table the swap replaced, and it is left.
		setup()
		exchange()
		execCH(t, ch, "DROP TABLE "+shadowTable, "CREATE TABLE "+shadowTable+" (v String) ENGINE = MergeTree ORDER BY v COMMENT 'made by hand'")
		if _, err := sink.SwapShadow(ctx, req); err != nil {
			t.Fatalf("%s: SwapShadow() with another table under the shadow name = %v", c.marker, err)
		}
		if got, want := names(), c.table+":,"+shadow+":made by hand"; got != want || value() != "new" {
			t.Errorf("%s: another table under the shadow name: tables %q value %q, want %q and new", c.marker, got, value(), want)
		}

		// An exchange sent before whose answer was lost, and that was not made: it is sent again.
		setup()
		repeat = req
		repeat.Copy = tableUUID(t, ch, "warehouse", shadow)
		if _, err := sink.SwapShadow(ctx, repeat); err != nil || names() != done || value() != "new" {
			t.Errorf("%s: a repeat of an exchange not made = %v, tables %q value %q", c.marker, err, names(), value())
		}

		// An exchange still running (sent by a process that crashed, its answer lost): no second is sent
		// while it runs, by a new process too, since the query id of every exchange of the table is the same.
		setup()
		running := make(chan error, 1)
		go func() {
			_, err := ch.DB.ExecContext(chgo.Context(ctx, chgo.WithQueryID(chsink.ExchangeQueryID("warehouse", c.table))), "SELECT sleep(3)")
			running <- err
		}()
		waitUntil(t, "the query to run", 10*time.Second, func() bool {
			var n uint64
			if err := ch.DB.QueryRow("SELECT count() FROM system.processes WHERE query_id = ?", chsink.ExchangeQueryID("warehouse", c.table)).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n > 0
		})
		copied, err := sink.SwapShadow(ctx, req)
		if copied != "" || !chsink.IsTransient(err) {
			t.Errorf("%s: an exchange while one runs: %q, %v, want a transient error and no exchange of this attempt", c.marker, copied, err)
		}
		if value() != "old" {
			t.Errorf("%s: an exchange was made while one ran", c.marker)
		}
		if err := <-running; err != nil {
			t.Fatal(err)
		}
		if _, err := sink.SwapShadow(ctx, req); err != nil || names() != done || value() != "new" {
			t.Errorf("%s: the swap once the query ended = %v, tables %q value %q", c.marker, err, names(), value())
		}

		// A shadow table lost before the swap: no swap was made, and nothing takes the table's place. Without
		// the UUID of a copy exchanged before this is not taken for a swap finished before.
		setup()
		execCH(t, ch, "DROP TABLE "+shadowTable)
		if copied, err := sink.SwapShadow(ctx, req); copied != "" || !chsink.IsTableProblem(err) {
			t.Errorf("%s: a lost shadow table: %q, %v, want an error of the table and nothing sent", c.marker, copied, err)
		}
		if value() != "old" {
			t.Errorf("%s: a lost shadow table changed the table", c.marker)
		}

		// A shadow table that is not the one binwave made is not put in the place of anything.
		setup()
		execCH(t, ch, "ALTER TABLE "+shadowTable+" MODIFY COMMENT 'made by hand'")
		marks(false, false, true, "with a shadow table without the marker")
		_, err = sink.SwapShadow(ctx, req)
		if !chsink.IsConflict(err) {
			t.Errorf("%s: a shadow table without the marker: %v, want a conflict", c.marker, err)
		}
		if value() != "old" {
			t.Errorf("%s: a table was replaced by a shadow table binwave did not make", c.marker)
		}
		execCH(t, ch, "DROP TABLE IF EXISTS "+table, "DROP TABLE IF EXISTS "+shadowTable)
	}
}

// TestATableIsDroppedOnlyByItsIdentity: binwave drops a table of its own by renaming it to a name made
// of its UUID, and dropping it there once the UUID is checked. A table under the name of a shadow table
// without the marker is left; a drop that a crash interrupted right after the rename is finished by the
// next drop of a shadow table, or the next swap, in the database; another table under the name of a drop
// is not moved, and the swap stops.
func TestATableIsDroppedOnlyByItsIdentity(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()
	execCH(t, ch, "CREATE DATABASE IF NOT EXISTS store")
	req := chsink.SwapRequest{Source: "shop.t", Database: "store", Table: "t", Shadow: "t__resync"}
	const (
		leftUUID  = "0d000000-0000-0000-0000-0000000000dd"
		oldUUID   = "0a000000-0000-0000-0000-0000000000aa"
		otherUUID = "0b000000-0000-0000-0000-0000000000bb"
	)
	trash := func(uuid string) string { return "binwave_drop_" + strings.ReplaceAll(uuid, "-", "") }
	tables := func() string {
		rows, err := textRows(ch.DB, "SELECT name || ':' || comment FROM system.tables WHERE database = 'store' ORDER BY name")
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(rows, ",")
	}
	reset := func() { execCH(t, ch, "DROP DATABASE IF EXISTS store SYNC", "CREATE DATABASE store") }

	// The shadow table of the resync is dropped, and nothing is left of it.
	reset()
	execCH(t, ch, "CREATE TABLE store.t__resync (v String) ENGINE = MergeTree ORDER BY v COMMENT '"+chsink.ResyncMarker("t")+"'")
	if dropped, err := sink.DropShadow(ctx, req); err != nil || !dropped || tables() != "" {
		t.Errorf("DropShadow() = %v, %v, tables %q: want it dropped and nothing left", dropped, err, tables())
	}

	// A table under its name without the marker is left.
	reset()
	execCH(t, ch, "CREATE TABLE store.t__resync (v String) ENGINE = MergeTree ORDER BY v COMMENT 'made by hand'")
	if dropped, err := sink.DropShadow(ctx, req); err != nil || dropped || tables() != "t__resync:made by hand" {
		t.Errorf("DropShadow() of a table made by hand = %v, %v, tables %q", dropped, err, tables())
	}

	// A crash right after the rename of the drop: the drop repeated finds the name of the shadow free, and
	// drops what the rename left all the same.
	reset()
	execCH(t, ch, "CREATE TABLE store.t__resync (v String) ENGINE = MergeTree ORDER BY v COMMENT '"+chsink.ResyncMarker("t")+"'")
	execCH(t, ch, "RENAME TABLE store.t__resync TO store."+trash(tableUUID(t, ch, "store", "t__resync")))
	if _, err := sink.DropShadow(ctx, req); err != nil || tables() != "" {
		t.Errorf("DropShadow() after a crash right after its rename = %v, tables %q: want nothing left", err, tables())
	}

	// The same, and the drop is not repeated (a restart does not): the next swap in the database drops it.
	reset()
	execCH(t, ch, "CREATE TABLE store."+trash(leftUUID)+" UUID '"+leftUUID+"' (v String) ENGINE = MergeTree ORDER BY v",
		"CREATE TABLE store.t (v String) ENGINE = MergeTree ORDER BY v",
		"CREATE TABLE store.t__resync (v String) ENGINE = MergeTree ORDER BY v COMMENT '"+chsink.ResyncMarker("t")+"'")
	if _, err := sink.SwapShadow(ctx, req); err != nil || tables() != "t:" {
		t.Errorf("SwapShadow() with a drop left by a crash = %v, tables %q: want only t", err, tables())
	}

	// After the exchange (the marker on the table names the old one): the old table, renamed by a drop a
	// crash interrupted, is dropped; a table the rename took by mistake goes back to its name.
	for _, c := range []struct{ name, uuid string }{{"the old table", oldUUID}, {"another table", otherUUID}} {
		reset()
		execCH(t, ch, "CREATE TABLE store.t (v String) ENGINE = MergeTree ORDER BY v",
			"CREATE TABLE store."+trash(oldUUID)+" UUID '"+c.uuid+"' (v String) ENGINE = MergeTree ORDER BY v")
		marker := chsink.Replacing(chsink.ResyncMarker("t"), oldUUID, tableUUID(t, ch, "store", "t"))
		execCH(t, ch, "ALTER TABLE store.t MODIFY COMMENT '"+marker+"'")
		_, err := sink.SwapShadow(ctx, req)
		if c.uuid == oldUUID {
			if err != nil || tables() != "t:" {
				t.Errorf("%s under the name of the drop: SwapShadow() = %v, tables %q, want it dropped", c.name, err, tables())
			}
			continue
		}
		// Another table under the name of the drop: it is not moved, and the swap stops.
		if want := trash(oldUUID) + ":,t:" + marker; !chsink.IsConflict(err) || tables() != want {
			t.Errorf("%s under the name of the drop: SwapShadow() = %v, tables %q, want a conflict and %q", c.name, err, tables(), want)
		}
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
	// The current release names the UUID of the table in the marker before the exchange, and the old table
	// is dropped by it; the marker of the earlier release names none, and the old table is left.
	saves := []struct {
		name   string
		done   bool
		marker string
		uuid   bool
	}{{"read in full", true, chsink.ResyncMarker("acct"), true}, {"still reading by an earlier release", false, "binwave: resync shadow", false}}
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
				execCH(t, ch, "CREATE TABLE replica.acct__resync AS replica.acct", "INSERT INTO replica.acct__resync SELECT * FROM replica.acct")
				marker := saved.marker
				if saved.uuid {
					marker = chsink.Replacing(marker, tableUUID(t, ch, "replica", "acct"), tableUUID(t, ch, "replica", "acct__resync"))
				}
				execCH(t, ch,
					"ALTER TABLE replica.acct__resync MODIFY COMMENT '"+marker+"'",
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
				wantTables := "acct"
				if !saved.uuid && crash.name == "after the exchange" {
					wantTables = "acct,acct__resync" // the old table, which nothing names
				}
				if tables := chTables(t, ch); strings.Join(tables, ",") != wantTables {
					t.Errorf("tables %v, want %s", tables, wantTables)
				}
				if err := e.stop(); err != nil {
					t.Errorf("graceful stop returned %v", err)
				}
			})
		}
	}
}
