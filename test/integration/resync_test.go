//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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
// anywhere. Repeating it from any of the states finishes the work, and a table that has no shadow table
// and is not marked is left alone.
func TestSwapShadowIsRepeatableAtEveryStep(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	sink := openSink(t, ch)
	ctx := context.Background()
	req := chsink.SwapRequest{Source: "shop.t", Database: "swapdb", Table: "t", Shadow: "t__resync"}
	execCH(t, ch, "CREATE DATABASE IF NOT EXISTS swapdb")

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
		execCH(t, ch, "DROP TABLE IF EXISTS swapdb.t", "DROP TABLE IF EXISTS swapdb.t__resync",
			"CREATE TABLE swapdb.t (v String) ENGINE = MergeTree ORDER BY v",
			"CREATE TABLE swapdb.t__resync (v String) ENGINE = MergeTree ORDER BY v COMMENT '"+chsink.ResyncMarker+"'",
			"INSERT INTO swapdb.t VALUES ('old')", "INSERT INTO swapdb.t__resync VALUES ('new')")
	}
	value := func() string {
		var v string
		if err := ch.DB.QueryRow("SELECT v FROM swapdb.t").Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}

	setup()
	if err := sink.SwapShadow(ctx, req); err != nil {
		t.Fatalf("SwapShadow() = %v", err)
	}
	if got, want := names(), "t:"; got != want || value() != "new" {
		t.Errorf("after the swap: tables %q value %q, want %q and new", got, value(), want)
	}
	// Repeated after the whole thing was done and only the position was lost.
	if err := sink.SwapShadow(ctx, req); err != nil || value() != "new" {
		t.Errorf("repeated swap = %v, value %q: it must change nothing", err, value())
	}

	// A crash right after the EXCHANGE: the table has the marker, the shadow name holds the old table.
	setup()
	execCH(t, ch, "EXCHANGE TABLES swapdb.t AND swapdb.t__resync")
	if err := sink.SwapShadow(ctx, req); err != nil {
		t.Fatalf("SwapShadow() after the exchange = %v", err)
	}
	if got := names(); got != "t:" || value() != "new" {
		t.Errorf("after finishing an exchange: tables %q value %q", got, value())
	}

	// A crash after the old table was dropped, before the marker was removed.
	setup()
	execCH(t, ch, "EXCHANGE TABLES swapdb.t AND swapdb.t__resync", "DROP TABLE swapdb.t__resync")
	if err := sink.SwapShadow(ctx, req); err != nil {
		t.Fatalf("SwapShadow() before the marker was removed = %v", err)
	}
	if got := names(); got != "t:" || value() != "new" {
		t.Errorf("after removing the marker: tables %q value %q", got, value())
	}

	// A shadow table that is not the one binwave made is not put in the place of anything.
	setup()
	execCH(t, ch, "ALTER TABLE swapdb.t__resync MODIFY COMMENT 'made by hand'")
	err := sink.SwapShadow(ctx, req)
	if !chsink.IsConflict(err) {
		t.Errorf("a shadow table without the marker: %v, want a conflict", err)
	}
	if value() != "old" {
		t.Errorf("a table was replaced by a shadow table binwave did not make")
	}
}
