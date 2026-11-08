//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/event"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/test/testenv"
)

// The tests in this file pin down behaviors of MySQL and ClickHouse that binwave's design depends
// on, found by studying the failure reports of comparable replication tools.

func opsOf(events []event.Event, table string) []string {
	var out []string
	for _, r := range rowsOf(events, table) {
		out = append(out, r.Op.String())
	}
	return out
}

// TestForeignKeyCascadesAreNotInTheBinlog documents a MySQL limitation no binlog reader can work
// around: rows removed or changed by ON DELETE / ON UPDATE CASCADE are handled inside InnoDB and
// never appear as row events (MySQL bug 32506). Whoever replicates such a schema must know that
// the child rows in ClickHouse are not updated. If a future MySQL starts logging them, this
// test fails and the documentation must change.
func TestForeignKeyCascadesAreNotInTheBinlog(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my,
		"CREATE TABLE binwave.parent (id INT PRIMARY KEY) ENGINE=InnoDB",
		`CREATE TABLE binwave.child (id INT PRIMARY KEY, parent_id INT NOT NULL,
			CONSTRAINT fk FOREIGN KEY (parent_id) REFERENCES binwave.parent (id) ON DELETE CASCADE ON UPDATE CASCADE) ENGINE=InnoDB`,
		"INSERT INTO binwave.parent VALUES (1), (2)",
		"INSERT INTO binwave.child VALUES (10, 1), (20, 2)",
	)
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, my,
		"DELETE FROM binwave.parent WHERE id = 1",        // cascades to child 10
		"UPDATE binwave.parent SET id = 22 WHERE id = 2", // cascades to child 20
	)
	events := streamCommits(t, src, start, 2)

	var remaining int
	if err := my.DB.QueryRow("SELECT COUNT(*) FROM binwave.child WHERE id = 10").Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("test premise broken: the cascade did not delete child 10 (%d rows left, %v)", remaining, err)
	}
	var parentID int
	if err := my.DB.QueryRow("SELECT parent_id FROM binwave.child WHERE id = 20").Scan(&parentID); err != nil || parentID != 22 {
		t.Fatalf("test premise broken: the cascade did not update child 20 (%d, %v)", parentID, err)
	}

	if got := opsOf(events, "binwave.parent"); len(got) != 2 {
		t.Errorf("parent changes = %v, want the delete and the update", got)
	}
	if got := opsOf(events, "binwave.child"); len(got) != 0 {
		t.Errorf("child changes = %v: cascades appeared in the binlog, so the documented limitation no longer holds", got)
	}
}

// TestChangesWithBinlogDisabledAreInvisible: a session with sql_log_bin=0 changes data without a
// trace in the binlog. No reader can see it; the operator must know.
func TestChangesWithBinlogDisabledAreInvisible(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	// Disabling the binlog for a session takes a privilege the test user does not have by default.
	mustExec(t, my, "CREATE TABLE binwave.quiet (id INT PRIMARY KEY)")
	if _, err := rootDB(t, my).Exec("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'binwave'@'%'"); err != nil {
		t.Fatal(err)
	}
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	conn, err := sql.Open("mysql", my.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	c, err := conn.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for _, s := range []string{"SET SESSION sql_log_bin = 0", "INSERT INTO binwave.quiet VALUES (1)", "SET SESSION sql_log_bin = 1", "INSERT INTO binwave.quiet VALUES (2)"} {
		if _, err := c.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	batches := rowsOf(streamCommits(t, src, start, 1), "binwave.quiet")
	if len(batches) != 1 || batches[0].Rows[0].After[0] != int32(2) {
		t.Errorf("stream delivered %#v, want only the row written with the binlog enabled", batches)
	}
}

func dumpThreadIDs(t *testing.T, my *testenv.MySQL) []int64 {
	t.Helper()
	rows, err := my.DB.Query("SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

// killDumpThreads closes the binlog connections of replicas on the server, like MySQL does when a
// replica stops reading (net_write_timeout) or an operator runs KILL.
func killDumpThreads(t *testing.T, my *testenv.MySQL) {
	t.Helper()
	ids := dumpThreadIDs(t, my)
	if len(ids) == 0 {
		t.Fatal("no binlog dump thread found")
	}
	root := rootDB(t, my)
	for _, id := range ids {
		if _, err := root.Exec("KILL " + strconv.FormatInt(id, 10)); err != nil {
			t.Fatalf("kill %d: %v", id, err)
		}
	}
}

// runningStream delivers the ids of inserted rows and remembers how the stream ended.
type runningStream struct {
	ids    chan int32
	commit chan struct{}
	done   chan error
}

func startIDStream(t *testing.T, src *mysqlsrc.Source, start string) *runningStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), streamTimeout)
	t.Cleanup(cancel)
	rs := &runningStream{ids: make(chan int32, 100000), commit: make(chan struct{}, 100), done: make(chan error, 1)}
	go func() {
		rs.done <- src.Stream(ctx, start, nil, func(_ context.Context, ev event.Event) error {
			switch e := ev.(type) {
			case event.Rows:
				for _, row := range e.Rows {
					rs.ids <- row.After[0].(int32)
				}
			case event.Commit:
				rs.commit <- struct{}{}
			}
			return nil
		})
	}()
	return rs
}

func (r *runningStream) next(t *testing.T) int32 {
	t.Helper()
	select {
	case id := <-r.ids:
		return id
	case err := <-r.done:
		t.Fatalf("the stream ended: %v", err)
	case <-time.After(45 * time.Second):
		t.Fatal("no change arrived")
	}
	return 0
}

// TestStreamResumesAfterTheServerClosesTheDumpConnection: the stream must resume without losing a
// change. go-mysql resumes from the last position it has fully accounted for, which lags by one
// transaction, so the last completed transaction may be delivered again. That is the
// at-least-once contract at the stream level; the pipeline absorbs it because a replay only
// rewrites the newest state.
func TestStreamResumesAfterTheServerClosesTheDumpConnection(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.k (id INT PRIMARY KEY)")
	var reconnects atomic.Int64
	src := openSource(t, my, mysqlsrc.Options{IdlePeriod: 500 * time.Millisecond, OnReconnect: func() { reconnects.Add(1) }})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs := startIDStream(t, src, start)

	mustExec(t, my, "INSERT INTO binwave.k VALUES (1)")
	if got := rs.next(t); got != 1 {
		t.Fatalf("first change = %d", got)
	}
	time.Sleep(time.Second) // let the transaction complete before the connection is cut
	killDumpThreads(t, my)
	mustExec(t, my, "INSERT INTO binwave.k VALUES (2)", "INSERT INTO binwave.k VALUES (3)")

	// Collect what arrives until 2 and 3 have both been seen.
	var got []int32
	seen := map[int32]bool{}
	for !seen[2] || !seen[3] {
		id := rs.next(t)
		got = append(got, id)
		seen[id] = true
	}
	for _, want := range []int32{2, 3} {
		if !seen[want] {
			t.Errorf("change %d was lost after the reconnect; delivered: %v", want, got)
		}
	}
	// Only the last completed transaction may repeat, and never after a newer one.
	for i, id := range got {
		if id == 1 && i > 0 {
			t.Errorf("change 1 came back after a newer change: %v", got)
		}
	}
	if len(got) > 3 {
		t.Errorf("more repeats than the one allowed: %v", got)
	}
	select {
	case err := <-rs.done:
		t.Errorf("the stream ended after recovering: %v", err)
	default:
	}
	if got := reconnects.Load(); got < 1 {
		t.Errorf("reconnects counted = %d, want at least 1 (binwave_source_reconnects_total)", got)
	}
}

// TestKillDuringALargeTransactionIsRecovered: when the connection dies in the middle of a
// transaction, go-mysql sends that transaction again from its first event. The stream must take
// it and deliver it whole; it must neither stall nor commit it with rows missing. Under backpressure
// MySQL closes the connection of a replica that does not read, so this happens in normal operation.
//
// The transaction is larger than the reader's look-ahead buffer (10240 events) and the handler is
// held back while the connection is killed, so the cut really falls inside the transaction.
func TestKillDuringALargeTransactionIsRecovered(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.big (id INT PRIMARY KEY, pad VARCHAR(250))")
	src := openSource(t, my, mysqlsrc.Options{IdlePeriod: 500 * time.Millisecond})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	const rows = 450_000
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	gate := make(chan struct{})
	ids := make(chan int32, 4096)
	committed := make(chan int, 1)
	done := make(chan error, 1)
	go func() {
		first := true
		done <- src.Stream(ctx, start, nil, func(_ context.Context, ev event.Event) error {
			switch e := ev.(type) {
			case event.Rows:
				if first {
					first = false
					select {
					case <-gate: // held here while the connection is killed
					case <-ctx.Done():
						return nil
					}
				}
				for _, row := range e.Rows {
					select {
					case ids <- row.After[0].(int32):
					case <-ctx.Done():
						return nil
					}
				}
			case event.Commit:
				committed <- 1
			}
			return nil
		})
	}()

	c, err := my.DB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for _, stmt := range []string{
		"SET SESSION cte_max_recursion_depth = 1000000",
		"START TRANSACTION",
		"INSERT INTO binwave.big SELECT seq, REPEAT('x', 200) FROM (WITH RECURSIVE s(seq) AS (SELECT 1 UNION ALL SELECT seq + 1 FROM s WHERE seq < 450000) SELECT seq FROM s) t",
		"COMMIT",
	} {
		if _, err := c.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("%.60s: %v", stmt, err)
		}
	}

	time.Sleep(3 * time.Second) // the reader fills its look-ahead buffer and blocks mid-transaction
	killDumpThreads(t, my)
	close(gate)

	deadline := time.After(3 * time.Minute)
	seen := map[int32]bool{}
	for {
		select {
		case id := <-ids:
			seen[id] = true
		case <-committed:
			// Every row was sent before the commit, so what is still buffered in the channel
			// belongs to the transaction too.
		drain:
			for {
				select {
				case id := <-ids:
					seen[id] = true
				default:
					break drain
				}
			}
			if len(seen) != rows {
				t.Fatalf("a transaction was committed with %d of %d rows delivered", len(seen), rows)
			}
			t.Log("the stream recovered and delivered the transaction whole")
			return
		case err := <-done:
			t.Fatalf("the stream ended after the connection was killed: %v", err)
		case <-deadline:
			t.Fatalf("the stream neither finished nor failed in 3 minutes; %d of %d rows seen", len(seen), rows)
		}
	}
}

func rootDB(t *testing.T, my *testenv.MySQL) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", "root:"+my.Password+"@tcp("+my.Host+":"+strconv.Itoa(my.Port)+")/")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestEqualVersionsKeepTheLastInsertedRow: rows of one transaction share a version, and a huge
// transaction is flushed in pieces, so a delete and a later re-insert of the same key can reach
// ClickHouse in separate inserts with equal versions. ReplacingMergeTree must keep the one
// inserted last, before and after a merge. The delete-then-reinsert ("nightly refresh") pattern
// broke a comparable tool (issue 1346), so this dependence is checked, not assumed.
func TestEqualVersionsKeepTheLastInsertedRow(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	execCH(t, ch,
		"CREATE DATABASE IF NOT EXISTS lessons",
		"CREATE TABLE lessons.a (k Int32, v String, _version UInt64, _is_deleted UInt8) ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY k",
		"CREATE TABLE lessons.b (k Int32, v String, _version UInt64, _is_deleted UInt8) ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY k",
	)
	// a: the tombstone arrives first, the re-insert last, with the same version.
	execCH(t, ch, "INSERT INTO lessons.a VALUES (1, 'old', 5, 1)", "INSERT INTO lessons.a VALUES (1, 'new', 5, 0)")
	// b: the re-insert first, the tombstone last: the row must be gone.
	execCH(t, ch, "INSERT INTO lessons.b VALUES (1, 'new', 5, 0)", "INSERT INTO lessons.b VALUES (1, 'old', 5, 1)")

	count := func(table string) int {
		var n int
		if err := ch.DB.QueryRow("SELECT count() FROM lessons." + table + " FINAL").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	check := func(stage string) {
		t.Helper()
		if got := count("a"); got != 1 {
			t.Errorf("%s: tombstone then re-insert with equal versions shows %d rows, want 1", stage, got)
		}
		if got := count("b"); got != 0 {
			t.Errorf("%s: re-insert then tombstone with equal versions shows %d rows, want 0", stage, got)
		}
	}
	check("before merge")
	execCH(t, ch, "OPTIMIZE TABLE lessons.a FINAL", "OPTIMIZE TABLE lessons.b FINAL")
	check("after merge")
}

// docker runs a docker CLI command for a test that has to freeze or kill a container.
func docker(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// TestStreamNoticesAFrozenServer: a server that stops answering without closing the connection
// (a network partition, a frozen VM) must not look healthy. A comparable tool stalled silently in
// this state until restarted by a workaround (issue 380). The source asks the server for
// heartbeats and fails with ErrSourceSilent when none arrive, so the process is restarted.
func TestStreamNoticesAFrozenServer(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.f (id INT PRIMARY KEY)")
	src := openSource(t, my, mysqlsrc.Options{IdlePeriod: 500 * time.Millisecond, HeartbeatPeriod: time.Second})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- src.Stream(ctx, start, nil, func(context.Context, event.Event) error { return nil })
	}()
	time.Sleep(4 * time.Second) // healthy and idle: server heartbeats keep the stream alive
	select {
	case err := <-done:
		t.Fatalf("an idle but healthy stream failed: %v", err)
	default:
	}

	docker(t, "pause", my.ContainerID)
	pausedAt := time.Now()
	t.Cleanup(func() { _ = exec.Command("docker", "unpause", my.ContainerID).Run() })

	select {
	case err := <-done:
		if !errors.Is(err, mysqlsrc.ErrSourceSilent) {
			t.Fatalf("the stream failed with %v, want ErrSourceSilent", err)
		}
		if took := time.Since(pausedAt); took > 20*time.Second {
			t.Errorf("the frozen server was noticed after %v, want a few heartbeat periods", took.Round(time.Second))
		}
	case <-time.After(45 * time.Second):
		t.Fatal("45s after the server froze the stream is still running: the watchdog would see a healthy process")
	}
}

// TestSlowConsumerIsNotMistakenForASilentSource: while the consumer is blocked (a slow ClickHouse
// applies backpressure), the source cannot be heard; that must not be blamed on the source.
func TestSlowConsumerIsNotMistakenForASilentSource(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my, "CREATE TABLE binwave.slow (id INT PRIMARY KEY)")
	src := openSource(t, my, mysqlsrc.Options{IdlePeriod: 500 * time.Millisecond, HeartbeatPeriod: time.Second})
	start, err := src.ExecutedGTIDSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	seen := make(chan int32, 16)
	done := make(chan error, 1)
	go func() {
		blocked := false
		done <- src.Stream(ctx, start, nil, func(_ context.Context, ev event.Event) error {
			if r, ok := ev.(event.Rows); ok {
				if !blocked {
					blocked = true
					time.Sleep(5 * time.Second) // longer than three heartbeat periods
				}
				for _, row := range r.Rows {
					seen <- row.After[0].(int32)
				}
			}
			return nil
		})
	}()

	mustExec(t, my, "INSERT INTO binwave.slow VALUES (1)", "INSERT INTO binwave.slow VALUES (2)")
	for _, want := range []int32{1, 2} {
		select {
		case got := <-seen:
			if got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		case err := <-done:
			t.Fatalf("the stream failed after a slow consumer: %v", err)
		case <-time.After(45 * time.Second):
			t.Fatalf("change %d never arrived", want)
		}
	}
}
