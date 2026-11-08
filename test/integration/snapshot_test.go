//go:build integration

package integration

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/gtid"
	"github.com/thealexjo/binwave/internal/snapshot"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/internal/typemap"
	"github.com/thealexjo/binwave/test/testenv"
)

// TestTableDefinitionFromInformationSchemaIsTheOneTheBinlogGives: a row read with SELECT and a row of
// the binlog must be the same to the pipeline, so the definition of the table read for a snapshot has
// to equal, column by column, the one the binlog describes, for every type.
func TestTableDefinitionFromInformationSchemaIsTheOneTheBinlogGives(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my, typesDDL)
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	insertTypeRows(t, my)
	batches := rowsOf(streamCommits(t, src, start, 4), "binwave.types")
	fromBinlog := batches[0].Table

	fromSchema, err := src.TableDefinition(ctx, "binwave", "types")
	if err != nil {
		t.Fatal(err)
	}

	if fromSchema.Signature() != fromBinlog.Signature() {
		t.Errorf("signatures differ:\n from information_schema:\n%s\n from the binlog:\n%s", fromSchema.Signature(), fromBinlog.Signature())
	}
	for i, want := range fromBinlog.Columns {
		if i >= len(fromSchema.Columns) {
			t.Fatalf("column %d %s is missing", i, want.Name)
		}
		if got := fromSchema.Columns[i]; !reflect.DeepEqual(got, want) {
			t.Errorf("column %s\n from information_schema: %+v\n from the binlog:        %+v", want.Name, got, want)
		}
	}
	if !reflect.DeepEqual(fromSchema.PrimaryKey, fromBinlog.PrimaryKey) {
		t.Errorf("primary key %v, the binlog says %v", fromSchema.PrimaryKey, fromBinlog.PrimaryKey)
	}
}

// TestRowsReadWithSelectConvertToTheValuesTheBinlogGives: the edge values of every type, read with
// SELECT (zero dates, extremes, NULLs, a TIMESTAMP written under another time zone, a latin1 text,
// binary values, JSON) convert with typemap to what the binlog path gives.
func TestRowsReadWithSelectConvertToTheValuesTheBinlogGives(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my, typesDDL)
	insertTypeRows(t, my)
	src := openSource(t, my, mysqlsrc.Options{})
	tbl, err := src.TableDefinition(ctx, "binwave", "types")
	if err != nil {
		t.Fatal(err)
	}

	rows, _, err := src.ReadChunk(ctx, snapshot.ChunkRequest{Table: tbl, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}

	want := expectedTypeRows()
	if len(rows) != len(want) {
		t.Fatalf("read %d rows, want %d", len(rows), len(want))
	}
	for i, row := range rows {
		got := convertRow(t, event.Rows{Table: tbl}, row)
		for _, col := range typeColumns {
			if !sameValue(got[col], want[i][col]) {
				t.Errorf("row %d, %s: SELECT gives %#v, the binlog path %#v", i+1, col, got[col], want[i][col])
			}
		}
	}
}

// TestReaderReadsTablesOfEveryKeyShape: a snapshot has to cut every kind of primary key: an integer
// (dense and sparse), a composite one, text, binary, a decimal and a date. Each table is read whole
// with chunks that are far smaller than it, and must come back complete, in order, without a row twice.
func TestReaderReadsTablesOfEveryKeyShape(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	shapes := []struct {
		name, ddl, insert string
	}{
		{"int", "id INT NOT NULL, PRIMARY KEY (id)", "SELECT n FROM seq"},
		{"sparse_bigint", "id BIGINT UNSIGNED NOT NULL, PRIMARY KEY (id)", "SELECT n * 1000003 FROM seq"},
		{"composite", "a INT NOT NULL, b VARCHAR(10) NOT NULL, PRIMARY KEY (a, b)", "SELECT n DIV 7, CONCAT('k', n MOD 7) FROM seq"},
		{"varchar_utf8", "id VARCHAR(40) NOT NULL, PRIMARY KEY (id)", "SELECT CONCAT('ключ-', LPAD(n, 6, '0')) FROM seq"},
		{"varchar_ci", "id VARCHAR(40) COLLATE utf8mb4_0900_ai_ci NOT NULL, PRIMARY KEY (id)", "SELECT CONCAT('Ab', n) FROM seq"},
		{"binary", "id VARBINARY(16) NOT NULL, PRIMARY KEY (id)", "SELECT UNHEX(LPAD(HEX(n), 8, '0')) FROM seq"},
		{"decimal", "id DECIMAL(20,4) NOT NULL, PRIMARY KEY (id)", "SELECT n / 8 FROM seq"},
		{"datetime", "id DATETIME(3) NOT NULL, PRIMARY KEY (id)", "SELECT '2020-01-01 00:00:00.000' + INTERVAL n * 1500 MICROSECOND FROM seq"},
		{"negative_int", "id INT NOT NULL, PRIMARY KEY (id)", "SELECT n - 1500 FROM seq"},
	}
	mustExec(t, my, "CREATE TABLE binwave.seq (n INT PRIMARY KEY)")
	// One session for the setup: the recursive query needs a deeper limit than the default 1000.
	conn, err := my.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, q := range []string{
		"SET SESSION cte_max_recursion_depth = 100000",
		"INSERT INTO binwave.seq WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 3000) SELECT n FROM s",
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatal(q, err)
		}
	}

	src := openSource(t, my, mysqlsrc.Options{})
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			name := "binwave.k_" + sh.name
			mustExec(t, my, "CREATE TABLE "+name+" ("+sh.ddl+")")
			if _, err := conn.ExecContext(ctx, "INSERT INTO "+name+" "+sh.insert); err != nil {
				t.Fatal(err)
			}
			tbl, err := src.TableDefinition(ctx, "binwave", "k_"+sh.name)
			if err != nil {
				t.Fatal(err)
			}

			r := snapshot.NewReader(src, tbl, nil, snapshot.Options{Sizer: snapshot.SizerOptions{Initial: 250, Min: 100, Max: 400}})
			var got [][]any
			chunks := 0
			for {
				c, err := r.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				chunks++
				got = append(got, c.Rows...)
				if c.Done {
					break
				}
			}

			var want int
			if err := my.DB.QueryRow("SELECT COUNT(*) FROM " + name).Scan(&want); err != nil {
				t.Fatal(err)
			}
			if len(got) != want || want != 3000 {
				t.Fatalf("read %d rows of %d (3000 were written)", len(got), want)
			}
			if chunks < 6 {
				t.Errorf("%d chunks: the table was not read in pieces", chunks)
			}
			// The order and the absence of repeats: the same as MySQL's own ORDER BY of the key.
			keys := strings.Join(tbl.PrimaryKeyColumns(), ", ")
			shown := make([]string, len(tbl.PrimaryKey))
			for i, idx := range tbl.PrimaryKey {
				shown[i] = tbl.Columns[idx].Name
				if k := tbl.Columns[idx].Kind; k == typemap.KindDateTime || k == typemap.KindDate || k == typemap.KindTimestamp {
					shown[i] = "CAST(" + shown[i] + " AS CHAR)" // the test's own connection parses dates
				}
			}
			order, err := my.DB.Query("SELECT " + strings.Join(shown, ", ") + " FROM " + name + " ORDER BY " + keys)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = order.Close() }()
			for i := 0; order.Next(); i++ {
				dest := make([]any, len(tbl.PrimaryKey))
				ptrs := make([]any, len(dest))
				for j := range ptrs {
					ptrs[j] = &dest[j]
				}
				if err := order.Scan(ptrs...); err != nil {
					t.Fatal(err)
				}
				if err := order.Err(); err != nil {
					t.Fatal(err)
				}
				gotKey := make([]any, len(tbl.PrimaryKey))
				for j, idx := range tbl.PrimaryKey {
					gotKey[j] = got[i][idx]
				}
				if fmt.Sprint(normalizeKey(gotKey)) != fmt.Sprint(normalizeKey(dest)) {
					t.Fatalf("row %d: the reader has key %v, ORDER BY has %v", i, normalizeKey(gotKey), normalizeKey(dest))
				}
			}
		})
	}
}

// normalizeKey renders key values in one form, whether they came as bytes or as text.
func normalizeKey(key []any) []string {
	out := make([]string, len(key))
	for i, v := range key {
		switch x := v.(type) {
		case []byte:
			out[i] = string(x)
		default:
			out[i] = fmt.Sprint(x)
		}
	}
	return out
}

func TestTablesThatCannotBeReadInChunksAreRefusedWithTheReason(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my,
		"CREATE TABLE binwave.no_key (a INT)",
		"CREATE TABLE binwave.float_key (f DOUBLE NOT NULL, PRIMARY KEY (f))",
		"CREATE TABLE binwave.enum_key (e ENUM('a','b') NOT NULL, PRIMARY KEY (e))",
		"CREATE TABLE binwave.prefix_key (s VARCHAR(100) NOT NULL, PRIMARY KEY (s(10)))")
	src := openSource(t, my, mysqlsrc.Options{})

	for table, want := range map[string]string{
		"no_key": "no primary key", "float_key": "DOUBLE", "enum_key": "ENUM", "prefix_key": "prefix", "missing": "does not exist",
	} {
		if _, err := src.TableDefinition(ctx, "binwave", table); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: TableDefinition = %v, want an error that says %q", table, err, want)
		}
	}
}

func TestVirtualColumnsAreReadLikeTheBinlogCarriesThem(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my, "CREATE TABLE binwave.g (id INT PRIMARY KEY, a INT NOT NULL, twice INT AS (a * 2) VIRTUAL, thrice INT AS (a * 3) STORED)")
	src := openSource(t, my, mysqlsrc.Options{})
	start, err := src.ExecutedGTIDSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, my, "INSERT INTO binwave.g (id, a) VALUES (1, 5)")
	fromBinlog := rowsOf(streamCommits(t, src, start, 1), "binwave.g")[0]

	tbl, err := src.TableDefinition(ctx, "binwave", "g")
	if err != nil {
		t.Fatal(err)
	}
	rows, _, err := src.ReadChunk(ctx, snapshot.ChunkRequest{Table: tbl, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}

	if tbl.Signature() != fromBinlog.Table.Signature() {
		t.Errorf("signatures differ:\n%s\n%s", tbl.Signature(), fromBinlog.Table.Signature())
	}
	if len(rows) != 1 || len(rows[0]) != len(fromBinlog.Rows[0].After) {
		t.Fatalf("the row has %d values, the binlog's has %d", len(rows[0]), len(fromBinlog.Rows[0].After))
	}
	if got, want := fmt.Sprint(rows[0]), fmt.Sprint(fromBinlog.Rows[0].After); got != want {
		t.Errorf("SELECT gives %s, the binlog %s", got, want)
	}
}

// TestASelectAfterTheWatermarkSeesEveryTransactionTheWatermarkHas is the assumption the whole snapshot
// stands on: when @@gtid_executed says a transaction is committed, a SELECT made after it sees the
// transaction. Writers commit single-row transactions as fast as they can while readers take a watermark
// and then count the rows: the rows must never be fewer than the transactions of the watermark.
func TestASelectAfterTheWatermarkSeesEveryTransactionTheWatermarkHas(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my, "CREATE TABLE binwave.w (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL)")
	src := openSource(t, my, mysqlsrc.Options{})
	base, err := src.ExecutedGTIDSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	baseCount := gtidCount(t, base)

	stop := make(chan struct{})
	var writers sync.WaitGroup
	var written atomic.Int64
	for w := 0; w < 8; w++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := my.DB.Exec("INSERT INTO binwave.w (v) VALUES (1)"); err == nil {
					written.Add(1)
				}
			}
		}()
	}

	var checks, violations atomic.Int64
	var readers sync.WaitGroup
	deadline := time.Now().Add(12 * time.Second)
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for time.Now().Before(deadline) {
				mark, err := src.ExecutedGTIDSet(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				var rows int64
				if err := my.DB.QueryRow("SELECT COUNT(*) FROM binwave.w").Scan(&rows); err != nil {
					t.Error(err)
					return
				}
				checks.Add(1)
				// Every transaction of the watermark after the table was made is a row.
				if inserts := gtidCount(t, mark) - baseCount; rows < inserts {
					violations.Add(1)
					t.Errorf("the watermark has %d transactions that insert a row, and a SELECT after it sees %d rows", inserts, rows)
					return
				}
			}
		}()
	}
	readers.Wait()
	close(stop)
	writers.Wait()

	if checks.Load() < 500 || written.Load() < 5000 {
		t.Fatalf("only %d checks against %d commits: the test proved little", checks.Load(), written.Load())
	}
	if violations.Load() != 0 {
		t.Fatalf("%d violations of %d checks", violations.Load(), checks.Load())
	}
}

// gtidCount counts the transactions of a GTID set.
func gtidCount(t *testing.T, set string) int64 {
	t.Helper()
	n, err := gtid.Count(set)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestChunksOfATableAreReadWithOneStatement: the statement that reads a chunk takes the size of the
// chunk as a parameter and is kept ready, so reading a table in many chunks costs one prepare, not one
// per chunk (two round-trips each, which is most of the cost of a small chunk).
func TestChunksOfATableAreReadWithOneStatement(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my, "CREATE TABLE binwave.many (a INT NOT NULL, b INT NOT NULL, v INT NOT NULL, PRIMARY KEY (a, b))")
	for i := 0; i < 500; i++ {
		if _, err := my.DB.Exec("INSERT INTO binwave.many (a, b, v) VALUES (?, ?, ?)", i/5, i%5, i); err != nil {
			t.Fatal(err)
		}
	}
	src := openSource(t, my, mysqlsrc.Options{})
	tbl, err := src.TableDefinition(ctx, "binwave", "many")
	if err != nil {
		t.Fatal(err)
	}
	before := statusCounter(t, my, "Com_stmt_prepare")

	r := snapshot.NewReader(src, tbl, nil, snapshot.Options{
		Sizer: snapshot.SizerOptions{Initial: 25, Fixed: true},
	})
	rows, chunks := 0, 0
	for {
		c, err := r.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		chunks++
		rows += len(c.Rows)
		if c.Done {
			break
		}
	}

	if rows != 500 || chunks < 20 {
		t.Fatalf("read %d rows in %d chunks, want 500 rows in at least 20 chunks", rows, chunks)
	}
	// One statement without a cursor, one with it, and room for the queries of the read itself
	// (the watermark is not a prepared statement, but the count must not grow with the chunks).
	if prepares := statusCounter(t, my, "Com_stmt_prepare") - before; prepares > 6 {
		t.Errorf("%d statements were prepared for %d chunks, want the statement to be kept ready", prepares, chunks)
	}
}

func statusCounter(t *testing.T, my *testenv.MySQL, name string) int64 {
	t.Helper()
	var key, raw string
	// SHOW takes no parameters, and performance_schema may be off in the image, so the name goes into
	// the statement; it is a constant of this test, not anything from outside.
	if err := my.DB.QueryRow("SHOW GLOBAL STATUS LIKE '"+name+"'").Scan(&key, &raw); err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("%s = %q: %v", name, raw, err)
	}
	return value
}

// TestAPartitionedTableIsReadPartitionByPartition: a real partitioned table with a composite primary
// key (the shape MySQL forces when a table is partitioned by a date) is read partition by partition,
// every row exactly once, with the keys overlapping between partitions and gaps inside them.
func TestAPartitionedTableIsReadPartitionByPartition(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my, `CREATE TABLE binwave.parted (
			id BIGINT NOT NULL,
			at DATETIME NOT NULL,
			v VARCHAR(20) NOT NULL,
			PRIMARY KEY (id, at)
		) PARTITION BY RANGE (TO_DAYS(at)) (
			PARTITION p2024 VALUES LESS THAN (TO_DAYS('2025-01-01')),
			PARTITION p2025 VALUES LESS THAN (TO_DAYS('2026-01-01')),
			PARTITION p2026 VALUES LESS THAN (TO_DAYS('2027-01-01')),
			PARTITION pempty VALUES LESS THAN MAXVALUE
		)`)
	// The same ids in different partitions, and ids that repeat inside one partition with another date:
	// the cursor has to be the whole key, and each partition is a key space of its own.
	dates := []string{"2024-06-01", "2025-06-01", "2026-06-01"}
	written := 0
	for _, d := range dates {
		for id := 1; id <= 40; id++ {
			if _, err := my.DB.Exec("INSERT INTO binwave.parted (id, at, v) VALUES (?, ?, ?)", id*3, d+" 00:00:00", d); err != nil {
				t.Fatal(err)
			}
			if _, err := my.DB.Exec("INSERT INTO binwave.parted (id, at, v) VALUES (?, ?, ?)", id*3, d+" 12:00:00", d); err != nil {
				t.Fatal(err)
			}
			written += 2
		}
	}

	src := openSource(t, my, mysqlsrc.Options{})
	tbl, err := src.TableDefinition(ctx, "binwave", "parted")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"p2024", "p2025", "p2026", "pempty"}; !reflect.DeepEqual(tbl.Partitions, want) {
		t.Fatalf("partitions %v, want %v", tbl.Partitions, want)
	}
	if plan := snapshot.PlanFor(tbl); plan.Strategy != snapshot.StrategyPartitioned {
		t.Fatalf("strategy %q, want partitioned", plan.Strategy)
	}

	r := snapshot.NewReader(src, tbl, nil, snapshot.Options{Sizer: snapshot.SizerOptions{Initial: 7, Fixed: true}})
	seen := map[string]int{}
	partitions := map[string]int{}
	rows := 0
	for {
		chunk, err := r.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if chunk.Partition == "" {
			t.Fatalf("a chunk was read from the whole table, not from a partition")
		}
		for _, row := range chunk.Rows {
			key := fmt.Sprintf("%v|%v", row[0], row[1])
			seen[key]++
			partitions[chunk.Partition]++
			rows++
		}
		if chunk.Done {
			break
		}
	}

	if rows != written || len(seen) != written {
		t.Errorf("read %d rows (%d distinct keys) of %d written", rows, len(seen), written)
	}
	for key, n := range seen {
		if n != 1 {
			t.Errorf("the row %s was read %d times", key, n)
		}
	}
	for _, p := range []string{"p2024", "p2025", "p2026"} {
		if partitions[p] != 80 {
			t.Errorf("partition %s gave %d rows, want 80", p, partitions[p])
		}
	}
	if p := r.Progress(); p.Partitions != 4 || p.PartitionsDone != 4 {
		t.Errorf("progress = %d/%d partitions, want 4/4", p.PartitionsDone, p.Partitions)
	}
}

// TestASubpartitionedTableIsReadByItsLeaves: with subpartitions the leaves are what a chunk is read
// from, and FROM t PARTITION (name) takes a subpartition name.
func TestASubpartitionedTableIsReadByItsLeaves(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my, `CREATE TABLE binwave.subparted (
			id BIGINT NOT NULL,
			at DATETIME NOT NULL,
			PRIMARY KEY (id, at)
		) PARTITION BY RANGE (YEAR(at)) SUBPARTITION BY HASH (id) SUBPARTITIONS 2 (
			PARTITION p2025 VALUES LESS THAN (2026),
			PARTITION p2026 VALUES LESS THAN (2027)
		)`)
	for id := 1; id <= 25; id++ {
		for _, y := range []string{"2025", "2026"} {
			if _, err := my.DB.Exec("INSERT INTO binwave.subparted (id, at) VALUES (?, ?)", id, y+"-06-01 00:00:00"); err != nil {
				t.Fatal(err)
			}
		}
	}

	src := openSource(t, my, mysqlsrc.Options{})
	tbl, err := src.TableDefinition(ctx, "binwave", "subparted")
	if err != nil {
		t.Fatal(err)
	}
	if len(tbl.Partitions) != 4 {
		t.Fatalf("partitions %v, want the four subpartitions", tbl.Partitions)
	}

	r := snapshot.NewReader(src, tbl, nil, snapshot.Options{Sizer: snapshot.SizerOptions{Initial: 4, Fixed: true}})
	rows := 0
	for {
		chunk, err := r.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rows += len(chunk.Rows)
		if chunk.Done {
			break
		}
	}
	if rows != 50 {
		t.Errorf("read %d rows, want 50", rows)
	}
}

// TestAPartitionDroppedWhileReadingIsSkipped: the monthly rotation of a table partitioned by date drops
// a partition under a read that has not got to it yet. The server answers "unknown partition"; the read
// must go on with the rest of the table instead of failing, because the rows of that partition are gone
// from the source as well.
func TestAPartitionDroppedWhileReadingIsSkipped(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ctx := context.Background()
	mustExec(t, my, `CREATE TABLE binwave.rotating (
			id BIGINT NOT NULL,
			at DATETIME NOT NULL,
			PRIMARY KEY (id, at)
		) PARTITION BY RANGE (YEAR(at)) (
			PARTITION p2024 VALUES LESS THAN (2025),
			PARTITION p2025 VALUES LESS THAN (2026),
			PARTITION p2026 VALUES LESS THAN (2027)
		)`)
	for _, y := range []string{"2024", "2025", "2026"} {
		for id := 1; id <= 10; id++ {
			if _, err := my.DB.Exec("INSERT INTO binwave.rotating (id, at) VALUES (?, ?)", id, y+"-06-01 00:00:00"); err != nil {
				t.Fatal(err)
			}
		}
	}

	src := openSource(t, my, mysqlsrc.Options{})
	tbl, err := src.TableDefinition(ctx, "binwave", "rotating")
	if err != nil {
		t.Fatal(err)
	}

	r := snapshot.NewReader(src, tbl, nil, snapshot.Options{Sizer: snapshot.SizerOptions{Initial: 4, Fixed: true}})
	dropped := false
	rows := 0
	seen := map[string]bool{}
	for {
		chunk, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("the read failed after a partition was dropped: %v", err)
		}
		seen[chunk.Partition] = true
		rows += len(chunk.Rows)
		if !dropped && chunk.PartitionDone && chunk.Partition == "p2024" {
			// The read has finished the first partition and has not got to the last one yet.
			mustExec(t, my, "ALTER TABLE binwave.rotating DROP PARTITION p2026")
			dropped = true
		}
		if chunk.Done {
			break
		}
	}

	if !dropped {
		t.Fatal("the partition was never dropped: the test did not exercise the case")
	}
	if rows != 20 {
		t.Errorf("read %d rows, want the 20 of the two partitions that are left", rows)
	}
	if !seen["p2026"] {
		t.Error("the dropped partition was never attempted, so nothing was skipped")
	}
}
