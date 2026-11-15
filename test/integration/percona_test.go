//go:build integration

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/test/testenv"
)

// perconaVersion skips the test unless the server is Percona Server, and returns its version.
func perconaVersion(t *testing.T, my *testenv.MySQL) string {
	t.Helper()
	var version, comment string
	if err := rootDB(t, my).QueryRow("SELECT @@GLOBAL.version, @@GLOBAL.version_comment").Scan(&version, &comment); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(comment, "Percona Server") {
		t.Skipf("needs Percona Server (BINWAVE_TEST_MYSQL_IMAGE), the server is %s %s", version, comment)
	}
	return version
}

// TestPerconaCompressedColumnsReplicateAsOrdinaryColumns: the binlog carries the values of a
// compressed column uncompressed, and the statements that declare one are applied like any other.
func TestPerconaCompressedColumnsReplicateAsOrdinaryColumns(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	perconaVersion(t, my)
	ch := testenv.StartClickHouse(t)
	root := rootDB(t, my)
	// A dictionary is a server object the reset of the pool does not remove.
	t.Cleanup(func() {
		_, _ = root.Exec("DROP TABLE IF EXISTS binwave.docs")
		_, _ = root.Exec("DROP COMPRESSION_DICTIONARY IF EXISTS binwave_dict")
	})
	stop := runEngine(t, engineConfig(t, my, ch, "clickhouse"))
	defer stop()

	if _, err := root.Exec("CREATE COMPRESSION_DICTIONARY binwave_dict ('hello world hello world')"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, my,
		"CREATE TABLE binwave.docs (id INT PRIMARY KEY, body BLOB COLUMN_FORMAT COMPRESSED, note TEXT COLUMN_FORMAT COMPRESSED WITH COMPRESSION_DICTIONARY binwave_dict)",
		"INSERT INTO binwave.docs VALUES (1, REPEAT('x', 5000), 'hello world'), (2, NULL, '')",
		"ALTER TABLE binwave.docs ADD COLUMN extra JSON COLUMN_FORMAT COMPRESSED",
		"INSERT INTO binwave.docs VALUES (3, 'b', 'c', '{\"a\": [1, 2]}')",
		"UPDATE binwave.docs SET note = 'changed', extra = '{\"b\": true}' WHERE id = 1",
		"ALTER TABLE binwave.docs MODIFY note TEXT",
		"DELETE FROM binwave.docs WHERE id = 2",
	)

	waitSame(t, my, ch, pair{
		"docs",
		"SELECT id, IFNULL(MD5(body), 'null'), note, IFNULL(REPLACE(CAST(extra AS CHAR), ' ', ''), 'null') FROM binwave.docs ORDER BY id",
		"SELECT id, if(isNull(body), 'null', lower(hex(MD5(body)))), note, ifNull(toString(extra), 'null') FROM replica.docs FINAL WHERE _is_deleted = 0 ORDER BY id",
	})
}

// TestPerconaCompressedColumnOfAPartitionedTableIsRefusedBefore8035: before 8.0.35 Percona Server
// may write wrong values of such a column into the binlog (PS-7806), so the table is set aside,
// unless the column is excluded.
func TestPerconaCompressedColumnOfAPartitionedTableIsRefusedBefore8035(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	v := perconaVersion(t, my)
	var major, minor, patch int
	if _, err := fmt.Sscanf(v, "%d.%d.%d", &major, &minor, &patch); err != nil || major != 8 || minor != 0 || patch >= 35 {
		t.Skipf("Percona Server %s has the fix", v)
	}
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.parts (id INT PRIMARY KEY, body BLOB COLUMN_FORMAT COMPRESSED) PARTITION BY HASH (id) PARTITIONS 4",
		"CREATE TABLE binwave.parts_ok (id INT PRIMARY KEY, body BLOB COLUMN_FORMAT COMPRESSED, n INT) PARTITION BY HASH (id) PARTITIONS 4",
	)
	e := startHubEngine(t, engineConfigWith(t, my, ch, "clickhouse",
		`tables: {include: ["binwave.*"], overrides: {"binwave.parts_ok": {exclude_columns: [body]}}}`))

	mustExec(t, my,
		"INSERT INTO binwave.parts VALUES (1, 'a'), (2, 'b')",
		"INSERT INTO binwave.parts_ok VALUES (1, 'a', 10), (2, 'b', 20)",
	)

	waitUntil(t, "the table to be quarantined", 60*time.Second, func() bool { return len(e.quarantinedTables()) == 1 })
	if q := e.hub.Status().Quarantined[0]; q.Table != "binwave.parts" || !strings.Contains(q.Reason, "PS-7806") {
		t.Errorf("quarantined %s for %q, want binwave.parts for PS-7806", q.Table, q.Reason)
	}
	waitSame(t, my, ch, pair{"parts_ok", "SELECT id, n FROM binwave.parts_ok ORDER BY id", "SELECT id, n FROM replica.parts_ok FINAL ORDER BY id"})
	e.stillRunning(t)
}
