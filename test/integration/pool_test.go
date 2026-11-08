//go:build integration

package integration

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/test/testenv"
)

// TestPooledServersAreWipedBetweenTests dirties a server in every way tests do, then takes a
// server again and checks it is the same container in the state of a fresh one. If a wipe misses
// something, a later test would depend on what an earlier one left behind.
func TestPooledServersAreWipedBetweenTests(t *testing.T) {
	// Not parallel: the second subtest must find the server the first one returned.
	var mysqlID string
	var mysqlGTIDs string

	t.Run("dirty", func(t *testing.T) {
		my := testenv.StartMySQL(t)
		ch := testenv.StartClickHouse(t)
		mysqlID = my.ContainerID
		root := rootDB(t, my)
		for _, q := range []string{
			"CREATE TABLE binwave.leftover (id INT PRIMARY KEY)",
			"INSERT INTO binwave.leftover VALUES (1)",
			"CREATE DATABASE other",
			"CREATE USER 'stranger'@'%' IDENTIFIED BY 'x'",
			"GRANT ALL ON other.* TO 'binwave'@'%'",
			"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'binwave'@'%'",
			"SET GLOBAL time_zone = 'America/Chicago'",
			"FLUSH BINARY LOGS",
		} {
			if _, err := root.Exec(q); err != nil {
				t.Fatal(q, err)
			}
		}
		mysqlGTIDs = executedGTIDs(t, my)
		for _, q := range []string{
			"CREATE DATABASE stray",
			"CREATE TABLE stray.t (a Int32) ENGINE = MergeTree ORDER BY a",
			"CREATE TABLE binwave.t (a Int32) ENGINE = MergeTree ORDER BY a",
			"CREATE TABLE default.d (a Int32) ENGINE = MergeTree ORDER BY a",
		} {
			if _, err := ch.DB.Exec(q); err != nil {
				t.Fatal(q, err)
			}
		}
		// A session that is still open when the test ends.
		if _, err := root.Exec("SELECT 1"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("clean", func(t *testing.T) {
		my := testenv.StartMySQL(t)
		ch := testenv.StartClickHouse(t)
		if my.ContainerID != mysqlID {
			t.Skip("another test took the server in between; the wipe is checked on whichever server this is")
		}

		databases := column(t, my.DB, "SHOW DATABASES")
		if got := strings.Join(databases, ","); strings.Contains(got, "other") || strings.Contains(got, "stray") {
			t.Errorf("databases of the previous test are still there: %s", got)
		}
		var n int
		if err := my.DB.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'binwave'").Scan(&n); err != nil || n != 0 {
			t.Errorf("tables left in the test database: %d (err %v)", n, err)
		}

		root := rootDB(t, my)
		if err := root.QueryRow("SELECT COUNT(*) FROM mysql.user WHERE user = 'stranger'").Scan(&n); err != nil || n != 0 {
			t.Errorf("the account of the previous test survived: %d (err %v)", n, err)
		}
		var tz string
		if err := root.QueryRow("SELECT @@GLOBAL.time_zone").Scan(&tz); err != nil || tz != "SYSTEM" {
			t.Errorf("global time zone = %q (err %v)", tz, err)
		}
		grants := strings.Join(column(t, root, "SHOW GRANTS FOR 'binwave'@'%'"), "\n")
		if strings.Contains(grants, "SYSTEM_VARIABLES_ADMIN") || strings.Contains(grants, "other") {
			t.Errorf("privileges of the previous test survived:\n%s", grants)
		}
		if got := executedGTIDs(t, my); got == mysqlGTIDs && strings.Count(got, ",") > 0 {
			t.Errorf("the GTID history was not reset: %s", got)
		}

		var chn uint64
		if err := ch.DB.QueryRow("SELECT count() FROM system.tables WHERE database IN ('stray', 'binwave', 'default')").Scan(&chn); err != nil || chn != 0 {
			t.Errorf("ClickHouse tables of the previous test survived: %d (err %v)", chn, err)
		}
		if err := ch.DB.QueryRow("SELECT count() FROM system.databases WHERE name = 'stray'").Scan(&chn); err != nil || chn != 0 {
			t.Errorf("a ClickHouse database of the previous test survived: %d (err %v)", chn, err)
		}
	})
}

// column runs a query and returns its first column as text.
func column(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(query, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(query, err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(query, err)
	}
	return out
}
