//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/engine"
	"github.com/thealexjo/binwave/internal/preflight"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/test/testenv"
)

// runUntilItEnds runs the engine and returns how it ended, and whether it became ready first.
// An engine that is still running after limit is stopped and reported as running.
func runUntilItEnds(t *testing.T, cfg *config.Config, limit time.Duration) (ready, running bool, err error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readied := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- engine.New(cfg, quietLogger()).Run(ctx, app.Hooks{
			Ready:    func() { readied <- struct{}{} },
			Progress: func() {},
		})
	}()
	select {
	case err = <-done:
	case <-time.After(limit):
		cancel()
		select {
		case err = <-done:
		case <-time.After(60 * time.Second):
			t.Fatal("the engine did not stop")
		}
		running = true
	}
	select {
	case <-readied:
		ready = true
	default:
	}
	return ready, running, err
}

func requireConfigError(t *testing.T, err error, wantInMessage ...string) {
	t.Helper()
	var cfgErr *app.ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("error = %v, want a ConfigError so that the process exits with the configuration code", err)
	}
	for _, want := range wantInMessage {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message lacks %q:\n%v", want, err)
		}
	}
}

// TestSourceCatalogDescribesKeysAndForeignKeys reads what the preflight relies on from a real
// server: which tables have a key the binlog can report, and which foreign keys cascade.
func TestSourceCatalogDescribesKeysAndForeignKeys(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	mustExec(t, my,
		"CREATE TABLE binwave.pk (id INT PRIMARY KEY, a INT)",
		"CREATE TABLE binwave.composite (a INT, b INT, PRIMARY KEY (b, a))",
		"CREATE TABLE binwave.uniq (u INT NOT NULL, v INT, UNIQUE KEY uk (u))",
		"CREATE TABLE binwave.nullable_uniq (u INT NULL, UNIQUE KEY uk (u))",
		"CREATE TABLE binwave.keyless (a INT, b INT)",
		"CREATE TABLE binwave.parent (id INT PRIMARY KEY)",
		"CREATE TABLE binwave.child (id INT PRIMARY KEY, parent_id INT, "+
			"CONSTRAINT fk_cascade FOREIGN KEY (parent_id) REFERENCES binwave.parent (id) ON DELETE CASCADE, "+
			"CONSTRAINT fk_plain FOREIGN KEY (id) REFERENCES binwave.parent (id))",
		"CREATE VIEW binwave.a_view AS SELECT id FROM binwave.pk",
	)
	src := openSource(t, my, mysqlsrc.Options{Include: func(db, _ string) bool { return db == "binwave" }})

	cat, err := src.Catalog(context.Background(), "", "")
	if err != nil {
		t.Fatal(err)
	}

	byName := map[string]bool{}
	for _, tbl := range cat.Tables {
		byName[tbl.Name] = tbl.HasKey()
		if tbl.Name == "a_view" {
			t.Error("a view is not a table and has no binlog events")
		}
	}
	wantKey := map[string]bool{
		"pk": true, "composite": true, "uniq": true, "nullable_uniq": false, "keyless": false, "parent": true, "child": true,
	}
	for name, want := range wantKey {
		if got, ok := byName[name]; !ok || got != want {
			t.Errorf("table %s: has a key = %v (present %v), want %v", name, got, ok, want)
		}
	}
	for _, tbl := range cat.Tables {
		if tbl.Name == "composite" && strings.Join(tbl.PrimaryKey, ",") != "b,a" {
			t.Errorf("composite primary key = %v, want columns in key order b,a", tbl.PrimaryKey)
		}
		if tbl.Name == "pk" && strings.Join(tbl.Columns, ",") != "id,a" {
			t.Errorf("pk columns = %v", tbl.Columns)
		}
	}

	cascading := map[string]bool{}
	for _, fk := range cat.ForeignKeys {
		cascading[fk.Name] = fk.Cascades()
	}
	if len(cascading) != 2 || !cascading["fk_cascade"] || cascading["fk_plain"] {
		t.Errorf("foreign keys = %v, want fk_cascade cascading and fk_plain not", cascading)
	}

	one, err := src.Catalog(context.Background(), "binwave", "child")
	if err != nil {
		t.Fatal(err)
	}
	if len(one.Tables) != 1 || one.Tables[0].Name != "child" || len(one.ForeignKeys) != 2 {
		t.Errorf("a single-table catalog = %+v", one)
	}
}

func TestEngineStopsAfterStartWhenATableCannotBeReplicatedWithThePolicyStop(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.fine (id INT PRIMARY KEY, v INT)",
		"CREATE TABLE binwave.keyless_one (a INT, b INT)",
		"CREATE TABLE binwave.keyless_two (a INT)",
	)

	cfg := engineConfig(t, my, ch, "file")
	cfg.Tables.OnReplicationError = config.OnErrorStop // by default the tables are set aside instead
	ready, _, err := runUntilItEnds(t, cfg, 60*time.Second)

	if !ready {
		t.Error("the service must start first: looking at every table is not part of starting")
	}
	requireConfigError(t, err, "binwave.keyless_one", "binwave.keyless_two", "order_by")
	if strings.Contains(err.Error(), "binwave.fine") {
		t.Errorf("a healthy table is listed as a problem: %v", err)
	}
	var pe *preflight.Error
	if !errors.As(err, &pe) || len(pe.Problems) != 2 {
		t.Errorf("want both problems in one report, got %v", err)
	}
}

func TestEngineAcceptsKeylessTablesWithAnOrderByOverride(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.keyless (a INT, b INT)")
	cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.*"], overrides: {"binwave.keyless": {order_by: [a]}}}`)

	ready, running, err := runUntilItEnds(t, cfg, 5*time.Second)

	if !ready || !running {
		t.Errorf("ready = %v, running = %v, err = %v; want a service that keeps running", ready, running, err)
	}
}

func TestForeignKeyCascadePolicy(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.parent (id INT PRIMARY KEY)",
		"CREATE TABLE binwave.child (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES binwave.parent (id) ON DELETE CASCADE)",
	)
	for _, tt := range []struct {
		policy, onError string
		wantStopped     bool
	}{
		{"", "stop", true}, // the default cascade policy is error
		{"error", "stop", true},
		{"error", "quarantine", false}, // the table is set aside, the service goes on
		{"warn", "", false},
		{"ignore", "", false},
	} {
		t.Run("policy "+tt.policy+" on error "+tt.onError, func(t *testing.T) {
			settings := ""
			if tt.policy != "" {
				settings += `, foreign_key_cascades: ` + tt.policy
			}
			if tt.onError != "" {
				settings += `, on_replication_error: ` + tt.onError
			}
			tables := `tables: {include: ["binwave.*"]` + settings + `}`
			ready, running, err := runUntilItEnds(t, engineConfigWith(t, my, ch, "file", tables), 5*time.Second)

			if !ready {
				t.Error("the service did not start")
			}
			if tt.wantStopped {
				requireConfigError(t, err, "binwave.child", "fk_child_parent", "foreign_key_cascades")
				return
			}
			if !running {
				t.Errorf("the service stopped although the policy accepts cascades: %v", err)
			}
		})
	}
}

// TestTableCreatedLaterIsVettedBeforeItsFirstRow: a table that did not exist at start is held to
// the same rules when its first row arrives.
func TestTableCreatedLaterIsVettedBeforeItsFirstRow(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.parent (id INT PRIMARY KEY)")
	cfg := engineConfig(t, my, ch, "file")
	cfg.Tables.OnReplicationError = config.OnErrorStop

	// Start with a clean source, then create the offending table and write into it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- engine.New(cfg, quietLogger()).Run(ctx, app.Hooks{Ready: func() { ready <- struct{}{} }, Progress: func() {}})
	}()
	select {
	case <-ready:
	case <-time.After(60 * time.Second):
		t.Fatal("the engine did not become ready")
	}
	mustExec(t, my,
		"CREATE TABLE binwave.late (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_late FOREIGN KEY (parent_id) REFERENCES binwave.parent (id) ON DELETE CASCADE)",
		"INSERT INTO binwave.parent VALUES (1)",
		"INSERT INTO binwave.late VALUES (1, 1)",
	)

	select {
	case err := <-done:
		requireConfigError(t, err, "binwave.late", "fk_late")
	case <-time.After(60 * time.Second):
		t.Fatal("the engine kept running with a cascading foreign key in a table that appeared later")
	}
}

func TestEngineExplainsAWrongMySQLPasswordAsAConfigurationProblem(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	cfg := engineConfig(t, my, ch, "file")
	cfg.Source.MySQL.Password = "not-the-password"

	ready, _, err := runUntilItEnds(t, cfg, 60*time.Second)

	if ready {
		t.Error("the service must not become ready without a login")
	}
	requireConfigError(t, err, "access denied", "source.mysql.user")
}

func TestEngineExplainsAWrongClickHousePasswordAsAConfigurationProblem(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	cfg := engineConfig(t, my, ch, "file")
	cfg.Target.ClickHouse.Password = "not-the-password"

	ready, _, err := runUntilItEnds(t, cfg, 60*time.Second)

	if ready {
		t.Error("the service must not become ready without a login")
	}
	requireConfigError(t, err, "target.clickhouse.user")
}

func TestEngineRefusesADatabaseThatIsNotVisibleToTheUser(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	cfg := engineConfigWith(t, my, ch, "file", `tables: {include: ["binwave.*", "no_such_db.*"]}`)

	ready, _, err := runUntilItEnds(t, cfg, 60*time.Second)

	if ready {
		t.Error("the service must not start when a database it was asked to replicate is not there")
	}
	requireConfigError(t, err, "no_such_db")
	var reqErr *mysqlsrc.RequirementsError
	if !errors.As(err, &reqErr) || len(reqErr.Problems) != 1 {
		t.Errorf("want exactly the missing database, got %v", err)
	}
}

// TestEngineRefusesAPositionThatTheBinaryLogsNoLongerHold: MySQL answers error 1236 when the
// binary logs with the requested GTIDs were purged. No restart brings them back.
func TestEngineRefusesAPositionThatTheBinaryLogsNoLongerHold(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.t (id INT PRIMARY KEY)", "INSERT INTO binwave.t VALUES (1)", "INSERT INTO binwave.t VALUES (2)")
	var uuid string
	if err := my.DB.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	purgeBinaryLogs(t, rootDB(t, my))

	cfg := engineConfig(t, my, ch, "file")
	cfg.Replication.InitialPosition = config.StartFromGTID
	cfg.Replication.GTID = uuid + ":1"

	ready, _, err := runUntilItEnds(t, cfg, 60*time.Second)

	if ready {
		t.Error("the service became ready on a position the server cannot serve")
	}
	requireConfigError(t, err, "binary logs", "purged")
}

func TestCheckConnectReportsWhatTheServiceWouldFindOnStart(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my,
		"CREATE TABLE binwave.good (id INT PRIMARY KEY)",
		"CREATE TABLE binwave.bad (a INT)",
	)

	t.Run("a table that cannot be replicated would be quarantined", func(t *testing.T) {
		var out bytes.Buffer
		err := engine.New(engineConfig(t, my, ch, "file"), quietLogger()).Check(context.Background(), &out)
		if err != nil {
			t.Fatalf("Check() = %v: with quarantine, one table's problem does not make the configuration unusable\n%s", err, out.String())
		}
		report := out.String()
		for _, want := range []string{"MySQL      ok", "ClickHouse ok", "Tables     ok", "2 checked, 0 errors", "1 would be quarantined", "quarantine", "binwave.bad"} {
			if !strings.Contains(report, want) {
				t.Errorf("the report lacks %q:\n%s", want, report)
			}
		}
		if strings.Contains(report, "binwave.good") {
			t.Errorf("a healthy table appears in the report:\n%s", report)
		}
	})

	t.Run("with the policy stop it fails", func(t *testing.T) {
		cfg := engineConfig(t, my, ch, "file")
		cfg.Tables.OnReplicationError = config.OnErrorStop
		var out bytes.Buffer

		err := engine.New(cfg, quietLogger()).Check(context.Background(), &out)

		requireConfigError(t, err, "binwave.bad")
		if !strings.Contains(out.String(), "Tables     FAILED") || !strings.Contains(out.String(), "1 errors") {
			t.Errorf("report:\n%s", out.String())
		}
	})

	// The check changes nothing: no ClickHouse table was created for the tables it looked at.
	var n uint64
	if err := ch.DB.QueryRow("SELECT count() FROM system.tables WHERE database IN ('replica', 'binwave')").Scan(&n); err != nil || n != 0 {
		t.Errorf("ClickHouse holds %d tables after a check (err %v), want none", n, err)
	}
}

func TestCheckConnectPassesForAHealthySource(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.good (id INT PRIMARY KEY)")

	var out bytes.Buffer
	if err := engine.New(engineConfig(t, my, ch, "file"), quietLogger()).Check(context.Background(), &out); err != nil {
		t.Fatalf("Check() = %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "Tables     ok") {
		t.Errorf("report:\n%s", out.String())
	}
}

// limitedMySQLUser creates an account with exactly the given privileges and returns a config
// that logs in as it.
func limitedMySQLUser(t *testing.T, my *testenv.MySQL, ch *testenv.ClickHouse, name, grants string) *config.Config {
	t.Helper()
	root := rootDB(t, my)
	for _, stmt := range []string{
		"CREATE USER '" + name + "'@'%' IDENTIFIED BY 'pw'",
		"GRANT " + grants + " TO '" + name + "'@'%'",
	} {
		if _, err := root.Exec(stmt); err != nil {
			t.Fatal(stmt, err)
		}
	}
	cfg := engineConfig(t, my, ch, "file")
	cfg.Source.MySQL.User = name
	cfg.Source.MySQL.Password = "pw"
	return cfg
}

func TestEngineDoesNotStartWithoutTheReplicationPrivilege(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.t (id INT PRIMARY KEY)")
	cfg := limitedMySQLUser(t, my, ch, "no_slave", "SELECT ON binwave.*")

	ready, _, err := runUntilItEnds(t, cfg, 60*time.Second)

	if ready {
		t.Error("the service became ready although the server refuses to serve the binlog to this user")
	}
	requireConfigError(t, err, "privilege")
}

func TestEngineDoesNotStartWhenTheUserCannotSeeTheDatabase(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.t (id INT PRIMARY KEY)")
	cfg := limitedMySQLUser(t, my, ch, "no_select", "REPLICATION SLAVE, REPLICATION CLIENT ON *.*")

	ready, _, err := runUntilItEnds(t, cfg, 60*time.Second)

	if ready {
		t.Error("the service became ready although the user has no privileges on the database")
	}
	requireConfigError(t, err, `database "binwave"`, "grant SELECT")
}

func TestEngineStartsWithExactlyTheDocumentedPrivileges(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t)
	ch := testenv.StartClickHouse(t)
	mustExec(t, my, "CREATE TABLE binwave.t (id INT PRIMARY KEY)")
	cfg := limitedMySQLUser(t, my, ch, "documented", "REPLICATION SLAVE, REPLICATION CLIENT ON *.*")
	if _, err := rootDB(t, my).Exec("GRANT SELECT ON binwave.* TO 'documented'@'%'"); err != nil {
		t.Fatal(err)
	}

	ready, running, err := runUntilItEnds(t, cfg, 5*time.Second)

	if !ready || !running {
		t.Errorf("ready = %v, running = %v, err = %v; the documented privileges must be enough", ready, running, err)
	}
}

// purgeBinaryLogs removes every binary log but the newest, so the GTIDs written so far can no
// longer be served. BEFORE NOW() would not do: a log written to during the current second is not
// "before" it.
func purgeBinaryLogs(t *testing.T, root *sql.DB) {
	t.Helper()
	if _, err := root.Exec("FLUSH BINARY LOGS"); err != nil {
		t.Fatal(err)
	}
	rows, err := root.Query("SHOW BINARY LOGS")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	newest := ""
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		newest = string(vals[0].([]byte))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec("PURGE BINARY LOGS TO '" + newest + "'"); err != nil {
		t.Fatal(err)
	}
}
