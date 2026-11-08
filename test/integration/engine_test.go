//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/engine"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/test/testenv"
)

// engineConfig builds a configuration that replicates the test database of the MySQL container
// into the test ClickHouse. store is "clickhouse" or "file".
func engineConfig(t *testing.T, my *testenv.MySQL, c *testenv.ClickHouse, store string) *config.Config {
	t.Helper()
	return engineConfigWith(t, my, c, store, `tables: {include: ["binwave.*"]}`)
}

// engineConfigWith is engineConfig with the "tables" section given as YAML.
func engineConfigWith(t *testing.T, my *testenv.MySQL, c *testenv.ClickHouse, store, tables string) *config.Config {
	t.Helper()
	yaml := fmt.Sprintf(`
source: {mysql: {host: %s, port: %d, user: %s, password: %s, server_id: 4101}}
target: {clickhouse: {addresses: ["%s"], database: %s, user: %s, password: %s}}
%s
buffer: {flush_interval: 200ms}
checkpoint: {store: %s, file_path: %q}
`, my.Host, my.Port, my.User, my.Password, c.Addr, targetDB, c.User, c.Password, tables, store,
		filepath.Join(t.TempDir(), "checkpoint.json"))
	cfg, err := config.Parse([]byte(yaml), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	// Most scenarios collect everything in one ClickHouse database; the default, a database named
	// like the source, has its own test.
	cfg.Tables.TargetDatabase = targetDB
	return cfg
}

// runningEngine is an engine started in the background.
type runningEngine struct {
	cancel context.CancelFunc
	done   chan error
	ready  chan struct{}
}

func startEngine(t *testing.T, cfg *config.Config) *runningEngine {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningEngine{cancel: cancel, done: make(chan error, 1), ready: make(chan struct{}, 1)}
	go func() {
		r.done <- engine.New(cfg, quietLogger()).Run(ctx, app.Hooks{
			Ready: func() {
				select {
				case r.ready <- struct{}{}:
				default:
				}
			},
			Progress: func() {},
		})
	}()
	t.Cleanup(cancel)

	select {
	case <-r.ready:
	case err := <-r.done:
		t.Fatalf("the engine stopped before it was ready: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the engine did not become ready")
	}
	return r
}

// stop shuts the engine down like SIGTERM and returns its result.
func (r *runningEngine) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		return err
	case <-time.After(60 * time.Second):
		t.Fatal("the engine did not stop")
		return nil
	}
}

func waitUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type acct struct {
	Owner string
	Qty   int32
}

func mysqlAccounts(t *testing.T, my *testenv.MySQL) map[int32]acct {
	t.Helper()
	rows, err := my.DB.Query("SELECT id, owner, qty FROM binwave.acct")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int32]acct{}
	for rows.Next() {
		var id int32
		var a acct
		if err := rows.Scan(&id, &a.Owner, &a.Qty); err != nil {
			t.Fatal(err)
		}
		out[id] = a
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func clickhouseAccounts(t *testing.T, c *testenv.ClickHouse) (map[int32]acct, bool) {
	t.Helper()
	rows, err := c.DB.Query("SELECT id, owner, qty FROM replica.acct FINAL")
	if err != nil {
		return nil, false // the table may not exist yet
	}
	defer func() { _ = rows.Close() }()
	out := map[int32]acct{}
	for rows.Next() {
		var id int32
		var a acct
		if err := rows.Scan(&id, &a.Owner, &a.Qty); err != nil {
			t.Fatal(err)
		}
		out[id] = a
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out, true
}

func sameAccounts(a, b map[int32]acct) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func waitForConvergence(t *testing.T, my *testenv.MySQL, c *testenv.ClickHouse) {
	t.Helper()
	waitUntil(t, "ClickHouse to match MySQL", 60*time.Second, func() bool {
		got, ok := clickhouseAccounts(t, c)
		return ok && sameAccounts(got, mysqlAccounts(t, my))
	})
}

func executedGTIDs(t *testing.T, my *testenv.MySQL) string {
	t.Helper()
	var set string
	if err := my.DB.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&set); err != nil {
		t.Fatal(err)
	}
	return strings.NewReplacer("\n", "", " ", "").Replace(set)
}

// TestEngineReplicatesStopsAndResumes drives the real engine against real servers: changes made
// while it runs arrive, a graceful stop saves the position, changes made while it is down
// arrive after a restart, and nothing is repeated or lost.
func TestEngineReplicatesStopsAndResumes(t *testing.T) {
	for _, store := range []string{"clickhouse", "file"} {
		t.Run(store, func(t *testing.T) {
			t.Parallel()
			my := testenv.StartMySQL(t)
			ch := testenv.StartClickHouse(t)
			mustExec(t, my, "CREATE TABLE binwave.acct (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, qty INT NOT NULL)")
			cfg := engineConfig(t, my, ch, store)

			first := startEngine(t, cfg)
			mustExec(t, my,
				"INSERT INTO binwave.acct VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'cy', 30)",
				"UPDATE binwave.acct SET qty = qty + 1 WHERE id IN (1, 2)",
				"UPDATE binwave.acct SET id = 30 WHERE id = 3", // primary key change
				"DELETE FROM binwave.acct WHERE id = 2",
			)
			waitForConvergence(t, my, ch)

			if err := first.stop(t); err != nil {
				t.Fatalf("graceful stop returned %v", err)
			}
			pos := loadPosition(t, ch, cfg)
			if got, want := pos.GTIDSet, executedGTIDs(t, my); got != want {
				t.Errorf("saved position %q, MySQL has executed %q", got, want)
			}
			if pos.Version == 0 {
				t.Error("no version was saved with the position")
			}

			// Changes while binwave is down.
			mustExec(t, my,
				"INSERT INTO binwave.acct VALUES (4, 'di', 40)",
				"UPDATE binwave.acct SET owner = 'anna' WHERE id = 1",
				"DELETE FROM binwave.acct WHERE id = 30",
			)

			second := startEngine(t, cfg)
			mustExec(t, my, "INSERT INTO binwave.acct VALUES (5, 'ed', 50)")
			waitForConvergence(t, my, ch)
			if err := second.stop(t); err != nil {
				t.Fatalf("second stop returned %v", err)
			}

			if got, want := loadPosition(t, ch, cfg).GTIDSet, executedGTIDs(t, my); got != want {
				t.Errorf("final position %q, MySQL has executed %q", got, want)
			}
		})
	}
}

func loadPosition(t *testing.T, c *testenv.ClickHouse, cfg *config.Config) checkpoint.Position {
	t.Helper()
	var store interface {
		Load(context.Context) (checkpoint.Position, bool, error)
	}
	if cfg.Checkpoint.Store == config.CheckpointFile {
		store = checkpoint.NewFile(cfg.Checkpoint.FilePath)
	} else {
		sink := openSink(t, c)
		store = checkpoint.NewClickHouse(sink.Conn(), targetDB, cfg.Checkpoint.Table, nil)
	}
	pos, found, err := store.Load(context.Background())
	if err != nil || !found {
		t.Fatalf("load position: found=%v err=%v", found, err)
	}
	return pos
}

func TestEngineRefusesASourceThatDoesNotMeetTheRequirements(t *testing.T) {
	t.Parallel()
	my := testenv.StartMySQL(t, testenv.WithMySQLArgs("--binlog-format=MIXED"))
	ch := testenv.StartClickHouse(t)

	err := engine.New(engineConfig(t, my, ch, "file"), quietLogger()).Run(context.Background(), app.Hooks{Ready: func() { t.Error("Ready was signalled") }})

	var cfgErr *app.ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("Run() = %v, want a ConfigError, so the process exits with EX_CONFIG", err)
	}
	var reqErr *mysqlsrc.RequirementsError
	if !errors.As(err, &reqErr) {
		t.Errorf("the requirements are not in the error chain: %v", err)
	}
}
