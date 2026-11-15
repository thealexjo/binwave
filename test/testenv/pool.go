//go:build integration || e2e

package testenv

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Starting a database container takes seconds and stopping one takes seconds more, which added up
// to most of the run time of the suites. Servers are therefore pooled: a test takes an idle
// server (or starts one when none is idle), and when the test ends the server is wiped back to
// the state of a fresh container and returned to the pool. Tests run concurrently, so the pool
// grows to the number of concurrent tests and never makes one wait for another.
//
// A wipe removes everything a test can leave behind: databases, users, privileges, sessions,
// the binary logs and GTID history, and the global time zone. A server whose wipe fails is
// thrown away, so a broken test cannot poison the next one. Tests that need a differently
// configured server (mysqld arguments) get their own pool, keyed by the arguments.

var pools = struct {
	sync.Mutex
	mysql      map[string][]*mysqlServer
	clickhouse []*clickhouseServer
}{mysql: map[string][]*mysqlServer{}}

// Run is the TestMain of the suites: it runs the tests and then removes the pooled servers.
func Run(m *testing.M) int {
	code := m.Run()
	shutdown()
	return code
}

// terminateFast stops a container without waiting for a graceful shutdown: the data is
// throwaway, and a graceful MySQL or ClickHouse shutdown takes seconds.
func terminateFast(ctr testcontainers.Container) {
	if ctr == nil {
		return
	}
	_ = testcontainers.TerminateContainer(ctr, testcontainers.StopTimeout(0))
}

func shutdown() {
	pools.Lock()
	var my []*mysqlServer
	for _, list := range pools.mysql {
		my = append(my, list...)
	}
	ch := pools.clickhouse
	pools.mysql, pools.clickhouse = map[string][]*mysqlServer{}, nil
	pools.Unlock()

	var wg sync.WaitGroup
	for _, s := range my {
		wg.Add(1)
		go func() { defer wg.Done(); s.close() }()
	}
	for _, s := range ch {
		wg.Add(1)
		go func() { defer wg.Done(); s.close() }()
	}
	wg.Wait()
}

// ---- MySQL ----

type mysqlServer struct {
	key         string
	ctr         testcontainers.Container
	host        string
	port        int
	containerID string
	root        *sql.DB // connects as root, for wiping
}

func (s *mysqlServer) close() {
	if s.root != nil {
		_ = s.root.Close()
	}
	terminateFast(s.ctr)
}

func acquireMySQL(args []string) (*mysqlServer, error) {
	key := strings.Join(args, "\x00")
	pools.Lock()
	if list := pools.mysql[key]; len(list) > 0 {
		s := list[len(list)-1]
		pools.mysql[key] = list[:len(list)-1]
		pools.Unlock()
		return s, nil
	}
	pools.Unlock()
	return startMySQLServer(key, args)
}

func releaseMySQL(s *mysqlServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := s.wipe(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "testenv: the MySQL server could not be wiped and is discarded: %v\n", err)
		s.close()
		return
	}
	pools.Lock()
	pools.mysql[s.key] = append(pools.mysql[s.key], s)
	pools.Unlock()
}

func startMySQLServer(key string, extra []string) (*mysqlServer, error) {
	ctx := context.Background()
	args := append([]string{
		"--server-id=1",
		"--log-bin=mysql-bin",
		"--binlog-format=ROW",
		"--binlog-row-image=FULL",
		"--binlog-row-metadata=FULL",
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
	}, extra...)

	ctr, err := tcmysql.Run(ctx, imageFromEnv("BINWAVE_TEST_MYSQL_IMAGE", DefaultMySQLImage),
		tcmysql.WithUsername(user),
		tcmysql.WithPassword(password),
		tcmysql.WithDatabase(database),
		testcontainers.WithCmdArgs(args...),
		// Ready once the server listens on its port, whichever edition it is (MySQL or Percona Server).
		testcontainers.WithWaitStrategy(wait.ForLog("port: 3306  ")),
	)
	if err != nil {
		terminateFast(ctr)
		return nil, fmt.Errorf("start container: %w", err)
	}
	fail := func(err error) (*mysqlServer, error) {
		terminateFast(ctr)
		return nil, err
	}

	s := &mysqlServer{key: key, ctr: ctr, containerID: ctr.GetContainerID()}
	if err := s.readdress(ctx); err != nil {
		return fail(err)
	}
	// The same statements bring a fresh server to the state every test starts from.
	if err := s.wipe(ctx); err != nil {
		s.close()
		return nil, fmt.Errorf("prepare: %w", err)
	}
	return s, nil
}

// readdress reads the address of the container again and reopens the root connection the pool wipes
// with: Docker gives a container a new host port every time it is started (see MySQL.Restart).
func (s *mysqlServer) readdress(ctx context.Context) error {
	host, err := s.ctr.Host(ctx)
	if err != nil {
		return fmt.Errorf("host: %w", err)
	}
	port, err := s.ctr.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return fmt.Errorf("port: %w", err)
	}
	root, err := sql.Open("mysql", fmt.Sprintf("root:%s@tcp(%s:%d)/", password, host, port.Num()))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if s.root != nil {
		_ = s.root.Close()
	}
	s.host, s.port, s.root = host, int(port.Num()), root
	return nil
}

// wipe brings the server back to the state of a fresh container: a test database and a test
// user with replication rights, a binary log holding just those statements, no other sessions.
func (s *mysqlServer) wipe(ctx context.Context) error {
	conn, err := s.root.Conn(ctx)
	if err != nil {
		return fmt.Errorf("connect as root: %w", err)
	}
	defer func() { _ = conn.Close() }()

	exec := func(stmt string) error {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
		return nil
	}
	column := func(query string) ([]string, error) {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", query, err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				return nil, fmt.Errorf("%s: %w", query, err)
			}
			out = append(out, v)
		}
		return out, rows.Err()
	}

	// Sessions left by the last test, binlog dump threads included.
	ids, err := column("SELECT CAST(id AS CHAR) FROM information_schema.processlist WHERE id <> CONNECTION_ID()")
	if err != nil {
		return err
	}
	for _, id := range ids {
		_ = exec("KILL " + id) // it may have ended already
	}

	// None of this may reach the binary log: the reset below empties it anyway.
	if err := exec("SET SESSION sql_log_bin = 0"); err != nil {
		return err
	}
	databases, err := column("SELECT schema_name FROM information_schema.schemata " +
		"WHERE schema_name NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')")
	if err != nil {
		return err
	}
	for _, d := range databases {
		if err := exec("DROP DATABASE `" + strings.ReplaceAll(d, "`", "``") + "`"); err != nil {
			return err
		}
	}
	accounts, err := column("SELECT CONCAT(QUOTE(user), '@', QUOTE(host)) FROM mysql.user " +
		"WHERE user NOT IN ('root', 'mysql.sys', 'mysql.session', 'mysql.infoschema')")
	if err != nil {
		return err
	}
	for _, a := range accounts {
		if err := exec("DROP USER " + a); err != nil {
			return err
		}
	}
	if err := exec("SET GLOBAL time_zone = 'SYSTEM'"); err != nil {
		return err
	}
	// MySQL 8.4 renamed RESET MASTER; the old form is still accepted there, but not forever.
	if err := exec("RESET BINARY LOGS AND GTIDS"); err != nil {
		if err := exec("RESET MASTER"); err != nil {
			return err
		}
	}
	if err := exec("SET SESSION sql_log_bin = 1"); err != nil {
		return err
	}

	for _, stmt := range []string{
		"CREATE DATABASE `" + database + "`",
		"CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + password + "'",
		"GRANT ALL ON `" + database + "`.* TO '" + user + "'@'%'",
		// Replication needs more than the database the image grants: exactly what a production
		// binwave user holds.
		"GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO '" + user + "'@'%'",
	} {
		if err := exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

// ---- ClickHouse ----

type clickhouseServer struct {
	ctr  testcontainers.Container
	dsn  string
	addr string
	db   *sql.DB // for wiping
}

func (s *clickhouseServer) close() {
	if s.db != nil {
		_ = s.db.Close()
	}
	terminateFast(s.ctr)
}

// readdress reads the address of the container again and reopens the connection the pool wipes with.
// Docker gives a container a new host port every time it is started, so everything that held the old
// one has to be rebuilt (see ClickHouse.Restart).
func (s *clickhouseServer) readdress(ctx context.Context) error {
	dsn, err := s.ctr.(interface {
		ConnectionString(context.Context, ...string) (string, error)
	}).ConnectionString(ctx)
	if err != nil {
		return fmt.Errorf("connection string: %w", err)
	}
	addr, err := s.ctr.(interface {
		ConnectionHost(context.Context) (string, error)
	}).ConnectionHost(ctx)
	if err != nil {
		return fmt.Errorf("address: %w", err)
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if s.db != nil {
		_ = s.db.Close()
	}
	s.dsn, s.addr, s.db = dsn, addr, db
	return nil
}

func acquireClickHouse() (*clickhouseServer, error) {
	pools.Lock()
	if n := len(pools.clickhouse); n > 0 {
		s := pools.clickhouse[n-1]
		pools.clickhouse = pools.clickhouse[:n-1]
		pools.Unlock()
		return s, nil
	}
	pools.Unlock()
	return startClickHouseServer()
}

func releaseClickHouse(s *clickhouseServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := s.wipe(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "testenv: the ClickHouse server could not be wiped and is discarded: %v\n", err)
		s.close()
		return
	}
	pools.Lock()
	pools.clickhouse = append(pools.clickhouse, s)
	pools.Unlock()
}

func startClickHouseServer() (*clickhouseServer, error) {
	ctx := context.Background()
	ctr, err := tcclickhouse.Run(ctx, imageFromEnv("BINWAVE_TEST_CLICKHOUSE_IMAGE", DefaultClickHouseImage),
		// The stand runs with the durability settings recommended for production (deploy/clickhouse).
		tcclickhouse.WithConfigFile(durabilityConfig()),
		tcclickhouse.WithUsername(user),
		tcclickhouse.WithPassword(password),
		tcclickhouse.WithDatabase(database),
		// The test user may create users, so that a test can check what binwave needs of a
		// limited account. wipe drops the users a test made.
		testcontainers.WithEnv(map[string]string{"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}),
	)
	if err != nil {
		terminateFast(ctr)
		return nil, fmt.Errorf("start container: %w", err)
	}
	fail := func(err error) (*clickhouseServer, error) {
		terminateFast(ctr)
		return nil, err
	}

	dsn, err := ctr.ConnectionString(ctx)
	if err != nil {
		return fail(fmt.Errorf("connection string: %w", err))
	}
	addr, err := ctr.ConnectionHost(ctx)
	if err != nil {
		return fail(fmt.Errorf("address: %w", err))
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fail(fmt.Errorf("open: %w", err))
	}
	s := &clickhouseServer{ctr: ctr, dsn: dsn, addr: addr, db: db}
	if err := s.wipe(ctx); err != nil {
		s.close()
		return nil, fmt.Errorf("prepare: %w", err)
	}
	return s, nil
}

// wipe drops every database a test created and the tables it put into "default", and makes sure
// the test database exists (older images do not create it from the container environment).
func (s *clickhouseServer) wipe(ctx context.Context) error {
	column := func(query string) ([]string, error) {
		rows, err := s.db.QueryContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", query, err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				return nil, fmt.Errorf("%s: %w", query, err)
			}
			out = append(out, v)
		}
		return out, rows.Err()
	}
	quote := func(name string) string { return "`" + strings.ReplaceAll(name, "`", "\\`") + "`" }

	databases, err := column("SELECT name FROM system.databases WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema', 'default')")
	if err != nil {
		return err
	}
	var errs []error
	for _, d := range databases {
		if _, err := s.db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quote(d)+" SYNC"); err != nil {
			errs = append(errs, fmt.Errorf("drop database %s: %w", d, err))
		}
	}
	tables, err := column("SELECT name FROM system.tables WHERE database = 'default'")
	if err != nil {
		return err
	}
	for _, tbl := range tables {
		if _, err := s.db.ExecContext(ctx, "DROP TABLE IF EXISTS default."+quote(tbl)+" SYNC"); err != nil {
			errs = append(errs, fmt.Errorf("drop table default.%s: %w", tbl, err))
		}
	}
	users, err := column("SELECT name FROM system.users WHERE storage = 'local_directory' AND name NOT IN ('default', '" + user + "')")
	if err != nil {
		return err
	}
	for _, u := range users {
		if _, err := s.db.ExecContext(ctx, "DROP USER IF EXISTS "+quote(u)); err != nil {
			errs = append(errs, fmt.Errorf("drop user %s: %w", u, err))
		}
	}
	if _, err := s.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quote(database)); err != nil {
		errs = append(errs, fmt.Errorf("create database %s: %w", database, err))
	}
	return errors.Join(errs...)
}
