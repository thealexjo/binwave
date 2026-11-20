//go:build integration || e2e

// Package testenv starts ephemeral MySQL and ClickHouse containers for
// integration and e2e tests. It must never point at external hosts.
package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // registers the "clickhouse" database/sql driver
	_ "github.com/go-sql-driver/mysql"         // registers the "mysql" database/sql driver
)

// Default images; CI overrides them to run the version matrix.
const (
	DefaultMySQLImage      = "mysql:8.4"
	DefaultClickHouseImage = "clickhouse/clickhouse-server:26.8"
)

const (
	user     = "binwave"
	password = "binwave"
	database = "binwave"
)

// MySQL is a running MySQL container configured the way binwave requires.
type MySQL struct {
	// DSN and DB connect as the test user to the test database.
	DSN string
	DB  *sql.DB

	// Host, Port, User and Password are what a replication source needs. The user holds
	// REPLICATION SLAVE, REPLICATION CLIENT and SELECT, exactly like a production binwave user.
	Host     string
	Port     int
	User     string
	Password string

	// ContainerID lets tests pause or kill the server (docker pause, docker kill).
	ContainerID string

	srv *mysqlServer
}

// Restart stops and starts the server the way an operator does (a clean shutdown of mysqld, then a
// start): a client sees its connections closed, then refused, then a server that answers again.
// Like ClickHouse.Restart, it reads the address again, since Docker hands the container a new host
// port; a test that needs binwave to keep its address retargets the proxy in front of it.
func (m *MySQL) Restart(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	timeout := 60 * time.Second
	if err := m.srv.ctr.Stop(ctx, &timeout); err != nil {
		t.Fatalf("stop mysql: %v", err)
	}
	if err := m.srv.ctr.Start(ctx); err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	if err := m.srv.readdress(ctx); err != nil {
		t.Fatalf("mysql after the restart: %v", err)
	}
	_ = m.DB.Close()
	m.Host, m.Port = m.srv.host, m.srv.port
	m.DSN = mysqlDSN(m.Host, m.Port)

	// The server takes connections before it is ready to answer; wait for an answer.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		db, err := sql.Open("mysql", m.DSN)
		if err == nil {
			if err = db.PingContext(ctx); err == nil {
				m.DB = db
				t.Cleanup(func() { _ = db.Close() })
				return
			}
			_ = db.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("mysql did not answer after the restart: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// writer is the account of Writer, an application next to binwave.
const writer = "binwave_writer"

// Writer connects to the test database as an application that holds the given global privileges
// on top of writing to it (TRANSACTION_GTID_TAG and SESSION_VARIABLES_ADMIN, to write tagged GTIDs),
// which binwave's user does not: User keeps exactly the rights of a production binwave user. The
// pool wipe drops the account.
func (m *MySQL) Writer(t *testing.T, privileges string) *sql.DB {
	t.Helper()
	for _, stmt := range []string{
		"CREATE USER '" + writer + "'@'%' IDENTIFIED BY '" + password + "'",
		"GRANT ALL ON `" + database + "`.* TO '" + writer + "'@'%'",
		"GRANT " + privileges + " ON *.* TO '" + writer + "'@'%'",
	} {
		if _, err := m.srv.root.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	return openDB(t, "mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", writer, password, m.Host, m.Port, database))
}

func mysqlDSN(host string, port int) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user, password, host, port, database)
}

// MySQLOption customizes StartMySQL.
type MySQLOption func(*mysqlOptions)

type mysqlOptions struct{ args []string }

// WithMySQLArgs appends mysqld arguments after the defaults, so they win (e.g. "--binlog-format=MIXED"
// to build a source that violates binwave's requirements).
func WithMySQLArgs(args ...string) MySQLOption {
	return func(o *mysqlOptions) { o.args = append(o.args, args...) }
}

// ClickHouse is a running ClickHouse container.
type ClickHouse struct {
	// DSN and DB are a database/sql connection for assertions.
	DSN string
	DB  *sql.DB

	// Addr is the native protocol address (host:port), with User and Password, for binwave itself.
	Addr     string
	User     string
	Password string

	srv *clickhouseServer
}

// Restart stops and starts the server, as an operator or a package upgrade does: a client sees its
// connections reset, then refused, then a server that answers again.
//
// Docker hands a stopped container a new host port, which a real server keeps, so Addr and DSN are
// read again and the connection used for assertions is reopened. A test that needs binwave to keep
// one address across the restart puts a proxy in front of it and retargets the proxy afterwards.
func (c *ClickHouse) Restart(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	timeout := 10 * time.Second
	if err := c.srv.ctr.Stop(ctx, &timeout); err != nil {
		t.Fatalf("stop clickhouse: %v", err)
	}
	if err := c.srv.ctr.Start(ctx); err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	if err := c.srv.readdress(ctx); err != nil {
		t.Fatalf("clickhouse after the restart: %v", err)
	}
	_ = c.DB.Close()
	c.DSN, c.Addr = c.srv.dsn, c.srv.addr
	c.DB = openDB(t, "clickhouse", c.DSN)

	// The server accepts connections again before it is ready to answer; wait for an answer.
	deadline := time.Now().Add(time.Minute)
	for {
		var one int
		if err := c.DB.QueryRowContext(ctx, "SELECT 1").Scan(&one); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("clickhouse did not answer after the restart")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// StartMySQL hands out a MySQL server configured the way binwave requires: GTID, row-based
// binlog with full metadata, and a test user with replication rights. The server comes from a
// pool and is wiped when the test finishes (see pool.go), so the cost of starting one is paid
// once per distinct set of arguments and concurrent test, not once per test.
func StartMySQL(t *testing.T, opts ...MySQLOption) *MySQL {
	t.Helper()

	var o mysqlOptions
	for _, opt := range opts {
		opt(&o)
	}
	srv, err := acquireMySQL(o.args)
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	// Registered first, so it runs after the cleanup that closes the connection below.
	t.Cleanup(func() { releaseMySQL(srv) })

	dsn := mysqlDSN(srv.host, srv.port)
	return &MySQL{DSN: dsn, DB: openDB(t, "mysql", dsn), Host: srv.host, Port: srv.port, User: user, Password: password, ContainerID: srv.containerID, srv: srv}
}

// StartClickHouse hands out a ClickHouse server from a pool; it is wiped when the test finishes.
func StartClickHouse(t *testing.T) *ClickHouse {
	t.Helper()

	srv, err := acquireClickHouse()
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { releaseClickHouse(srv) })

	return &ClickHouse{DSN: srv.dsn, DB: openDB(t, "clickhouse", srv.dsn), Addr: srv.addr, User: user, Password: password, srv: srv}
}

func openDB(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()

	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping %s: %v", driver, err)
	}

	return db
}

// durabilityConfig locates deploy/clickhouse/binwave-durability.xml relative to this source file,
// so the test stand and the documented recommendation cannot drift apart.
func durabilityConfig() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "deploy", "clickhouse", "binwave-durability.xml")
}

func imageFromEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
