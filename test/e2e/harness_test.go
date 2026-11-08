//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thealexjo/binwave/test/testenv"
)

// stack is a MySQL and a ClickHouse container with a fault-injecting proxy in front of each.
// binwave is pointed at the proxies; the test talks to the databases directly.
type stack struct {
	t       *testing.T
	my      *testenv.MySQL
	ch      *testenv.ClickHouse
	myProxy *proxy
	chProxy *proxy
	dir     string
	// obs is the address binwave serves metrics and health on, free when the stack was made.
	obs string
	// sock is the control socket of binwave; the path is short because a Unix socket path is.
	sock string
}

func newStack(t *testing.T, mysqlOpts ...testenv.MySQLOption) *stack {
	t.Helper()
	s := &stack{t: t, dir: t.TempDir()}
	s.my = testenv.StartMySQL(t, mysqlOpts...)
	s.ch = testenv.StartClickHouse(t)
	s.myProxy = newProxy(t, fmt.Sprintf("%s:%d", s.my.Host, s.my.Port))
	s.chProxy = newProxy(t, s.ch.Addr)
	s.obs = freeAddr(t)
	dir, err := os.MkdirTemp("/tmp", "bwe")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	s.sock = filepath.Join(dir, "c.sock")
	return s
}

// settings are the parts of the configuration a scenario may change.
type settings struct {
	store    string // "clickhouse" (default) or "file"
	serverID uint32
	// mysqlUser and mysqlPassword override the account binwave connects to MySQL with.
	mysqlUser     string
	mysqlPassword string
	include       string
	ddlMode       string
	// bufferRows overrides buffer.max_rows; small values force many flushes, so that a
	// transaction is written in pieces.
	bufferRows int
	// logLevel and logFormat override observability.log_level and log_format.
	logLevel, logFormat string
	// startupBudget overrides retry.startup_budget.
	startupBudget string
	// onReplicationError overrides tables.on_replication_error.
	onReplicationError string
	// dryRun sets ddl.dry_run, and onMissingStructure ddl.on_missing_structure.
	dryRun             bool
	onMissingStructure string
	extra              string // appended verbatim
}

// writeConfig writes a configuration for binwave. The outages of the scenarios last seconds, not
// minutes, so binwave is told to retry quickly, to cut a hung request after ten seconds (no insert
// of the scenarios takes that long, even on a loaded machine), and to call a frozen source frozen
// after six.
func (s *stack) writeConfig(o settings) string {
	s.t.Helper()
	if o.store == "" {
		o.store = "clickhouse"
	}
	if o.serverID == 0 {
		o.serverID = 4101
	}
	if o.mysqlUser == "" {
		o.mysqlUser, o.mysqlPassword = s.my.User, s.my.Password
	}
	if o.include == "" {
		o.include = "binwave.*"
	}
	if o.ddlMode == "" {
		o.ddlMode = "full"
	}
	cfg := fmt.Sprintf(`
source: {mysql: {host: %s, port: %s, user: %s, password: %s, server_id: %d, heartbeat_period: 2s}}
target: {clickhouse: {addresses: ["%s"], database: replica, user: %s, password: %s}}
tables: {include: ["%s"], target_database: replica%s}
buffer: {flush_interval: 200ms%s}
checkpoint: {store: %s, file_path: %q}
ddl: {mode: %s%s}
retry: {initial_delay: 100ms, max_delay: 1s, attempt_timeout: 10s%s}
observability: {listen: "%s"%s}
control: {socket: "%s"}
%s
`, hostOf(s.myProxy.addr), portOf(s.myProxy.addr), o.mysqlUser, o.mysqlPassword, o.serverID,
		s.chProxy.addr, s.ch.User, s.ch.Password, o.include, onErrorSetting(o.onReplicationError), rowsSetting(o.bufferRows), o.store,
		filepath.Join(s.dir, "checkpoint.json"), o.ddlMode, ddlSettings(o), startupBudgetSetting(o.startupBudget),
		s.obs, logSettings(o), s.sock, o.extra)

	path := filepath.Join(s.dir, "binwave.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		s.t.Fatal(err)
	}
	return path
}

func startupBudgetSetting(d string) string {
	if d == "" {
		return ""
	}
	return ", startup_budget: " + d
}

func ddlSettings(o settings) string {
	out := ""
	if o.dryRun {
		out += ", dry_run: true"
	}
	if o.onMissingStructure != "" {
		out += ", on_missing_structure: " + o.onMissingStructure
	}
	return out
}

func onErrorSetting(policy string) string {
	if policy == "" {
		return ""
	}
	return ", on_replication_error: " + policy
}

func logSettings(o settings) string {
	out := ""
	if o.logLevel != "" {
		out += ", log_level: " + o.logLevel
	}
	if o.logFormat != "" {
		out += ", log_format: " + o.logFormat
	}
	return out
}

// freeAddr returns a local address nothing listens on at the moment.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

// httpGet fetches a path of binwave's observability server.
func (s *stack) httpGet(path string) (code int, body string, err error) {
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + s.obs + path)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data), err
}

// status reads /status.
func (s *stack) status() (map[string]any, error) {
	_, body, err := s.httpGet("/status")
	if err != nil {
		return nil, err
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		return nil, fmt.Errorf("/status is not JSON: %w\n%s", err, body)
	}
	return st, nil
}

// metric reads one series of /metrics, for example "binwave_lag_seconds".
func (s *stack) metric(series string) (float64, error) {
	_, body, err := s.httpGet("/metrics")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(body, "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			return strconv.ParseFloat(strings.TrimSpace(rest), 64)
		}
	}
	return 0, fmt.Errorf("series %s not exposed", series)
}

func rowsSetting(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(", max_rows: %d", n)
}

func hostOf(addr string) string { h, _, _ := strings.Cut(addr, ":"); return h }
func portOf(addr string) string { _, p, _ := strings.Cut(addr, ":"); return p }

// mustExec runs statements against MySQL as the application user.
func (s *stack) mustExec(stmts ...string) {
	s.t.Helper()
	for _, q := range stmts {
		if _, err := s.my.DB.ExecContext(context.Background(), q); err != nil {
			s.t.Fatalf("%.120s\n%v", q, err)
		}
	}
}

// lockedBuffer collects a process's output while the test reads it.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// proc is a running binwave process.
type proc struct {
	t    *testing.T
	cmd  *exec.Cmd
	out  *lockedBuffer
	done chan struct{}
	exit int
}

func startBinwave(t *testing.T, cfgPath string) *proc {
	t.Helper()
	p := &proc{t: t, out: &lockedBuffer{}, done: make(chan struct{})}
	p.cmd = exec.Command(binaryPath, "run", "--config", cfgPath)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		err := p.cmd.Wait()
		p.exit = exitCode(err)
		close(p.done)
	}()
	t.Cleanup(func() {
		running := !p.exited()
		if running {
			_ = p.cmd.Process.Kill()
			<-p.done
		}
		if t.Failed() {
			state := fmt.Sprintf("exited with code %d", p.exit)
			if running {
				state = "was still running"
			}
			t.Logf("binwave %s; its output:\n%s", state, p.out.String())
		}
	})
	return p
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode() // -1 when killed by a signal
	}
	return -2
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// kill9 ends the process without any chance to clean up.
func (p *proc) kill9() {
	p.t.Helper()
	if p.exited() {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGKILL)
	<-p.done
}

// term sends SIGTERM and waits for the process to leave; it returns the exit code.
func (p *proc) term(within time.Duration) int {
	p.t.Helper()
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	return p.waitExit(within)
}

// waitExit waits for the process to exit and returns its code.
func (p *proc) waitExit(within time.Duration) int {
	p.t.Helper()
	select {
	case <-p.done:
		return p.exit
	case <-time.After(within):
		p.t.Fatalf("binwave did not exit within %v\nlogs:\n%s", within, tail(p.out.String(), 3000))
		return 0
	}
}

func (p *proc) logs() string { return p.out.String() }

// waitLog waits until the output contains text.
func (p *proc) waitLog(text string, within time.Duration) {
	p.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if strings.Contains(p.out.String(), text) {
			return
		}
		if p.exited() {
			p.t.Fatalf("binwave exited (%d) before logging %q\nlogs:\n%s", p.exit, text, tail(p.out.String(), 3000))
		}
		time.Sleep(100 * time.Millisecond)
	}
	p.t.Fatalf("no %q in the logs within %v\nlogs:\n%s", text, within, tail(p.out.String(), 3000))
}

// waitReady waits for the log line that follows the start of replication.
func (p *proc) waitReady() { p.t.Helper(); p.waitLog("replication starting", 60*time.Second) }

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}

// queryRows runs a query and returns every row as one tab-separated string.
func queryRows(db *sql.DB, query string) ([]string, error) {
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			parts[i] = v.String
			if !v.Valid {
				parts[i] = "<NULL>"
			}
		}
		out = append(out, strings.Join(parts, "\t"))
	}
	return out, rows.Err()
}

// diff compares two ordered lists of rows and describes up to limit differences.
func diff(mysqlRows, chRows []string, limit int) []string {
	var out []string
	i, j := 0, 0
	key := func(r string) string { k, _, _ := strings.Cut(r, "\t"); return k }
	less := func(a, b string) bool {
		x, errX := strconv.ParseInt(a, 10, 64)
		y, errY := strconv.ParseInt(b, 10, 64)
		if errX == nil && errY == nil {
			return x < y
		}
		return a < b
	}
	for (i < len(mysqlRows) || j < len(chRows)) && len(out) < limit {
		switch {
		case i >= len(mysqlRows):
			out = append(out, "only in ClickHouse: "+chRows[j])
			j++
		case j >= len(chRows):
			out = append(out, "missing in ClickHouse: "+mysqlRows[i])
			i++
		case mysqlRows[i] == chRows[j]:
			i++
			j++
		case key(mysqlRows[i]) == key(chRows[j]):
			out = append(out, fmt.Sprintf("differs: MySQL %q, ClickHouse %q", mysqlRows[i], chRows[j]))
			i++
			j++
		case less(key(mysqlRows[i]), key(chRows[j])):
			out = append(out, "missing in ClickHouse: "+mysqlRows[i])
			i++
		default:
			out = append(out, "only in ClickHouse: "+chRows[j])
			j++
		}
	}
	return out
}

// comparison names a table and the two queries that render it in the same canonical text form.
type comparison struct {
	name  string
	mysql string
	ch    string
}

// compare returns the differences between MySQL and ClickHouse for the table, ordered by the
// first column, which must be the key and sort the same way on both sides.
func (s *stack) compare(c comparison) ([]string, error) {
	m, err := queryRows(s.my.DB, c.mysql)
	if err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}
	h, err := queryRows(s.ch.DB, c.ch)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	d := diff(m, h, 10)
	if len(d) > 0 {
		d = append([]string{fmt.Sprintf("%s: MySQL has %d rows, ClickHouse has %d", c.name, len(m), len(h))}, d...)
	}
	return d, nil
}

// waitConverged waits until every comparison is clean, and fails with the differences otherwise.
func (s *stack) waitConverged(within time.Duration, p *proc, cs ...comparison) {
	s.t.Helper()
	var last []string
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if p != nil && p.exited() {
			// Nothing would restart it here, and an unexpected exit is a finding in itself.
			s.t.Fatalf("binwave exited (code %d) before ClickHouse converged\nlogs:\n%s", p.exit, tail(p.logs(), 3000))
		}
		last = nil
		for _, c := range cs {
			d, err := s.compare(c)
			if err != nil {
				last = append(last, c.name+": "+err.Error())
				continue
			}
			last = append(last, d...)
		}
		if len(last) == 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	logs := ""
	if p != nil {
		logs = "\nbinwave logs:\n" + tail(p.logs(), 2500)
	}
	s.t.Fatalf("ClickHouse did not converge with MySQL within %v:\n  %s%s", within, strings.Join(last, "\n  "), logs)
}

// rootDB opens the MySQL administrator account, for statements the application user cannot run.
func rootDB(t *testing.T, s *stack) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("root:%s@tcp(%s:%d)/", s.my.Password, s.my.Host, s.my.Port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
