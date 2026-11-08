package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/control"
	"github.com/thealexjo/binwave/internal/engine"
	failinfo "github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/observability"
	"github.com/thealexjo/binwave/internal/snapshot"
)

type runnerFunc func(ctx context.Context, h app.Hooks) error

func (f runnerFunc) Run(ctx context.Context, h app.Hooks) error { return f(ctx, h) }

func depsWith(env map[string]string, r app.Runner) runDeps {
	return runDeps{
		env: func(name string) (string, bool) {
			v, ok := env[name]
			return v, ok
		},
		pid: 42,
		newRunner: func(*config.Config, *slog.Logger, *observability.Hub) (app.Runner, error) {
			return r, nil
		},
		// The tests must not open a real port.
		serve: func(string, http.Handler, *slog.Logger) (func(), error) { return func() {}, nil },
	}
}

func TestRunCommandExitCodes(t *testing.T) {
	valid := writeConfig(t, validConfig)
	invalid := writeConfig(t, "ddl: {mode: nope}\n")

	tests := []struct {
		name       string
		args       []string
		env        map[string]string
		runner     app.Runner
		wantCode   int
		wantStderr string
	}{
		{name: "clean stop", args: []string{"run", "--config", valid}, runner: runnerFunc(func(context.Context, app.Hooks) error { return nil }), wantCode: exitOK, wantStderr: `msg=stopped`},
		{name: "engine failure", args: []string{"run", "--config", valid}, runner: runnerFunc(func(context.Context, app.Hooks) error { return errors.New("boom") }), wantCode: exitError, wantStderr: "boom"},
		{name: "unmet requirements are not restartable", args: []string{"run", "--config", valid}, runner: runnerFunc(func(context.Context, app.Hooks) error {
			return &app.ConfigError{Err: errors.New("binlog_format must be ROW")}
		}), wantCode: exitConfig, wantStderr: "binlog_format must be ROW"},
		{name: "wrapped config error", args: []string{"run", "--config", valid}, runner: runnerFunc(func(context.Context, app.Hooks) error {
			return errors.Join(errors.New("start"), &app.ConfigError{Err: errors.New("bad")})
		}), wantCode: exitConfig, wantStderr: "bad"},
		{name: "invalid config", args: []string{"run", "--config", invalid}, wantCode: exitConfig, wantStderr: "ddl.mode: must be one of"},
		{name: "missing config file", args: []string{"run", "--config", filepath.Join(t.TempDir(), "nope.yaml")}, wantCode: exitConfig, wantStderr: "read config"},
		{name: "empty config flag", args: []string{"run", "--config", ""}, wantCode: exitUsage, wantStderr: "--config must not be empty"},
		{name: "extra args", args: []string{"run", "--config", valid, "x"}, wantCode: exitUsage, wantStderr: "no positional arguments"},
		{name: "bad flag", args: []string{"run", "--nope"}, wantCode: exitUsage, wantStderr: "flag provided but not defined"},
		{name: "help", args: []string{"run", "-h"}, wantCode: exitOK, wantStderr: "-config"},
		{name: "bad notify socket", args: []string{"run", "--config", valid}, env: map[string]string{"NOTIFY_SOCKET": "vsock:1:2"}, wantCode: exitError, wantStderr: "invalid systemd environment"},
		{name: "bad watchdog", args: []string{"run", "--config", valid}, env: map[string]string{"WATCHDOG_USEC": "soon"}, wantCode: exitError, wantStderr: "invalid systemd environment"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			runner := tt.runner
			if runner == nil {
				runner = runnerFunc(func(context.Context, app.Hooks) error { return nil })
			}

			code := runWith(context.Background(), tt.args, &stdout, &stderr, depsWith(tt.env, runner))

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", code, tt.wantCode, stderr.String())
			}
			assertContains(t, "stderr", stderr.String(), tt.wantStderr)
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q: logs must go to stderr only", stdout.String())
			}
		})
	}
}

// TestRunCommandTalksToSystemd checks the wiring end to end: READY and STOPPING reach NOTIFY_SOCKET.
func TestRunCommandTalksToSystemd(t *testing.T) {
	dir, err := os.MkdirTemp("", "sd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "n")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnerFunc(func(ctx context.Context, h app.Hooks) error {
		h.Ready()
		<-ctx.Done()
		return nil
	})

	done := make(chan int, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		done <- runWith(ctx, []string{"run", "--config", writeConfig(t, validConfig)}, &stdout, &stderr,
			depsWith(map[string]string{"NOTIFY_SOCKET": sock}, runner))
	}()

	read := func() string {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("no notification: %v", err)
		}
		return string(buf[:n])
	}

	if got := read(); got != "READY=1" {
		t.Errorf("first notification = %q, want READY=1", got)
	}
	cancel()
	if got := read(); got != "STOPPING=1" {
		t.Errorf("second notification = %q, want STOPPING=1", got)
	}
	if code := <-done; code != exitOK {
		t.Errorf("exit code = %d, want %d", code, exitOK)
	}
}

type checkerFunc func(ctx context.Context, out io.Writer) error

func (f checkerFunc) Check(ctx context.Context, out io.Writer) error { return f(ctx, out) }

func TestCheckConnectExitCodes(t *testing.T) {
	valid := writeConfig(t, validConfig)
	tests := []struct {
		name       string
		checker    checkerFunc
		wantCode   int
		wantStdout string
	}{
		{"everything fine", func(_ context.Context, out io.Writer) error { _, _ = io.WriteString(out, "MySQL ok\n"); return nil }, exitOK, "MySQL ok"},
		{"a problem the configuration must fix", func(context.Context, io.Writer) error {
			return &app.ConfigError{Err: errors.New("no key")}
		}, exitConfig, "is valid"},
		{"a database that cannot be reached", func(context.Context, io.Writer) error { return errors.New("connection refused") }, exitError, "is valid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := depsWith(nil, nil)
			deps.newChecker = func(*config.Config, *slog.Logger) (checker, error) { return tt.checker, nil }
			var stdout, stderr bytes.Buffer

			code := runWith(context.Background(), []string{"check", "--config", valid, "--connect"}, &stdout, &stderr, deps)

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", code, tt.wantCode, stderr.String())
			}
			assertContains(t, "stdout", stdout.String(), tt.wantStdout)
		})
	}
}

func TestCheckWithoutConnectDoesNotTouchTheDatabases(t *testing.T) {
	deps := depsWith(nil, nil)
	deps.newChecker = func(*config.Config, *slog.Logger) (checker, error) {
		t.Error("the checker was built without --connect")
		return nil, errors.New("must not be called")
	}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"check", "--config", writeConfig(t, validConfig)}, &stdout, &stderr, deps)

	if code != exitOK {
		t.Errorf("exit code = %d", code)
	}
}

func TestRunServesObservabilityFromTheStartAndStopsItAtTheEnd(t *testing.T) {
	var servedOn string
	var stopped bool
	var hubSeen *observability.Hub
	deps := depsWith(nil, nil)
	deps.serve = func(addr string, _ http.Handler, _ *slog.Logger) (func(), error) {
		servedOn = addr
		return func() { stopped = true }, nil
	}
	deps.newRunner = func(_ *config.Config, _ *slog.Logger, hub *observability.Hub) (app.Runner, error) {
		hubSeen = hub
		return runnerFunc(func(context.Context, app.Hooks) error {
			if stopped {
				t.Error("the server was stopped before the engine finished")
			}
			return nil
		}), nil
	}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"run", "--config", writeConfig(t, validConfig)}, &stdout, &stderr, deps)

	if code != exitOK || servedOn != config.DefaultListen || !stopped || hubSeen == nil {
		t.Errorf("code = %d, served on %q, stopped = %v, hub = %v\n%s", code, servedOn, stopped, hubSeen, stderr.String())
	}
}

func TestRunFailsAtOnceWhenTheMetricsAddressIsTaken(t *testing.T) {
	deps := depsWith(nil, runnerFunc(func(context.Context, app.Hooks) error {
		t.Error("the engine started although the observability address is taken")
		return nil
	}))
	deps.serve = func(string, http.Handler, *slog.Logger) (func(), error) {
		return nil, errors.New("listen on observability.listen 127.0.0.1:9187: address already in use")
	}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"run", "--config", writeConfig(t, validConfig)}, &stdout, &stderr, deps)

	if code != exitError {
		t.Errorf("exit code = %d, want %d: another process may be shutting down, so a restart can help", code, exitError)
	}
	assertContains(t, "stderr", stderr.String(), "address already in use")
}

// fakeAdmin is a tablesAdmin over a list in memory.
type fakeAdmin struct {
	entries  []checkpoint.Quarantined
	err      error
	forceArg bool
}

func (f *fakeAdmin) Quarantined(context.Context) ([]checkpoint.Quarantined, error) {
	return f.entries, f.err
}

func (f *fakeAdmin) Release(_ context.Context, tables []string, force bool) (released, unknown []string, err error) {
	f.forceArg = force
	if f.err != nil {
		return nil, nil, f.err
	}
	for _, t := range tables {
		found := false
		for _, e := range f.entries {
			if e.Table == t {
				found = true
			}
		}
		if found {
			released = append(released, t)
		} else {
			unknown = append(unknown, t)
		}
	}
	return released, unknown, nil
}

func (f *fakeAdmin) Resync(_ context.Context, tables []string) ([]control.ResyncResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []control.ResyncResult
	for _, t := range tables {
		r := control.ResyncResult{Table: t}
		if t == "shop.nope" {
			r.Error = "shop.nope is not a replicated table"
		}
		out = append(out, r)
	}
	return out, nil
}

func tablesDeps(admin *fakeAdmin) runDeps {
	deps := depsWith(nil, nil)
	deps.newTables = func(*config.Config, *slog.Logger) (tablesAdmin, error) { return admin, nil }
	return deps
}

func TestTablesListShowsWhatIsQuarantinedAndWhy(t *testing.T) {
	admin := &fakeAdmin{entries: []checkpoint.Quarantined{{Table: "shop.orders", Since: "u:1-5", Code: "TBL101", Reason: "no primary key", At: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}}}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"tables", "list", "--config", writeConfig(t, validConfig)}, &stdout, &stderr, tablesDeps(admin))

	if code != exitOK {
		t.Fatalf("exit code = %d\n%s", code, stderr.String())
	}
	for _, want := range []string{"shop.orders", "u:1-5", "TBL101", "no primary key", "2030-01-02T03:04:05Z"} {
		assertContains(t, "stdout", stdout.String(), want)
	}

	stdout.Reset()
	code = runWith(context.Background(), []string{"tables", "list", "--config", writeConfig(t, validConfig)}, &stdout, &stderr, tablesDeps(&fakeAdmin{}))
	if code != exitOK {
		t.Errorf("exit code = %d", code)
	}
	assertContains(t, "stdout", stdout.String(), "no tables are quarantined")
}

func TestTablesReleaseSaysWhatIsLostAndReportsUnknownTables(t *testing.T) {
	admin := &fakeAdmin{entries: []checkpoint.Quarantined{{Table: "shop.orders"}}}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"tables", "release", "--accept-data-gap", "--config", writeConfig(t, validConfig), "shop.orders"}, &stdout, &stderr, tablesDeps(admin))

	if code != exitOK {
		t.Fatalf("exit code = %d\n%s", code, stderr.String())
	}
	assertContains(t, "stdout", stdout.String(), "released shop.orders")
	assertContains(t, "stdout", stdout.String(), "not in ClickHouse")

	stdout.Reset()
	stderr.Reset()
	code = runWith(context.Background(), []string{"tables", "release", "--accept-data-gap", "--config", writeConfig(t, validConfig), "shop.nope"}, &stdout, &stderr, tablesDeps(admin))
	if code != exitError {
		t.Errorf("a table that is not quarantined: exit code = %d, want %d", code, exitError)
	}
	assertContains(t, "stderr", stderr.String(), "shop.nope is not quarantined")
}

func TestTablesReleasePassesForceAndRefusesARunningService(t *testing.T) {
	admin := &fakeAdmin{entries: []checkpoint.Quarantined{{Table: "shop.orders"}}}
	var stdout, stderr bytes.Buffer
	runWith(context.Background(), []string{"tables", "release", "--accept-data-gap", "--force", "--config", writeConfig(t, validConfig), "shop.orders"}, &stdout, &stderr, tablesDeps(admin))
	if !admin.forceArg {
		t.Error("--force was not passed on")
	}

	stderr.Reset()
	running := &fakeAdmin{err: fmt.Errorf("%w: observability.listen answers", engine.ErrServiceRunning)}
	code := runWith(context.Background(), []string{"tables", "release", "--accept-data-gap", "--config", writeConfig(t, validConfig), "shop.orders"}, &stdout, &stderr, tablesDeps(running))
	if code != exitError {
		t.Errorf("exit code = %d, want %d", code, exitError)
	}
	assertContains(t, "stderr", stderr.String(), "seems to be running")
}

func TestTablesReleaseWithoutTheAcknowledgementIsRefused(t *testing.T) {
	admin := &fakeAdmin{entries: []checkpoint.Quarantined{{Table: "shop.orders"}}}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"tables", "release", "--config", writeConfig(t, validConfig), "shop.orders"}, &stdout, &stderr, tablesDeps(admin))

	if code != exitUsage {
		t.Errorf("exit code = %d, want %d", code, exitUsage)
	}
	assertContains(t, "stderr", stderr.String(), "--accept-data-gap")
	assertContains(t, "stderr", stderr.String(), "stay missing in ClickHouse")
	if len(admin.entries) != 1 || admin.forceArg {
		t.Error("the command went on although the gap was not acknowledged")
	}
}

func TestTablesListShowsAFenceThatIsNotPassedYet(t *testing.T) {
	admin := &fakeAdmin{entries: []checkpoint.Quarantined{{Table: "shop.orders", Until: "u:1-9"}}}
	var stdout, stderr bytes.Buffer

	runWith(context.Background(), []string{"tables", "list", "--config", writeConfig(t, validConfig)}, &stdout, &stderr, tablesDeps(admin))

	assertContains(t, "stdout", stdout.String(), "still skipped")
	assertContains(t, "stdout", stdout.String(), "u:1-9")
}

func TestTablesUsageErrors(t *testing.T) {
	cfg := writeConfig(t, validConfig)
	for name, args := range map[string][]string{
		"no subcommand":          {"tables"},
		"unknown subcommand":     {"tables", "drop", "--config", cfg},
		"empty config":           {"tables", "list", "--config", ""},
		"release without tables": {"tables", "release", "--config", cfg},
		"list with arguments":    {"tables", "list", "--config", cfg, "shop.orders"},
	} {
		var stdout, stderr bytes.Buffer
		if code := runWith(context.Background(), args, &stdout, &stderr, tablesDeps(&fakeAdmin{})); code != exitUsage {
			t.Errorf("%s: exit code = %d, want %d", name, code, exitUsage)
		}
	}
}

// fakeControl is a replicationControl that records calls.
type fakeControl struct {
	calls  []string
	err    error
	status observability.Status
}

func (f *fakeControl) Pause(context.Context) error { f.calls = append(f.calls, "pause"); return f.err }

func (f *fakeControl) Resume(_ context.Context, req control.ResumeRequest) error {
	f.calls = append(f.calls, fmt.Sprintf("resume skip=%d gtid=%s", req.SkipTransactions, req.GTIDNext))
	return f.err
}

func (f *fakeControl) Status(context.Context) (observability.Status, error) {
	f.calls = append(f.calls, "status")
	return f.status, f.err
}

func (f *fakeControl) SnapshotRate(_ context.Context, n int) error {
	f.calls = append(f.calls, fmt.Sprintf("snapshot-rate %d", n))
	return f.err
}

func (f *fakeControl) ChunkSize(_ context.Context, rows int) error {
	f.calls = append(f.calls, fmt.Sprintf("chunk-size %d", rows))
	return f.err
}

func controlDeps(f *fakeControl) runDeps {
	deps := depsWith(nil, nil)
	deps.newControl = func(string) replicationControl { return f }
	return deps
}

// controlConfig is a valid configuration with a control socket.
const controlConfig = validConfig + "control: {socket: /tmp/binwave-test.sock}\n"

func TestReplicationCommandsReachTheService(t *testing.T) {
	cfg := writeConfig(t, controlConfig)
	for sub, want := range map[string]string{"pause": "replication is paused", "resume": "resumes from the saved position"} {
		f := &fakeControl{}
		var stdout, stderr bytes.Buffer

		code := runWith(context.Background(), []string{"replication", sub, "--config", cfg}, &stdout, &stderr, controlDeps(f))

		if code != exitOK || len(f.calls) != 1 || !strings.HasPrefix(f.calls[0], sub) {
			t.Errorf("%s: exit %d, calls %v\n%s", sub, code, f.calls, stderr.String())
		}
		assertContains(t, sub+" stdout", stdout.String(), want)
	}
}

func TestReplicationStatusPrintsWhatMattersToTheOperator(t *testing.T) {
	since := time.Now().Add(-90 * time.Second)
	f := &fakeControl{status: observability.Status{
		State: observability.StatePaused, PausedSince: &since, Position: "u:1-5", LagSeconds: 90, Tables: 4,
		Quarantined: []checkpoint.Quarantined{{Table: "a.b"}},
		LastError:   &observability.LastError{Kind: "clickhouse", Message: "too many parts", Time: time.Now()},
	}}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))

	if code != exitOK {
		t.Fatalf("exit code = %d\n%s", code, stderr.String())
	}
	for _, want := range []string{"state:        paused", "paused since:", "u:1-5", "lag:          90s", "4 changed since start, 1 quarantined", "too many parts"} {
		assertContains(t, "stdout", stdout.String(), want)
	}
}

func TestReplicationCommandFailures(t *testing.T) {
	cfg := writeConfig(t, controlConfig)
	tests := []struct {
		name     string
		err      error
		wantCode int
		wantErr  string
	}{
		{"service not running", control.ErrNotRunning, exitError, "Is the service running?"},
		{"still starting", control.ErrStarting, exitError, "try again in a moment"},
		{"the pause failed", errors.New("final flush failed"), exitError, "final flush failed"},
	}
	for _, tt := range tests {
		var stdout, stderr bytes.Buffer
		code := runWith(context.Background(), []string{"replication", "pause", "--config", cfg}, &stdout, &stderr, controlDeps(&fakeControl{err: tt.err}))
		if code != tt.wantCode {
			t.Errorf("%s: exit code = %d, want %d", tt.name, code, tt.wantCode)
		}
		assertContains(t, tt.name+" stderr", stderr.String(), tt.wantErr)
	}
}

func TestReplicationUsageAndConfigurationErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runWith(context.Background(), []string{"replication"}, &stdout, &stderr, controlDeps(&fakeControl{})); code != exitUsage {
		t.Errorf("no subcommand: exit code = %d", code)
	}
	if code := runWith(context.Background(), []string{"replication", "pause", "--config", ""}, &stdout, &stderr, controlDeps(&fakeControl{})); code != exitUsage {
		t.Errorf("empty config: exit code = %d", code)
	}
	// A configuration without a control socket cannot be used with these commands.
	stderr.Reset()
	code := runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, validConfig)}, &stdout, &stderr, controlDeps(&fakeControl{}))
	if code != exitConfig {
		t.Errorf("no control socket: exit code = %d, want %d", code, exitConfig)
	}
	assertContains(t, "stderr", stderr.String(), "control.socket is not configured")
}

func TestReplicationResumeCanSkipTransactions(t *testing.T) {
	cfg := writeConfig(t, controlConfig)
	f := &fakeControl{}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"replication", "resume", "--config", cfg, "--skip-transactions", "2", "--gtid-next", "u:7"}, &stdout, &stderr, controlDeps(f))

	if code != exitOK || len(f.calls) != 1 || f.calls[0] != "resume skip=2 gtid=u:7" {
		t.Errorf("exit %d, calls %v\n%s", code, f.calls, stderr.String())
	}
	assertContains(t, "stdout", stdout.String(), "NOT applied")
}

func TestReplicationSnapshotRateParsesAndReachesTheService(t *testing.T) {
	cfg := writeConfig(t, controlConfig)

	f := &fakeControl{}
	var stdout, stderr bytes.Buffer
	code := runWith(context.Background(), []string{"replication", "snapshot-rate", "--config", cfg, "5000"}, &stdout, &stderr, controlDeps(f))
	if code != exitOK || len(f.calls) != 1 || f.calls[0] != "snapshot-rate 5000" {
		t.Errorf("exit %d, calls %v\n%s", code, f.calls, stderr.String())
	}
	assertContains(t, "stdout", stdout.String(), "5000 rows/s")

	f = &fakeControl{}
	stdout.Reset()
	code = runWith(context.Background(), []string{"replication", "snapshot-rate", "--config", cfg, "unlimited"}, &stdout, &stderr, controlDeps(f))
	if code != exitOK || len(f.calls) != 1 || f.calls[0] != "snapshot-rate 0" {
		t.Errorf("exit %d, calls %v", code, f.calls)
	}
	assertContains(t, "stdout", stdout.String(), "removed")

	for _, arg := range []string{"-1", "garbage", ""} {
		stderr.Reset()
		args := []string{"replication", "snapshot-rate", "--config", cfg}
		if arg != "" {
			args = append(args, arg)
		}
		code := runWith(context.Background(), args, &stdout, &stderr, controlDeps(&fakeControl{}))
		if code != exitUsage {
			t.Errorf("snapshot-rate %q: exit code = %d, want %d", arg, code, exitUsage)
		}
	}
}

func TestReplicationChunkSizeParsesAndReachesTheService(t *testing.T) {
	cfg := writeConfig(t, controlConfig)

	f := &fakeControl{}
	var stdout, stderr bytes.Buffer
	code := runWith(context.Background(), []string{"replication", "chunk-size", "--config", cfg, "5000"}, &stdout, &stderr, controlDeps(f))
	if code != exitOK || len(f.calls) != 1 || f.calls[0] != "chunk-size 5000" {
		t.Errorf("exit %d, calls %v\n%s", code, f.calls, stderr.String())
	}
	assertContains(t, "stdout", stdout.String(), "5000 rows")

	f = &fakeControl{}
	stdout.Reset()
	code = runWith(context.Background(), []string{"replication", "chunk-size", "--config", cfg, "adaptive"}, &stdout, &stderr, controlDeps(f))
	if code != exitOK || len(f.calls) != 1 || f.calls[0] != "chunk-size 0" {
		t.Errorf("exit %d, calls %v", code, f.calls)
	}
	assertContains(t, "stdout", stdout.String(), "how long they take")

	// 0 is not a size, and neither is a negative number or a word that is not "adaptive".
	for _, arg := range []string{"0", "-100", "unlimited", "garbage", ""} {
		stderr.Reset()
		args := []string{"replication", "chunk-size", "--config", cfg}
		if arg != "" {
			args = append(args, arg)
		}
		code := runWith(context.Background(), args, &stdout, &stderr, controlDeps(&fakeControl{}))
		if code != exitUsage {
			t.Errorf("chunk-size %q: exit code = %d, want %d", arg, code, exitUsage)
		}
	}
}

func TestOnlyResumeTakesTheSkipFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig), "--skip-transactions", "1"}, &stdout, &stderr, controlDeps(&fakeControl{}))
	if code != exitUsage {
		t.Errorf("exit code = %d, want the usage error", code)
	}
}

func TestReplicationStatusShowsTheFailureAndHowToSkipIt(t *testing.T) {
	f := &fakeControl{status: observability.Status{
		State: observability.StateFailed, Position: "u:1-5", Tables: 4,
		Failure: &failinfo.Failure{
			Time: time.Now().Add(-time.Minute), GTID: "6d4d8f60-1111-2222-3333-444455556666:6", Kind: failinfo.KindRows,
			Table: "shop.orders", Op: "insert", Rows: 3, Key: "PRIMARY KEY (id)", KeyValues: []string{"42"},
			Message: "convert shop.orders.total: the value cannot be converted", Detail: "value: 1e400",
		},
	}}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))

	if code != exitOK {
		t.Fatalf("exit code = %d\n%s", code, stderr.String())
	}
	for _, want := range []string{
		"state:        failed", "transaction:  6d4d8f60-1111-2222-3333-444455556666:6", "table:        shop.orders", "insert (3 rows in the batch)",
		"PRIMARY KEY (id) = (42)", "detail:       value: 1e400",
		"resume --skip-transactions 1", "resume --gtid-next 6d4d8f60-1111-2222-3333-444455556666:6",
	} {
		assertContains(t, "stdout", stdout.String(), want)
	}
}

func TestReplicationStatusOfAFailureOfAWriteOffersNoSkip(t *testing.T) {
	f := &fakeControl{status: observability.Status{
		State:   observability.StateFailed,
		Failure: &failinfo.Failure{Time: time.Now(), Kind: failinfo.KindWrite, Table: "shop.orders", Rows: 10, Message: "the table is gone"},
	}}
	var stdout, stderr bytes.Buffer

	runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))

	if strings.Contains(stdout.String(), "--skip-transactions") {
		t.Errorf("a failure that belongs to no transaction offers a skip:\n%s", stdout.String())
	}
	assertContains(t, "stdout", stdout.String(), "resume ")
}

func TestReplicationStatusShowsARebuildInProgress(t *testing.T) {
	f := &fakeControl{status: observability.Status{State: observability.StateReady, Rebuilding: []string{"shop.orders"}}}
	var stdout, stderr bytes.Buffer

	runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))

	assertContains(t, "stdout", stdout.String(), "rebuilding:   shop.orders")
}

func TestTablesResyncReportsWhichBeganAndWhichWereRefused(t *testing.T) {
	admin := &fakeAdmin{}
	var stdout, stderr bytes.Buffer

	code := runWith(context.Background(), []string{"tables", "resync", "--config", writeConfig(t, validConfig), "shop.orders", "shop.nope"}, &stdout, &stderr, tablesDeps(admin))

	if code != exitError {
		t.Errorf("exit code = %d, want %d: one table was refused", code, exitError)
	}
	assertContains(t, "stdout", stdout.String(), "shop.orders: resync begun")
	assertContains(t, "stderr", stderr.String(), "shop.nope: not resynchronized: shop.nope is not a replicated table")

	stdout.Reset()
	stderr.Reset()
	code = runWith(context.Background(), []string{"tables", "resync", "--config", writeConfig(t, validConfig)}, &stdout, &stderr, tablesDeps(admin))
	if code != exitUsage {
		t.Errorf("exit code without tables = %d, want %d", code, exitUsage)
	}
}

func TestReplicationStatusShowsTheProgressOfAResync(t *testing.T) {
	f := &fakeControl{status: observability.Status{State: observability.StateReady, Resyncing: []snapshot.Status{
		{Table: "shop.orders", Rows: 1200, Fraction: 0.25, RowsPerSecond: 800, RemainingSeconds: 90, Restarts: 1},
		{Table: "shop.items", Rows: 50, Fraction: 1, Done: true},
	}}}
	var stdout, stderr bytes.Buffer

	runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))

	for _, want := range []string{
		"resyncing:    " + pad("shop.orders") + " [#####···············]  25%  1200 rows · 800 rows/s · ~1m30s left · begun again 1 times",
		"resyncing:    " + pad("shop.items") + " [####################] 100%  50 rows · all read, waiting for the stream",
	} {
		assertContains(t, "stdout", stdout.String(), want)
	}
}

// TestReplicationStatusSummarizesTheInitialLoad: a source with thousands of tables has an entry per
// table in the initial load, and the status must stay a page an operator can read: how many of how
// many are loaded, what the slots are doing, and a line only for the tables being read right now.
func TestReplicationStatusSummarizesTheInitialLoad(t *testing.T) {
	f := &fakeControl{status: observability.Status{
		State: observability.StateReady,
		InitialLoad: &observability.InitialLoad{
			Total: 8000, Loaded: 4000, Left: 4000,
			Reading: 2, WaitingForStream: 1000, WaitingForSlot: 2998, RowsPerSecond: 23909,
		},
		Resyncing: []snapshot.Status{
			{Table: "warehouse.stock_movements_log", Kind: "initial_load", Reading: true, Rows: 519307, Fraction: 0.68, RowsPerSecond: 14897, RemainingSeconds: 35},
			{Table: "shop.users", Kind: "initial_load", Reading: true, Rows: 120000, Fraction: -1, RowsPerSecond: 9012},
			{Table: "shop.orders", Rows: 80, Fraction: 0.41},
		},
	}}
	var stdout, stderr bytes.Buffer

	runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))

	out := stdout.String()
	for _, want := range []string{
		"initial load: 4000/8000 tables loaded [##########===·······] 50%",
		"1000 read and waiting for the stream, 2 reading (23909 rows/s together), 2998 waiting for a slot",
		pad("warehouse.stock_movements_log") + " [##############······]  68%  519307 rows · 14897 rows/s · ~35s left",
		pad("shop.users") + " [····················]       120000 rows · 9012 rows/s",
		"resyncing:    shop.orders",
	} {
		assertContains(t, "stdout", out, want)
	}
	// The tables of the load are printed under it, not again as resyncs of their own.
	if got := strings.Count(out, "warehouse.stock_movements_log"); got != 1 {
		t.Errorf("the table being loaded appears %d times, want once", got)
	}
	if strings.Contains(out, "resyncing:    shop.users") {
		t.Error("a table of the initial load is listed as a resync of its own")
	}
}

// pad is a table name as the name column of the status prints it.
func pad(name string) string { return fmt.Sprintf("%-*s", nameWidth, name) }

// TestReplicationStatusKeepsItsLayoutWhileTablesComeAndGo: the status is watched with watch, and the
// tables being read change all the time. Everything else must keep its line, so the lists come last,
// and every bar must keep its column, so the names take a fixed width whatever is on screen.
func TestReplicationStatusKeepsItsLayoutWhileTablesComeAndGo(t *testing.T) {
	long := "warehouse." + strings.Repeat("x", 30) + "_stock_movements_log"
	f := &fakeControl{status: observability.Status{
		State:       observability.StateReady,
		Position:    "u:1-5",
		LagSeconds:  3,
		InitialLoad: &observability.InitialLoad{Total: 10, Loaded: 4, Left: 6, Reading: 2},
		Resyncing: []snapshot.Status{
			{Table: "shop.users", Kind: "initial_load", Reading: true, Rows: 10, Fraction: 0.5},
			{Table: long, Kind: "initial_load", Reading: true, Rows: 20, Fraction: 0.1},
			{Table: "shop.orders", Rows: 80, Fraction: 0.41},
		},
	}}
	var stdout, stderr bytes.Buffer

	runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))

	out := stdout.String()
	order := []string{"state:", "position:", "lag:", "tables:", "initial load:", "shop.users", "resyncing:    shop.orders"}
	at := -1
	for _, want := range order {
		i := strings.Index(out, want)
		if i < at {
			t.Fatalf("%q is printed before what comes above it, want the order %q:\n%s", want, order, out)
		}
		at = i
	}

	bars := map[int]bool{}
	for _, line := range strings.Split(out, "\n") {
		if i := strings.Index(line, " ["); i >= 0 && !strings.HasPrefix(line, "initial load:") {
			bars[utf8.RuneCountInString(line[:i])] = true
		}
	}
	if len(bars) != 1 {
		t.Errorf("the bars of the tables start at columns %v, want one column:\n%s", bars, out)
	}
	cut := tableName(long)
	if n := utf8.RuneCountInString(cut); n != nameWidth {
		t.Errorf("a long name takes %d columns, want %d", n, nameWidth)
	}
	if !strings.HasPrefix(cut, "warehouse.") || !strings.HasSuffix(cut, "_stock_movements_log") || !strings.Contains(out, cut) {
		t.Errorf("a long name is cut to %q, want the database and the end of the table kept", cut)
	}
}

// TestReplicationStatusColorsOnlyDecorateTheText: on a terminal the bars and the state that needs a
// look get colors, and with the escape codes taken out the output is exactly the plain one.
func TestReplicationStatusColorsOnlyDecorateTheText(t *testing.T) {
	f := &fakeControl{status: observability.Status{
		State:       observability.StatePaused,
		InitialLoad: &observability.InitialLoad{Total: 10, Loaded: 4, Left: 6, WaitingForStream: 3, Reading: 1},
		Resyncing:   []snapshot.Status{{Table: "shop.users", Kind: "initial_load", Reading: true, Rows: 10, Fraction: 0.5}},
	}}
	status := func() string {
		var stdout, stderr bytes.Buffer
		runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))
		return stdout.String()
	}
	t.Setenv("NO_COLOR", "")
	t.Setenv("COLORTERM", "")
	t.Setenv("CLICOLOR_FORCE", "")
	plain := status()
	t.Setenv("CLICOLOR_FORCE", "1")
	colored := status()

	if strings.Contains(plain, "\x1b[") {
		t.Errorf("output that is not a terminal has escape codes:\n%q", plain)
	}
	for _, want := range []string{"\x1b[38;5;208mpaused\x1b[0m", "\x1b[38;5;33m#", "\x1b[38;5;208m="} {
		if !strings.Contains(colored, want) {
			t.Errorf("colored status has no %q:\n%q", want, colored)
		}
	}
	if got := regexp.MustCompile("\x1b\\[[0-9;]*m").ReplaceAllString(colored, ""); got != plain {
		t.Errorf("without escape codes the colored status differs from the plain one:\n%s\nwant:\n%s", got, plain)
	}
}

func TestVersionDrawsTheMarkOnlyWithColor(t *testing.T) {
	version := func() string {
		var stdout, stderr bytes.Buffer
		runWith(context.Background(), []string{"version"}, &stdout, &stderr, runDeps{})
		return stdout.String()
	}
	t.Setenv("NO_COLOR", "")
	t.Setenv("CLICOLOR_FORCE", "")
	if plain := version(); strings.Count(plain, "\n") != 1 || !strings.HasPrefix(plain, "binwave ") {
		t.Errorf("plain version is %q, want one line of build information", plain)
	}
	t.Setenv("CLICOLOR_FORCE", "1")
	colored := version()
	if !strings.Contains(colored, "▀") || !strings.Contains(colored, "binwave ") {
		t.Errorf("version on a terminal is %q, want the mark beside the build information", colored)
	}
}

// TestInitialLoadBarShowsTablesReadEvenWhenNoneIsSwappedIn: a load whose stream lags has nothing
// swapped in for a long while. The bar must still move as tables are read, or it reads as "nothing
// is happening" while thousands of tables are being loaded.
func TestInitialLoadBarShowsTablesReadEvenWhenNoneIsSwappedIn(t *testing.T) {
	f := &fakeControl{status: observability.Status{
		State:       observability.StateReady,
		InitialLoad: &observability.InitialLoad{Total: 8000, Loaded: 0, Left: 8000, Reading: 5, WaitingForStream: 137, WaitingForSlot: 7858},
	}}
	var stdout, stderr bytes.Buffer

	runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))

	assertContains(t, "stdout", stdout.String(), "initial load: 0/8000 tables loaded [=···················] 0%")
}

// TestStatusSaysWhetherTheErrorIsStillHappening: the record of a failure is written on every retry and
// kept afterwards, so printing it the same way either case reads as an open problem forever.
func TestStatusSaysWhetherTheErrorIsStillHappening(t *testing.T) {
	since := time.Now().Add(-90 * time.Second)
	resolved := time.Now().Add(-5 * time.Minute)
	cases := map[string]struct {
		status observability.Status
		want   []string
		absent string
	}{
		"failing now": {
			status: observability.Status{
				State: observability.StateDegraded,
				LastError: &observability.LastError{
					Time: time.Now(), Kind: "clickhouse", Message: "connection refused",
					Ongoing: true, Since: since, Attempts: 12,
				},
			},
			want:   []string{"state:        degraded", "failing:      clickhouse: connection refused (for 1m30s, 12 attempts)"},
			absent: "last error:",
		},
		"resolved": {
			status: observability.Status{
				State: observability.StateReady,
				LastError: &observability.LastError{
					Time: resolved, Kind: "clickhouse", Message: "connection refused", ResolvedAt: &resolved,
				},
			},
			want:   []string{"state:        ready", "resolved 5m0s ago"},
			absent: "failing:",
		},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			f := &fakeControl{status: tt.status}
			var stdout, stderr bytes.Buffer
			runWith(context.Background(), []string{"replication", "status", "--config", writeConfig(t, controlConfig)}, &stdout, &stderr, controlDeps(f))
			out := stdout.String()
			for _, want := range tt.want {
				assertContains(t, "stdout", out, want)
			}
			if strings.Contains(out, tt.absent) {
				t.Errorf("stdout contains %q, which belongs to the other case:\n%s", tt.absent, out)
			}
		})
	}
}
