package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/app"
)

const validConfig = `
source: {mysql: {host: h, user: u, password: top-secret, server_id: 1}}
target: {clickhouse: {addresses: ["h:9000"], database: replica}}
tables: {include: ["shop.*"]}
`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "binwave.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRun(t *testing.T) {
	valid := writeConfig(t, validConfig)
	invalid := writeConfig(t, "ddl: {mode: nope}\n")

	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{name: "no args", args: nil, wantCode: exitUsage, wantStderr: "Usage: binwave"},
		{name: "version", args: []string{"version"}, wantCode: exitOK, wantStdout: "binwave dev"},
		{name: "help", args: []string{"help"}, wantCode: exitOK, wantStdout: "Usage: binwave"},
		{name: "--help", args: []string{"--help"}, wantCode: exitOK, wantStdout: "Usage: binwave"},
		{name: "unknown", args: []string{"bogus"}, wantCode: exitUsage, wantStderr: `unknown command "bogus"`},

		{name: "check valid", args: []string{"check", "--config", valid}, wantCode: exitOK, wantStdout: "is valid"},
		{name: "check print", args: []string{"check", "--config", valid, "--print"}, wantCode: exitOK, wantStdout: "password: '******'"},
		{name: "check invalid", args: []string{"check", "--config", invalid}, wantCode: exitConfig, wantStderr: "ddl.mode: must be one of"},
		{name: "check missing file", args: []string{"check", "--config", filepath.Join(t.TempDir(), "nope.yaml")}, wantCode: exitConfig, wantStderr: "read config"},
		{name: "check with empty config", args: []string{"check", "--config", ""}, wantCode: exitUsage, wantStderr: "--config must not be empty"},
		{name: "check extra args", args: []string{"check", "--config", valid, "extra"}, wantCode: exitUsage, wantStderr: "no positional arguments"},
		{name: "check bad flag", args: []string{"check", "--nope"}, wantCode: exitUsage, wantStderr: "flag provided but not defined"},
		{name: "check help", args: []string{"check", "-h"}, wantCode: exitOK, wantStderr: "-config"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer

			code := run(context.Background(), tt.args, &stdout, &stderr)

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", code, tt.wantCode, stderr.String())
			}
			assertContains(t, "stdout", stdout.String(), tt.wantStdout)
			assertContains(t, "stderr", stderr.String(), tt.wantStderr)
		})
	}
}

// TestCommandsUseTheDefaultConfigPathWhenConfigIsOmitted covers every subcommand that takes
// --config: without the flag, each one must fall back to defaultConfigPath rather than demand it,
// so a single-instance deployment (deploy/systemd/binwave.service) never has to pass it. None of
// these hit the network: run's engine is a fake, check never connects without --connect, and
// tables/replication work off a fake admin/control client, exactly like the rest of this suite.
func TestCommandsUseTheDefaultConfigPathWhenConfigIsOmitted(t *testing.T) {
	orig := defaultConfigPath
	defaultConfigPath = writeConfig(t, validConfig)
	t.Cleanup(func() { defaultConfigPath = orig })

	t.Run("run", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		runner := runnerFunc(func(context.Context, app.Hooks) error { return nil })
		code := runWith(context.Background(), []string{"run"}, &stdout, &stderr, depsWith(nil, runner))
		if code != exitOK {
			t.Fatalf("exit code = %d, want %d (stderr: %s)", code, exitOK, stderr.String())
		}
		assertContains(t, "stderr", stderr.String(), defaultConfigPath)
	})

	t.Run("check", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := runWith(context.Background(), []string{"check"}, &stdout, &stderr, runDeps{})
		if code != exitOK {
			t.Errorf("exit code = %d, want %d (stderr: %s)", code, exitOK, stderr.String())
		}
	})

	t.Run("tables list", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := runWith(context.Background(), []string{"tables", "list"}, &stdout, &stderr, tablesDeps(&fakeAdmin{}))
		if code != exitOK {
			t.Errorf("exit code = %d, want %d (stderr: %s)", code, exitOK, stderr.String())
		}
	})

	t.Run("replication status", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		// validConfig has no control.socket, so this ends in exitConfig, not exitOK — the point
		// here is only that it is not exitUsage, i.e. --config was not treated as missing.
		code := runWith(context.Background(), []string{"replication", "status"}, &stdout, &stderr, controlDeps(&fakeControl{}))
		if code == exitUsage {
			t.Errorf("exit code = %d (usage), want the default config path to be used; stderr: %s", code, stderr.String())
		}
	})
}

func TestCheckPrintNeverLeaksSecrets(t *testing.T) {
	var stdout, stderr bytes.Buffer
	run(context.Background(), []string{"check", "--config", writeConfig(t, validConfig), "--print"}, &stdout, &stderr)

	if strings.Contains(stdout.String()+stderr.String(), "top-secret") {
		t.Error("check --print leaked a password")
	}
}

func assertContains(t *testing.T, stream, got, want string) {
	t.Helper()
	if want == "" {
		if got != "" {
			t.Errorf("%s = %q, want empty", stream, got)
		}
		return
	}
	if !strings.Contains(got, want) {
		t.Errorf("%s = %q, want it to contain %q", stream, got, want)
	}
}
