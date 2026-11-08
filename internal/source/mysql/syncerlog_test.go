package mysql

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestSyncerLoggerDropsConfigurationDumps(t *testing.T) {
	var buf bytes.Buffer
	log := syncerLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), nil)

	type config struct{ Password string }
	log.Info("create BinlogSyncer", "config", config{Password: "hunter2"})
	log.Info("begin to sync binlog from GTID set", "GTID set", "u:1-5")
	log.Info("Connected to server", "version", "8.4")
	log.Warn("retry sync err, wait 1s and retry again", "error", "refused")

	out := buf.String()
	if strings.Contains(out, "hunter2") || strings.Contains(out, "create BinlogSyncer") {
		t.Errorf("a configuration dump reached the log:\n%s", out)
	}
	if strings.Contains(out, "begin to sync binlog") {
		t.Errorf("the GTID dump reached the log:\n%s", out)
	}
	for _, keep := range []string{"Connected to server", "retry sync err"} {
		if !strings.Contains(out, keep) {
			t.Errorf("%q was dropped; only the dumps should be:\n%s", keep, out)
		}
	}
}

func TestSyncerLoggerKeepsAttributesAndGroups(t *testing.T) {
	var buf bytes.Buffer
	log := syncerLogger(slog.New(slog.NewTextHandler(&buf, nil)), nil).With("component", "syncer").WithGroup("g")
	log.Warn("something", "k", "v")
	if out := buf.String(); !strings.Contains(out, "component=syncer") || !strings.Contains(out, "g.k=v") {
		t.Errorf("attributes or groups were lost: %s", out)
	}
}

func TestReconnectsAreCountedAndStillLogged(t *testing.T) {
	var buf bytes.Buffer
	reconnects := 0
	log := syncerLogger(slog.New(slog.NewTextHandler(&buf, nil)), func() { reconnects++ })

	log.Info("begin to sync binlog from GTID set") // the first start is not a reconnect
	log.With("component", "syncer").WithGroup("g").Info("begin to re-sync")
	log.Info("begin to re-sync")
	log.Info("something else")

	if reconnects != 2 {
		t.Errorf("reconnects = %d, want 2, also through derived loggers", reconnects)
	}
	if !strings.Contains(buf.String(), "begin to re-sync") {
		t.Error("the reconnect message must still reach the log")
	}
}

func TestReconnectsAreCountedEvenWhenTheLogLevelHidesThem(t *testing.T) {
	var buf bytes.Buffer
	reconnects := 0
	log := syncerLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})), func() { reconnects++ })

	log.Info("begin to re-sync")
	log.Info("something else")

	if reconnects != 1 {
		t.Errorf("reconnects = %d, want 1: a metric must not depend on the log level", reconnects)
	}
	if buf.Len() != 0 {
		t.Errorf("the level was not applied to the output: %q", buf.String())
	}
}

// traced is how a go-mysql error prints: the message, then the stack of every errors.Trace it went
// through, a few kilobytes of it per line of the log.
type traced struct{ msg string }

func (e traced) Error() string {
	return e.msg + "\ngithub.com/pingcap/errors.Trace\n\t/go/pkg/mod/github.com/pingcap/errors/juju_adaptor.go:15\n" +
		"github.com/go-mysql-org/go-mysql/client.(*Conn).handshake\n\t/go/pkg/mod/github.com/go-mysql-org/go-mysql/client/conn.go:250"
}

// TestSyncerLoggerKeepsTheMessageOfAnErrorWithoutItsStack: every attempt to reconnect while MySQL
// restarts used to write the whole stack, at the error level; the first line is what says what failed.
// Waiting to try again is what is meant to happen, so it is a warning; giving up stays an error.
func TestSyncerLoggerKeepsTheMessageOfAnErrorWithoutItsStack(t *testing.T) {
	var buf bytes.Buffer
	log := syncerLogger(slog.New(slog.NewTextHandler(&buf, nil)), nil)

	err := traced{"readInitialHandshake: io.ReadFull(header) failed. err EOF: connection was bad"}
	log.Error("retry sync err, wait 1s and retry again", slog.Any("error", err), slog.Int("retryCount", 1))
	log.Error(err.Error())
	log.Error("retry sync err, exceeded max retries", slog.Any("error", err))

	out := buf.String()
	if strings.Contains(out, "pingcap") || strings.Contains(out, `\n`) {
		t.Errorf("a stack reached the log:\n%s", out)
	}
	if got := strings.Count(out, "connection was bad"); got != 3 {
		t.Errorf("the message of the error is in %d lines, want 3:\n%s", got, out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines = %d, want 3:\n%s", len(lines), out)
	}
	for i, want := range []string{"level=WARN", "level=ERROR", "level=ERROR"} {
		if !strings.Contains(lines[i], want) {
			t.Errorf("line %d = %s, want %s", i+1, lines[i], want)
		}
	}
	if !strings.Contains(lines[0], "retryCount=1") {
		t.Errorf("the other attributes were lost: %s", lines[0])
	}
}

// TestTheOldConnectionGoneAfterAReconnectIsNotAnError: after reconnecting, go-mysql kills the
// connection it had, which a restarted server does not know any more. That is the expected outcome.
func TestTheOldConnectionGoneAfterAReconnectIsNotAnError(t *testing.T) {
	var buf bytes.Buffer
	log := syncerLogger(slog.New(slog.NewTextHandler(&buf, nil)), nil)

	log.Error("kill connection", slog.Any("error", errors.New("ERROR 1094 (HY000): Unknown thread id: 11")), slog.Int64("id", 11))
	if buf.Len() != 0 {
		t.Errorf("logged at the info level or above: %s", buf.String())
	}
	log.Error("kill connection", slog.Any("error", errors.New("ERROR 1227 (42000): Access denied")), slog.Int64("id", 12))
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("another failure to kill the connection is still an error: %q", buf.String())
	}
}
