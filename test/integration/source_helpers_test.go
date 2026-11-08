//go:build integration

package integration

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
	"github.com/thealexjo/binwave/test/testenv"
)

const streamTimeout = 60 * time.Second

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func sourceConfig(m *testenv.MySQL, mode config.TLSMode) config.MySQL {
	return config.MySQL{
		Host: m.Host, Port: m.Port, User: m.User, Password: m.Password,
		ServerID: 4101,
		TLS:      config.TLS{Mode: mode},
	}
}

// openSource opens a Source on the container; it is closed when the test ends.
func openSource(t *testing.T, m *testenv.MySQL, opts mysqlsrc.Options) *mysqlsrc.Source {
	t.Helper()
	src := mysqlsrc.New(sourceConfig(m, config.TLSPreferred), opts, quietLogger())
	if err := src.Open(context.Background()); err != nil {
		t.Fatalf("open source: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// streamCommits streams from the given GTID set and returns every event up to and including the
// n-th Commit. It fails the test if the stream ends earlier or takes too long.
func streamCommits(t *testing.T, src *mysqlsrc.Source, from string, n int) []event.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), streamTimeout)
	defer cancel()

	var events []event.Event
	commits := 0
	err := src.Stream(ctx, from, nil, func(_ context.Context, ev event.Event) error {
		if _, ok := ev.(event.Heartbeat); ok {
			return nil
		}
		if commits >= n {
			// Events already buffered when the stream was cancelled must not leak into the
			// result: a caller resuming from the last commit would skip a transaction.
			return nil
		}
		events = append(events, ev)
		if _, ok := ev.(event.Commit); ok {
			commits++
			if commits == n {
				cancel()
			}
		}
		return nil
	})
	if commits < n {
		t.Fatalf("stream ended after %d of %d commits: %v\nevents: %#v", commits, n, err, events)
	}
	return events
}

// rowsOf returns the Rows events of the given table.
func rowsOf(events []event.Event, table string) []event.Rows {
	var out []event.Rows
	for _, ev := range events {
		if r, ok := ev.(event.Rows); ok && r.Table.FullName() == table {
			out = append(out, r)
		}
	}
	return out
}
