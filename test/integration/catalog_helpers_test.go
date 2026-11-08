//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/test/testenv"
)

// textRows runs a query and renders every row as one tab-separated line; NULL is "<NULL>".
func textRows(db *sql.DB, query string) ([]string, error) {
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

// pair is a table rendered the same way by a MySQL query and a ClickHouse query.
type pair struct{ name, mysql, ch string }

// waitSame waits up to a minute until every pair renders identically on both sides.
func waitSame(t *testing.T, my *testenv.MySQL, c *testenv.ClickHouse, pairs ...pair) {
	t.Helper()
	const within = 60 * time.Second
	var last []string
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		last = nil
		for _, p := range pairs {
			m, err := textRows(my.DB, p.mysql)
			if err != nil {
				last = append(last, p.name+": mysql: "+err.Error())
				continue
			}
			h, err := textRows(c.DB, p.ch)
			if err != nil {
				last = append(last, p.name+": clickhouse: "+err.Error())
				continue
			}
			last = append(last, firstDiffs(p.name, m, h)...)
		}
		if len(last) == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("ClickHouse did not match MySQL within %v:\n  %s", within, strings.Join(last, "\n  "))
}

func firstDiffs(name string, m, h []string) []string {
	var out []string
	if len(m) != len(h) {
		out = append(out, fmt.Sprintf("%s: MySQL has %d rows, ClickHouse %d", name, len(m), len(h)))
	}
	for i := 0; i < len(m) && i < len(h) && len(out) < 6; i++ {
		if m[i] != h[i] {
			out = append(out, fmt.Sprintf("%s row %d: MySQL %q, ClickHouse %q", name, i, trunc(m[i]), trunc(h[i])))
		}
	}
	if len(out) == 0 && len(m) != len(h) {
		out = append(out, name+": row counts differ")
	}
	return out
}

func trunc(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

// runEngine starts the engine for cfg and returns a stop function that checks for a clean stop.
func runEngine(t *testing.T, cfg *config.Config) (stop func()) {
	t.Helper()
	r := startEngine(t, cfg)
	return func() {
		t.Helper()
		if err := r.stop(t); err != nil {
			t.Errorf("the engine stopped with an error: %v", err)
		}
	}
}

// failed returns the error the engine stopped with, if it has stopped.
func (r *runningEngine) failed() error {
	select {
	case err := <-r.done:
		r.done <- err // leave it for stop
		return err
	default:
		return nil
	}
}

// waitSameWatching is waitSame that also gives up, with the engine's error, if the engine dies.
func waitSameWatching(t *testing.T, eng *runningEngine, my *testenv.MySQL, c *testenv.ClickHouse, within time.Duration, pairs ...pair) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if err := eng.failed(); err != nil {
			t.Fatalf("the engine stopped before ClickHouse matched MySQL: %v", err)
		}
		var last []string
		for _, p := range pairs {
			m, err := textRows(my.DB, p.mysql)
			if err != nil {
				last = append(last, p.name+": mysql: "+err.Error())
				continue
			}
			h, err := textRows(c.DB, p.ch)
			if err != nil {
				last = append(last, p.name+": clickhouse: "+err.Error())
				continue
			}
			last = append(last, firstDiffs(p.name, m, h)...)
		}
		if len(last) == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := eng.failed(); err != nil {
		t.Fatalf("the engine stopped: %v", err)
	}
	t.Fatalf("ClickHouse did not match MySQL within %v", within)
}
