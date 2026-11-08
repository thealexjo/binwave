//go:build integration

package integration

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/test/testenv"
)

const targetDB = "replica"

func clickhouseConfig(c *testenv.ClickHouse) config.ClickHouse {
	return config.ClickHouse{
		Addresses: []string{c.Addr},
		Database:  targetDB,
		User:      c.User,
		Password:  c.Password,
		TLS:       config.TLS{Mode: config.TLSDisabled},
	}
}

func openSink(t *testing.T, c *testenv.ClickHouse) *chsink.Client {
	t.Helper()
	client, err := chsink.Open(context.Background(), clickhouseConfig(c), chsink.Options{Logger: quietLogger()})
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func replicaConfig(overrides map[string]config.TableOverride) *config.Config {
	return &config.Config{Tables: config.Tables{TargetDatabase: targetDB, Overrides: overrides}}
}

func resolveTable(t *testing.T, src *schema.Table, overrides map[string]config.TableOverride) *chsink.Table {
	t.Helper()
	tbl, err := chsink.Resolve(src, replicaConfig(overrides), nil)
	if err != nil {
		t.Fatalf("resolve %s: %v", src.FullName(), err)
	}
	return tbl
}

var additive = chsink.DDL{Mode: config.DDLAdditive}

// applyEvents feeds a source event stream to the sink the way the pipeline will: each
// transaction's rows get its version, repeated changes collapse, and the rows are inserted.
func applyEvents(t *testing.T, sink *chsink.Client, tables map[string]*chsink.Table, events []event.Event) {
	t.Helper()
	ctx := context.Background()

	versions := chsink.NewVersioner(0)
	var version uint64
	byTable := map[string][]chsink.Row{}
	var order []string
	flush := func() {
		for _, name := range order {
			if err := sink.Insert(ctx, tables[name], chsink.Collapse(byTable[name])); err != nil {
				t.Fatalf("insert into %s: %v", name, err)
			}
		}
		byTable, order = map[string][]chsink.Row{}, nil
	}

	for _, ev := range events {
		switch e := ev.(type) {
		case event.Begin:
			v, err := versions.Next(e.CommitTime)
			if err != nil {
				t.Fatal(err)
			}
			version = v
		case event.Rows:
			name := e.Table.FullName()
			tbl, ok := tables[name]
			if !ok {
				continue
			}
			rows, err := tbl.Rows(e, version)
			if err != nil {
				t.Fatalf("convert rows of %s: %v", name, err)
			}
			if _, seen := byTable[name]; !seen {
				order = append(order, name)
			}
			byTable[name] = append(byTable[name], rows...)
		case event.Commit:
			flush()
		}
	}
}

// readTable returns the visible rows of a ClickHouse table (FINAL hides superseded versions and
// tombstones), without the service columns, keyed by column name.
func readTable(t *testing.T, c *testenv.ClickHouse, table, orderBy string) []map[string]any {
	t.Helper()
	query := fmt.Sprintf("SELECT * EXCEPT (_version, _is_deleted, _synced_at) FROM `%s`.`%s` FINAL ORDER BY %s", targetDB, table, orderBy)
	rows, err := c.DB.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for rows.Next() {
		dest := make([]any, len(cols))
		for i, ct := range cols {
			dest[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		row := make(map[string]any, len(cols))
		for i, ct := range cols {
			row[ct.Name()] = deref(dest[i])
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// deref unwraps the pointer(s) a scan destination holds; a nil pointer is NULL.
func deref(v any) any {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	return rv.Interface()
}

// requireClickHouse skips the test on servers older than major.minor, for features that older
// versions lack (the supported versions are the ClickHouse LTS releases in the CI matrix).
func requireClickHouse(t *testing.T, c *testenv.ClickHouse, major, minor int) {
	t.Helper()
	var version string
	if err := c.DB.QueryRow("SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	var gotMajor, gotMinor int
	if _, err := fmt.Sscanf(version, "%d.%d", &gotMajor, &gotMinor); err != nil {
		t.Fatalf("cannot parse ClickHouse version %q: %v", version, err)
	}
	if gotMajor < major || (gotMajor == major && gotMinor < minor) {
		t.Skipf("needs ClickHouse %d.%d or newer, have %s", major, minor, version)
	}
}

func execCH(t *testing.T, c *testenv.ClickHouse, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := c.DB.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("%s\n%v", s, err)
		}
	}
}
