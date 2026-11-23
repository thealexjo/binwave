//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"testing"

	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/test/testenv"
)

// planStep is a line of the plan a dry run logs.
type planStep struct{ Msg, Statement, Query, Expect string }

// dryRunRebuild runs the rebuild with ddl.dry_run and returns the plan it logged.
func dryRunRebuild(t *testing.T, ch *testenv.ClickHouse, req chsink.RebuildRequest) ([]planStep, error) {
	t.Helper()
	var logged bytes.Buffer
	client, err := chsink.Open(context.Background(), clickhouseConfig(ch), chsink.Options{Logger: slog.New(slog.NewJSONHandler(&logged, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	req.DryRun = true
	err = client.Rebuild(context.Background(), req)
	var plan []planStep
	for _, line := range bytes.Split(bytes.TrimSpace(logged.Bytes()), []byte("\n")) {
		var s planStep
		if len(line) == 0 {
			continue
		}
		if err := json.Unmarshal(line, &s); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(s.Msg, "dry run:") {
			plan = append(plan, s)
		}
	}
	return plan, err
}

func statements(plan []planStep) []string {
	var got []string
	for _, s := range plan {
		if s.Statement != "" {
			got = append(got, s.Statement)
		}
	}
	return got
}

var planned = regexp.MustCompile(`^(\S+) with uuid (\S+) and comment '(.*)'$`)

// followPlan runs a plan by hand: its statements, the UUID of the copy put in once the copy is made, and
// its reads, each checked against what the plan says it is to find. It returns the UUID of the copy.
func followPlan(t *testing.T, ch *testenv.ClickHouse, database, copyName string, plan []planStep) string {
	t.Helper()
	standIn, copyUUID := "{uuid of `"+database+"`.`"+copyName+"`}", ""
	put := func(s string) string { return strings.ReplaceAll(s, standIn, copyUUID) }
	for i, s := range plan {
		if s.Statement != "" {
			if _, err := ch.DB.Exec(put(s.Statement)); err != nil {
				t.Fatalf("step %d %q: %v", i+1, s.Statement, err)
			}
			if strings.HasPrefix(s.Statement, "CREATE TABLE") {
				copyUUID = tableUUID(t, ch, database, copyName) // known once the copy is made
			}
			continue
		}
		if !strings.Contains(s.Query, "system.tables") || !strings.Contains(s.Query, " IN (") {
			if _, err := ch.DB.Exec(s.Query); err != nil {
				t.Fatalf("step %d %q: %v", i+1, s.Query, err)
			}
			continue
		}
		// A read: what is there must be what the plan says.
		rows, err := textRows(ch.DB, s.Query)
		if err != nil {
			t.Fatalf("step %d %q: %v", i+1, s.Query, err)
		}
		there := map[string]string{}
		for _, r := range rows {
			f := strings.Split(r, "\t")
			there[f[0]] = f[2] + " " + f[1]
		}
		for _, want := range strings.Split(put(s.Expect), "; ") {
			if name, ok := strings.CutPrefix(want, "no table "); ok {
				if _, is := there[name]; is {
					t.Errorf("step %d: %s is there, the plan says it is not", i+1, name)
				}
				continue
			}
			m := planned.FindStringSubmatch(want)
			if m == nil {
				t.Fatalf("step %d: cannot read the expectation %q", i+1, want)
			}
			if got := there[m[1]]; got != m[2]+" "+m[3] {
				t.Errorf("step %d: %s is %q, the plan says %q", i+1, m[1], got, m[2]+" "+m[3])
			}
		}
	}
	return copyUUID
}

// TestTheDryRunOfARebuildIsAPlanToFollowByHand: a rebuild with ddl.dry_run changes nothing and logs every
// query of the rebuild in order. Followed by hand, the UUID of the copy put in once it is made, every
// read finds what the plan says it is to find, the table ends as the rebuild leaves it, and the
// statements are those a rebuild sends.
func TestTheDryRunOfARebuildIsAPlanToFollowByHand(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	client := openSink(t, ch)
	ctx := context.Background()
	binwaveShaped(t, ch, "t", "id", "(1, 'a', 1, 0, now()), (1, 'a2', 2, 0, now()), (2, 'b', 1, 0, now()), (3, 'c', 1, 0, now()), (3, 'c', 2, 1, now())")
	before := tableUUID(t, ch, "replica", "t")

	plan, err := dryRunRebuild(t, ch, chsink.RebuildRequest{Source: "s.t", Database: "replica", Table: "t", Key: []string{"code"}})
	if !chsink.IsTableProblem(err) || chsink.IsConflict(err) {
		t.Fatalf("Rebuild() in a dry run = %v, want a table problem that says it was not done", err)
	}
	if got := chTables(t, ch); strings.Join(got, ",") != "t" || chSortingKey(t, ch, "t") != "id" {
		t.Fatalf("the dry run changed ClickHouse: tables %v, sort key %s", got, chSortingKey(t, ch, "t"))
	}
	copyUUID := followPlan(t, ch, "replica", "t__rebuild", plan)

	if got, err := textRows(ch.DB, "SELECT id, code FROM replica.t FINAL WHERE _is_deleted = 0 ORDER BY id"); err != nil || strings.Join(got, ";") != "1\ta2;2\tb" {
		t.Errorf("rows %v (%v), want what the rebuild keeps", got, err)
	}
	if got := chTables(t, ch); strings.Join(got, ",") != "t" || chSortingKey(t, ch, "t") != "code" || chTableComment(t, ch, "t") != "" {
		t.Errorf("tables %v, sort key %s, comment %q: want only t, under the new key, unmarked", got, chSortingKey(t, ch, "t"), chTableComment(t, ch, "t"))
	}
	if tableUUID(t, ch, "replica", "t") == before {
		t.Error("t is the old table")
	}

	// The statements of the plan are those a rebuild sends: the same table in another database, rebuilt,
	// and what ClickHouse logged of it.
	for _, q := range []string{
		"CREATE DATABASE store",
		"CREATE TABLE store.t AS replica.t ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY id",
		"INSERT INTO store.t (id, code, _version, _is_deleted, _synced_at) VALUES (1, 'a', 1, 0, now()), (2, 'b', 1, 0, now())",
	} {
		if _, err := ch.DB.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	old := tableUUID(t, ch, "store", "t")
	var since string
	if err := ch.DB.QueryRow("SELECT toString(now64(6))").Scan(&since); err != nil {
		t.Fatal(err)
	}
	if err := client.Rebuild(ctx, chsink.RebuildRequest{Source: "s.t", Database: "store", Table: "t", Key: []string{"code"}}); err != nil {
		t.Fatal(err)
	}
	made := tableUUID(t, ch, "store", "t")
	if _, err := ch.DB.Exec("SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	sent, err := textRows(ch.DB, "SELECT query FROM system.query_log WHERE type = 'QueryStart' AND event_time_microseconds >= toDateTime64('"+since+"', 6) "+
		"AND has(databases, 'store') AND query_kind NOT IN ('Select', 'System') ORDER BY event_time_microseconds")
	if err != nil {
		t.Fatal(err)
	}
	same := strings.NewReplacer("`store`", "`replica`", "'store'", "'replica'", old, before, strings.ReplaceAll(old, "-", ""), strings.ReplaceAll(before, "-", ""),
		made, copyUUID, strings.ReplaceAll(made, "-", ""), strings.ReplaceAll(copyUUID, "-", ""))
	want := statements(plan)
	for i := range want {
		want[i] = strings.ReplaceAll(want[i], "{uuid of `replica`.`t__rebuild`}", copyUUID)
	}
	for i := range sent {
		sent[i] = same.Replace(sent[i])
	}
	if strings.Join(sent, "\n") != strings.Join(want, "\n") {
		t.Errorf("the plan:\n%s\nthe rebuild sent:\n%s", strings.Join(want, "\n"), strings.Join(sent, "\n"))
	}
}

// TestADryRunOfARebuildDecidesAsTheRebuild: on what a crash leaves in ClickHouse, a dry run and the rebuild
// decide the same. A copy left with the marker and the new key is reused by both (the plan makes none);
// a copy without the marker or under another key, and a marker on the table that names another copy,
// stop both with the same conflict, nothing changed; a swap interrupted after its exchange, also after
// the old table was renamed to be dropped, is finished by both. Each state is made, and gone through,
// once for the dry run and once for the rebuild: the end of a swap drops every table a drop left in the
// database, the other one's too.
func TestADryRunOfARebuildDecidesAsTheRebuild(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)
	client := openSink(t, ch)
	ctx := context.Background()
	const rows = "(1, 'a', 1, 0, now()), (2, 'b', 1, 0, now())"
	for _, c := range []struct {
		name     string
		setup    func(table string)
		conflict bool
	}{
		{"a copy left with the marker and the new key", func(table string) {
			binwaveShaped(t, ch, table, "id", rows)
			binwaveShaped(t, ch, table+"__rebuild", "code", "(2, 'b', 1, 0, now())")
			markRebuild(t, ch, table+"__rebuild", chsink.RebuildMarker(table))
		}, false},
		{"a copy left with the marker naming the tables, right before the exchange", func(table string) {
			binwaveShaped(t, ch, table, "id", rows)
			binwaveShaped(t, ch, table+"__rebuild", "code", "(2, 'b', 1, 0, now())")
			markRebuild(t, ch, table+"__rebuild", chsink.Replacing(chsink.RebuildMarker(table),
				tableUUID(t, ch, "replica", table), tableUUID(t, ch, "replica", table+"__rebuild")))
		}, false},
		{"a copy left without the marker", func(table string) {
			binwaveShaped(t, ch, table, "id", rows)
			binwaveShaped(t, ch, table+"__rebuild", "code", "")
		}, true},
		{"a copy left under another key", func(table string) {
			binwaveShaped(t, ch, table, "id", rows)
			binwaveShaped(t, ch, table+"__rebuild", "id, code", "")
			markRebuild(t, ch, table+"__rebuild", chsink.RebuildMarker(table))
		}, true},
		{"the table rebuilt, its marker naming another copy", func(table string) {
			binwaveShaped(t, ch, table, "code", rows)
			binwaveShaped(t, ch, table+"__rebuild", "id", rows)
			markRebuild(t, ch, table, chsink.Replacing(chsink.RebuildMarker(table), tableUUID(t, ch, "replica", table+"__rebuild"), "0c000000-0000-0000-0000-00000000000c"))
		}, true},
		{"the table rebuilt, its own marker on it", func(table string) {
			binwaveShaped(t, ch, table, "code", rows)
			binwaveShaped(t, ch, table+"__rebuild", "id", rows)
			markRebuild(t, ch, table, chsink.Replacing(chsink.RebuildMarker(table), tableUUID(t, ch, "replica", table+"__rebuild"), tableUUID(t, ch, "replica", table)))
		}, false},
		{"the table rebuilt, the old table renamed to be dropped", func(table string) {
			binwaveShaped(t, ch, table, "code", rows)
			binwaveShaped(t, ch, table+"__rebuild", "id", rows)
			old := tableUUID(t, ch, "replica", table+"__rebuild")
			execCH(t, ch, "RENAME TABLE replica."+table+"__rebuild TO replica.binwave_drop_"+strings.ReplaceAll(old, "-", ""))
			markRebuild(t, ch, table, chsink.Replacing(chsink.RebuildMarker(table), old, tableUUID(t, ch, "replica", table)))
		}, false},
	} {
		dropAll := func() {
			for _, name := range chTables(t, ch) {
				execCH(t, ch, "DROP TABLE replica."+name+" SYNC")
			}
		}
		state := func(table string) string {
			got := []string{}
			for _, name := range chTables(t, ch) {
				if name == table || strings.HasPrefix(name, table+"__") || strings.HasPrefix(name, "binwave_drop_") {
					got = append(got, name+":"+chSortingKey(t, ch, name)+":"+chTableComment(t, ch, name))
				}
			}
			return strings.ReplaceAll(strings.Join(got, ","), table, "x")
		}
		// The copy left, or the table that carried its own marker, is the one in place after the swap.
		inPlace := func(table string) []string {
			return []string{tableUUID(t, ch, "replica", table), uuidOf(t, ch, "replica", table+"__rebuild")}
		}

		// The dry run, followed by hand.
		dropAll()
		c.setup("tp")
		before, want := state("tp"), inPlace("tp")
		plan, dryErr := dryRunRebuild(t, ch, chsink.RebuildRequest{Source: "s.tp", Database: "replica", Table: "tp", Key: []string{"code"}})
		changed := state("tp") != before
		if changed {
			t.Errorf("%s: the dry run changed ClickHouse: %q, was %q", c.name, state("tp"), before)
		}
		if c.conflict && dryErr == nil || !c.conflict && dryErr != nil && (chsink.IsConflict(dryErr) || !chsink.IsTableProblem(dryErr)) {
			t.Errorf("%s: the dry run = %v", c.name, dryErr)
			continue
		}
		var planned string
		if !c.conflict {
			if slices.ContainsFunc(statements(plan), func(s string) bool { return strings.HasPrefix(s, "CREATE TABLE") }) {
				t.Errorf("%s: the plan makes a copy, and one is there: %v", c.name, statements(plan))
			}
			followPlan(t, ch, "replica", "tp__rebuild", plan)
			planned = state("tp")
			if got := tableUUID(t, ch, "replica", "tp"); !slices.Contains(want, got) {
				t.Errorf("%s: the plan followed by hand puts %s in place, want one of %v", c.name, got, want)
			}
		}

		// The rebuild.
		dropAll()
		c.setup("tr")
		before, want = state("tr"), inPlace("tr")
		realErr := client.Rebuild(ctx, chsink.RebuildRequest{Source: "s.tr", Database: "replica", Table: "tr", Key: []string{"code"}})
		if c.conflict {
			if !chsink.IsConflict(dryErr) || !chsink.IsConflict(realErr) ||
				strings.ReplaceAll(dryErr.Error(), "tp", "x") != strings.ReplaceAll(realErr.Error(), "tr", "x") {
				t.Errorf("%s: the dry run = %v, the rebuild = %v: want the same conflict", c.name, dryErr, realErr)
			}
			if len(statements(plan)) > 0 || state("tr") != before {
				t.Errorf("%s: statements %v, the rebuild left %q of %q: want nothing planned or changed", c.name, statements(plan), state("tr"), before)
			}
			continue
		}
		if realErr != nil {
			t.Errorf("%s: the rebuild = %v", c.name, realErr)
			continue
		}
		if planned != state("tr") || planned != "x:code:" {
			t.Errorf("%s: the plan followed by hand leaves %q, the rebuild %q, want %q", c.name, planned, state("tr"), "x:code:")
		}
		if got := tableUUID(t, ch, "replica", "tr"); !slices.Contains(want, got) {
			t.Errorf("%s: the rebuild puts %s in place, want one of %v", c.name, got, want)
		}
	}
}

// uuidOf is the UUID of a ClickHouse table, empty when it is not there.
func uuidOf(t *testing.T, ch *testenv.ClickHouse, database, table string) string {
	t.Helper()
	got, err := textRows(ch.DB, "SELECT toString(uuid) FROM system.tables WHERE database = '"+database+"' AND name = '"+table+"'")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(got, "")
}
