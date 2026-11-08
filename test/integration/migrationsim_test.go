//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/test/migrationsim"
	"github.com/thealexjo/binwave/test/testenv"
)

// simScenarios are the migrations whose statements were captured from the real tools; the alter of
// each is the one the tool ran.
var simScenarios = []struct {
	name  string
	alter string
	run   func(ctx context.Context, r *migrationsim.Runner, alter string) error
}{
	{"pt-add", "ADD COLUMN c INT NOT NULL DEFAULT 5", func(ctx context.Context, r *migrationsim.Runner, a string) error {
		return r.PtOSC(ctx, a, migrationsim.PtOSCOptions{})
	}},
	{"pt-change", "CHANGE COLUMN v title VARCHAR(30) NOT NULL, MODIFY qty BIGINT NOT NULL", func(ctx context.Context, r *migrationsim.Runner, a string) error {
		return r.PtOSC(ctx, a, migrationsim.PtOSCOptions{})
	}},
	{"pt-drop-swap", "ADD COLUMN c INT NOT NULL DEFAULT 5", func(ctx context.Context, r *migrationsim.Runner, a string) error {
		return r.PtOSC(ctx, a, migrationsim.PtOSCOptions{DropSwap: true})
	}},
	{"gh-add", "ADD COLUMN c INT NOT NULL DEFAULT 5", func(ctx context.Context, r *migrationsim.Runner, a string) error {
		return r.Ghost(ctx, a, migrationsim.GhostOptions{})
	}},
	{"gh-two-step", "CHANGE COLUMN v title VARCHAR(30) NOT NULL, DROP COLUMN qty", func(ctx context.Context, r *migrationsim.Runner, a string) error {
		return r.Ghost(ctx, a, migrationsim.GhostOptions{TwoStep: true})
	}},
	{"gh-keep-del", "ADD COLUMN c INT NOT NULL DEFAULT 5", func(ctx context.Context, r *migrationsim.Runner, a string) error {
		return r.Ghost(ctx, a, migrationsim.GhostOptions{Options: migrationsim.Options{KeepOld: true}})
	}},
}

// TestSimulationMatchesTheStatementsOfTheRealTools: the DDL that the simulation writes to the binlog
// is, statement for statement, what pt-online-schema-change and gh-ost wrote (testdata/*.golden,
// captured from the real tools on MySQL 8.4). This is what allows the tests to use the simulation
// instead of installing the tools.
func TestSimulationMatchesTheStatementsOfTheRealTools(t *testing.T) {
	t.Parallel()
	for _, sc := range simScenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			my := testenv.StartMySQL(t)
			mustExec(t, my,
				"CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, qty INT NOT NULL)",
				"INSERT INTO binwave.orders VALUES (1,'a',1),(2,'b',2),(3,'c',3),(4,'d',4),(5,'e',5)")
			r := migrationsim.New(rootDB(t, my), "binwave", "orders")

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := sc.run(ctx, r, sc.alter); err != nil {
				t.Fatal(err)
			}

			golden, err := os.ReadFile(filepath.Join("..", "migrationsim", "testdata", sc.name+".golden"))
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Split(strings.TrimSpace(string(golden)), "\n")
			got := r.DDL()
			for i := 0; i < len(want) || i < len(got); i++ {
				var w, g string
				if i < len(want) {
					w = want[i]
				}
				if i < len(got) {
					g = got[i]
				}
				if w != g {
					t.Errorf("statement %d differs\n  real tool: %s\n  simulation: %s", i+1, w, g)
				}
			}
		})
	}
}

// TestSimulatedMigrationsLeaveTheTableMigrated: whatever the sequence, the table ends with the new
// structure, every row, and none of the tools' tables or triggers.
func TestSimulatedMigrationsLeaveTheTableMigrated(t *testing.T) {
	t.Parallel()
	for _, sc := range simScenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			my := testenv.StartMySQL(t)
			mustExec(t, my,
				"CREATE TABLE binwave.orders (id INT PRIMARY KEY, v VARCHAR(20) NOT NULL, qty INT NOT NULL)",
				"INSERT INTO binwave.orders VALUES (1,'a',1),(2,'b',2),(3,'c',3),(4,'d',4),(5,'e',5)")
			r := migrationsim.New(rootDB(t, my), "binwave", "orders")
			// Changes made while the copy runs must arrive as well.
			hooks := migrationsim.Options{ChunkSize: 2, Hooks: migrationsim.Hooks{DuringCopy: func(chunk int) {
				if chunk == 1 {
					mustExec(t, my, "INSERT INTO binwave.orders (id, v, qty) VALUES (6, 'f', 6)", "DELETE FROM binwave.orders WHERE id = 2")
				}
			}}}
			var run func(ctx context.Context, r *migrationsim.Runner, a string) error
			switch {
			case strings.HasPrefix(sc.name, "pt-"):
				drop := sc.name == "pt-drop-swap"
				run = func(ctx context.Context, r *migrationsim.Runner, a string) error {
					return r.PtOSC(ctx, a, migrationsim.PtOSCOptions{Options: hooks, DropSwap: drop})
				}
			default:
				two, keep := sc.name == "gh-two-step", sc.name == "gh-keep-del"
				h := hooks
				h.KeepOld = keep
				run = func(ctx context.Context, r *migrationsim.Runner, a string) error {
					return r.Ghost(ctx, a, migrationsim.GhostOptions{Options: h, TwoStep: two})
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := run(ctx, r, sc.alter); err != nil {
				t.Fatal(err)
			}

			ids, err := textRows(my.DB, "SELECT id FROM binwave.orders ORDER BY id")
			if err != nil || strings.Join(ids, ",") != "1,3,4,5,6" {
				t.Errorf("rows %v (%v), want 1,3,4,5,6: the insert and the delete made during the copy must arrive", ids, err)
			}
			cols, err := textRows(my.DB, "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'binwave' AND TABLE_NAME = 'orders' ORDER BY ORDINAL_POSITION")
			want := "id,v,qty,c"
			switch sc.name {
			case "pt-change":
				want = "id,title,qty"
			case "gh-two-step":
				want = "id,title"
			}
			if err != nil || strings.Join(cols, ",") != want {
				t.Errorf("columns %v (%v), want %s", cols, err, want)
			}
			left, err := textRows(my.DB, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'binwave' AND TABLE_NAME <> 'orders' ORDER BY TABLE_NAME")
			wantLeft := ""
			if sc.name == "gh-keep-del" {
				wantLeft = "_orders_del"
			}
			if err != nil || strings.Join(left, ",") != wantLeft {
				t.Errorf("tables left behind: %v (%v), want %q", left, err, wantLeft)
			}
			triggers, err := textRows(my.DB, "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = 'binwave'")
			if err != nil || len(triggers) != 0 {
				t.Errorf("triggers left behind: %v (%v)", triggers, err)
			}
		})
	}
}
