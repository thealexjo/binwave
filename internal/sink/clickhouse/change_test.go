package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/ddl"
	"github.com/thealexjo/binwave/internal/typemap"
)

func TestWidens(t *testing.T) {
	tests := []struct {
		from, to string
		want     bool
	}{
		// integers
		{"Int32", "Int64", true},
		{"Int8", "Int16", true},
		{"UInt16", "UInt64", true},
		{"Int32", "Int32", true},
		{"UInt32", "Int64", true},
		{"UInt8", "Int16", true},
		{"Int64", "Int32", false},
		{"UInt64", "UInt32", false},
		{"Int32", "UInt32", false},
		{"Int32", "UInt64", false},
		{"Int8", "UInt8", false},
		{"UInt32", "Int32", false},
		{"UInt64", "Int64", false},
		{"UInt8", "Int8", false},
		// nullability
		{"Int32", "Nullable(Int32)", true},
		{"Int32", "Nullable(Int64)", true},
		{"Nullable(Int32)", "Int32", false},
		{"Nullable(Int32)", "Nullable(Int64)", true},
		{"Nullable(Int64)", "Nullable(Int32)", false},
		// decimals
		{"Decimal(10, 2)", "Decimal(12, 2)", true},
		{"Decimal(10, 2)", "Decimal(12, 4)", true},
		{"Decimal(10, 2)", "Decimal(10, 4)", false},
		{"Decimal(10, 2)", "Decimal(8, 2)", false},
		{"Decimal(10, 4)", "Decimal(12, 2)", false},
		{"Decimal(10, 2)", "Decimal(10, 2)", true},
		// floats
		{"Float32", "Float64", true},
		{"Float64", "Float32", false},
		{"Int32", "Float64", false},
		{"Int32", "Decimal(10, 2)", false},
		// time
		{"DateTime64(0, 'UTC')", "DateTime64(3, 'UTC')", true},
		{"DateTime64(6, 'UTC')", "DateTime64(3, 'UTC')", false},
		{"DateTime64(3, 'UTC')", "DateTime64(3, 'Europe/Kyiv')", false},
		// everything else is a change of meaning
		{"String", "Int32", false},
		{"Int32", "String", false},
		{"Date32", "DateTime64(0, 'UTC')", false},
		{"String", "String", true},
		{"LowCardinality(String)", "LowCardinality(String)", true},
		{"LowCardinality(String)", "String", false},
	}
	for _, tt := range tests {
		if got := Widens(tt.from, tt.to); got != tt.want {
			t.Errorf("Widens(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

type adjustments []typemap.Adjustment

func (a *adjustments) Report(x typemap.Adjustment) { *a = append(*a, x) }

var ddlTime = time.Date(2030, 5, 6, 7, 8, 9, 0, time.UTC)

func testConfig() *config.Config { return &config.Config{} }

// planned plans the first column of "ALTER TABLE shop.t ADD COLUMN <def>".
func planned(t *testing.T, def string, cfg *config.Config) (plan *ColumnPlan, skip bool, rep adjustments, err error) {
	t.Helper()
	op, perr := ddl.Parse("ALTER TABLE shop.t ADD COLUMN "+def, "shop")
	if perr != nil {
		t.Fatal(perr)
	}
	add := op.(ddl.AlterTable).Specs[0].(ddl.AddColumns)
	plan, skip, err = PlanColumn(cfg, "shop", "t", add.Columns[0], add.Position, ddlTime, &rep)
	return plan, skip, rep, err
}

// TestPlanColumn: what every kind of added column means in ClickHouse, and what existing rows
// get. The expectations are what MySQL 8.4 gave the existing rows of the same ALTER TABLE.
func TestPlanColumn(t *testing.T) {
	tests := []struct {
		def         string
		wantType    string
		wantDefault string // "" means the type's own default is right
	}{
		// nullable and NOT NULL without DEFAULT: NULL, and the zero values ClickHouse also uses
		{"c INT NULL", "Nullable(Int32)", ""},
		{"c INT NOT NULL", "Int32", ""},
		{"c BIGINT UNSIGNED NOT NULL", "UInt64", ""},
		{"c DECIMAL(6,2) NOT NULL", "Decimal(6, 2)", ""},
		{"c DOUBLE NOT NULL", "Float64", ""},
		{"c VARCHAR(5) NOT NULL", "String", ""},
		{"c TEXT NULL", "Nullable(String)", ""},
		{"c TIME NOT NULL", "Int64", ""},
		{"c YEAR NOT NULL", "UInt16", ""},
		{"c SET('x','y') NOT NULL", "Array(LowCardinality(String))", ""},
		// MySQL gives an ENUM its first member
		{"c ENUM('p','q','r') NOT NULL", "LowCardinality(String)", "DEFAULT 'p'"},
		// constants
		{"c INT NOT NULL DEFAULT 7", "Int32", "DEFAULT 7"},
		{"c INT NOT NULL DEFAULT -7", "Int32", "DEFAULT -7"},
		{"c TINYINT UNSIGNED NOT NULL DEFAULT 200", "UInt8", "DEFAULT 200"},
		{"c INT NULL DEFAULT 3", "Nullable(Int32)", "DEFAULT 3"},
		{"c DOUBLE NOT NULL DEFAULT 1.5", "Float64", "DEFAULT 1.5"},
		{"c DECIMAL(10,2) NOT NULL DEFAULT 12.30", "Decimal(10, 2)", "DEFAULT CAST('12.30' AS Decimal(10, 2))"},
		{"c VARCHAR(10) NOT NULL DEFAULT 'x y'", "String", "DEFAULT 'x y'"},
		{"c VARCHAR(10) NOT NULL DEFAULT 'it''s'", "String", `DEFAULT 'it\'s'`},
		{"c ENUM('a','b') NOT NULL DEFAULT 'b'", "LowCardinality(String)", "DEFAULT 'b'"},
		{"c SET('x','y') NOT NULL DEFAULT 'x,y'", "Array(LowCardinality(String))", "DEFAULT ['x', 'y']"},
		{"c YEAR NOT NULL DEFAULT 2024", "UInt16", "DEFAULT 2024"},
		{"c DATE NOT NULL DEFAULT '2020-01-02'", "Date32", "DEFAULT CAST('2020-01-02' AS Date32)"},
		{"c DATETIME(3) NOT NULL DEFAULT '2020-01-02 03:04:05.678'", "DateTime64(3, 'UTC')", "DEFAULT CAST('2020-01-02 03:04:05.678' AS DateTime64(3, 'UTC'))"},
		{"c TINYINT(1) NOT NULL DEFAULT 1", "Int8", "DEFAULT 1"},
		// the ALTER's own time for a TIMESTAMP, in UTC
		{"c TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP", "DateTime64(0, 'UTC')", "DEFAULT CAST('2030-05-06 07:08:09' AS DateTime64(0, 'UTC'))"},
		{"c TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)", "DateTime64(3, 'UTC')", "DEFAULT CAST('2030-05-06 07:08:09.000' AS DateTime64(3, 'UTC'))"},
	}
	for _, tt := range tests {
		t.Run(tt.def, func(t *testing.T) {
			plan, skip, _, err := planned(t, tt.def, testConfig())
			if err != nil || skip {
				t.Fatalf("PlanColumn() = %v, skip %v", err, skip)
			}
			if plan.Type != tt.wantType || plan.Default != tt.wantDefault {
				t.Errorf("plan = {%s, %q}, want {%s, %q}", plan.Type, plan.Default, tt.wantType, tt.wantDefault)
			}
		})
	}
}

// TestPlanColumnRefusesWhatItCannotReproduce: the existing rows' values are computed by the source
// or depend on its session; guessing would put wrong values in the replica.
func TestPlanColumnRefusesWhatItCannotReproduce(t *testing.T) {
	tests := []struct {
		def  string
		want string
	}{
		{"c INT AS (id * 2) STORED", "generated column"},
		{"c INT NOT NULL AUTO_INCREMENT UNIQUE", "AUTO_INCREMENT"},
		{"c VARCHAR(5) NOT NULL DEFAULT (UPPER('x'))", "expression"},
		{"c DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP", "time zone"},
		{"c TIMESTAMP NOT NULL DEFAULT '2020-01-01 00:00:00'", "time zone"},
		{"c TIMESTAMP NOT NULL", "zero date"},
		{"c DATE NOT NULL", "zero date"},
		{"c DATETIME NOT NULL", "zero date"},
		{"c JSON NOT NULL", "JSON"},
		{"c INT NOT NULL DEFAULT 99999999999", "not a valid"},
		{"c TINYINT UNSIGNED NOT NULL DEFAULT 300", "not a valid"},
		{"c DECIMAL(10,2) NOT NULL DEFAULT 1.234", "more decimals"},
		{"c ENUM('a','b') NOT NULL DEFAULT 'z'", "not a member"},
		{"c SET('a','b') NOT NULL DEFAULT 'a,z'", "lacks"},
		{"c DATE NOT NULL DEFAULT '0000-00-00'", "not a valid"},
		{"c BLOB NOT NULL DEFAULT ('x')", "not supported"},
		{"c BIT(4) NOT NULL DEFAULT b'1'", "not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.def, func(t *testing.T) {
			_, _, _, err := planned(t, tt.def, testConfig())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("PlanColumn() = %v, want an error containing %q", err, tt.want)
			}
			if !IsTableProblem(err) {
				t.Errorf("%v is not a table problem: it must set the table aside, not stop the service", err)
			}
			// A resync rereads the table after the column already exists, with its real value
			// already there (computed, numbered or evaluated by the source itself): no guessing.
			if !Recoverable(err) {
				t.Errorf("%v is not Recoverable: tables.autoresync must be able to fix it on its own", err)
			}
		})
	}
}

// TestPlanColumnRefusalsBinwaveItselfCannotHandleAreNotRecoverable: unlike a value it cannot
// reproduce for existing rows, a name or a type binwave does not support is not something a resync
// fixes either — it would hit the exact same problem rereading the table.
func TestPlanColumnRefusalsBinwaveItselfCannotHandleAreNotRecoverable(t *testing.T) {
	tests := []struct{ def, want string }{
		{"_version INT NOT NULL", "collides"},
		{"`c,d` INT NOT NULL", "cannot be inserted"},
	}
	for _, tt := range tests {
		t.Run(tt.def, func(t *testing.T) {
			_, _, _, err := planned(t, tt.def, testConfig())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("PlanColumn() = %v, want an error containing %q", err, tt.want)
			}
			if Recoverable(err) {
				t.Errorf("%v is Recoverable, but a resync would fail on it identically", err)
			}
		})
	}
}

func TestPlanColumnReportsTheTimestampItPutIn(t *testing.T) {
	_, _, rep, err := planned(t, "c TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP", testConfig())
	if err != nil || len(rep) != 1 || rep[0].Kind != typemap.AdjustDDLTimestamp || rep[0].Replacement != "2030-05-06 07:08:09" {
		t.Errorf("adjustments = %+v, %v: the replaced value must be reported like every replaced value", rep, err)
	}
}

func TestPlanColumnHonoursTheConfiguration(t *testing.T) {
	cfg := &config.Config{Tables: config.Tables{Overrides: map[string]config.TableOverride{
		"shop.t": {ExcludeColumns: []string{"secret"}, Columns: map[string]config.ColumnOverride{"flag": {Type: "Bool"}}},
	}}}

	if _, skip, _, err := planned(t, "secret VARCHAR(10) NULL", cfg); err != nil || !skip {
		t.Errorf("an excluded column: skip = %v, err = %v; it must not reach ClickHouse", skip, err)
	}
	plan, _, _, err := planned(t, "flag TINYINT NOT NULL", cfg)
	if err != nil || plan.Type != "Bool" {
		t.Errorf("a type override: %+v, %v", plan, err)
	}
	if _, _, _, err := planned(t, "flag TINYINT NOT NULL DEFAULT 1", cfg); err == nil || !strings.Contains(err.Error(), "override") {
		t.Errorf("a default with an override: err = %v", err)
	}
	if _, _, _, err := planned(t, "_version INT NULL", testConfig()); !IsTableProblem(err) {
		t.Errorf("a service column name: %v", err)
	}
}

func TestPlanColumnKeepsThePosition(t *testing.T) {
	plan, _, _, err := planned(t, "c INT NULL AFTER id", testConfig())
	if err != nil || plan.After != "id" || plan.First {
		t.Errorf("plan = %+v, %v", plan, err)
	}
	plan, _, _, _ = planned(t, "c INT NULL FIRST", testConfig())
	if !plan.First {
		t.Errorf("plan = %+v", plan)
	}
}
