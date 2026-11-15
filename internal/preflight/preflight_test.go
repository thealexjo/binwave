package preflight

import (
	"errors"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/schema"
)

func table(db, name string, pk []string, cols ...string) schema.CatalogTable {
	return schema.CatalogTable{Database: db, Name: name, Columns: cols, PrimaryKey: pk}
}

func cfgWith(t *testing.T, mutate func(*config.Config)) *config.Config {
	t.Helper()
	cfg := &config.Config{}
	cfg.Tables.ForeignKeyCascades = config.CascadesError
	if mutate != nil {
		mutate(cfg)
	}
	return cfg
}

// wantProblems checks the findings by severity and a fragment of the message, in order.
func wantProblems(t *testing.T, got []Problem, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d problems, want %d:\n%v", len(got), len(want), got)
	}
	for i, w := range want {
		sev, fragment, _ := strings.Cut(w, ": ")
		if got[i].Severity.String() != sev || !strings.Contains(got[i].String(), fragment) {
			t.Errorf("problem %d = %s %q, want %s containing %q", i, got[i].Severity, got[i], sev, fragment)
		}
	}
}

func TestCleanCatalogHasNoProblems(t *testing.T) {
	cat := &schema.Catalog{Tables: []schema.CatalogTable{
		table("shop", "users", []string{"id"}, "id", "name"),
		{Database: "shop", Name: "logs", Columns: []string{"a", "b"}, UniqueKeys: []schema.UniqueKey{{Name: "u", Columns: []string{"a"}}}},
	}}
	r := Check(cat, cfgWith(t, nil))
	if len(r.Problems) != 0 || r.Err() != nil || r.Tables != 2 {
		t.Errorf("report = %+v, err = %v", r, r.Err())
	}
}

func TestKeyChecks(t *testing.T) {
	nullableUnique := schema.CatalogTable{
		Database: "shop", Name: "t", Columns: []string{"a"},
		UniqueKeys: []schema.UniqueKey{{Name: "u", Columns: []string{"a"}, Nullable: true}},
	}
	tests := []struct {
		name string
		tbl  schema.CatalogTable
		mut  func(*config.Config)
		want []string
	}{
		{"no key at all", table("shop", "t", nil, "a"), nil, []string{"error: shop.t: the table has no primary key"}},
		{"only a nullable unique key does not identify rows", nullableUnique, nil, []string{"error: shop.t: the table has no primary key"}},
		{"order_by rescues a keyless table", table("shop", "t", nil, "a"), func(c *config.Config) {
			c.Tables.Overrides = map[string]config.TableOverride{"shop.t": {OrderBy: []string{"a"}}}
		}, nil},
		{"order_by names an unknown column", table("shop", "t", nil, "a"), func(c *config.Config) {
			c.Tables.Overrides = map[string]config.TableOverride{"shop.t": {OrderBy: []string{"zzz"}}}
		}, []string{`error: shop.t: tables.overrides.shop.t.order_by names unknown column "zzz"`}},
		{"order_by must keep every primary key column", table("shop", "t", []string{"a", "b"}, "a", "b", "c"), func(c *config.Config) {
			c.Tables.Overrides = map[string]config.TableOverride{"shop.t": {OrderBy: []string{"a", "c"}}}
		}, []string{`error: shop.t: order_by must include every primary key column, missing "b"`}},
		{"order_by may extend the key", table("shop", "t", []string{"a"}, "a", "c"), func(c *config.Config) {
			c.Tables.Overrides = map[string]config.TableOverride{"shop.t": {OrderBy: []string{"a", "c"}}}
		}, nil},
		{"a primary key column cannot be excluded", table("shop", "t", []string{"a"}, "a", "b"), func(c *config.Config) {
			c.Tables.Overrides = map[string]config.TableOverride{"shop.t": {ExcludeColumns: []string{"a"}}}
		}, []string{`error: shop.t: primary key column "a" is excluded`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := cfgWith(t, tt.mut)
			wantProblems(t, CheckTable(tt.tbl, nil, cfg), tt.want...)
		})
	}
}

func TestColumnChecks(t *testing.T) {
	ov := func(o config.TableOverride) func(*config.Config) {
		return func(c *config.Config) { c.Tables.Overrides = map[string]config.TableOverride{"shop.t": o} }
	}
	tests := []struct {
		name string
		cols []string
		mut  func(*config.Config)
		want []string
	}{
		{
			"exclude_columns typo",
			[]string{"id", "email"},
			ov(config.TableOverride{ExcludeColumns: []string{"emial"}}),
			[]string{`error: shop.t: tables.overrides.shop.t.exclude_columns names unknown column "emial"`},
		},
		{
			"column override typo",
			[]string{"id"},
			ov(config.TableOverride{Columns: map[string]config.ColumnOverride{"idd": {Type: "String"}}}),
			[]string{`error: shop.t: tables.overrides.shop.t.columns names unknown column "idd"`},
		},
		{"service column", []string{"id", "_version"}, nil, []string{`error: shop.t: column "_version" collides`}},
		{"service column excluded is fine", []string{"id", "_version"}, ov(config.TableOverride{ExcludeColumns: []string{"_version"}}), nil},
		{"comma in a name", []string{"id", "a,b"}, nil, []string{`error: shop.t: column "a,b" cannot be inserted`}},
		{"backtick in a name", []string{"id", "a`b"}, nil, []string{"error: shop.t: column \"a`b\" cannot be inserted"}},
		{"a bad name can be excluded", []string{"id", "a,b"}, ov(config.TableOverride{ExcludeColumns: []string{"a,b"}}), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantProblems(t, CheckTable(table("shop", "t", []string{"id"}, tt.cols...), nil, cfgWith(t, tt.mut)), tt.want...)
		})
	}
}

func TestColumnsTheSourceWritesWronglyAreRefusedUnlessExcluded(t *testing.T) {
	tbl := table("shop", "docs", []string{"id"}, "id", "body", "note")
	tbl.UnreliableColumns = []string{"body", "note"}
	exclude := func(c *config.Config) {
		c.Tables.Overrides = map[string]config.TableOverride{"shop.docs": {ExcludeColumns: []string{"note"}}}
	}

	got := CheckTable(tbl, nil, cfgWith(t, exclude))

	wantProblems(t, got, `error: shop.docs: column "body" may be written wrongly into the binlog`)
	if got[0].Code != "TBL501" || got[0].Scope != ScopeTable {
		t.Errorf("problem = %+v, want a TBL501 problem of the table", got[0])
	}
}

func TestForeignKeyCascadesFollowThePolicy(t *testing.T) {
	tbl := table("shop", "orders", []string{"id"}, "id", "user_id")
	fks := []schema.ForeignKey{
		{Database: "shop", Table: "orders", Name: "fk_user", ReferencedTable: "users", UpdateRule: "RESTRICT", DeleteRule: "CASCADE"},
		{Database: "shop", Table: "orders", Name: "fk_plain", ReferencedTable: "regions", UpdateRule: "NO ACTION", DeleteRule: "RESTRICT"},
		{Database: "shop", Table: "orders", Name: "fk_null", ReferencedTable: "agents", UpdateRule: "SET NULL", DeleteRule: "NO ACTION"},
	}
	tests := []struct {
		policy config.CascadePolicy
		want   []string
	}{
		{config.CascadesError, []string{"error: shop.orders: foreign key fk_null", "error: shop.orders: foreign key fk_user"}},
		{config.CascadesWarn, []string{"warning: shop.orders: foreign key fk_null", "warning: shop.orders: foreign key fk_user"}},
		{config.CascadesIgnore, nil},
	}
	for _, tt := range tests {
		t.Run(string(tt.policy), func(t *testing.T) {
			cfg := cfgWith(t, func(c *config.Config) { c.Tables.ForeignKeyCascades = tt.policy })
			got := CheckTable(tbl, fks, cfg)
			sortProblems(got)
			wantProblems(t, got, tt.want...)
		})
	}
}

func TestTargetCollisionsAreReported(t *testing.T) {
	cat := &schema.Catalog{Tables: []schema.CatalogTable{
		table("a", "users", []string{"id"}, "id"),
		table("b", "users", []string{"id"}, "id"),
		table("b", "orders", []string{"id"}, "id"),
	}}

	separate := Check(cat, cfgWith(t, nil))
	if len(separate.Problems) != 0 {
		t.Errorf("same-name tables of different databases go to different ClickHouse databases by default: %v", separate.Problems)
	}

	one := Check(cat, cfgWith(t, func(c *config.Config) { c.Tables.TargetDatabase = "all" }))
	wantProblems(t, one.Problems, "error: tables a.users, b.users would all be replicated into ClickHouse table all.users")

	renamed := Check(cat, cfgWith(t, func(c *config.Config) {
		c.Tables.TargetDatabase = "all"
		c.Tables.Overrides = map[string]config.TableOverride{"b.users": {TargetTable: "b_users"}}
	}))
	if len(renamed.Problems) != 0 {
		t.Errorf("an override separates the targets: %v", renamed.Problems)
	}

	// database_map goes through the same TargetName computation, so mapping a and b to the same
	// ClickHouse database collides on "users" exactly like target_database would.
	mapped := Check(cat, cfgWith(t, func(c *config.Config) {
		c.Tables.DatabaseMap = map[string]string{"a": "all", "b": "all"}
	}))
	wantProblems(t, mapped.Problems, "error: tables a.users, b.users would all be replicated into ClickHouse table all.users")
}

// TestATargetThatIsTheShadowOfAnotherIsReported: a resync of shop.items would fill the table of
// warehouse.items.
func TestATargetThatIsTheShadowOfAnotherIsReported(t *testing.T) {
	cat := &schema.Catalog{Tables: []schema.CatalogTable{
		table("shop", "items", []string{"id"}, "id"),
		table("warehouse", "items", []string{"id"}, "id"),
	}}
	r := Check(cat, cfgWith(t, func(c *config.Config) {
		c.Tables.TargetDatabase = "replica"
		c.Tables.Overrides = map[string]config.TableOverride{"warehouse.items": {TargetTable: "items__resync"}}
	}))
	wantProblems(t, r.Problems, "error: table warehouse.items would be replicated into ClickHouse table replica.items__resync, the shadow table of a resync of shop.items")
}

// TestATargetThatIsTheCopyOfARebuildOfAnotherIsReported: a rebuild of shop.items under a new sort
// key would fill the table of warehouse.items.
func TestATargetThatIsTheCopyOfARebuildOfAnotherIsReported(t *testing.T) {
	cat := &schema.Catalog{Tables: []schema.CatalogTable{
		table("shop", "items", []string{"id"}, "id"),
		table("warehouse", "items", []string{"id"}, "id"),
	}}
	r := Check(cat, cfgWith(t, func(c *config.Config) {
		c.Tables.TargetDatabase = "replica"
		c.Tables.Overrides = map[string]config.TableOverride{"warehouse.items": {TargetTable: "items__rebuild"}}
	}))
	wantProblems(t, r.Problems, "error: table warehouse.items would be replicated into ClickHouse table replica.items__rebuild, the copy a rebuild of shop.items fills")
}

func TestEmptyCatalogAndOrphanOverridesAreOnlyWarnings(t *testing.T) {
	empty := Check(&schema.Catalog{}, cfgWith(t, nil))
	wantProblems(t, empty.Problems, "warning: no existing table matches tables.include")
	if empty.Err() != nil {
		t.Errorf("nothing to replicate yet is not an error: %v", empty.Err())
	}

	orphan := Check(&schema.Catalog{Tables: []schema.CatalogTable{table("shop", "t", []string{"id"}, "id")}}, cfgWith(t, func(c *config.Config) {
		c.Tables.Overrides = map[string]config.TableOverride{"shop.tt": {TargetTable: "x"}}
	}))
	wantProblems(t, orphan.Problems, "warning: shop.tt: tables.overrides names a table that does not exist")
}

func TestReportErrListsEveryError(t *testing.T) {
	cat := &schema.Catalog{Tables: []schema.CatalogTable{
		table("shop", "a", nil, "x"),
		table("shop", "b", nil, "y"),
	}}
	r := Check(cat, cfgWith(t, nil))

	var pe *Error
	if err := r.Err(); !errors.As(err, &pe) || len(pe.Problems) != 2 {
		t.Fatalf("Err() = %v", err)
	}
	msg := r.Err().Error()
	for _, want := range []string{"shop.a", "shop.b", "2 problems"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the message lacks %q: %s", want, msg)
		}
	}
}

// TestProblemsAreSplitBetweenTheConfigurationAndTheTable: a mistake in the configuration stops the
// service, a problem of one table can be set aside with that table.
func TestProblemsAreSplitBetweenTheConfigurationAndTheTable(t *testing.T) {
	ov := func(o config.TableOverride) func(*config.Config) {
		return func(c *config.Config) { c.Tables.Overrides = map[string]config.TableOverride{"shop.t": o} }
	}
	tests := []struct {
		name      string
		tbl       schema.CatalogTable
		mut       func(*config.Config)
		wantScope Scope
	}{
		{"no key", table("shop", "t", nil, "a"), nil, ScopeTable},
		{"a service column", table("shop", "t", []string{"id"}, "id", "_version"), nil, ScopeTable},
		{"a column the client cannot insert", table("shop", "t", []string{"id"}, "id", "a,b"), nil, ScopeTable},
		{"exclude_columns typo", table("shop", "t", []string{"id"}, "id"), ov(config.TableOverride{ExcludeColumns: []string{"x"}}), ScopeConfig},
		{"order_by typo", table("shop", "t", nil, "a"), ov(config.TableOverride{OrderBy: []string{"zzz"}}), ScopeConfig},
		{"order_by drops a key column", table("shop", "t", []string{"a", "b"}, "a", "b"), ov(config.TableOverride{OrderBy: []string{"a"}}), ScopeConfig},
		{"everything excluded", table("shop", "t", []string{"a"}, "a"), ov(config.TableOverride{ExcludeColumns: []string{"a"}}), ScopeConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := CheckTable(tt.tbl, nil, cfgWith(t, tt.mut))
			if len(problems) == 0 {
				t.Fatal("no problems")
			}
			for _, p := range problems {
				if p.Scope != tt.wantScope {
					t.Errorf("problem %q has scope %v, want %v", p, p.Scope, tt.wantScope)
				}
			}
		})
	}

	fk := []schema.ForeignKey{{Database: "shop", Table: "t", Name: "fk", ReferencedTable: "p", DeleteRule: "CASCADE", UpdateRule: "RESTRICT"}}
	for _, p := range CheckTable(table("shop", "t", []string{"id"}, "id"), fk, cfgWith(t, nil)) {
		if p.Scope != ScopeTable {
			t.Errorf("a cascading foreign key belongs to the table: %+v", p)
		}
	}

	cat := &schema.Catalog{Tables: []schema.CatalogTable{table("a", "users", []string{"id"}, "id"), table("b", "users", []string{"id"}, "id")}}
	r := Check(cat, cfgWith(t, func(c *config.Config) { c.Tables.TargetDatabase = "all" }))
	if len(r.ConfigProblems()) != 1 || len(r.TableProblems()) != 0 || r.ConfigErr() == nil {
		t.Errorf("a target collision is a clash of settings: config %v, table %v", r.ConfigProblems(), r.TableProblems())
	}
}

func TestReplaceTableKeepsTheProblemsOfOtherTablesAndOfTheConfiguration(t *testing.T) {
	r := &Report{Problems: []Problem{
		{Severity: SevError, Scope: ScopeTable, Table: "shop.items", Message: "no key"},
		{Severity: SevWarning, Table: "shop.items", Message: "an override names no column"},
		{Severity: SevError, Scope: ScopeTable, Table: "shop.orders", Message: "no key"},
	}}
	r.ReplaceTable("shop.items", []Problem{{Severity: SevError, Scope: ScopeTable, Table: "shop.items", Message: "a cascade"}})
	wantProblems(t, r.Problems, "error: shop.items: a cascade", "warning: shop.items: an override names no column", "error: shop.orders: no key")

	r.ReplaceTable("shop.items", nil)
	wantProblems(t, r.Problems, "warning: shop.items: an override names no column", "error: shop.orders: no key")
}
