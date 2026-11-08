package clickhouse

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/typemap"
)

var update = flag.Bool("update", false, "rewrite golden files")

func cfgWith(overrides map[string]config.TableOverride) *config.Config {
	return &config.Config{Tables: config.Tables{TargetDatabase: "replica", Overrides: overrides}}
}

func ordersTable() *schema.Table {
	return &schema.Table{
		Database: "shop",
		Name:     "orders",
		Columns: []typemap.Column{
			{Name: "id", Kind: typemap.KindBigInt, Unsigned: true},
			{Name: "customer", Kind: typemap.KindVarChar, Length: 100, Charset: "utf8mb4", Nullable: true},
			{Name: "total", Kind: typemap.KindDecimal, Precision: 12, Scale: 2},
			{Name: "created_at", Kind: typemap.KindDateTime, FSP: 3},
			{Name: "status", Kind: typemap.KindEnum, Values: []string{"new", "paid"}, Nullable: true},
			{Name: "tags", Kind: typemap.KindSet, Values: []string{"gift", "rush"}},
			{Name: "is_vip", Kind: typemap.KindTinyInt},
			{Name: "email", Kind: typemap.KindVarChar, Length: 100, Charset: "utf8mb4"},
		},
		PrimaryKey: []int{0},
	}
}

func TestCreateStatementGolden(t *testing.T) {
	compositeKey := &schema.Table{
		Database: "shop",
		Name:     "order_lines",
		Columns: []typemap.Column{
			{Name: "order_id", Kind: typemap.KindBigInt, Unsigned: true},
			{Name: "line", Kind: typemap.KindSmallInt},
			{Name: "sku", Kind: typemap.KindBinary, Length: 16},
			{Name: "payload", Kind: typemap.KindJSON, Nullable: true},
		},
		PrimaryKey: []int{0, 1},
	}
	odd := &schema.Table{
		Database: "we`ird",
		Name:     "ta`ble",
		Columns:  []typemap.Column{{Name: "co l", Kind: typemap.KindInt}},
		// no primary key: the sort key comes from the override
	}

	tests := []struct {
		name  string
		table *schema.Table
		cfg   *config.Config
	}{
		{"orders", ordersTable(), cfgWith(nil)},
		{"orders_overrides", ordersTable(), cfgWith(map[string]config.TableOverride{
			"shop.orders": {
				TargetTable:    "orders_v2",
				ExcludeColumns: []string{"email"},
				Columns: map[string]config.ColumnOverride{
					"is_vip": {Type: "Bool"},
					"status": {Type: "Enum8('new' = 1, 'paid' = 2)"},
				},
			},
		})},
		{"composite_key", compositeKey, cfgWith(nil)},
		{"quoted_identifiers", odd, &config.Config{Tables: config.Tables{
			TargetDatabase: "we`ird",
			Overrides:      map[string]config.TableOverride{"we`ird.ta`ble": {OrderBy: []string{"co l"}}},
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl, err := Resolve(tt.table, tt.cfg, nil)
			if err != nil {
				t.Fatalf("Resolve() = %v", err)
			}
			got := tbl.CreateStatement() + "\n"

			golden := filepath.Join("testdata", tt.name+".golden.sql")
			if *update {
				if err := os.WriteFile(golden, []byte(got), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Errorf("DDL mismatch (run go test ./internal/sink/clickhouse -update)\n--- got ---\n%s\n--- want ---\n%s", got, want)
			}
		})
	}
}

// TestTablesGoToTheDatabaseOfTheSameNameByDefault: shop.orders in MySQL is shop.orders in ClickHouse
// unless the configuration says otherwise, so tables of the same name in different databases
// never meet.
func TestTablesGoToTheDatabaseOfTheSameNameByDefault(t *testing.T) {
	cfg := &config.Config{} // no tables.target_database
	shop, err := Resolve(ordersTable(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	other := ordersTable()
	other.Database = "warehouse"
	warehouse, err := Resolve(other, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	if shop.FullName() != "`shop`.`orders`" || warehouse.FullName() != "`warehouse`.`orders`" {
		t.Errorf("targets = %s and %s, want each in a database named like its source", shop.FullName(), warehouse.FullName())
	}
}

func TestExplicitTargetDatabaseCollectsEverything(t *testing.T) {
	cfg := &config.Config{Tables: config.Tables{TargetDatabase: "analytics"}}
	tbl, err := Resolve(ordersTable(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tbl.FullName() != "`analytics`.`orders`" {
		t.Errorf("target = %s, want the configured database", tbl.FullName())
	}
}

// TestDatabaseMapRenamesOnlyTheMappedDatabase: shop is renamed to shop_mirror, warehouse (not in the
// map) keeps its own name, and an override for shop.orders is still found under the source name,
// not the mapped one.
func TestDatabaseMapRenamesOnlyTheMappedDatabase(t *testing.T) {
	cfg := &config.Config{Tables: config.Tables{
		DatabaseMap: map[string]string{"shop": "shop_mirror"},
		Overrides: map[string]config.TableOverride{
			"shop.orders": {TargetTable: "orders_v2"},
		},
	}}

	shop, err := Resolve(ordersTable(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if shop.FullName() != "`shop_mirror`.`orders_v2`" {
		t.Errorf("shop.orders target = %s, want shop_mirror.orders_v2", shop.FullName())
	}

	other := ordersTable()
	other.Database = "warehouse"
	warehouse, err := Resolve(other, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if warehouse.FullName() != "`warehouse`.`orders`" {
		t.Errorf("warehouse.orders target = %s, want its own name unchanged", warehouse.FullName())
	}
}

func TestResolveNamesAndColumns(t *testing.T) {
	tbl, err := Resolve(ordersTable(), cfgWith(map[string]config.TableOverride{
		"shop.orders": {TargetTable: "orders_v2", ExcludeColumns: []string{"email", "customer"}},
	}), nil)
	if err != nil {
		t.Fatal(err)
	}

	if tbl.FullName() != "`replica`.`orders_v2`" {
		t.Errorf("FullName() = %s", tbl.FullName())
	}
	var names []string
	for _, c := range tbl.Columns {
		names = append(names, c.Name)
	}
	if got := strings.Join(names, ","); got != "id,total,created_at,status,tags,is_vip" {
		t.Errorf("columns = %s", got)
	}
	// The source index survives exclusion: values are picked from the full row image.
	if tbl.Columns[1].Name != "total" || tbl.Columns[1].SourceIndex != 2 {
		t.Errorf("column total = %+v", tbl.Columns[1])
	}
	if strings.Join(tbl.OrderBy, ",") != "id" {
		t.Errorf("OrderBy = %v", tbl.OrderBy)
	}
}

// compositePK is shop.orders(a INT, b INT, c INT) with PRIMARY KEY (a, b).
func compositePK() *schema.Table {
	return &schema.Table{
		Database: "shop", Name: "orders",
		Columns: []typemap.Column{
			{Name: "a", Kind: typemap.KindInt},
			{Name: "b", Kind: typemap.KindInt},
			{Name: "c", Kind: typemap.KindInt},
		},
		PrimaryKey: []int{0, 1},
	}
}

func TestResolveOrderByMayExtendButNotShrinkThePrimaryKey(t *testing.T) {
	tests := []struct {
		name    string
		orderBy []string
	}{
		{"same order", []string{"a", "b"}},
		{"different order", []string{"b", "a"}},
		{"extended in front", []string{"c", "a", "b"}},
		{"extended at the end", []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl, err := Resolve(compositePK(), cfgWith(map[string]config.TableOverride{"shop.orders": {OrderBy: tt.orderBy}}), nil)
			if err != nil {
				t.Fatalf("Resolve() = %v", err)
			}
			if got := strings.Join(tbl.OrderBy, ","); got != strings.Join(tt.orderBy, ",") {
				t.Errorf("OrderBy = %s, want %v", got, tt.orderBy)
			}
		})
	}
}

func TestResolveOrderByOverride(t *testing.T) {
	tbl, err := Resolve(ordersTable(), cfgWith(map[string]config.TableOverride{
		"shop.orders": {OrderBy: []string{"created_at", "id"}},
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(tbl.OrderBy, ","); got != "created_at,id" {
		t.Errorf("OrderBy = %s", got)
	}
}

func TestResolveRejections(t *testing.T) {
	noPK := ordersTable()
	noPK.PrimaryKey = nil

	tests := []struct {
		name    string
		table   *schema.Table
		ov      config.TableOverride
		wantMsg string
	}{
		{"no primary key", noPK, config.TableOverride{}, "has no primary key: set tables.overrides.shop.orders.order_by"},
		{"exclude typo", ordersTable(), config.TableOverride{ExcludeColumns: []string{"emial"}}, `unknown column "emial"`},
		{"override typo", ordersTable(), config.TableOverride{Columns: map[string]config.ColumnOverride{"stauts": {Type: "String"}}}, `unknown column "stauts"`},
		{"excluded key column", ordersTable(), config.TableOverride{ExcludeColumns: []string{"id"}}, "excluded but is part of the sort key"},
		{"order by drops a primary key column", compositePK(), config.TableOverride{OrderBy: []string{"a"}}, `must include every primary key column, missing "b"`},
		{"order by replaces the primary key", ordersTable(), config.TableOverride{OrderBy: []string{"created_at"}}, `must include every primary key column, missing "id"`},
		{"order by typo", ordersTable(), config.TableOverride{OrderBy: []string{"id", "nope"}}, `order_by refers to unknown column "nope"`},
		{"nullable sort key", ordersTable(), config.TableOverride{OrderBy: []string{"id", "customer"}}, "does not allow NULL in the sort key"},
		{"nullable enum sort key", ordersTable(), config.TableOverride{OrderBy: []string{"id", "status"}}, "does not allow NULL in the sort key"},
		{
			"everything excluded", &schema.Table{Database: "shop", Name: "orders", Columns: []typemap.Column{{Name: "a", Kind: typemap.KindInt}}, PrimaryKey: []int{0}},
			config.TableOverride{ExcludeColumns: []string{"a"}},
			"every column is excluded",
		},
		{"incompatible type override", ordersTable(), config.TableOverride{Columns: map[string]config.ColumnOverride{"total": {Type: "String"}}}, "total"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Resolve(tt.table, cfgWith(map[string]config.TableOverride{"shop.orders": tt.ov}), nil)
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("Resolve() = %v, want error containing %q", err, tt.wantMsg)
			}
		})
	}
}

func TestResolveRejectsServiceColumnCollision(t *testing.T) {
	tbl := &schema.Table{
		Database: "shop", Name: "t",
		Columns:    []typemap.Column{{Name: "id", Kind: typemap.KindInt}, {Name: "_version", Kind: typemap.KindInt}},
		PrimaryKey: []int{0},
	}
	_, err := Resolve(tbl, cfgWith(nil), nil)
	if err == nil || !strings.Contains(err.Error(), "service column") {
		t.Fatalf("Resolve() = %v", err)
	}
}

func TestQuote(t *testing.T) {
	tests := map[string]string{
		"plain":      "`plain`",
		`back\slash`: "`back\\\\slash`",
		"with space": "`with space`",
		"we`ird":     "`we\\`ird`",
	}
	for in, want := range tests {
		if got := quote(in); got != want {
			t.Errorf("quote(%q) = %s, want %s", in, got, want)
		}
	}
}

// TestResolveSaysWhichProblemsBelongToTheTable: the problems of one table can be set aside with
// the table; the others are mistakes of the configuration, which no table can be blamed for.
func TestResolveSaysWhichProblemsBelongToTheTable(t *testing.T) {
	noPK := ordersTable()
	noPK.PrimaryKey = nil
	badName := ordersTable()
	badName.Columns = append(badName.Columns, typemap.Column{Name: "a,b", Kind: typemap.KindInt})
	service := ordersTable()
	service.Columns = append(service.Columns, typemap.Column{Name: "_version", Kind: typemap.KindInt})

	tests := []struct {
		name      string
		table     *schema.Table
		ov        config.TableOverride
		wantTable bool
	}{
		{"no key", noPK, config.TableOverride{}, true},
		{"a column name the client cannot insert", badName, config.TableOverride{}, true},
		{"a service column", service, config.TableOverride{}, true},
		{"exclude typo", ordersTable(), config.TableOverride{ExcludeColumns: []string{"emial"}}, false},
		{"order by typo", ordersTable(), config.TableOverride{OrderBy: []string{"id", "nope"}}, false},
		{"order by drops a key column", compositePK(), config.TableOverride{OrderBy: []string{"a"}}, false},
		{"incompatible type override", ordersTable(), config.TableOverride{Columns: map[string]config.ColumnOverride{"total": {Type: "String"}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Resolve(tt.table, cfgWith(map[string]config.TableOverride{"shop.orders": tt.ov}), nil)
			if err == nil {
				t.Fatal("Resolve() succeeded")
			}
			if got := IsTableProblem(err); got != tt.wantTable {
				t.Errorf("IsTableProblem(%v) = %v, want %v", err, got, tt.wantTable)
			}
		})
	}
}
