package pipeline

import (
	"reflect"
	"testing"

	"github.com/thealexjo/binwave/internal/ddl"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantKind   string
		wantTables []string
		wantAny    bool
	}{
		{"drop table", "DROP TABLE orders", "DROP TABLE", []string{"shop.orders"}, false},
		{"drop table qualified", "drop table `warehouse`.`invoices`", "DROP TABLE", []string{"warehouse.invoices"}, false},
		{"drop table if exists list", "DROP TABLE IF EXISTS a, b.c, `d`", "DROP TABLE", []string{"shop.a", "b.c", "shop.d"}, false},
		{"drop temporary", "DROP TEMPORARY TABLE tmp1", "DROP TABLE", []string{"shop.tmp1"}, false},
		{"drop with cascade", "DROP TABLE t CASCADE", "DROP TABLE", []string{"shop.t"}, false},
		{"trailing semicolon and space", "DROP TABLE t ;  ", "DROP TABLE", []string{"shop.t"}, false},
		{"leading comment", "/* app:migrate */ DROP TABLE t", "DROP TABLE", []string{"shop.t"}, false},
		{"truncate", "TRUNCATE TABLE orders", "TRUNCATE", []string{"shop.orders"}, false},
		{"truncate without table keyword", "truncate orders", "TRUNCATE", []string{"shop.orders"}, false},
		{"drop database", "DROP DATABASE shop", "DROP DATABASE", nil, false},
		{"drop schema", "drop schema if exists `shop`", "DROP DATABASE", nil, false},
		{"drop partition", "ALTER TABLE orders DROP PARTITION p2023", "partition operation", []string{"shop.orders"}, false},
		{"truncate partition", "ALTER TABLE `shop`.`orders` TRUNCATE PARTITION p1, p2", "partition operation", []string{"shop.orders"}, false},
		{"exchange partition", "alter table orders exchange partition p1 with table staging", "partition operation", []string{"shop.orders"}, false},
		{"multi line", "ALTER TABLE orders\n  DROP PARTITION p1", "partition operation", []string{"shop.orders"}, false},
		{"unparseable name is treated as any", "DROP TABLE weird!name", "DROP TABLE", nil, true},
		// The parser rejects these, so the keyword scan still has to see them.
		{"dialect the parser rejects, drop", "DROP TABLE orders WAIT 5", "DROP TABLE", nil, true},
		{"dialect the parser rejects, truncate", "TRUNCATE TABLE orders WAIT 5", "TRUNCATE", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := classify(tt.query, "shop")
			if !ok {
				t.Fatalf("classify(%q) did not recognize a destructive statement", tt.query)
			}
			if got.kind == "DROP DATABASE" && got.database != "shop" {
				t.Errorf("classify(%q) database = %q, want shop", tt.query, got.database)
			}
			if got.kind != tt.wantKind || got.any != tt.wantAny || !reflect.DeepEqual(got.tables, tt.wantTables) {
				t.Errorf("classify(%q) = %+v, want kind %q tables %v any %v", tt.query, got, tt.wantKind, tt.wantTables, tt.wantAny)
			}
		})
	}
}

func TestTheFallbackIsReachedOnlyForStatementsTheParserRejects(t *testing.T) {
	for _, q := range []string{"DROP TABLE orders WAIT 5", "TRUNCATE TABLE orders WAIT 5"} {
		if _, err := ddl.Parse(q, "shop"); err == nil {
			t.Errorf("the parser accepts %q now: the fallback tests above no longer exercise the fallback", q)
		}
	}
	// Statements the parser understands never go through the keyword scan.
	if got, _ := classify("DROP TABLE a, b", "shop"); got.any {
		t.Errorf("a parsed statement was classified as any: %+v", got)
	}
}

func TestClassifyIgnoresHarmlessStatements(t *testing.T) {
	for _, q := range []string{
		"CREATE TABLE t (a INT)",
		"ALTER TABLE orders ADD COLUMN x INT",
		"ALTER TABLE orders ADD PARTITION (PARTITION p3 VALUES LESS THAN (2030))",
		"ALTER TABLE orders REORGANIZE PARTITION p1 INTO (PARTITION p1 VALUES LESS THAN (10))",
		"ALTER TABLE orders REMOVE PARTITIONING",
		"RENAME TABLE a TO b",
		"CREATE INDEX i ON t (a)",
		"ALTER TABLE t DROP COLUMN c",
		"ALTER TABLE t DROP INDEX i",
		"CREATE TABLE `shop`.`_orders_new` LIKE `shop`.`orders`",
		"ALTER TABLE `shop`.`_orders_new` ADD COLUMN c INT",
		"DROP VIEW v",
		"INSERT INTO t VALUES (1)",
		"",
	} {
		if got, ok := classify(q, "shop"); ok {
			t.Errorf("classify(%q) = %+v, want no match", q, got)
		}
	}
}
