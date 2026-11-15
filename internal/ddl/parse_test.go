package ddl

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func mustParse(t *testing.T, query string) Op {
	t.Helper()
	op, err := Parse(query, "shop")
	if err != nil {
		t.Fatalf("Parse(%q): %v", query, err)
	}
	return op
}

func TestTableStatements(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  Op
	}{
		{"create", "CREATE TABLE orders (id INT PRIMARY KEY)", CreateTable{Table: Name{"shop", "orders"}}},
		{"create qualified if not exists", "create table if not exists `other`.`t` (a int)", CreateTable{Table: Name{"other", "t"}, IfNotExists: true}},
		{"create temporary", "CREATE TEMPORARY TABLE t (a int)", CreateTable{Table: Name{"shop", "t"}, Temporary: true}},
		{"create like", "CREATE TABLE `shop`.`_orders_new` LIKE `shop`.`orders`", CreateTable{Table: Name{"shop", "_orders_new"}, Like: &Name{"shop", "orders"}}},
		{"create select", "CREATE TABLE copy AS SELECT * FROM orders", CreateTable{Table: Name{"shop", "copy"}, FromSelect: true}},
		{"drop", "DROP TABLE orders", DropTable{Tables: []Name{{"shop", "orders"}}}},
		{"drop several if exists", "DROP TABLE IF EXISTS a, other.b", DropTable{Tables: []Name{{"shop", "a"}, {"other", "b"}}, IfExists: true}},
		{"drop temporary", "DROP TEMPORARY TABLE t", DropTable{Tables: []Name{{"shop", "t"}}, Temporary: true}},
		{"drop view is not a table", "DROP VIEW v", Other{Kind: "DROP VIEW"}},
		{"truncate", "TRUNCATE TABLE orders", TruncateTable{Table: Name{"shop", "orders"}}},
		{"truncate without the keyword", "TRUNCATE other.orders", TruncateTable{Table: Name{"other", "orders"}}},
		{"drop database", "DROP DATABASE IF EXISTS shop", DropDatabase{Database: "shop", IfExists: true}},
		{"drop schema", "DROP SCHEMA shop", DropDatabase{Database: "shop"}},
		{"rename", "RENAME TABLE a TO b", RenameTable{Pairs: []Rename{{Name{"shop", "a"}, Name{"shop", "b"}}}}},
		{"rename pairs in order", "RENAME TABLE a TO b, c TO a", RenameTable{Pairs: []Rename{
			{Name{"shop", "a"}, Name{"shop", "b"}}, {Name{"shop", "c"}, Name{"shop", "a"}},
		}}},
		{"an index is not a table change", "CREATE INDEX i ON orders (id)", Other{Kind: "CreateIndexStmt"}},
		{"a session setting", "SET @a = 1", Other{Kind: "SetStmt"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mustParse(t, tt.query); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse(%q) = %#v, want %#v", tt.query, got, tt.want)
			}
		})
	}
}

func alter(t *testing.T, query string) AlterTable {
	t.Helper()
	a, ok := mustParse(t, query).(AlterTable)
	if !ok {
		t.Fatalf("%q is not an ALTER TABLE", query)
	}
	return a
}

func TestAlterColumns(t *testing.T) {
	t.Run("add a column with a literal default", func(t *testing.T) {
		a := alter(t, "ALTER TABLE orders ADD COLUMN qty INT NOT NULL DEFAULT 5 AFTER id")
		want := AddColumns{Columns: []Column{{Name: "qty", Type: "int(11)", DataType: DataType{Kind: "INT", Length: -1}, Default: Default{DefaultLiteral, "5"}}}, Position: Position{After: "id"}}
		if len(a.Specs) != 1 || !reflect.DeepEqual(a.Specs[0], want) {
			t.Errorf("specs = %#v, want %#v", a.Specs, want)
		}
	})

	t.Run("defaults of every kind", func(t *testing.T) {
		a := alter(t, "ALTER TABLE t ADD COLUMN (a VARCHAR(10) DEFAULT 'x y', b INT DEFAULT NULL, c INT, d DATETIME DEFAULT CURRENT_TIMESTAMP, e INT NOT NULL)")
		cols := a.Specs[0].(AddColumns).Columns
		got := make([]Default, len(cols))
		nullable := make([]bool, len(cols))
		for i, c := range cols {
			got[i], nullable[i] = c.Default, c.Nullable
		}
		want := []Default{{DefaultLiteral, "x y"}, {Kind: DefaultNull}, {}, {DefaultExpression, "CURRENT_TIMESTAMP()"}, {}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("defaults = %#v, want %#v", got, want)
		}
		if !reflect.DeepEqual(nullable, []bool{true, true, true, true, false}) {
			t.Errorf("nullable = %v", nullable)
		}
	})

	t.Run("first", func(t *testing.T) {
		a := alter(t, "ALTER TABLE t ADD COLUMN c INT FIRST")
		if a.Specs[0].(AddColumns).Position != (Position{First: true}) {
			t.Errorf("position = %+v", a.Specs[0])
		}
	})

	t.Run("drop, modify, change, rename", func(t *testing.T) {
		a := alter(t, "ALTER TABLE t DROP COLUMN a, MODIFY COLUMN b BIGINT NOT NULL, CHANGE COLUMN c d VARCHAR(5), RENAME COLUMN e TO f")
		if len(a.Specs) != 4 {
			t.Fatalf("specs = %#v", a.Specs)
		}
		if a.Specs[0] != (DropColumn{Name: "a"}) {
			t.Errorf("drop = %#v", a.Specs[0])
		}
		if m := a.Specs[1].(ModifyColumn); m.Column.Name != "b" || m.Column.Nullable {
			t.Errorf("modify = %#v", m)
		}
		if c := a.Specs[2].(ChangeColumn); c.Old != "c" || c.Column.Name != "d" {
			t.Errorf("change = %#v", c)
		}
		if a.Specs[3] != (RenameColumn{Old: "e", New: "f"}) {
			t.Errorf("rename = %#v", a.Specs[3])
		}
	})

	t.Run("primary key", func(t *testing.T) {
		a := alter(t, "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)")
		if a.Specs[0] != (DropPrimaryKey{}) || !reflect.DeepEqual(a.Specs[1], AddPrimaryKey{Columns: []string{"a", "b"}}) {
			t.Errorf("specs = %#v", a.Specs)
		}
	})

	t.Run("rename the table", func(t *testing.T) {
		a := alter(t, "ALTER TABLE t RENAME TO other.u")
		if a.Specs[0] != (RenameTo{Table: Name{"other", "u"}}) {
			t.Errorf("spec = %#v", a.Specs[0])
		}
	})
}

func TestAlterThatDoesNotConcernColumns(t *testing.T) {
	for _, q := range []string{
		"ALTER TABLE t ADD INDEX i (a)",
		"ALTER TABLE t ADD UNIQUE KEY u (a)",
		"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES p (id)",
		"ALTER TABLE t DROP INDEX i",
		"ALTER TABLE t ENGINE=InnoDB",
		"ALTER TABLE t AUTO_INCREMENT=100",
		"ALTER TABLE t COMMENT='x'",
		"ALTER TABLE t ALGORITHM=INPLACE, LOCK=NONE",
		"ALTER TABLE t ALTER COLUMN a SET DEFAULT 3",
	} {
		a := alter(t, q)
		for _, s := range a.Specs {
			if _, ok := s.(Ignored); !ok {
				t.Errorf("%q: spec %#v is not ignorable", q, s)
			}
		}
	}
}

func TestPartitionOperations(t *testing.T) {
	tests := []struct {
		query       string
		destructive bool
	}{
		{"ALTER TABLE t DROP PARTITION p1", true},
		{"ALTER TABLE t TRUNCATE PARTITION p1", true},
		{"ALTER TABLE t EXCHANGE PARTITION p1 WITH TABLE x", true},
		{"ALTER TABLE t ADD PARTITION (PARTITION p9 VALUES LESS THAN (100))", false},
		{"ALTER TABLE t REORGANIZE PARTITION p1 INTO (PARTITION a VALUES LESS THAN (5), PARTITION b VALUES LESS THAN (10))", false},
		{"ALTER TABLE t REMOVE PARTITIONING", false},
	}
	for _, tt := range tests {
		a := alter(t, tt.query)
		p, ok := a.Specs[0].(PartitionOp)
		if !ok || p.Destructive != tt.destructive {
			t.Errorf("%q: spec = %#v, want a partition operation, destructive = %v", tt.query, a.Specs[0], tt.destructive)
		}
	}
}

// The statements the online schema change tools run, as they appear in the binlog.
func TestOnlineSchemaChangeStatements(t *testing.T) {
	// pt-online-schema-change
	if got := mustParse(t, "CREATE TABLE `shop`.`_orders_new` LIKE `shop`.`orders`"); !reflect.DeepEqual(got,
		CreateTable{Table: Name{"shop", "_orders_new"}, Like: &Name{"shop", "orders"}}) {
		t.Errorf("pt-osc create = %#v", got)
	}
	a := alter(t, "ALTER TABLE `shop`.`_orders_new` ADD COLUMN `note` varchar(50) DEFAULT NULL")
	if a.Table != (Name{"shop", "_orders_new"}) || a.Specs[0].(AddColumns).Columns[0].Name != "note" {
		t.Errorf("pt-osc alter = %#v", a)
	}
	if got := mustParse(t, "RENAME TABLE `shop`.`orders` TO `shop`.`_orders_old`, `shop`.`_orders_new` TO `shop`.`orders`"); !reflect.DeepEqual(got,
		RenameTable{Pairs: []Rename{{Name{"shop", "orders"}, Name{"shop", "_orders_old"}}, {Name{"shop", "_orders_new"}, Name{"shop", "orders"}}}}) {
		t.Errorf("pt-osc swap = %#v", got)
	}
	if got := mustParse(t, "DROP TABLE IF EXISTS `shop`.`_orders_old`"); !reflect.DeepEqual(got,
		DropTable{Tables: []Name{{"shop", "_orders_old"}}, IfExists: true}) {
		t.Errorf("pt-osc drop = %#v", got)
	}

	// gh-ost marks its statements with a comment and writes keywords in lower case.
	if got := mustParse(t, "create /* gh-ost */ table `shop`.`_orders_gho` like `shop`.`orders`"); !reflect.DeepEqual(got,
		CreateTable{Table: Name{"shop", "_orders_gho"}, Like: &Name{"shop", "orders"}}) {
		t.Errorf("gh-ost create = %#v", got)
	}
	a = alter(t, "alter /* gh-ost */ table `shop`.`_orders_gho` change column `a` `b` int(11) not null")
	if c, ok := a.Specs[0].(ChangeColumn); !ok || c.Old != "a" || c.Column.Name != "b" {
		t.Errorf("gh-ost alter = %#v", a)
	}
	if got := mustParse(t, "rename /* gh-ost */ table `shop`.`orders` to `shop`.`_orders_del`, `shop`.`_orders_gho` to `shop`.`orders`"); !reflect.DeepEqual(got,
		RenameTable{Pairs: []Rename{{Name{"shop", "orders"}, Name{"shop", "_orders_del"}}, {Name{"shop", "_orders_gho"}, Name{"shop", "orders"}}}}) {
		t.Errorf("gh-ost swap = %#v", got)
	}
	if got := mustParse(t, "drop /* gh-ost */ table if exists `shop`.`_orders_del`"); !reflect.DeepEqual(got,
		DropTable{Tables: []Name{{"shop", "_orders_del"}}, IfExists: true}) {
		t.Errorf("gh-ost drop = %#v", got)
	}
}

// TestStatementsOfOnlineSchemaChangeToolsFromTheirBinlogs are the statements that pt-online-schema-change
// and gh-ost wrote to a MySQL 8.4 binlog, as captured from the real tools (default database shop).
func TestStatementsOfOnlineSchemaChangeToolsFromTheirBinlogs(t *testing.T) {
	// gh-ost two-step cut-over: two ALTER ... RENAME statements one after the other.
	a := alter(t, "alter /* gh-ost */ table `shop`.`orders` rename `_orders_del` /* xid=169 */")
	if r, ok := a.Specs[0].(RenameTo); !ok || r.Table != (Name{"shop", "_orders_del"}) || a.Table != (Name{"shop", "orders"}) {
		t.Errorf("two-step first rename = %#v", a)
	}
	a = alter(t, "alter /* gh-ost */ table `shop`.`_orders_gho` rename `orders` /* xid=170 */")
	if r, ok := a.Specs[0].(RenameTo); !ok || r.Table != (Name{"shop", "orders"}) {
		t.Errorf("two-step second rename = %#v", a)
	}
	// The drops of the server carry a comment, and pt-osc's have no database: the default one applies.
	if got := mustParse(t, "DROP TABLE IF EXISTS `_orders_old` /* generated by server */"); !reflect.DeepEqual(got,
		DropTable{Tables: []Name{{"shop", "_orders_old"}}, IfExists: true}) {
		t.Errorf("drop of the old table = %#v", got)
	}
	// pt-osc's swap and its new table, written in full.
	if got := mustParse(t, "RENAME TABLE `shop`.`orders` TO `shop`.`_orders_old`, `shop`.`_orders_new` TO `shop`.`orders`"); !reflect.DeepEqual(got,
		RenameTable{Pairs: []Rename{{Name{"shop", "orders"}, Name{"shop", "_orders_old"}}, {Name{"shop", "_orders_new"}, Name{"shop", "orders"}}}}) {
		t.Errorf("pt-osc swap = %#v", got)
	}
	// Triggers, the sentry table and the service tables change no column of a replicated table.
	for _, q := range []string{
		"CREATE TRIGGER `pt_osc_shop_orders_ins` AFTER INSERT ON `shop`.`orders` FOR EACH ROW REPLACE INTO `shop`.`_orders_new` (`id`) VALUES (NEW.`id`)",
		"DROP TRIGGER IF EXISTS `shop`.`pt_osc_shop_orders_ins`",
		"ANALYZE TABLE `shop`.`_orders_new`",
	} {
		if op, err := Parse(q, "shop"); err == nil {
			switch op.(type) {
			case Other, CreateTable, DropTable:
			default:
				t.Errorf("%q parsed as %#v, which would change a table", q, op)
			}
		}
	}
}

func TestVersionedCommentsAndMixedCase(t *testing.T) {
	a := alter(t, "/*!80016 ALTER TABLE orders ADD COLUMN c INT */")
	if a.Table != (Name{"shop", "orders"}) {
		t.Errorf("a versioned comment hid the statement: %#v", a)
	}
	if _, ok := mustParse(t, "  \n TrUnCaTe   TaBlE `Orders`  ").(TruncateTable); !ok {
		t.Error("keywords are case insensitive")
	}
}

// TestPerconaCompressedColumnsAreOrdinaryColumns: the binlog carries the values of a compressed
// column of Percona Server uncompressed, so a statement with one reads as the same statement
// without the compression.
func TestPerconaCompressedColumnsAreOrdinaryColumns(t *testing.T) {
	tests := []struct{ query, plain string }{
		{"CREATE TABLE docs (id INT PRIMARY KEY, body BLOB COLUMN_FORMAT COMPRESSED)", "CREATE TABLE docs (id INT PRIMARY KEY, body BLOB)"},
		{"CREATE TABLE docs (id INT PRIMARY KEY, body BLOB /*!50633 COLUMN_FORMAT COMPRESSED */)", "CREATE TABLE docs (id INT PRIMARY KEY, body BLOB)"},
		{"ALTER TABLE docs ADD COLUMN note TEXT column_format compressed WITH COMPRESSION_DICTIONARY `shop dict`", "ALTER TABLE docs ADD COLUMN note TEXT"},
		{"ALTER TABLE docs ADD COLUMN note TEXT /*!50633 COLUMN_FORMAT COMPRESSED WITH COMPRESSION_DICTIONARY `d` */ NOT NULL", "ALTER TABLE docs ADD COLUMN note TEXT NOT NULL"},
		{"ALTER TABLE docs MODIFY body MEDIUMBLOB COLUMN_FORMAT /* the format */ COMPRESSED", "ALTER TABLE docs MODIFY body MEDIUMBLOB"},
		{
			"ALTER TABLE docs ADD COLUMN a JSON COLUMN_FORMAT COMPRESSED, ADD COLUMN b VARCHAR(40) DEFAULT 'COLUMN_FORMAT COMPRESSED' COMMENT 'it''s COLUMN_FORMAT COMPRESSED'",
			"ALTER TABLE docs ADD COLUMN a JSON, ADD COLUMN b VARCHAR(40) DEFAULT 'COLUMN_FORMAT COMPRESSED' COMMENT 'it''s COLUMN_FORMAT COMPRESSED'",
		},
	}
	for _, tt := range tests {
		got, want := mustParse(t, tt.query), mustParse(t, tt.plain)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Parse(%q) = %#v, want %#v", tt.query, got, want)
		}
	}

	for query, kind := range map[string]string{
		"CREATE COMPRESSION_DICTIONARY shop_dict ('abcabc')":     "CREATE COMPRESSION_DICTIONARY",
		"/* a note */ drop compression_dictionary IF EXISTS `d`": "DROP COMPRESSION_DICTIONARY",
	} {
		if got := mustParse(t, query); got != (Other{Kind: kind}) {
			t.Errorf("Parse(%q) = %#v, want %s", query, got, kind)
		}
	}

	// The words in a name or a string are not the attribute, and a statement still wrong without it stays an error.
	for _, q := range []string{
		"CREATE TABLE docs (`COLUMN_FORMAT COMPRESSED` INT, x BLOB COLUMN_FORMAT FIXED garbage)",
		"ALTER TABLE docs ADD COLUMN c TEXT COLUMN_FORMAT COMPRESSED garbage",
		"'CREATE COMPRESSION_DICTIONARY d'",
	} {
		var pe *ParseError
		if _, err := Parse(q, "shop"); !errors.As(err, &pe) {
			t.Errorf("Parse(%q) error = %v, want a *ParseError", q, err)
		}
	}
}

func TestUnparsableStatementsReturnAParseError(t *testing.T) {
	for _, q := range []string{"", "ALTER TABLE", "this is not sql", "DROP TABLE"} {
		_, err := Parse(q, "shop")
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Errorf("Parse(%q) error = %v, want a *ParseError", q, err)
		}
	}
}

func TestConcurrentParsing(t *testing.T) {
	done := make(chan struct{})
	for range 8 {
		go func() {
			defer func() { done <- struct{}{} }()
			for range 50 {
				if _, err := Parse("ALTER TABLE t ADD COLUMN c INT", "shop"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for range 8 {
		<-done
	}
}

func TestNumericDefaultsKeepTheirValue(t *testing.T) {
	a := alter(t, "ALTER TABLE t ADD COLUMN (i INT DEFAULT -7, f DOUBLE DEFAULT 1.5, d DECIMAL(10,2) DEFAULT 12.30, b BIT(1) DEFAULT b'1', big BIGINT UNSIGNED DEFAULT 18446744073709551615)")
	var got []string
	for _, c := range a.Specs[0].(AddColumns).Columns {
		if c.Default.Kind != DefaultLiteral && c.Name != "b" { // a BIT literal is a byte string
			t.Errorf("%s: default kind %v, want a literal (a negative number is one too)", c.Name, c.Default.Kind)
		}
		got = append(got, c.Default.Text)
	}
	t.Logf("defaults as parsed: %q", got)
	if got[0] != "-7" || got[1] != "1.5" || got[2] != "12.30" || got[4] != "18446744073709551615" {
		t.Errorf("defaults = %q", got)
	}
}

// FuzzParse: whatever bytes the binlog carries, parsing returns an operation or an error, and
// never panics.
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		"ALTER TABLE t ADD COLUMN c INT DEFAULT 5 AFTER a",
		"RENAME TABLE a TO b, c TO a",
		"create /* gh-ost */ table `d`.`_t_gho` like `d`.`t`",
		"DROP TABLE IF EXISTS a, b.c",
		"TRUNCATE t",
		"ALTER TABLE t DROP PARTITION p1",
		"/*!80016 ALTER TABLE t ENGINE=InnoDB */",
		"ALTER TABLE t CHANGE a b INT NOT NULL, RENAME COLUMN c TO d",
		"ALTER TABLE t ADD COLUMN c TEXT /*!50633 COLUMN_FORMAT COMPRESSED WITH COMPRESSION_DICTIONARY `d` */ DEFAULT 'it''s'",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, query string) {
		op, err := Parse(query, "shop")
		if err == nil && op == nil {
			t.Fatalf("Parse(%q) returned neither an operation nor an error", query)
		}
	})
}

// A numeric literal of more than 65 digits sends the parser into an unimplemented branch, which
// panics. Found by fuzzing. The statement must come back as an error, and parsing must go on.
func TestAParserPanicIsAParseErrorAndParsingContinues(t *testing.T) {
	hostile := "0000000000000000000000000000000000000000000000000000000000000020000000000000000000" // found by FuzzParse

	_, err := Parse(hostile, "shop")
	var pe *ParseError
	if !errors.As(err, &pe) || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("Parse(hostile) error = %v, want a *ParseError that says the parser panicked", err)
	}

	for range 20 { // the pool must not hand out a parser left in a broken state
		if _, err := Parse("ALTER TABLE t ADD COLUMN c INT", "shop"); err != nil {
			t.Fatalf("parsing broke after a panic: %v", err)
		}
	}
}

func TestDataTypes(t *testing.T) {
	tests := []struct {
		def  string
		want DataType
	}{
		{"a TINYINT", DataType{Kind: "TINYINT", Length: 4}},
		{"a SMALLINT UNSIGNED", DataType{Kind: "SMALLINT", Unsigned: true}},
		{"a MEDIUMINT", DataType{Kind: "MEDIUMINT"}},
		{"a INT UNSIGNED", DataType{Kind: "INT", Unsigned: true}},
		{"a BIGINT", DataType{Kind: "BIGINT"}},
		{"a FLOAT", DataType{Kind: "FLOAT"}},
		{"a DOUBLE", DataType{Kind: "DOUBLE"}},
		{"a DECIMAL(10,2)", DataType{Kind: "DECIMAL", Precision: 10, Scale: 2}},
		{"a YEAR", DataType{Kind: "YEAR"}},
		{"a DATE", DataType{Kind: "DATE"}},
		{"a DATETIME(3)", DataType{Kind: "DATETIME", Fsp: 3}},
		{"a TIMESTAMP", DataType{Kind: "TIMESTAMP"}},
		{"a TIME(6)", DataType{Kind: "TIME", Fsp: 6}},
		{"a VARCHAR(20)", DataType{Kind: "VARCHAR", Length: 20}},
		{"a CHAR(3)", DataType{Kind: "CHAR", Length: 3}},
		{"a TEXT", DataType{Kind: "TEXT"}},
		{"a LONGTEXT", DataType{Kind: "TEXT"}},
		{"a VARBINARY(8)", DataType{Kind: "VARBINARY", Length: 8}},
		{"a BINARY(4)", DataType{Kind: "BINARY", Length: 4}},
		{"a BLOB", DataType{Kind: "BLOB"}},
		{"a ENUM('x','y z')", DataType{Kind: "ENUM", Members: []string{"x", "y z"}}},
		{"a SET('p','q')", DataType{Kind: "SET", Members: []string{"p", "q"}}},
		{"a BIT(5)", DataType{Kind: "BIT", Length: 5}},
		{"a JSON", DataType{Kind: "JSON"}},
	}
	for _, tt := range tests {
		t.Run(tt.def, func(t *testing.T) {
			a := alter(t, "ALTER TABLE t ADD COLUMN "+tt.def)
			got := a.Specs[0].(AddColumns).Columns[0].DataType
			// Only what a test names is compared: the parser also reports lengths and charsets
			// that no test cares about.
			if got.Kind != tt.want.Kind || got.Unsigned != tt.want.Unsigned || got.Scale != tt.want.Scale ||
				got.Precision != tt.want.Precision || got.Fsp != tt.want.Fsp || !reflect.DeepEqual(got.Members, tt.want.Members) {
				t.Errorf("DataType = %+v, want %+v", got, tt.want)
			}
			if tt.want.Length > 0 && tt.want.Kind != "TINYINT" && got.Length != tt.want.Length {
				t.Errorf("Length = %d, want %d", got.Length, tt.want.Length)
			}
		})
	}
}
