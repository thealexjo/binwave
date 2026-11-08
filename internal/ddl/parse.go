package ddl

import (
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // the parser needs a value driver to build literals
	"github.com/pingcap/tidb/pkg/parser/types"
)

// parsers pools parsers: one is not safe for concurrent use, and making one is not free.
var parsers = sync.Pool{New: func() any { return parser.New() }}

// Parse parses one statement of the binlog. Names written without a database are qualified with
// defaultDB, the database the statement was executed in. A statement the parser rejects returns
// a *ParseError; statements that do not concern tables are returned as Other.
func Parse(query, defaultDB string) (op Op, err error) {
	p := parsers.Get().(*parser.Parser)
	// The parser panics on some input: a numeric literal of more than 65 digits reaches an
	// unimplemented branch of its test value driver. A statement of the binlog must never bring
	// the service down, so a panic is a parse error, and a parser that panicked is not reused.
	healthy := true
	defer func() {
		if r := recover(); r != nil {
			healthy = false
			op, err = nil, &ParseError{Query: query, Err: fmt.Errorf("the parser panicked: %v", r)}
		}
		if healthy {
			parsers.Put(p)
		}
	}()

	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, &ParseError{Query: query, Err: err}
	}
	q := qualifier{db: defaultDB}

	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		op := CreateTable{Table: q.name(s.Table), IfNotExists: s.IfNotExists, Temporary: s.TemporaryKeyword != ast.TemporaryNone, FromSelect: s.Select != nil}
		if s.ReferTable != nil {
			like := q.name(s.ReferTable)
			op.Like = &like
		}
		return op, nil
	case *ast.DropTableStmt:
		if s.IsView {
			return Other{Kind: "DROP VIEW"}, nil
		}
		op := DropTable{IfExists: s.IfExists, Temporary: s.TemporaryKeyword != ast.TemporaryNone}
		for _, t := range s.Tables {
			op.Tables = append(op.Tables, q.name(t))
		}
		return op, nil
	case *ast.TruncateTableStmt:
		return TruncateTable{Table: q.name(s.Table)}, nil
	case *ast.DropDatabaseStmt:
		return DropDatabase{Database: s.Name.O, IfExists: s.IfExists}, nil
	case *ast.RenameTableStmt:
		op := RenameTable{}
		for _, pair := range s.TableToTables {
			op.Pairs = append(op.Pairs, Rename{From: q.name(pair.OldTable), To: q.name(pair.NewTable)})
		}
		return op, nil
	case *ast.AlterTableStmt:
		op := AlterTable{Table: q.name(s.Table)}
		for _, spec := range s.Specs {
			op.Specs = append(op.Specs, q.spec(spec))
		}
		return op, nil
	default:
		return Other{Kind: strings.TrimPrefix(fmt.Sprintf("%T", stmt), "*ast.")}, nil
	}
}

type qualifier struct{ db string }

func (q qualifier) name(t *ast.TableName) Name {
	db := t.Schema.O
	if db == "" {
		db = q.db
	}
	return Name{Database: db, Table: t.Name.O}
}

func (q qualifier) spec(s *ast.AlterTableSpec) Spec {
	switch s.Tp {
	case ast.AlterTableAddColumns:
		return AddColumns{Columns: columns(s.NewColumns), Position: position(s.Position)}
	case ast.AlterTableDropColumn:
		return DropColumn{Name: s.OldColumnName.Name.O}
	case ast.AlterTableModifyColumn:
		return ModifyColumn{Column: column(s.NewColumns[0]), Position: position(s.Position)}
	case ast.AlterTableChangeColumn:
		return ChangeColumn{Old: s.OldColumnName.Name.O, Column: column(s.NewColumns[0]), Position: position(s.Position)}
	case ast.AlterTableRenameColumn:
		return RenameColumn{Old: s.OldColumnName.Name.O, New: s.NewColumnName.Name.O}
	case ast.AlterTableRenameTable:
		return RenameTo{Table: q.name(s.NewTable)}
	case ast.AlterTableDropPrimaryKey:
		return DropPrimaryKey{}
	case ast.AlterTableAddConstraint:
		if s.Constraint != nil && s.Constraint.Tp == ast.ConstraintPrimaryKey {
			var cols []string
			for _, k := range s.Constraint.Keys {
				if k.Column != nil {
					cols = append(cols, k.Column.Name.O)
				}
			}
			return AddPrimaryKey{Columns: cols}
		}
		return Ignored{What: "ADD CONSTRAINT or INDEX"}

	case ast.AlterTableDropPartition:
		return PartitionOp{Kind: "DROP PARTITION", Destructive: true}
	case ast.AlterTableTruncatePartition:
		return PartitionOp{Kind: "TRUNCATE PARTITION", Destructive: true}
	case ast.AlterTableExchangePartition:
		return PartitionOp{Kind: "EXCHANGE PARTITION", Destructive: true}
	case ast.AlterTableAddPartitions, ast.AlterTableCoalescePartitions, ast.AlterTablePartition,
		ast.AlterTableReorganizePartition, ast.AlterTableRemovePartitioning, ast.AlterTableRebuildPartition,
		ast.AlterTableOptimizePartition, ast.AlterTableCheckPartitions, ast.AlterTableRepairPartition:
		return PartitionOp{Kind: "partition change"}

	case ast.AlterTableOption, ast.AlterTableDropIndex, ast.AlterTableDropForeignKey, ast.AlterTableRenameIndex,
		ast.AlterTableLock, ast.AlterTableAlgorithm, ast.AlterTableForce, ast.AlterTableEnableKeys,
		ast.AlterTableDisableKeys, ast.AlterTableAlterColumn, ast.AlterTableIndexInvisible,
		ast.AlterTableAlterCheck, ast.AlterTableDropCheck, ast.AlterTableOrderByColumns,
		ast.AlterTableWithValidation, ast.AlterTableWithoutValidation:
		return Ignored{What: "table or index option"}
	default:
		return Unsupported{What: fmt.Sprintf("ALTER TABLE specification %d", int(s.Tp))}
	}
}

func position(p *ast.ColumnPosition) Position {
	if p == nil {
		return Position{}
	}
	switch p.Tp {
	case ast.ColumnPositionFirst:
		return Position{First: true}
	case ast.ColumnPositionAfter:
		return Position{After: p.RelativeColumn.Name.O}
	default:
		return Position{}
	}
}

func columns(defs []*ast.ColumnDef) []Column {
	out := make([]Column, len(defs))
	for i, d := range defs {
		out[i] = column(d)
	}
	return out
}

func column(d *ast.ColumnDef) Column {
	c := Column{Name: d.Name.Name.O, Nullable: true}
	if d.Tp != nil {
		c.Type = d.Tp.String()
		c.DataType = dataType(d.Tp)
	}
	for _, o := range d.Options {
		switch o.Tp {
		case ast.ColumnOptionNotNull:
			c.Nullable = false
		case ast.ColumnOptionNull:
			c.Nullable = true
		case ast.ColumnOptionPrimaryKey:
			c.PrimaryKey = true
			c.Nullable = false
		case ast.ColumnOptionAutoIncrement:
			c.AutoIncrement = true
		case ast.ColumnOptionGenerated:
			c.Generated = true
		case ast.ColumnOptionDefaultValue:
			c.Default = defaultOf(o.Expr)
		}
	}
	return c
}

func defaultOf(e ast.ExprNode) Default {
	// A negative number is a unary minus on a literal, not a literal of its own.
	if u, ok := e.(*ast.UnaryOperationExpr); ok && u.Op == opcode.Minus {
		if inner := defaultOf(u.V); inner.Kind == DefaultLiteral && !strings.HasPrefix(inner.Text, "-") {
			return Default{Kind: DefaultLiteral, Text: "-" + inner.Text}
		}
	}
	if v, ok := e.(ast.ValueExpr); ok {
		switch val := v.GetValue().(type) {
		case nil:
			return Default{Kind: DefaultNull}
		case string:
			return Default{Kind: DefaultLiteral, Text: val}
		case []byte:
			return Default{Kind: DefaultLiteral, Text: string(val)}
		default:
			// Numbers: integers, floats and decimals print as MySQL wrote them.
			return Default{Kind: DefaultLiteral, Text: fmt.Sprint(val)}
		}
	}
	var sb strings.Builder
	if err := e.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return Default{Kind: DefaultExpression}
	}
	return Default{Kind: DefaultExpression, Text: sb.String()}
}

// dataType reads the structured type of a column definition.
func dataType(ft *types.FieldType) DataType {
	flag := ft.GetFlag()
	t := DataType{
		Unsigned: mysql.HasUnsignedFlag(flag),
		Length:   ft.GetFlen(),
		Members:  ft.GetElems(),
		Charset:  ft.GetCharset(),
	}
	binary := mysql.HasBinaryFlag(flag) || ft.GetCharset() == "binary"
	switch ft.GetType() {
	case mysql.TypeTiny:
		t.Kind = "TINYINT"
	case mysql.TypeShort:
		t.Kind = "SMALLINT"
	case mysql.TypeInt24:
		t.Kind = "MEDIUMINT"
	case mysql.TypeLong:
		t.Kind = "INT"
	case mysql.TypeLonglong:
		t.Kind = "BIGINT"
	case mysql.TypeFloat:
		t.Kind = "FLOAT"
	case mysql.TypeDouble:
		t.Kind = "DOUBLE"
	case mysql.TypeNewDecimal:
		t.Kind = "DECIMAL"
		t.Precision, t.Scale = ft.GetFlen(), ft.GetDecimal()
		// DECIMAL without arguments is DECIMAL(10,0).
		if t.Precision <= 0 {
			t.Precision = 10
		}
		if t.Scale < 0 {
			t.Scale = 0
		}
	case mysql.TypeYear:
		t.Kind = "YEAR"
	case mysql.TypeDate, mysql.TypeNewDate:
		t.Kind = "DATE"
	case mysql.TypeDatetime:
		t.Kind, t.Fsp = "DATETIME", ft.GetDecimal()
	case mysql.TypeTimestamp:
		t.Kind, t.Fsp = "TIMESTAMP", ft.GetDecimal()
	case mysql.TypeDuration:
		t.Kind, t.Fsp = "TIME", ft.GetDecimal()
	case mysql.TypeVarchar, mysql.TypeVarString:
		t.Kind = "VARCHAR"
		if binary {
			t.Kind = "VARBINARY"
		}
	case mysql.TypeString:
		t.Kind = "CHAR"
		if binary {
			t.Kind = "BINARY"
		}
	case mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		t.Kind = "TEXT"
		if binary {
			t.Kind = "BLOB"
		}
	case mysql.TypeEnum:
		t.Kind = "ENUM"
	case mysql.TypeSet:
		t.Kind = "SET"
	case mysql.TypeBit:
		t.Kind = "BIT"
	case mysql.TypeJSON:
		t.Kind = "JSON"
	case mysql.TypeGeometry:
		t.Kind = "GEOMETRY"
	}
	if t.Fsp < 0 {
		t.Fsp = 0 // no fractional seconds unless the statement asks for them
	}
	return t
}
