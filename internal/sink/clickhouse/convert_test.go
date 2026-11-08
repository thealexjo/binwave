package clickhouse

import (
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/typemap"
)

func col(k typemap.Kind) typemap.Column { return typemap.Column{Kind: k} }

func TestClassify(t *testing.T) {
	dec := func(p, s int) typemap.Column {
		return typemap.Column{Kind: typemap.KindDecimal, Precision: p, Scale: s}
	}
	tests := []struct {
		name        string
		old         typemap.Column
		oldCH       string
		nw          typemap.Column
		newCH       string
		want        Action
		exprHas     string
		reasonHas   string
		oldUnknown  bool
		description string
	}{
		{name: "nothing", old: col(typemap.KindInt), oldCH: "Int32", nw: col(typemap.KindInt), newCH: "Int32", want: ActionNothing},
		{name: "wider integer in place", old: col(typemap.KindInt), oldCH: "Int32", nw: col(typemap.KindBigInt), newCH: "Int64", want: ActionInPlace},
		{name: "narrower integer is a copy", old: col(typemap.KindBigInt), oldCH: "Int64", nw: col(typemap.KindInt), newCH: "Int32", want: ActionRebuild, exprHas: "accurateCast"},
		{name: "signed to unsigned", old: col(typemap.KindInt), oldCH: "Int32", nw: typemap.Column{Kind: typemap.KindInt, Unsigned: true}, newCH: "UInt32", want: ActionRebuild, exprHas: "accurateCast"},
		{name: "NULL to NOT NULL must stop at a NULL", old: col(typemap.KindInt), oldCH: "Nullable(Int32)", nw: col(typemap.KindInt), newCH: "Int32", want: ActionRebuild, exprHas: "accurateCast"},
		{name: "NOT NULL to NULL", old: col(typemap.KindInt), oldCH: "Int32", nw: col(typemap.KindInt), newCH: "Nullable(Int32)", want: ActionInPlace},
		{name: "varchar longer", old: col(typemap.KindVarChar), oldCH: "String", nw: col(typemap.KindText), newCH: "String", want: ActionNothing},
		{name: "decimal wider", old: dec(10, 2), oldCH: "Decimal(10, 2)", nw: dec(14, 4), newCH: "Decimal(14, 4)", want: ActionInPlace},
		{name: "decimal scale down rounds like MySQL", old: dec(10, 4), oldCH: "Decimal(10, 4)", nw: dec(10, 2), newCH: "Decimal(10, 2)", want: ActionRebuild, exprHas: "round({col}, 2)"},
		{name: "decimal to int rounds", old: dec(10, 2), oldCH: "Decimal(10, 2)", nw: col(typemap.KindInt), newCH: "Int32", want: ActionRebuild, exprHas: "round({col}, 0)"},
		{name: "decimal to text keeps the scale", old: dec(10, 2), oldCH: "Decimal(10, 2)", nw: col(typemap.KindVarChar), newCH: "String", want: ActionRebuild, exprHas: "toDecimalString({col}, 2)"},
		{name: "date to datetime", old: col(typemap.KindDate), oldCH: "Date32", nw: col(typemap.KindDateTime), newCH: "DateTime64(0, 'UTC')", want: ActionRebuild, exprHas: "toDateTime64({col}, 0, 'UTC')"},
		{name: "datetime finer", old: typemap.Column{Kind: typemap.KindDateTime}, oldCH: "DateTime64(0, 'UTC')", nw: typemap.Column{Kind: typemap.KindDateTime, FSP: 6}, newCH: "DateTime64(6, 'UTC')", want: ActionInPlace},
		{name: "datetime coarser is rounded by MySQL", old: typemap.Column{Kind: typemap.KindDateTime, FSP: 6}, oldCH: "DateTime64(6, 'UTC')", nw: typemap.Column{Kind: typemap.KindDateTime}, newCH: "DateTime64(0, 'UTC')", want: ActionRefuse, reasonHas: "rounding"},
		{name: "datetime to timestamp has the same ClickHouse type and another meaning", old: col(typemap.KindDateTime), oldCH: "DateTime64(0, 'UTC')", nw: col(typemap.KindTimestamp), newCH: "DateTime64(0, 'UTC')", want: ActionRefuse, reasonHas: "time zone"},
		{name: "int to time has a ClickHouse type that a BIGINT has", old: col(typemap.KindBigInt), oldCH: "Int64", nw: col(typemap.KindTime), newCH: "Int64", want: ActionRefuse},
		{name: "time with fewer digits", old: typemap.Column{Kind: typemap.KindTime, FSP: 6}, oldCH: "Int64", nw: col(typemap.KindTime), newCH: "Int64", want: ActionRefuse, reasonHas: "TIME"},
		{name: "float to int", old: col(typemap.KindDouble), oldCH: "Float64", nw: col(typemap.KindInt), newCH: "Int32", want: ActionRefuse},
		{name: "float widens", old: col(typemap.KindFloat), oldCH: "Float32", nw: col(typemap.KindDouble), newCH: "Float64", want: ActionInPlace},
		{name: "double to float is rounded like MySQL and stops at a value too big", old: col(typemap.KindDouble), oldCH: "Float64", nw: col(typemap.KindFloat), newCH: "Float32", want: ActionRebuild, exprHas: "toFloat32"},
		{name: "text to enum stops at a non-member", old: col(typemap.KindVarChar), oldCH: "String", nw: typemap.Column{Kind: typemap.KindEnum, Values: []string{"a", "b"}}, newCH: "LowCardinality(String)", want: ActionRebuild, exprHas: "IN ('a', 'b')"},
		{name: "enum to text", old: col(typemap.KindEnum), oldCH: "LowCardinality(String)", nw: col(typemap.KindVarChar), newCH: "String", want: ActionRebuild},
		{name: "enum members added", old: col(typemap.KindEnum), oldCH: "LowCardinality(String)", nw: col(typemap.KindEnum), newCH: "LowCardinality(String)", want: ActionNothing},
		{name: "text to number", old: col(typemap.KindVarChar), oldCH: "String", nw: col(typemap.KindInt), newCH: "Int32", want: ActionRefuse},
		{name: "int to text", old: col(typemap.KindInt), oldCH: "Int32", nw: col(typemap.KindVarChar), newCH: "String", want: ActionRebuild, exprHas: "toString"},
		{name: "int to year", old: col(typemap.KindInt), oldCH: "Int32", nw: col(typemap.KindYear), newCH: "UInt16", want: ActionRefuse},
		{name: "year to int", old: col(typemap.KindYear), oldCH: "UInt16", nw: col(typemap.KindInt), newCH: "Int32", want: ActionRebuild, exprHas: "accurateCast"},
		{name: "bit wider keeps the type", old: typemap.Column{Kind: typemap.KindBit, Length: 8}, oldCH: "UInt64", nw: typemap.Column{Kind: typemap.KindBit, Length: 16}, newCH: "UInt64", want: ActionNothing},
		{name: "binary length", old: typemap.Column{Kind: typemap.KindBinary, Length: 4}, oldCH: "FixedString(4)", nw: typemap.Column{Kind: typemap.KindBinary, Length: 8}, newCH: "FixedString(8)", want: ActionRefuse},

		// What the column was is not known: it is guessed from its ClickHouse type, and a change that
		// two candidates take differently is refused.
		{name: "unknown: int to bigint is the same for the only candidate", oldUnknown: true, oldCH: "Int32", nw: col(typemap.KindBigInt), newCH: "Int64", want: ActionInPlace},
		{name: "unknown: an Int64 that may be a TIME", oldUnknown: true, oldCH: "Int64", nw: col(typemap.KindTimestamp), newCH: "DateTime64(0, 'UTC')", want: ActionRefuse, reasonHas: "MySQL converts"},
		{name: "unknown: DATETIME or TIMESTAMP, nothing changes in the type", oldUnknown: true, oldCH: "DateTime64(0, 'UTC')", nw: col(typemap.KindTimestamp), newCH: "DateTime64(0, 'UTC')", want: ActionRefuse, reasonHas: "does not know"},
		{name: "unknown: the same type stays", oldUnknown: true, oldCH: "String", nw: col(typemap.KindVarChar), newCH: "String", want: ActionNothing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.old, !tt.oldUnknown, tt.oldCH, tt.nw, tt.newCH)
			if got.Action != tt.want {
				t.Fatalf("Classify = %+v, want action %d", got, tt.want)
			}
			if !strings.Contains(got.Expr, tt.exprHas) {
				t.Errorf("expression %q lacks %q", got.Expr, tt.exprHas)
			}
			if !strings.Contains(got.Reason, tt.reasonHas) {
				t.Errorf("reason %q lacks %q", got.Reason, tt.reasonHas)
			}
		})
	}
}

func TestApplyPutsTheColumnInTheExpression(t *testing.T) {
	c := accurate("Int32")
	if got := c.Apply("`v`"); got != "accurateCast(`v`, 'Int32')" {
		t.Errorf("Apply = %s", got)
	}
}
