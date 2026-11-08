package clickhouse

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/thealexjo/binwave/internal/typemap"
)

// Action is what a change of the type of a column needs in ClickHouse.
type Action int

// Actions.
const (
	// ActionNothing: the ClickHouse type stays, and so do the values.
	ActionNothing Action = iota
	// ActionInPlace: MODIFY COLUMN on the table. Only for changes that keep every value and that
	// ClickHouse does without touching what a value means (Widens).
	ActionInPlace
	// ActionRebuild: the table is rebuilt with the values converted by Conversion.Expr, exactly.
	ActionRebuild
	// ActionRefuse: MySQL converts the values by rules that binwave cannot reproduce exactly from
	// the binlog and ClickHouse alone. The table is set aside until it is resynchronized.
	ActionRefuse
)

// Conversion is the decision about one column.
type Conversion struct {
	Action Action
	// Expr is the ClickHouse expression that turns the old column into the new value, for
	// ActionRebuild; {col} stands for the quoted old column. It raises an error for a value that
	// does not convert exactly, and never wraps, rounds in another way than MySQL, or turns NULL into
	// a default.
	Expr string
	// Reason says why the change is refused, for ActionRefuse.
	Reason string
}

func nothing() Conversion               { return Conversion{Action: ActionNothing} }
func refuse(why string) Conversion      { return Conversion{Action: ActionRefuse, Reason: why} }
func rebuild(expr string) Conversion    { return Conversion{Action: ActionRebuild, Expr: expr} }
func accurate(target string) Conversion { return rebuild("accurateCast({col}, '" + target + "')") }

var (
	floatType = regexp.MustCompile(`^Float(32|64)$`)
	fixedType = regexp.MustCompile(`^FixedString\((\d+)\)$`)
)

// group is the family of a MySQL type as far as conversions of its values are concerned.
type group int

const (
	gInt group = iota
	gBit
	gYear
	gDecimal
	gFloat
	gDate
	gDateTime
	gTimestamp
	gTime
	gText
	gVarBinary
	gBinary
	gEnum
	gSet
	gJSON
	gOther
)

func groupOf(k typemap.Kind) group {
	switch k {
	case typemap.KindTinyInt, typemap.KindSmallInt, typemap.KindMediumInt, typemap.KindInt, typemap.KindBigInt:
		return gInt
	case typemap.KindBit:
		return gBit
	case typemap.KindYear:
		return gYear
	case typemap.KindDecimal:
		return gDecimal
	case typemap.KindFloat, typemap.KindDouble:
		return gFloat
	case typemap.KindDate:
		return gDate
	case typemap.KindDateTime:
		return gDateTime
	case typemap.KindTimestamp:
		return gTimestamp
	case typemap.KindTime:
		return gTime
	case typemap.KindChar, typemap.KindVarChar, typemap.KindText:
		return gText
	case typemap.KindVarBinary, typemap.KindBlob:
		return gVarBinary
	case typemap.KindBinary:
		return gBinary
	case typemap.KindEnum:
		return gEnum
	case typemap.KindSet:
		return gSet
	case typemap.KindJSON:
		return gJSON
	default:
		return gOther
	}
}

// guesses are the columns that a ClickHouse type can come from when nothing says which; a type that
// more than one MySQL type maps to has several.
func guesses(chType string) []typemap.Column {
	base, _ := splitNullable(chType)
	switch {
	case base == "Int8":
		return []typemap.Column{{Kind: typemap.KindTinyInt}}
	case base == "UInt8":
		return []typemap.Column{{Kind: typemap.KindTinyInt, Unsigned: true}}
	case base == "Int16":
		return []typemap.Column{{Kind: typemap.KindSmallInt}}
	case base == "UInt16":
		return []typemap.Column{{Kind: typemap.KindSmallInt, Unsigned: true}, {Kind: typemap.KindYear}}
	case base == "Int32":
		return []typemap.Column{{Kind: typemap.KindInt}}
	case base == "UInt32":
		return []typemap.Column{{Kind: typemap.KindInt, Unsigned: true}}
	case base == "Int64":
		return []typemap.Column{{Kind: typemap.KindBigInt}, {Kind: typemap.KindTime}}
	case base == "UInt64":
		return []typemap.Column{{Kind: typemap.KindBigInt, Unsigned: true}, {Kind: typemap.KindBit, Length: 64}}
	case base == "Float32":
		return []typemap.Column{{Kind: typemap.KindFloat}}
	case base == "Float64":
		return []typemap.Column{{Kind: typemap.KindDouble}}
	case base == "Date32":
		return []typemap.Column{{Kind: typemap.KindDate}}
	case base == "String":
		return []typemap.Column{{Kind: typemap.KindVarChar}, {Kind: typemap.KindBlob}}
	case base == "LowCardinality(String)":
		return []typemap.Column{{Kind: typemap.KindEnum}}
	case strings.HasPrefix(base, "Array("):
		return []typemap.Column{{Kind: typemap.KindSet}}
	}
	if m := decimalType.FindStringSubmatch(base); m != nil {
		p, _ := strconv.Atoi(m[1])
		s, _ := strconv.Atoi(m[2])
		return []typemap.Column{{Kind: typemap.KindDecimal, Precision: p, Scale: s}}
	}
	if m := dtType.FindStringSubmatch(base); m != nil {
		fsp, _ := strconv.Atoi(m[1])
		return []typemap.Column{{Kind: typemap.KindDateTime, FSP: fsp}, {Kind: typemap.KindTimestamp, FSP: fsp}}
	}
	if m := fixedType.FindStringSubmatch(base); m != nil {
		n, _ := strconv.Atoi(m[1])
		return []typemap.Column{{Kind: typemap.KindBinary, Length: n}}
	}
	return nil
}

// Classify decides what the change of a column from old to nw needs. oldKnown is false when
// nothing recorded what the column was (a table binwave made before it wrote the comments, or
// made by hand): the old column is then one of what its ClickHouse type can come from, and the
// change is only accepted when every one of them gives the same answer.
func Classify(old typemap.Column, oldKnown bool, oldCH string, nw typemap.Column, newCH string) Conversion {
	if oldKnown {
		return classify(old, oldCH, nw, newCH)
	}
	candidates := guesses(oldCH)
	if base, _ := splitNullable(oldCH); base == "String" {
		// A text and a binary column look alike in ClickHouse. A change to a text or a binary type is
		// taken for a change within the family the new type is in: a VARCHAR that gets longer must
		// not be refused for what a BLOB might have meant.
		var same []typemap.Column
		for _, c := range candidates {
			if groupOf(c.Kind) == groupOf(nw.Kind) {
				same = append(same, c)
			}
		}
		if g := groupOf(nw.Kind); (g == gText || g == gVarBinary) && len(same) > 0 {
			candidates = same
		}
	}
	if len(candidates) == 0 {
		if oldCH == newCH {
			return nothing()
		}
		return refuse(fmt.Sprintf("binwave does not know what the column was in the source (its ClickHouse type is %s), so it cannot tell what the change does to the values", oldCH))
	}
	first := classify(candidates[0], oldCH, nw, newCH)
	for _, c := range candidates[1:] {
		got := classify(c, oldCH, nw, newCH)
		if got.Action != first.Action || got.Expr != first.Expr {
			return refuse(fmt.Sprintf("binwave does not know what the column was in the source (its ClickHouse type %s comes from %s or %s), "+
				"and the change to %s does something different to each", oldCH, candidates[0].SQLType(), c.SQLType(), nw.SQLType()))
		}
	}
	return first
}

func classify(old typemap.Column, oldCH string, nw typemap.Column, newCH string) Conversion {
	oldBase, oldNull := splitNullable(oldCH)
	newBase, newNull := splitNullable(newCH)
	og, ng := groupOf(old.Kind), groupOf(nw.Kind)

	// The type that a value keeps: nothing to do, unless it stops being NULL-able.
	sameValues := false
	if og == ng {
		switch og {
		case gTime:
			if nw.FSP < old.FSP {
				return refuse("MySQL rounds the fractional seconds of a TIME that loses digits, and binwave cannot reproduce the rounding")
			}
			sameValues = true
		case gEnum, gText, gVarBinary, gBit, gYear, gJSON, gSet, gOther:
			sameValues = oldBase == newBase
		case gBinary:
			sameValues = oldBase == newBase
			if !sameValues {
				return refuse("MySQL pads or cuts BINARY values to the new length, and binwave does not reproduce that")
			}
		}
	}
	if sameValues {
		if oldCH == newCH || (!oldNull && newNull) {
			if oldCH == newCH {
				return nothing()
			}
			return Conversion{Action: ActionInPlace}
		}
		return accurate(newCH) // Nullable(T) to T: a NULL must stop the copy, not become a default
	}

	if oldBase == newBase && og == ng && (og == gInt || og == gDecimal || og == gFloat || og == gDate || og == gDateTime || og == gTimestamp) {
		// The same ClickHouse type and the same family: only the nullability can differ.
		switch {
		case oldCH == newCH:
			return nothing()
		case !oldNull && newNull:
			return Conversion{Action: ActionInPlace}
		default:
			return accurate(newCH)
		}
	}

	// Widening within a family keeps every value and needs no copy.
	if og == ng && (og == gInt || og == gDecimal || og == gFloat || og == gDateTime || og == gTimestamp) && Widens(oldCH, newCH) {
		return Conversion{Action: ActionInPlace}
	}

	switch og {
	case gInt, gBit, gYear:
		return fromInteger(ng, newBase, newCH, og == gInt)
	case gDecimal:
		return fromDecimal(oldBase, ng, newBase, newCH)
	case gFloat:
		if ng == gFloat {
			// A double that becomes a float is rounded to the nearest float, as MySQL does, and one
			// that is too big for it stops the copy.
			return rebuild("if(abs({col}) > 3.4028234663852886e38, toFloat32(throwIf(1, 'a value that does not fit a FLOAT')), toFloat32({col}))")
		}
		return refuse("MySQL rounds a floating point number that becomes another type by its own rules, which binwave does not reproduce")
	case gDate:
		if ng == gDateTime {
			if m := dtType.FindStringSubmatch(newBase); m != nil {
				return rebuild("toDateTime64({col}, " + m[1] + ", 'UTC')")
			}
		}
		return refuse("MySQL converts a DATE by rules that depend on the target type, which binwave does not reproduce")
	case gDateTime, gTimestamp:
		return refuse("MySQL converts a date and time to another type or to fewer fractional digits by rounding, and a TIMESTAMP by the time zone of the session, which binwave does not reproduce")
	case gEnum:
		if ng == gText {
			return rebuild("CAST({col} AS String)")
		}
	case gText:
		if ng == gEnum {
			return textToEnum(nw)
		}
		if ng == gText {
			return accurate(newCH)
		}
	case gVarBinary:
		if ng == gVarBinary {
			return accurate(newCH)
		}
	}
	return refuse(fmt.Sprintf("MySQL converts the values of %s to %s by its own rules, which binwave does not reproduce exactly", old.SQLType(), nw.SQLType()))
}

// fromInteger is the change of an integer, a BIT or a YEAR (asInt: an integer proper).
func fromInteger(ng group, newBase, newCH string, asInt bool) Conversion {
	switch ng {
	case gInt, gBit:
		if ng == gBit && !asInt {
			return refuse("a YEAR becomes a BIT by a rule of MySQL that binwave does not reproduce")
		}
		return accurate(newCH)
	case gDecimal:
		return accurate(newCH)
	case gFloat:
		return rebuild("toFloat" + floatType.FindStringSubmatch(newBase)[1] + "({col})")
	case gText:
		return rebuild("toString({col})")
	case gYear:
		return refuse("MySQL maps an integer to a YEAR by its own rule (0, 1-69, 70-99), which binwave does not reproduce")
	}
	return refuse("MySQL converts an integer to this type by its own rules (a TIME is hhmmss, a DATE yyyymmdd, an ENUM an index), which binwave does not reproduce")
}

func fromDecimal(oldBase string, ng group, newBase, newCH string) Conversion {
	m := decimalType.FindStringSubmatch(oldBase)
	if m == nil {
		return refuse("the ClickHouse type of the column is not a decimal")
	}
	oldScale, _ := strconv.Atoi(m[2])
	switch ng {
	case gDecimal:
		t := decimalType.FindStringSubmatch(newBase)
		if t == nil {
			return refuse("the ClickHouse type of the column is not a decimal")
		}
		newScale, _ := strconv.Atoi(t[2])
		if newScale < oldScale {
			// MySQL rounds half away from zero, and so does round() on a decimal.
			return rebuild(fmt.Sprintf("accurateCast(round({col}, %d), '%s')", newScale, newCH))
		}
		return accurate(newCH)
	case gInt:
		return rebuild(fmt.Sprintf("accurateCast(round({col}, 0), '%s')", newCH))
	case gFloat:
		return rebuild("toFloat" + floatType.FindStringSubmatch(newBase)[1] + "({col})")
	case gText:
		return rebuild(fmt.Sprintf("toDecimalString({col}, %d)", oldScale))
	}
	return refuse("MySQL converts a DECIMAL to this type by its own rules, which binwave does not reproduce")
}

// textToEnum copies the text and stops at a value that is not a member: MySQL refuses it in strict
// mode and turns it into the empty value in the other, and the second must not go unseen.
func textToEnum(nw typemap.Column) Conversion {
	members := make([]string, len(nw.Values))
	for i, v := range nw.Values {
		members[i] = "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	list := strings.Join(members, ", ")
	return rebuild("if({col} IN (" + list + "), {col}, toString(throwIf(1, 'a value that is not a member of the new ENUM')))")
}

// Apply renders the conversion expression for a column.
func (c Conversion) Apply(column string) string {
	return strings.ReplaceAll(c.Expr, "{col}", column)
}
