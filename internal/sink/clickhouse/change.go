package clickhouse

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/ddl"
	"github.com/thealexjo/binwave/internal/typemap"
)

// ColumnPlan is the ClickHouse side of one column of an ALTER TABLE: its type and, for a new
// column, the default that gives the rows that exist already the value MySQL gave them.
type ColumnPlan struct {
	Name string
	Type string
	// Kind is what the column is in the source. Its comment goes on the ClickHouse column, so that a
	// later change of the type knows what the column was.
	Kind typemap.Column
	// Convert is, for a column of a rebuild, the ClickHouse expression that turns the old column
	// into the new value ({col} is the old column), and empty for a plain copy.
	Convert string
	// Default is the DEFAULT clause of an added column, empty when the type's own default is
	// what MySQL gave existing rows (NULL for a nullable column, 0 or '' otherwise).
	Default string
	First   bool
	After   string
}

// PlanColumn decides what a column of an ALTER TABLE means in ClickHouse. skip is true for a
// column that is not replicated (excluded in the configuration). at is when the source executed
// the statement, for DEFAULT CURRENT_TIMESTAMP. A column whose existing rows' values cannot be
// reproduced is a *TableError: guessing would put wrong values into the replica.
func PlanColumn(cfg *config.Config, database, table string, c ddl.Column, pos ddl.Position, at time.Time, reporter typemap.Reporter) (plan *ColumnPlan, skip bool, err error) {
	full := database + "." + table
	ov := cfg.Tables.Overrides[full]
	for _, name := range ov.ExcludeColumns {
		if name == c.Name {
			return nil, true, nil
		}
	}
	fail := func(format string, args ...any) (*ColumnPlan, bool, error) {
		return nil, false, &TableError{Table: full, Err: fmt.Errorf("column %s of table %s: %s", c.Name, full, fmt.Sprintf(format, args...))}
	}
	if serviceColumns[c.Name] {
		return fail("the name collides with a binwave service column; exclude it or rename it in the source")
	}
	if !Insertable(c.Name) {
		return fail("the name cannot be inserted by the ClickHouse client library; exclude it with tables.overrides.%s.exclude_columns", full)
	}

	col, err := typemapColumn(c)
	if err != nil {
		return fail("%v", err)
	}
	override := ov.Columns[c.Name].Type
	m, err := typemap.Resolve(full, col, typemap.Options{Override: override, Reporter: reporter})
	if err != nil {
		return fail("%v", err)
	}

	def, err := defaultClause(col, m.Type, override != "", c, at, reporter, full)
	if err != nil {
		// Unlike the failures above (a name or a type binwave itself cannot handle, which a resync
		// would hit again identically), this is only about what value to give existing rows for the
		// new column: a resync rereads the table after the column already exists, with its real
		// value already there, so it needs no guessing.
		return nil, false, &TableError{Table: full, Err: fmt.Errorf("column %s of table %s: %w", c.Name, full, err), Recoverable: true}
	}
	return &ColumnPlan{Name: c.Name, Type: m.Type, Kind: col, Default: def, First: pos.First, After: pos.After}, false, nil
}

var kinds = map[string]typemap.Kind{
	"TINYINT": typemap.KindTinyInt, "SMALLINT": typemap.KindSmallInt, "MEDIUMINT": typemap.KindMediumInt,
	"INT": typemap.KindInt, "BIGINT": typemap.KindBigInt, "FLOAT": typemap.KindFloat, "DOUBLE": typemap.KindDouble,
	"DECIMAL": typemap.KindDecimal, "YEAR": typemap.KindYear, "DATE": typemap.KindDate,
	"DATETIME": typemap.KindDateTime, "TIMESTAMP": typemap.KindTimestamp, "TIME": typemap.KindTime,
	"CHAR": typemap.KindChar, "VARCHAR": typemap.KindVarChar, "TEXT": typemap.KindText,
	"BINARY": typemap.KindBinary, "VARBINARY": typemap.KindVarBinary, "BLOB": typemap.KindBlob,
	"ENUM": typemap.KindEnum, "SET": typemap.KindSet, "BIT": typemap.KindBit, "JSON": typemap.KindJSON,
	"GEOMETRY": typemap.KindGeometry,
}

// typemapColumn describes a column of an ALTER TABLE the way the binlog's table structure would.
func typemapColumn(c ddl.Column) (typemap.Column, error) {
	kind, ok := kinds[c.DataType.Kind]
	if !ok {
		return typemap.Column{}, fmt.Errorf("the type %q is not understood", c.Type)
	}
	col := typemap.Column{
		Name: c.Name, Kind: kind, Unsigned: c.DataType.Unsigned, Nullable: c.Nullable,
		Values: c.DataType.Members, Charset: c.DataType.Charset,
	}
	if col.Charset == "" || col.Charset == "binary" {
		col.Charset = "utf8mb4"
	}
	switch kind {
	case typemap.KindChar, typemap.KindVarChar, typemap.KindBinary, typemap.KindVarBinary:
		col.Length = c.DataType.Length
	case typemap.KindBit:
		col.Length = max(c.DataType.Length, 1)
	case typemap.KindDecimal:
		col.Precision, col.Scale = c.DataType.Precision, c.DataType.Scale
	case typemap.KindDateTime, typemap.KindTimestamp, typemap.KindTime:
		col.FSP = c.DataType.Fsp
	}
	return col, nil
}

var currentTimestamp = regexp.MustCompile(`(?i)^(current_timestamp|now|localtime|localtimestamp)(\(\d*\))?$`)

// defaultClause renders what existing rows get when the column is added: MySQL's value, as a
// ClickHouse DEFAULT. It says so, in an error, when that value cannot be reproduced.
func defaultClause(col typemap.Column, chType string, overridden bool, c ddl.Column, at time.Time, reporter typemap.Reporter, table string) (string, error) {
	switch {
	case c.Generated:
		return "", fmt.Errorf("a generated column: the source computes the values of the existing rows, binwave cannot reproduce them")
	case c.AutoIncrement:
		return "", fmt.Errorf("an AUTO_INCREMENT column: the source numbers the existing rows, binwave cannot reproduce the numbers")
	}

	switch c.Default.Kind {
	case ddl.DefaultNull:
		return "", nil
	case ddl.DefaultNone:
		if col.Nullable {
			return "", nil // NULL
		}
		return implicitDefault(col)
	case ddl.DefaultExpression:
		if currentTimestamp.MatchString(strings.TrimSpace(c.Default.Text)) {
			return timestampDefault(col, chType, at, reporter, table, c.Name, c.Default.Text)
		}
		return "", fmt.Errorf("the default %s is an expression: the source evaluates it for the existing rows, binwave cannot reproduce the result", c.Default.Text)
	default: // DefaultLiteral
		if overridden {
			return "", fmt.Errorf("a default value together with a configured type override is not supported")
		}
		return literalDefault(col, chType, c.Default.Text)
	}
}

// implicitDefault is what MySQL gives the existing rows of a NOT NULL column that has no DEFAULT.
// For most types ClickHouse's own default is the same value, so nothing is written.
func implicitDefault(col typemap.Column) (string, error) {
	switch col.Kind {
	case typemap.KindEnum:
		if len(col.Values) == 0 {
			return "", fmt.Errorf("an ENUM without members")
		}
		return "DEFAULT " + stringLiteral(col.Values[0]), nil // MySQL uses the first member
	case typemap.KindDate, typemap.KindDateTime, typemap.KindTimestamp:
		return "", fmt.Errorf("a NOT NULL %s without DEFAULT: the existing rows get a zero date, which depends on the session that ran the statement", col.Kind)
	case typemap.KindJSON:
		return "", fmt.Errorf("a NOT NULL JSON without DEFAULT has no value for the existing rows")
	}
	return "", nil
}

func timestampDefault(col typemap.Column, chType string, at time.Time, reporter typemap.Reporter, table, column, original string) (string, error) {
	switch col.Kind {
	case typemap.KindTimestamp:
	case typemap.KindDateTime:
		return "", fmt.Errorf("DEFAULT %s on a DATETIME: the value is the wall clock of the session that ran the statement, whose time zone binwave does not know (a TIMESTAMP column is stored in UTC and can be reproduced)", original)
	default:
		return "", fmt.Errorf("DEFAULT %s on a %s column", original, col.Kind)
	}
	value := at.UTC().Format("2006-01-02 15:04:05")
	if col.FSP > 0 {
		value += "." + strings.Repeat("0", col.FSP)
	}
	if reporter != nil {
		reporter.Report(typemap.Adjustment{Table: table, Column: column, Kind: typemap.AdjustDDLTimestamp, Original: original, Replacement: value})
	}
	return "DEFAULT CAST(" + stringLiteral(value) + " AS " + chType + ")", nil
}

var (
	decimalText  = regexp.MustCompile(`^-?\d+(\.\d+)?$`)
	datetimeText = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$`)
	dateText     = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
)

// literalDefault renders a constant default, checking it fits the type: ClickHouse would either
// reject it or, worse, convert it silently.
func literalDefault(col typemap.Column, chType, text string) (string, error) {
	bad := func(what string) (string, error) {
		return "", fmt.Errorf("the default %q is not a valid %s", text, what)
	}

	switch col.Kind {
	case typemap.KindTinyInt, typemap.KindSmallInt, typemap.KindMediumInt, typemap.KindInt, typemap.KindBigInt:
		bits := map[typemap.Kind]int{typemap.KindTinyInt: 8, typemap.KindSmallInt: 16, typemap.KindMediumInt: 32, typemap.KindInt: 32, typemap.KindBigInt: 64}[col.Kind]
		if col.Unsigned {
			n, err := strconv.ParseUint(text, 10, bits)
			if err != nil {
				return bad(col.Kind.String())
			}
			return "DEFAULT " + strconv.FormatUint(n, 10), nil
		}
		n, err := strconv.ParseInt(text, 10, bits)
		if err != nil {
			return bad(col.Kind.String())
		}
		return "DEFAULT " + strconv.FormatInt(n, 10), nil
	case typemap.KindYear:
		n, err := strconv.ParseUint(text, 10, 16)
		if err != nil {
			return bad("YEAR")
		}
		return "DEFAULT " + strconv.FormatUint(n, 10), nil
	case typemap.KindFloat, typemap.KindDouble:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return bad("number")
		}
		return "DEFAULT " + strconv.FormatFloat(f, 'g', -1, 64), nil
	case typemap.KindDecimal:
		if !decimalText.MatchString(text) {
			return bad("DECIMAL")
		}
		if i := strings.IndexByte(text, '.'); i >= 0 && len(text)-i-1 > col.Scale {
			return "", fmt.Errorf("the default %s has more decimals than the column's scale %d", text, col.Scale)
		}
		return "DEFAULT CAST(" + stringLiteral(text) + " AS " + chType + ")", nil
	case typemap.KindChar, typemap.KindVarChar, typemap.KindText:
		return "DEFAULT " + stringLiteral(text), nil
	case typemap.KindEnum:
		for _, m := range col.Values {
			if m == text {
				return "DEFAULT " + stringLiteral(text), nil
			}
		}
		return "", fmt.Errorf("the default %q is not a member of the ENUM", text)
	case typemap.KindSet:
		if text == "" {
			return "DEFAULT []", nil
		}
		var parts []string
		for _, m := range strings.Split(text, ",") {
			known := false
			for _, v := range col.Values {
				known = known || v == m
			}
			if !known {
				return "", fmt.Errorf("the default %q has a member that the SET lacks", text)
			}
			parts = append(parts, stringLiteral(m))
		}
		return "DEFAULT [" + strings.Join(parts, ", ") + "]", nil
	case typemap.KindDate:
		if !dateText.MatchString(text) || text == "0000-00-00" {
			return bad("DATE")
		}
		return "DEFAULT CAST(" + stringLiteral(text) + " AS " + chType + ")", nil
	case typemap.KindDateTime:
		if !datetimeText.MatchString(text) || strings.HasPrefix(text, "0000-") {
			return bad("DATETIME")
		}
		return "DEFAULT CAST(" + stringLiteral(text) + " AS " + chType + ")", nil
	case typemap.KindTimestamp:
		return "", fmt.Errorf("a constant default of a TIMESTAMP column is read in the time zone of the session that ran the statement, which binwave does not know")
	}
	return "", fmt.Errorf("a default value of a %s column is not supported", col.Kind)
}

// stringLiteral quotes text as a ClickHouse string literal.
func stringLiteral(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

var (
	intType     = regexp.MustCompile(`^(U?)Int(8|16|32|64)$`)
	decimalType = regexp.MustCompile(`^Decimal\((\d+),\s*(\d+)\)$`)
	dtType      = regexp.MustCompile(`^DateTime64\((\d),\s*('[^']*')\)$`)
)

// Widens reports whether every value of the ClickHouse type from is a value of the type to, with
// the same meaning: a column can be changed from one to the other without losing anything, and
// the source's own ALTER did not either. Integers widen to a larger range (an unsigned integer
// to a signed one only if it is really larger); decimals must keep their integer digits and
// their scale; a float widens to a double; DateTime64 to a finer precision; and a column may
// become Nullable, never the other way.
func Widens(from, to string) bool {
	from, fromNull := splitNullable(from)
	to, toNull := splitNullable(to)
	if fromNull && !toNull {
		return false
	}
	if from == to {
		return true
	}

	if f, t := intType.FindStringSubmatch(from), intType.FindStringSubmatch(to); f != nil && t != nil {
		fromBits, _ := strconv.Atoi(f[2])
		toBits, _ := strconv.Atoi(t[2])
		fromUnsigned, toUnsigned := f[1] == "U", t[1] == "U"
		switch {
		case fromUnsigned == toUnsigned:
			return toBits >= fromBits
		case fromUnsigned && !toUnsigned:
			return toBits > fromBits
		default:
			return false // signed to unsigned loses the negative values
		}
	}
	if from == "Float32" && to == "Float64" {
		return true
	}
	if f, t := decimalType.FindStringSubmatch(from), decimalType.FindStringSubmatch(to); f != nil && t != nil {
		fp, _ := strconv.Atoi(f[1])
		fs, _ := strconv.Atoi(f[2])
		tp, _ := strconv.Atoi(t[1])
		ts, _ := strconv.Atoi(t[2])
		return ts >= fs && tp-ts >= fp-fs
	}
	if f, t := dtType.FindStringSubmatch(from), dtType.FindStringSubmatch(to); f != nil && t != nil {
		return f[2] == t[2] && t[1] >= f[1]
	}
	return false
}

// splitNullable returns the type inside Nullable(...) and whether there was one.
func splitNullable(t string) (base string, nullable bool) {
	if strings.HasPrefix(t, "Nullable(") && strings.HasSuffix(t, ")") {
		return t[len("Nullable(") : len(t)-1], true
	}
	return t, false
}

// commentClause renders the COMMENT of a column that says what it is in the source.
func commentClause(kind typemap.Column) string {
	if kind.Kind == typemap.KindInvalid {
		return ""
	}
	return " COMMENT '" + kind.TypeComment() + "'"
}
