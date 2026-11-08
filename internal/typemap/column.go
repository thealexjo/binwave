package typemap

import (
	"fmt"
	"strings"
)

// Kind is a MySQL column type family.
type Kind int

// MySQL column kinds. TINYTEXT/MEDIUMTEXT/LONGTEXT are KindText; the BLOB variants are KindBlob.
const (
	KindInvalid Kind = iota
	KindTinyInt
	KindSmallInt
	KindMediumInt
	KindInt
	KindBigInt
	KindFloat
	KindDouble
	KindDecimal
	KindYear
	KindDate
	KindDateTime
	KindTimestamp
	KindTime
	KindChar
	KindVarChar
	KindText
	KindBinary
	KindVarBinary
	KindBlob
	KindEnum
	KindSet
	KindBit
	KindJSON
	KindGeometry
)

var kindNames = map[Kind]string{
	KindTinyInt: "TINYINT", KindSmallInt: "SMALLINT", KindMediumInt: "MEDIUMINT", KindInt: "INT", KindBigInt: "BIGINT",
	KindFloat: "FLOAT", KindDouble: "DOUBLE", KindDecimal: "DECIMAL", KindYear: "YEAR",
	KindDate: "DATE", KindDateTime: "DATETIME", KindTimestamp: "TIMESTAMP", KindTime: "TIME",
	KindChar: "CHAR", KindVarChar: "VARCHAR", KindText: "TEXT",
	KindBinary: "BINARY", KindVarBinary: "VARBINARY", KindBlob: "BLOB",
	KindEnum: "ENUM", KindSet: "SET", KindBit: "BIT", KindJSON: "JSON", KindGeometry: "GEOMETRY",
}

func (k Kind) String() string {
	if name, ok := kindNames[k]; ok {
		return name
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

func (k Kind) isInteger() bool { return k >= KindTinyInt && k <= KindBigInt }

func (k Kind) isText() bool { return k == KindChar || k == KindVarChar || k == KindText }

// Column is a source-neutral description of a MySQL column. The source fills it from
// binlog TABLE_MAP metadata and DDL; typemap never depends on the binlog library.
type Column struct {
	Name     string
	Kind     Kind
	Unsigned bool
	Nullable bool
	// Length is the declared length: characters for CHAR/VARCHAR, bytes for BINARY/VARBINARY, bits for BIT.
	Length int
	// Precision and Scale apply to DECIMAL.
	Precision int
	Scale     int
	// FSP is fractional seconds precision (0-6) for DATETIME, TIMESTAMP and TIME.
	FSP int
	// Charset of CHAR/VARCHAR/TEXT (e.g. utf8mb4, latin1); empty means utf8mb4.
	Charset string
	// Values are the ENUM/SET members in declaration order.
	Values []string
}

// SQLType renders the column type in MySQL syntax, for messages.
func (c Column) SQLType() string {
	var b strings.Builder
	b.WriteString(c.Kind.String())
	switch c.Kind {
	case KindDecimal:
		fmt.Fprintf(&b, "(%d,%d)", c.Precision, c.Scale)
	case KindChar, KindVarChar, KindBinary, KindVarBinary, KindBit:
		fmt.Fprintf(&b, "(%d)", c.Length)
	case KindDateTime, KindTimestamp, KindTime:
		if c.FSP > 0 {
			fmt.Fprintf(&b, "(%d)", c.FSP)
		}
	case KindEnum, KindSet:
		quoted := make([]string, len(c.Values))
		for i, v := range c.Values {
			quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		fmt.Fprintf(&b, "(%s)", strings.Join(quoted, ","))
	}
	if c.Unsigned {
		b.WriteString(" UNSIGNED")
	}
	if c.Charset != "" && c.Kind.isText() {
		b.WriteString(" CHARACTER SET " + c.Charset)
	}
	if !c.Nullable {
		b.WriteString(" NOT NULL")
	}
	return b.String()
}

// Encode renders the column type in a compact form that DecodeType reads back: the kind and the
// attributes that decide how a value is converted, without the name, the nullability or the
// members of an ENUM or SET. binwave stores it in the comment of the ClickHouse column, so that a
// later change of the type knows what the column was, however the process got there.
func (c Column) Encode() string {
	var b strings.Builder
	b.WriteString(c.Kind.String())
	if c.Unsigned {
		b.WriteString(" unsigned")
	}
	switch c.Kind {
	case KindDecimal:
		fmt.Fprintf(&b, " %d,%d", c.Precision, c.Scale)
	case KindChar, KindVarChar, KindBinary, KindVarBinary, KindBit:
		fmt.Fprintf(&b, " %d", c.Length)
	case KindDateTime, KindTimestamp, KindTime:
		fmt.Fprintf(&b, " %d", c.FSP)
	}
	return b.String()
}

// typeCommentPrefix marks a column comment that binwave wrote.
const typeCommentPrefix = "mysql: "

// TypeComment is the comment binwave puts on the ClickHouse column of c.
func (c Column) TypeComment() string { return typeCommentPrefix + c.Encode() }

// DecodeTypeComment reads a comment written by TypeComment. ok is false for any other comment,
// which is what a column of a table that binwave did not make, or made before it wrote comments,
// has.
func DecodeTypeComment(comment string) (c Column, ok bool) {
	rest, found := strings.CutPrefix(comment, typeCommentPrefix)
	if !found {
		return Column{}, false
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return Column{}, false
	}
	for k, name := range kindNames {
		if name == fields[0] {
			c.Kind = k
		}
	}
	if c.Kind == KindInvalid {
		return Column{}, false
	}
	fields = fields[1:]
	if len(fields) > 0 && fields[0] == "unsigned" {
		c.Unsigned = true
		fields = fields[1:]
	}
	if len(fields) == 0 {
		return c, true
	}
	switch c.Kind {
	case KindDecimal:
		if _, err := fmt.Sscanf(fields[0], "%d,%d", &c.Precision, &c.Scale); err != nil {
			return Column{}, false
		}
	case KindChar, KindVarChar, KindBinary, KindVarBinary, KindBit:
		if _, err := fmt.Sscanf(fields[0], "%d", &c.Length); err != nil {
			return Column{}, false
		}
	case KindDateTime, KindTimestamp, KindTime:
		if _, err := fmt.Sscanf(fields[0], "%d", &c.FSP); err != nil {
			return Column{}, false
		}
	}
	return c, true
}
