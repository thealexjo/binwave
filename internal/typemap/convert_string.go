package typemap

import (
	"errors"
	"fmt"
	"math/bits"
	"strings"
)

func rawBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	}
	return nil, fmt.Errorf("want string or []byte, got %T", v)
}

func textConverter(tc transcoder) converter {
	return func(_ *Mapping, v any) (any, error) {
		raw, err := rawBytes(v)
		if err != nil {
			return nil, err
		}
		return tc(raw)
	}
}

func convertBytes(_ *Mapping, v any) (any, error) {
	raw, err := rawBytes(v)
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}

// convertFixedBinary restores the trailing 0x00 bytes MySQL strips from BINARY(n) in the binlog.
// Without padding, e.g. a BINARY(16) UUID ending in zero bytes would silently change.
func convertFixedBinary(m *Mapping, v any) (any, error) {
	raw, err := rawBytes(v)
	if err != nil {
		return nil, err
	}
	size := m.Column.Length
	if len(raw) > size {
		return nil, fmt.Errorf("%d bytes do not fit BINARY(%d)", len(raw), size)
	}
	padded := make([]byte, size)
	copy(padded, raw)
	return string(padded), nil
}

// enumMember resolves an ENUM value given as a 1-based index or as the member text.
func enumMember(m *Mapping, v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	}
	i, err := toInteger(v)
	if err != nil {
		return "", fmt.Errorf("want an ENUM index or string, got %T", v)
	}
	switch {
	case i.neg || i.abs > uint64(len(m.Column.Values)):
		return "", fmt.Errorf("ENUM index %s out of range 0..%d", i, len(m.Column.Values))
	case i.abs == 0:
		// MySQL stores invalid values (non-strict mode) as index 0 with the value ''.
		return "", nil
	}
	return m.Column.Values[i.abs-1], nil
}

func convertEnum(m *Mapping, v any) (any, error) {
	return enumMember(m, v)
}

func enumOverrideConverter(members map[string]int) converter {
	return func(m *Mapping, v any) (any, error) {
		member, err := enumMember(m, v)
		if err != nil {
			return nil, err
		}
		if _, ok := members[member]; !ok {
			return nil, fmt.Errorf("ENUM value %q is not a member of %s", member, m.Type)
		}
		return member, nil
	}
}

// setMembers resolves a SET value given as a bitmask or as comma-separated text.
func setMembers(m *Mapping, v any) ([]string, error) {
	switch x := v.(type) {
	case string:
		return splitSet(x), nil
	case []byte:
		return splitSet(string(x)), nil
	}
	i, err := toInteger(v)
	if err != nil {
		return nil, fmt.Errorf("want a SET bitmask or string, got %T", v)
	}
	if i.neg {
		return nil, fmt.Errorf("negative SET bitmask %s", i)
	}
	if bits.Len64(i.abs) > len(m.Column.Values) {
		return nil, fmt.Errorf("SET bitmask %#x has bits beyond %d members", i.abs, len(m.Column.Values))
	}
	members := make([]string, 0, bits.OnesCount64(i.abs))
	for bit, value := range m.Column.Values {
		if i.abs&(1<<uint(bit)) != 0 {
			members = append(members, value)
		}
	}
	return members, nil
}

func splitSet(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

func convertSetArray(m *Mapping, v any) (any, error) {
	return setMembers(m, v)
}

func convertSetString(m *Mapping, v any) (any, error) {
	members, err := setMembers(m, v)
	if err != nil {
		return nil, err
	}
	return strings.Join(members, ","), nil
}

var errEnumSyntax = errors.New("want Enum8('a' = 1, 'b' = 2)")

// parseEnumType extracts member names from a ClickHouse Enum8/Enum16 type definition.
func parseEnumType(def string) (map[string]int, error) {
	open := strings.IndexByte(def, '(')
	if open < 0 || !strings.HasSuffix(def, ")") {
		return nil, errEnumSyntax
	}
	body := def[open+1 : len(def)-1]

	members := make(map[string]int)
	for pos := 0; ; {
		pos = skipSpaces(body, pos)
		name, next, err := quoted(body, pos)
		if err != nil {
			return nil, err
		}
		pos = skipSpaces(body, next)
		if pos >= len(body) || body[pos] != '=' {
			return nil, errEnumSyntax
		}
		pos = skipSpaces(body, pos+1)
		start := pos
		if pos < len(body) && body[pos] == '-' {
			pos++
		}
		for pos < len(body) && body[pos] >= '0' && body[pos] <= '9' {
			pos++
		}
		if pos == start || body[start:pos] == "-" {
			return nil, errEnumSyntax
		}
		var number int
		if _, err := fmt.Sscan(body[start:pos], &number); err != nil {
			return nil, errEnumSyntax
		}
		if _, dup := members[name]; dup {
			return nil, fmt.Errorf("duplicate enum member %q", name)
		}
		members[name] = number

		pos = skipSpaces(body, pos)
		if pos == len(body) {
			return members, nil
		}
		if body[pos] != ',' {
			return nil, errEnumSyntax
		}
		pos++
	}
}

func skipSpaces(s string, pos int) int {
	for pos < len(s) && s[pos] == ' ' {
		pos++
	}
	return pos
}

// quoted reads a single-quoted ClickHouse string literal; a quote is escaped by a backslash or doubled.
func quoted(s string, pos int) (string, int, error) {
	if pos >= len(s) || s[pos] != '\'' {
		return "", pos, errEnumSyntax
	}
	var b strings.Builder
	for i := pos + 1; i < len(s); i++ {
		switch {
		case s[i] == '\\' && i+1 < len(s):
			i++
			b.WriteByte(s[i])
		case s[i] == '\'' && i+1 < len(s) && s[i+1] == '\'':
			i++
			b.WriteByte('\'')
		case s[i] == '\'':
			return b.String(), i + 1, nil
		default:
			b.WriteByte(s[i])
		}
	}
	return "", pos, errEnumSyntax
}
