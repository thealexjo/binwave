package ddl

import (
	"regexp"
	"strings"
)

// Percona Server compresses a column declared COLUMN_FORMAT COMPRESSED, optionally WITH
// COMPRESSION_DICTIONARY, and keeps the dictionaries in statements of their own. The binlog
// carries the values of such a column uncompressed, so for binwave it is an ordinary column.
var (
	reCompressed = regexp.MustCompile("(?i)\\bCOLUMN_FORMAT\\s+COMPRESSED\\b(?:\\s+WITH\\s+COMPRESSION_DICTIONARY\\s+(?:`[^`]*`|\"[^\"]*\"|[\\w$]+))?")
	reDictionary = regexp.MustCompile(`(?i)^\s*(CREATE|DROP)\s+COMPRESSION_DICTIONARY\b`)
)

// compressionDictionary returns the kind of a statement that creates or drops a compression dictionary.
func compressionDictionary(query string) (kind string, ok bool) {
	m := reDictionary.FindStringSubmatch(mask(query))
	if m == nil {
		return "", false
	}
	return strings.ToUpper(m[1]) + " COMPRESSION_DICTIONARY", true
}

// withoutCompression replaces the compression of the columns of a statement with the default
// column format; ok is false when the statement declares none.
func withoutCompression(query string) (string, bool) {
	spans := reCompressed.FindAllStringIndex(mask(query), -1)
	if spans == nil {
		return "", false
	}
	var b strings.Builder
	last := 0
	for _, s := range spans {
		b.WriteString(query[last:s[0]])
		b.WriteString("COLUMN_FORMAT DEFAULT")
		last = s[1]
	}
	b.WriteString(query[last:])
	return b.String(), true
}

// mask returns the statement with the insides of strings and quoted names replaced by x and the
// comments by spaces, byte for byte, so that a match in it is a match of the statement's own
// words at the same offsets. A versioned comment (/*!50633 ... */) is part of the statement.
func mask(query string) string {
	b := []byte(query)
	for i := 0; i < len(b); i++ {
		switch c := b[i]; {
		case c == '\'' || c == '"' || c == '`':
			for i++; i < len(b) && b[i] != c; i++ {
				if b[i] == '\\' && c != '`' && i+1 < len(b) {
					b[i] = 'x'
					i++
				}
				b[i] = 'x'
			}
		case c == '/' && i+1 < len(b) && b[i+1] == '*' && (i+2 >= len(b) || b[i+2] != '!'):
			end := strings.Index(query[i+2:], "*/")
			stop := len(b)
			if end >= 0 {
				stop = i + 2 + end + 2
			}
			for ; i < stop; i++ {
				b[i] = ' '
			}
			i--
		case c == '#' || (c == '-' && i+2 < len(b) && b[i+1] == '-' && (b[i+2] == ' ' || b[i+2] == '\t')):
			for ; i < len(b) && b[i] != '\n'; i++ {
				b[i] = ' '
			}
		}
	}
	return string(b)
}
