package typemap

import (
	"fmt"
	"strings"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
)

// charsets maps MySQL character set names to decoders. A nil encoding means the bytes
// are passed through unchanged (UTF-8 compatible or binary).
var charsets = map[string]encoding.Encoding{
	"utf8mb4": nil,
	"utf8mb3": nil,
	"utf8":    nil,
	"ascii":   nil,
	"binary":  nil,

	// MySQL "latin1" is actually cp1252.
	"latin1":   charmap.Windows1252,
	"latin2":   charmap.ISO8859_2,
	"latin5":   charmap.ISO8859_9,
	"latin7":   charmap.ISO8859_13,
	"greek":    charmap.ISO8859_7,
	"hebrew":   charmap.ISO8859_8,
	"cp1250":   charmap.Windows1250,
	"cp1251":   charmap.Windows1251,
	"cp1256":   charmap.Windows1256,
	"cp1257":   charmap.Windows1257,
	"cp850":    charmap.CodePage850,
	"cp852":    charmap.CodePage852,
	"cp866":    charmap.CodePage866,
	"koi8r":    charmap.KOI8R,
	"koi8u":    charmap.KOI8U,
	"macroman": charmap.Macintosh,
	"tis620":   charmap.Windows874,

	"ucs2":    unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
	"utf16":   unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
	"utf16le": unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM),
	"utf32":   utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM),

	"gbk":     simplifiedchinese.GBK,
	"gb2312":  simplifiedchinese.GBK,
	"gb18030": simplifiedchinese.GB18030,
	"big5":    traditionalchinese.Big5,
	"sjis":    japanese.ShiftJIS,
	"cp932":   japanese.ShiftJIS,
	"ujis":    japanese.EUCJP,
	"eucjpms": japanese.EUCJP,
	"euckr":   korean.EUCKR,
}

// transcoder converts raw column bytes to a UTF-8 string.
type transcoder func(raw []byte) (string, error)

func newTranscoder(charset string) (transcoder, error) {
	name := strings.ToLower(strings.TrimSpace(charset))
	if name == "" {
		name = "utf8mb4"
	}
	enc, ok := charsets[name]
	if !ok {
		return nil, fmt.Errorf("%w: character set %q", ErrUnsupported, charset)
	}
	if enc == nil {
		return func(raw []byte) (string, error) { return string(raw), nil }, nil
	}
	return func(raw []byte) (string, error) {
		// A fresh decoder per call: decoders are stateful and not safe for concurrent use.
		out, err := enc.NewDecoder().Bytes(raw)
		if err != nil {
			return "", fmt.Errorf("decode %s: %w", name, err)
		}
		return string(out), nil
	}, nil
}
