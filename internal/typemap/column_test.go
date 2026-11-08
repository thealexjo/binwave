package typemap

import "testing"

func TestTypeCommentRoundTrips(t *testing.T) {
	for _, c := range []Column{
		{Kind: KindInt},
		{Kind: KindBigInt, Unsigned: true},
		{Kind: KindDecimal, Precision: 65, Scale: 30},
		{Kind: KindVarChar, Length: 255},
		{Kind: KindDateTime, FSP: 6},
		{Kind: KindTimestamp},
		{Kind: KindTime, FSP: 3},
		{Kind: KindBit, Length: 12},
		{Kind: KindEnum},
		{Kind: KindJSON},
		{Kind: KindYear},
	} {
		got, ok := DecodeTypeComment(c.TypeComment())
		if !ok || got.Encode() != c.Encode() {
			t.Errorf("%q decodes to %+v (%v), want %+v", c.TypeComment(), got, ok, c)
		}
	}
	for _, bad := range []string{"", "a comment of somebody", "mysql: ", "mysql: NOPE", "mysql: DECIMAL x"} {
		if _, ok := DecodeTypeComment(bad); ok {
			t.Errorf("%q was taken for a type comment", bad)
		}
	}
}
