package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestRowBytesGrowsWithContent(t *testing.T) {
	small := RowBytes(Row{Values: []any{int32(1), "a"}})
	large := RowBytes(Row{Values: []any{int32(1), strings.Repeat("x", 10_000)}})
	if large-small < 9_000 {
		t.Errorf("a 10 KB string added only %d bytes to the estimate", large-small)
	}
}

func TestRowBytesCoversEveryValueKind(t *testing.T) {
	kinds := []any{
		nil, int8(1), uint64(1), 1.5, true, "text", []byte("raw"),
		[]string{"a", "b"},
		decimal.RequireFromString("1.5"), time.Now(),
	}
	base := RowBytes(Row{})
	for _, v := range kinds {
		if got := RowBytes(Row{Values: []any{v}}); got < base {
			t.Errorf("%T: estimate %d is below the empty row %d", v, got, base)
		}
	}
	if RowBytes(Row{Values: []any{[]string{"aaaa", "bbbb"}}}) <= RowBytes(Row{Values: []any{[]string{}}}) {
		t.Error("array members were not counted")
	}
}
