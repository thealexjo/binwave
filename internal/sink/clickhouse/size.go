package clickhouse

import (
	"time"

	"github.com/shopspring/decimal"
)

const (
	rowOverhead  = 64 // slice headers, service columns and allocator slack
	valueSize    = 16 // interface value
	decimalSize  = 48
	timeSize     = 24
	defaultValue = 16
)

// RowBytes estimates what a row holds, in bytes. It only has to be consistent and roughly right: the
// limits it feeds are safety bounds, not accounting — how much memory the buffers of the pipeline may
// hold, and how many rows one block of an insert may carry (see maxBlockBytes).
func RowBytes(r Row) int {
	n := rowOverhead
	for _, v := range r.Values {
		n += valueSize + valueBytes(v)
	}
	return n
}

func valueBytes(v any) int {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return len(x)
	case []byte:
		return len(x)
	case []string:
		n := 24
		for _, s := range x {
			n += 16 + len(s)
		}
		return n
	case decimal.Decimal:
		return decimalSize
	case time.Time:
		return timeSize
	default:
		return defaultValue
	}
}
