package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

// FuzzTableFromMap feeds arbitrary, possibly inconsistent TABLE_MAP metadata to the converter.
// A corrupt or unexpected event must produce an error, never a panic that kills the process.
func FuzzTableFromMap(f *testing.F) {
	f.Add(uint8(2), []byte{0x08, 0x0f}, []byte{0, 0xa0, 0}, []byte{0x40}, []byte{45}, []byte{0})
	f.Add(uint8(3), []byte{0xfe, 0xfe, 0xf5}, []byte{0xfe, 0x10, 0xf7, 0x01, 0x04, 0}, []byte{0}, []byte{63, 45}, []byte{0, 1, 2})
	f.Add(uint8(1), []byte{0x03}, []byte{}, []byte{0x80}, []byte{}, []byte{})

	f.Fuzz(func(_ *testing.T, count uint8, types, metaBytes, nulls, charsets, pk []byte) {
		n := int(count % 12)
		ev := &replication.TableMapEvent{
			Schema:      []byte("s"),
			Table:       []byte("t"),
			ColumnCount: uint64(n),
			ColumnType:  types,
			NullBitmap:  nulls,
		}
		for i := range metaBytes {
			ev.ColumnMeta = append(ev.ColumnMeta, uint16(metaBytes[i])<<(8*(uint(i)%2))|uint16(i))
		}
		for i := range n {
			ev.ColumnName = append(ev.ColumnName, []byte{byte('a' + i)})
		}
		for _, c := range charsets {
			ev.ColumnCharset = append(ev.ColumnCharset, uint64(c))
		}
		ev.SignednessBitmap = nulls
		for _, p := range pk {
			ev.PrimaryKey = append(ev.PrimaryKey, uint64(p))
		}
		ev.EnumStrValue = [][][]byte{{[]byte("a")}}
		ev.SetStrValue = [][][]byte{{[]byte("x")}}

		// Only the absence of a panic matters here.
		_, _ = tableFromMap(ev, testCollations)
	})
}
