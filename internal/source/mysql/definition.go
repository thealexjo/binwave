package mysql

import (
	"bytes"
	"slices"

	"github.com/go-mysql-org/go-mysql/replication"
)

// sameDefinition reports whether two TABLE_MAP events describe the same table structure. The
// table id and flags differ from event to event and say nothing about the structure. Comparing
// the fields exactly, rather than through a hash, means a schema change can never be mistaken
// for "no change". A test enumerates the fields of TableMapEvent so a new one cannot be forgotten.
func sameDefinition(a, b *replication.TableMapEvent) bool {
	return a.ColumnCount == b.ColumnCount &&
		bytes.Equal(a.Schema, b.Schema) &&
		bytes.Equal(a.Table, b.Table) &&
		bytes.Equal(a.ColumnType, b.ColumnType) &&
		slices.Equal(a.ColumnMeta, b.ColumnMeta) &&
		bytes.Equal(a.NullBitmap, b.NullBitmap) &&
		bytes.Equal(a.SignednessBitmap, b.SignednessBitmap) &&
		slices.Equal(a.DefaultCharset, b.DefaultCharset) &&
		slices.Equal(a.ColumnCharset, b.ColumnCharset) &&
		equalByteLists3(a.SetStrValue, b.SetStrValue) &&
		equalByteLists3(a.EnumStrValue, b.EnumStrValue) &&
		equalByteLists(a.ColumnName, b.ColumnName) &&
		slices.Equal(a.GeometryType, b.GeometryType) &&
		slices.Equal(a.PrimaryKey, b.PrimaryKey) &&
		slices.Equal(a.PrimaryKeyPrefix, b.PrimaryKeyPrefix) &&
		slices.Equal(a.EnumSetDefaultCharset, b.EnumSetDefaultCharset) &&
		slices.Equal(a.EnumSetColumnCharset, b.EnumSetColumnCharset) &&
		bytes.Equal(a.VisibilityBitmap, b.VisibilityBitmap)
}

func equalByteLists(a, b [][]byte) bool {
	return slices.EqualFunc(a, b, bytes.Equal)
}

func equalByteLists3(a, b [][][]byte) bool {
	return slices.EqualFunc(a, b, equalByteLists)
}
