package mysql

import (
	"reflect"
	"slices"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// ignoredFields do not describe the table structure: the id changes with every table open and
// the flags are transport details.
var ignoredFields = []string{"TableID", "Flags"}

// definitionMutations change one structural field of a TABLE_MAP event, keyed by field name.
var definitionMutations = map[string]func(*replication.TableMapEvent){
	"Schema":      func(e *replication.TableMapEvent) { e.Schema = []byte("other") },
	"Table":       func(e *replication.TableMapEvent) { e.Table = []byte("other") },
	"ColumnCount": func(e *replication.TableMapEvent) { e.ColumnCount++ },
	"ColumnType": func(e *replication.TableMapEvent) {
		e.ColumnType = append([]byte{}, e.ColumnType...)
		e.ColumnType[0]++
	},
	"ColumnMeta":       func(e *replication.TableMapEvent) { e.ColumnMeta = slices.Clone(e.ColumnMeta); e.ColumnMeta[0]++ },
	"NullBitmap":       func(e *replication.TableMapEvent) { e.NullBitmap = []byte{0xff} },
	"SignednessBitmap": func(e *replication.TableMapEvent) { e.SignednessBitmap = []byte{0x80} },
	"DefaultCharset":   func(e *replication.TableMapEvent) { e.DefaultCharset = []uint64{8} },
	"ColumnCharset":    func(e *replication.TableMapEvent) { e.ColumnCharset = []uint64{8, 8} },
	"SetStrValue":      func(e *replication.TableMapEvent) { e.SetStrValue = [][][]byte{{[]byte("z")}} },
	"EnumStrValue": func(e *replication.TableMapEvent) {
		e.EnumStrValue = [][][]byte{{[]byte("a"), []byte("b"), []byte("new")}}
	},
	"ColumnName":            func(e *replication.TableMapEvent) { e.ColumnName = [][]byte{[]byte("renamed")} },
	"GeometryType":          func(e *replication.TableMapEvent) { e.GeometryType = []uint64{1} },
	"PrimaryKey":            func(e *replication.TableMapEvent) { e.PrimaryKey = []uint64{1} },
	"PrimaryKeyPrefix":      func(e *replication.TableMapEvent) { e.PrimaryKeyPrefix = []uint64{5} },
	"EnumSetDefaultCharset": func(e *replication.TableMapEvent) { e.EnumSetDefaultCharset = []uint64{45} },
	"EnumSetColumnCharset":  func(e *replication.TableMapEvent) { e.EnumSetColumnCharset = []uint64{45} },
	"VisibilityBitmap":      func(e *replication.TableMapEvent) { e.VisibilityBitmap = []byte{0x00} },
}

func sampleMap() *replication.TableMapEvent {
	ev := buildMap([]uint64{0},
		mcol{name: "id", typ: gomysql.MYSQL_TYPE_LONG},
		mcol{name: "kind", typ: gomysql.MYSQL_TYPE_STRING, meta: uint16(gomysql.MYSQL_TYPE_ENUM)<<8 | 1, enum: []string{"a", "b"}},
	)
	ev.TableID = 10
	ev.Flags = 1
	return ev
}

func TestSameDefinitionIgnoresOnlyTransportFields(t *testing.T) {
	a, b := sampleMap(), sampleMap()
	b.TableID, b.Flags = 999, 0
	if !sameDefinition(a, b) {
		t.Error("events with equal structure but a different table id are reported as different")
	}
}

func TestSameDefinitionNoticesEveryStructuralField(t *testing.T) {
	for name, mutate := range definitionMutations {
		t.Run(name, func(t *testing.T) {
			a, b := sampleMap(), sampleMap()
			mutate(b)
			if sameDefinition(a, b) {
				t.Errorf("a change of %s went unnoticed: a schema change could be missed", name)
			}
		})
	}
}

// TestDefinitionCoversEveryFieldOfTableMapEvent fails when go-mysql adds a field to TableMapEvent:
// the new field must be either compared or explicitly declared irrelevant.
func TestDefinitionCoversEveryFieldOfTableMapEvent(t *testing.T) {
	typ := reflect.TypeFor[replication.TableMapEvent]()
	for i := range typ.NumField() {
		field := typ.Field(i)
		if !field.IsExported() || slices.Contains(ignoredFields, field.Name) {
			continue
		}
		if _, ok := definitionMutations[field.Name]; !ok {
			t.Errorf("TableMapEvent.%s is neither compared by sameDefinition nor listed as ignored; update definition.go and this test", field.Name)
		}
	}
}
