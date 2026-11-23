package checkpoint

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCursorSurvivesJSON(t *testing.T) {
	key := []any{int64(-5), uint64(18446744073709551615), []byte{0, 1, 0xff}, "ключ ' \"", int64(0)}

	saved, err := EncodeCursor(key)
	if err != nil {
		t.Fatal(err)
	}
	text := EncodeResyncs([]Resync{{Table: "a.b", Cursor: saved}})
	entries, err := DecodeResyncs(text)
	if err != nil || len(entries) != 1 {
		t.Fatalf("DecodeResyncs = %v, %v", entries, err)
	}
	got, err := DecodeCursor(entries[0].Cursor)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, key) {
		t.Errorf("cursor %#v, want %#v: the read must continue from exactly this key", got, key)
	}
}

func TestEmptyCursorAndListAreEmpty(t *testing.T) {
	if c, err := DecodeCursor(nil); c != nil || err != nil {
		t.Errorf("DecodeCursor(nil) = %v, %v", c, err)
	}
	if EncodeResyncs(nil) != "" {
		t.Error("no resyncs must encode as empty text, so a position without them is what it always was")
	}
	if list, err := DecodeResyncs(""); list != nil || err != nil {
		t.Errorf("DecodeResyncs(\"\") = %v, %v", list, err)
	}
}

func TestDamagedResyncStateIsAnErrorNotAnEmptyList(t *testing.T) {
	if _, err := DecodeResyncs("{not json"); err == nil {
		t.Error("a damaged list was read as empty: the resync would be forgotten")
	}
	if _, err := DecodeCursor([]CursorValue{{Type: "x", Value: "1"}}); err == nil {
		t.Error("a cursor value of an unknown type was accepted")
	}
	if _, err := EncodeCursor([]any{3.5}); err == nil {
		t.Error("a float key was accepted")
	}
}

func TestResyncListIsSortedSoEqualSetsGiveEqualText(t *testing.T) {
	a := EncodeResyncs([]Resync{{Table: "b.x"}, {Table: "a.y"}})
	b := EncodeResyncs([]Resync{{Table: "a.y"}, {Table: "b.x"}})
	if a != b {
		t.Errorf("%s != %s", a, b)
	}
}

// TestTheGTIDSetOfManyResyncsIsStoredOnce: the initial load writes one entry per table, all with the
// same Since, and the saved state is one row that is written again and again — the GTID set of a busy
// source must not be repeated thousands of times in it.
func TestTheGTIDSetOfManyResyncsIsStoredOnce(t *testing.T) {
	const set = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:11-18,rotated-uuid-0000-0000-000000000001:1-1000000"
	var entries []Resync
	for i := range 200 {
		entries = append(entries, Resync{Table: fmt.Sprintf("db.t%03d", i), Shadow: "t__resync", Since: set, Final: set, Done: true})
	}

	text := EncodeResyncs(entries)
	if n := strings.Count(text, set); n != 1 {
		t.Errorf("the GTID set appears %d times in %d bytes of state, want once", n, len(text))
	}

	got, err := DecodeResyncs(text)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, entries) {
		t.Errorf("the entries did not survive the pool: %+v", got[:1])
	}
}

// TestResyncStateWrittenBeforeTheGTIDPoolIsStillRead: a restart on a new binwave must continue the
// resyncs of the state an older one saved as a bare array.
func TestResyncStateWrittenBeforeTheGTIDPoolIsStillRead(t *testing.T) {
	old := `[{"table":"shop.items","shadow":"items__resync","version":7,"since":"u:1-9","rows":3,"started":"2026-01-02T03:04:05Z"}]`
	got, err := DecodeResyncs(old)
	if err != nil {
		t.Fatal(err)
	}
	want := []Resync{{
		Table: "shop.items", Shadow: "items__resync", Version: 7, Since: "u:1-9", Rows: 3,
		Started: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("read %+v, want %+v", got, want)
	}
}

// TestTheUUIDOfAShadowTableIsSavedWithItsResync: the UUID a resync made its shadow table with comes back
// from the saved state; a state saved without it (an earlier release) reads none.
func TestTheUUIDOfAShadowTableIsSavedWithItsResync(t *testing.T) {
	entry := Resync{Table: "shop.items", Shadow: "items__resync", ShadowUUID: "0a000000-0000-4000-8000-00000000000a", Version: 7, Since: "u:1-9"}
	text := EncodeResyncs([]Resync{entry})
	if !strings.Contains(text, `"shadow_uuid":"0a000000-0000-4000-8000-00000000000a"`) {
		t.Errorf("the saved state %s does not have the UUID", text)
	}
	got, err := DecodeResyncs(text)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ShadowUUID != entry.ShadowUUID {
		t.Errorf("read %+v, want the UUID %s", got, entry.ShadowUUID)
	}
	if text := EncodeResyncs([]Resync{{Table: "shop.items", Shadow: "items__resync"}}); strings.Contains(text, "shadow_uuid") {
		t.Errorf("a state without the UUID is saved as %s", text)
	}
}

func TestResyncStateWithABrokenGTIDReferenceIsAnError(t *testing.T) {
	if _, err := DecodeResyncs(`{"gtids":["u:1-9"],"resyncs":[{"table":"a.b","since":"#4"}]}`); err == nil {
		t.Error("a reference past the end of the pool was accepted: the resync would resume at an empty position")
	}
}
