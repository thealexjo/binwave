package typemap

import (
	"reflect"
	"sort"
	"testing"
	"time"
	"unicode/utf8"
)

func mustResolve(t testing.TB, col Column) *Mapping {
	t.Helper()
	col.Name = "c"
	m, err := Resolve("db.t", col, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// FuzzConvertTemporal: arbitrary text never panics, results are deterministic and
// always inside the ClickHouse range.
func FuzzConvertTemporal(f *testing.F) {
	for _, seed := range []string{
		"2024-06-01 12:34:56.123456", "0000-00-00", "0000-00-00 00:00:00", "2024-00-15",
		"9999-12-31 23:59:59", "1000-01-01", "2023-02-29", "-838:59:59", "12:00:00.5", "",
	} {
		f.Add(seed)
	}
	mappings := []*Mapping{
		mustResolve(f, Column{Kind: KindDate}),
		mustResolve(f, Column{Kind: KindDateTime, FSP: 6}),
		mustResolve(f, Column{Kind: KindTimestamp}),
	}
	timeMapping := mustResolve(f, Column{Kind: KindTime, FSP: 6})

	f.Fuzz(func(t *testing.T, in string) {
		for _, m := range mappings {
			got, err := m.Convert(in)
			again, errAgain := m.Convert(in)
			if (err == nil) != (errAgain == nil) || !reflect.DeepEqual(got, again) {
				t.Fatalf("%s: non-deterministic result for %q", m.Type, in)
			}
			if err != nil {
				continue
			}
			ts := got.(time.Time)
			if ts.Before(MinDate) || ts.After(MaxDateTime) || ts.Location() != time.UTC {
				t.Fatalf("%s: %q → %s outside ClickHouse range", m.Type, in, ts)
			}
		}

		if got, err := timeMapping.Convert(in); err == nil {
			micros := got.(int64)
			if limit := int64(3020399000000); micros > limit || micros < -limit {
				t.Fatalf("TIME %q → %d outside MySQL range", in, micros)
			}
		}
	})
}

// FuzzTranscode: any bytes in any supported charset decode without panics to valid UTF-8
// (except pass-through charsets, which keep bytes as is).
func FuzzTranscode(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte{0xff, 0xfe, 0x00, 0x80})
	f.Add([]byte{0xEF, 0xF0, 0xE8})

	names := make([]string, 0, len(charsets))
	for name := range charsets {
		names = append(names, name)
	}
	sort.Strings(names)

	f.Fuzz(func(t *testing.T, raw []byte) {
		for _, name := range names {
			tc, err := newTranscoder(name)
			if err != nil {
				t.Fatal(err)
			}
			out, err := tc(raw)
			if err != nil {
				continue
			}
			if charsets[name] == nil {
				if out != string(raw) {
					t.Fatalf("%s must pass bytes through", name)
				}
				continue
			}
			if !utf8.ValidString(out) {
				t.Fatalf("%s produced invalid UTF-8 for %x", name, raw)
			}
		}
	})
}

// FuzzConvertIntegers: conversion either returns the exact same number or an error.
func FuzzConvertIntegers(f *testing.F) {
	f.Add(int64(0), uint64(0))
	f.Add(int64(-128), uint64(255))
	f.Add(int64(-9223372036854775808), uint64(18446744073709551615))

	var signed, unsigned []*Mapping
	for kind := KindTinyInt; kind <= KindBigInt; kind++ {
		signed = append(signed, mustResolve(f, Column{Kind: kind}))
		unsigned = append(unsigned, mustResolve(f, Column{Kind: kind, Unsigned: true}))
	}

	f.Fuzz(func(t *testing.T, s int64, u uint64) {
		for _, m := range append(signed, unsigned...) {
			if got, err := m.Convert(s); err == nil && reflect.ValueOf(got).Convert(reflect.TypeFor[int64]()).Int() != s {
				t.Fatalf("%s: %d → %v", m.Type, s, got)
			}
			if got, err := m.Convert(u); err == nil {
				v := reflect.ValueOf(got)
				if v.CanUint() && v.Uint() != u || v.CanInt() && (v.Int() < 0 || uint64(v.Int()) != u) {
					t.Fatalf("%s: %d → %v", m.Type, u, got)
				}
			}
		}
	})
}

// FuzzParseEnumType must never panic.
func FuzzParseEnumType(f *testing.F) {
	f.Add("Enum8('a' = 1, 'b' = 2)")
	f.Add(`Enum16('it\'s' = -1)`)
	f.Add("Enum8('")
	f.Fuzz(func(_ *testing.T, def string) {
		_, _ = parseEnumType(def)
	})
}
