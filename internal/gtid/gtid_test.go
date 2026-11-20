package gtid

import "testing"

func TestContains(t *testing.T) {
	const u1, u2 = "6d4d8f60-1111-2222-3333-444455556666", "7e5e9071-1111-2222-3333-444455556666"
	set, err := Parse(u1 + ":1-10:15-20:batch:1-3," + u2 + ":1-5")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		gtid string
		want bool
	}{
		{u1 + ":1", true},
		{u1 + ":10", true},
		{u1 + ":11", false},
		{u1 + ":14", false},
		{u1 + ":15", true},
		{u1 + ":20", true},
		{u1 + ":21", false},
		{u2 + ":5", true},
		{u2 + ":6", false},
		{"8f6f0182-1111-2222-3333-444455556666:1", false},
		{u1 + ":batch:1", true}, // a tagged GTID (MySQL 8.4): uuid:tag:sequence
		{u1 + ":batch:3", true},
		{u1 + ":batch:4", false},
		{u1 + ":BATCH:2", true}, // MySQL lowercases tags
		{u1 + ":other:1", false},
		{u2 + ":batch:1", false},
	}
	for _, tt := range tests {
		got, err := set.Contains(tt.gtid)
		if err != nil || got != tt.want {
			t.Errorf("Contains(%s) = %v, %v, want %v", tt.gtid, got, err, tt.want)
		}
	}
}

func TestBadInputIsAnError(t *testing.T) {
	if _, err := Parse("not a set"); err == nil {
		t.Error("Parse() accepted garbage")
	}
	set, _ := Parse("6d4d8f60-1111-2222-3333-444455556666:1-5")
	if _, err := set.Contains("garbage"); err == nil {
		t.Error("Contains() accepted a garbage GTID")
	}
}

func TestCount(t *testing.T) {
	const u1, u2 = "6d4d8f60-1111-2222-3333-444455556666", "7e5e9071-1111-2222-3333-444455556666"
	for set, want := range map[string]int64{"": 0, u1 + ":1": 1, u1 + ":1-10:15-20": 16, u1 + ":1-10," + u2 + ":5-6": 12, u1 + ":1-10:batch:1-3": 13} {
		if got, err := Count(set); err != nil || got != want {
			t.Errorf("Count(%q) = %d, %v, want %d", set, got, err, want)
		}
	}
	if _, err := Count("garbage"); err == nil {
		t.Error("Count() accepted garbage")
	}
}

func TestSubset(t *testing.T) {
	const u1, u2 = "6d4d8f60-1111-2222-3333-444455556666", "7e5e9071-1111-2222-3333-444455556666"
	for _, tt := range []struct {
		sub, super string
		want       bool
	}{
		{"", "", true},
		{"", u1 + ":1-5", true},
		{u1 + ":1-5", "", false},
		{u1 + ":1-5", u1 + ":1-10", true},
		{u1 + ":1-11", u1 + ":1-10", false},
		{u1 + ":3-4", u1 + ":1-2:3-10", true},
		{u1 + ":1-5," + u2 + ":1", u1 + ":1-10", false},
		{u1 + ":1-5," + u2 + ":1", u1 + ":1-10," + u2 + ":1-3", true},
		{u1 + ":batch:1-2", u1 + ":1-10:batch:1-3", true},
		{u1 + ":batch:1-2", u1 + ":1-10", false},
		{u1 + ":2", u1 + ":batch:1-5", false},
		{u1 + ":3:batch:2", u1 + ":1-5:batch:1-3", true},
	} {
		if got, err := Subset(tt.sub, tt.super); err != nil || got != tt.want {
			t.Errorf("Subset(%q, %q) = %v, %v, want %v", tt.sub, tt.super, got, err, tt.want)
		}
	}
	if _, err := Subset("garbage", u1+":1"); err == nil {
		t.Error("Subset() accepted garbage")
	}
}
