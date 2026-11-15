package clickhouse

import "testing"

func TestIsBinwaveEngine(t *testing.T) {
	for _, tc := range []struct {
		engineFull string
		want       bool
	}{
		{"ReplacingMergeTree(_version, _is_deleted) ORDER BY id SETTINGS index_granularity = 8192", true},
		{"ReplacingMergeTree(`_version`, `_is_deleted`) ORDER BY (a, b)", true},
		{"ReplacingMergeTree ORDER BY id", false},
		{"ReplacingMergeTree() ORDER BY id", false},
		{"ReplacingMergeTree(_version) ORDER BY id", false},
		{"ReplacingMergeTree(_synced_at, _is_deleted) ORDER BY id", false},
		{"ReplacingMergeTree(_is_deleted, _version) ORDER BY id", false},
		{"ReplicatedReplacingMergeTree('/t', 'r', _version, _is_deleted) ORDER BY id", false},
		{"MergeTree ORDER BY id", false},
	} {
		if got := isBinwaveEngine(tc.engineFull); got != tc.want {
			t.Errorf("isBinwaveEngine(%q) = %v, want %v", tc.engineFull, got, tc.want)
		}
	}
	if got := engineCall("MergeTree ORDER BY extra SETTINGS index_granularity = 8192"); got != "MergeTree" {
		t.Errorf("engineCall() = %q", got)
	}
	if got := engineCall("ReplacingMergeTree(_version) ORDER BY id"); got != "ReplacingMergeTree(_version)" {
		t.Errorf("engineCall() = %q", got)
	}
}
