package buildinfo

import (
	"runtime"
	"testing"
)

func TestInfoString(t *testing.T) {
	info := Info{Version: "1.2.3", Commit: "abc123", Date: "2026-09-14", GoVersion: "go1.27.1"}

	want := "binwave 1.2.3 (commit abc123, built 2026-09-14, go1.27.1)"
	if got := info.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestGetUsesPackageVariables(t *testing.T) {
	oldVersion, oldCommit, oldDate := Version, Commit, Date
	t.Cleanup(func() { Version, Commit, Date = oldVersion, oldCommit, oldDate })

	Version, Commit, Date = "v9", "deadbeef", "today"

	got := Get()
	want := Info{Version: "v9", Commit: "deadbeef", Date: "today", GoVersion: runtime.Version()}
	if got != want {
		t.Errorf("Get() = %+v, want %+v", got, want)
	}
}
