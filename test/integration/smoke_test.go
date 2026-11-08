//go:build integration

package integration

import (
	"testing"

	"github.com/thealexjo/binwave/test/testenv"
)

// TestMySQLContainerMeetsSourceRequirements guards the test environment itself:
// every later integration test assumes a source configured like production.
func TestMySQLContainerMeetsSourceRequirements(t *testing.T) {
	t.Parallel()
	mysql := testenv.StartMySQL(t)

	want := map[string]string{
		"binlog_format":            "ROW",
		"binlog_row_image":         "FULL",
		"binlog_row_metadata":      "FULL",
		"gtid_mode":                "ON",
		"enforce_gtid_consistency": "ON",
	}

	for name, value := range want {
		var got string
		if err := mysql.DB.QueryRow("SELECT @@GLOBAL." + name).Scan(&got); err != nil {
			t.Fatalf("read @@%s: %v", name, err)
		}
		if got != value {
			t.Errorf("@@%s = %q, want %q", name, got, value)
		}
	}
}

func TestClickHouseContainerAcceptsQueries(t *testing.T) {
	t.Parallel()
	ch := testenv.StartClickHouse(t)

	var version string
	if err := ch.DB.QueryRow("SELECT version()").Scan(&version); err != nil {
		t.Fatalf("select version(): %v", err)
	}
	if version == "" {
		t.Error("ClickHouse returned an empty version")
	}
	t.Logf("ClickHouse %s", version)
}
