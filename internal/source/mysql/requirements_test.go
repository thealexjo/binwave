package mysql

import (
	"errors"
	"strings"
	"testing"
)

func goodVars() map[string]string {
	return map[string]string{
		"log_bin":                  "ON",
		"binlog_format":            "ROW",
		"binlog_row_image":         "FULL",
		"binlog_row_metadata":      "FULL",
		"gtid_mode":                "ON",
		"enforce_gtid_consistency": "ON",
		"binlog_row_value_options": "",
		"binlog_order_commits":     "ON",
		"log_replica_updates":      "ON",
	}
}

func TestCheckVariablesAcceptsRequiredSetup(t *testing.T) {
	if err := checkVariables(goodVars()); err != nil {
		t.Fatalf("checkVariables() = %v", err)
	}
}

func TestCheckVariablesIsCaseInsensitive(t *testing.T) {
	vars := goodVars()
	vars["binlog_format"] = "row"
	vars["gtid_mode"] = "on"
	if err := checkVariables(vars); err != nil {
		t.Fatalf("checkVariables() = %v", err)
	}
}

func TestCheckVariablesRejectsViolations(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]string)
		wantMsg string
	}{
		{"binlog off", func(v map[string]string) { v["log_bin"] = "OFF" }, "log_bin must be ON"},
		{"mixed format", func(v map[string]string) { v["binlog_format"] = "MIXED" }, `binlog_format must be ROW, got "MIXED"`},
		{"minimal image", func(v map[string]string) { v["binlog_row_image"] = "MINIMAL" }, "binlog_row_image must be FULL"},
		{"minimal metadata", func(v map[string]string) { v["binlog_row_metadata"] = "MINIMAL" }, "binlog_row_metadata must be FULL"},
		{"metadata missing", func(v map[string]string) { delete(v, "binlog_row_metadata") }, "binlog_row_metadata must be FULL, got missing"},
		{"gtid off", func(v map[string]string) { v["gtid_mode"] = "OFF" }, "gtid_mode must be ON"},
		{"gtid permissive", func(v map[string]string) { v["gtid_mode"] = "ON_PERMISSIVE" }, "gtid_mode must be ON"},
		{"consistency warn", func(v map[string]string) { v["enforce_gtid_consistency"] = "WARN" }, "enforce_gtid_consistency must be ON"},
		{"partial json", func(v map[string]string) { v["binlog_row_value_options"] = "PARTIAL_JSON" }, "binlog_row_value_options must be without PARTIAL_JSON"},
		{"partial json among options", func(v map[string]string) { v["binlog_row_value_options"] = "foo,partial_json" }, "PARTIAL_JSON"},
		{"unordered commits before 8.0.33", func(v map[string]string) { v["version"] = "8.0.32"; v["binlog_order_commits"] = "OFF" }, "binlog_order_commits must be ON"},
		{"unordered commits, version unknown", func(v map[string]string) { v["binlog_order_commits"] = "OFF" }, "binlog_order_commits must be ON"},
		{"replica updates off", func(v map[string]string) { v["log_replica_updates"] = "OFF" }, "log_replica_updates must be ON"},
		{"replica updates off, older name", func(v map[string]string) {
			delete(v, "log_replica_updates")
			v["log_slave_updates"] = "OFF"
		}, `log_replica_updates must be ON, got "OFF"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := goodVars()
			tt.mutate(vars)

			err := checkVariables(vars)

			var reqErr *RequirementsError
			if !errors.As(err, &reqErr) {
				t.Fatalf("checkVariables() = %v, want *RequirementsError", err)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q does not contain %q", err, tt.wantMsg)
			}
		})
	}
}

// TestReplicaUpdatesAreReadUnderTheOlderName: MySQL before 8.0.26 has only log_slave_updates.
func TestReplicaUpdatesAreReadUnderTheOlderName(t *testing.T) {
	vars := goodVars()
	delete(vars, "log_replica_updates")
	vars["log_slave_updates"] = "ON"
	vars["version"] = "8.0.23"
	if err := checkVariables(vars); err != nil {
		t.Fatalf("checkVariables() = %v", err)
	}
}

func TestCheckVariablesAllowsMissingRowValueOptions(t *testing.T) {
	vars := goodVars()
	delete(vars, "binlog_row_value_options") // MySQL 5.7 has no such variable
	if err := checkVariables(vars); err != nil {
		t.Fatalf("checkVariables() = %v", err)
	}
}

func TestUnorderedCommitsAreAcceptedFrom8033(t *testing.T) {
	for _, version := range []string{"8.0.33", "8.0.45", "8.4.0"} {
		vars := goodVars()
		vars["version"] = version
		vars["binlog_order_commits"] = "OFF"
		if err := checkVariables(vars); err != nil {
			t.Errorf("MySQL %s: checkVariables() = %v", version, err)
		}
	}
}

func TestCheckVariablesReportsEverythingAtOnce(t *testing.T) {
	err := checkVariables(map[string]string{"binlog_format": "STATEMENT"})

	var reqErr *RequirementsError
	if !errors.As(err, &reqErr) {
		t.Fatalf("checkVariables() = %v", err)
	}
	// Every requirement except the optional row-value options is violated.
	if got, want := len(reqErr.Problems), len(requirements)-1; got != want {
		t.Errorf("reported %d problems, want %d:\n%v", got, want, err)
	}
}

func TestVersionSupport(t *testing.T) {
	tests := []struct {
		version string
		want    bool
	}{
		{"8.0.23", true}, // the oldest supported 8.0 release
		{"8.0.22", false},
		{"8.0.0", false},
		{"8.0.28", true},
		{"8.0.29", false}, // withdrawn by Oracle
		{"8.0.29-21", false},
		{"8.0.30", true},
		{"8.0.45", true},
		{"8.0.28-20", true}, // Percona style suffix
		{"8.0.37-0ubuntu0.22.04.1", true},
		{"8.0.22-13", false},
		{"8.1.0", false}, // innovation releases between the LTS lines
		{"8.3.0", false},
		{"8.4.0", true},
		{"8.4.11", true},
		{"8.5.0", false}, // no such LTS line is supported
		{"9.0.1", false}, // 9.x is not supported
		{"9.7.0", false},
		{"5.7.44", false},
		{"10.11.6-MariaDB", false},
		{"8.0.28-MariaDB", false},
		{"garbage", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			problem := versionProblem(tt.version)
			if (problem == "") != tt.want {
				t.Errorf("versionProblem(%q) = %q, supported want %v", tt.version, problem, tt.want)
			}
			if problem != "" && !strings.Contains(problem, "8.0.23") {
				t.Errorf("the message does not say what is supported: %q", problem)
			}
		})
	}
}

func TestUnsupportedVersionIsReportedWithTheOtherProblems(t *testing.T) {
	vars := goodVars()
	vars["version"] = "8.0.22"
	vars["binlog_format"] = "MIXED"

	err := checkVariables(vars)

	var reqErr *RequirementsError
	if !errors.As(err, &reqErr) || len(reqErr.Problems) != 2 {
		t.Fatalf("checkVariables() = %v, want both problems in one RequirementsError", err)
	}
	if !strings.Contains(err.Error(), "MySQL 8.0.22 is not supported") {
		t.Errorf("the version problem is missing: %v", err)
	}
}

func TestSupportedVersionPasses(t *testing.T) {
	vars := goodVars()
	vars["version"] = "8.4.11"
	if err := checkVariables(vars); err != nil {
		t.Fatalf("checkVariables() = %v", err)
	}
}
