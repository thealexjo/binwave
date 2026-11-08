package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// requirement is one server variable binwave depends on.
type requirement struct {
	name string
	// ok reports whether the variable value (upper-cased) is acceptable; missing is true when the server does not have the variable.
	ok   func(value string, missing bool) bool
	want string
	why  string
}

func equals(want string) func(string, bool) bool {
	return func(v string, missing bool) bool { return !missing && v == want }
}

// requirements lists what binwave verifies at startup; a requirement that is not met stops the start.
var requirements = []requirement{
	{name: "log_bin", ok: equals("ON"), want: "ON", why: "the binary log must be enabled"},
	{name: "binlog_format", ok: equals("ROW"), want: "ROW", why: "only row-based events carry row values"},
	{name: "binlog_row_image", ok: equals("FULL"), want: "FULL", why: "updates and deletes need complete row images"},
	{name: "binlog_row_metadata", ok: equals("FULL"), want: "FULL", why: "column names, signedness, character sets and ENUM/SET members are read from the binlog"},
	{name: "gtid_mode", ok: equals("ON"), want: "ON", why: "the position is a GTID set"},
	{name: "enforce_gtid_consistency", ok: equals("ON"), want: "ON", why: "GTID positions are only reliable when consistency is enforced"},
	{
		name: "binlog_row_value_options",
		ok: func(v string, missing bool) bool {
			// The variable is absent before MySQL 8.0, which is fine: no partial JSON there.
			return missing || !strings.Contains(v, "PARTIAL_JSON")
		},
		want: "without PARTIAL_JSON",
		why:  "with PARTIAL_JSON the binlog carries JSON diffs instead of values",
	},
}

// versionRe reads the leading "major.minor.patch" of a server version; vendors append suffixes
// such as "-28" (Percona) or "-0ubuntu0.22.04.1".
var versionRe = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

const supportedVersions = "binwave supports MySQL 8.0.36 and newer 8.0 releases, and MySQL 8.4"

// versionProblem returns why a server version is not supported, or "" when it is. The supported
// set is 8.0.36 and later 8.0 releases plus the 8.4 LTS line; the innovation releases 8.1 to 8.3,
// older 8.0 releases, 9.x, 5.7 and MariaDB are not.
func versionProblem(version string) string {
	unsupported := fmt.Sprintf("MySQL %s is not supported: %s", version, supportedVersions)
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return "MariaDB is not supported: " + supportedVersions
	}
	m := versionRe.FindStringSubmatch(version)
	if m == nil {
		return fmt.Sprintf("cannot read the server version %q: %s", version, supportedVersions)
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	patch, _ := strconv.Atoi(m[3])

	switch {
	case major == 8 && minor == 0 && patch >= 36:
		return ""
	case major == 8 && minor == 4:
		return ""
	default:
		return unsupported
	}
}

// RequirementsError lists every unmet source requirement. Restarting cannot fix it, so callers
// treat it as a configuration error.
type RequirementsError struct {
	Problems []string
}

func (e *RequirementsError) Error() string {
	return "MySQL source does not meet binwave requirements:\n  - " + strings.Join(e.Problems, "\n  - ")
}

// checkVariables compares the server variables (upper-cased values keyed by lower-case name)
// with the requirements and reports all violations at once.
func checkVariables(vars map[string]string) error {
	var problems []string
	for _, r := range requirements {
		value, present := vars[r.name]
		if r.ok(strings.ToUpper(value), !present) {
			continue
		}
		got := "missing"
		if present {
			got = fmt.Sprintf("%q", value)
		}
		problems = append(problems, fmt.Sprintf("%s must be %s, got %s: %s", r.name, r.want, got, r.why))
	}
	if version, ok := vars["version"]; ok {
		if p := versionProblem(version); p != "" {
			problems = append([]string{p}, problems...)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return &RequirementsError{Problems: problems}
}

// readVariables fetches the global values of the variables binwave cares about.
func readVariables(ctx context.Context, db *sql.DB) (map[string]string, error) {
	names := []string{"'version'"}
	for _, r := range requirements {
		names = append(names, "'"+r.name+"'")
	}
	sort.Strings(names)

	rows, err := db.QueryContext(ctx, "SHOW GLOBAL VARIABLES WHERE Variable_name IN ("+strings.Join(names, ",")+")")
	if err != nil {
		return nil, fmt.Errorf("read source variables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	vars := make(map[string]string, len(requirements))
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("read source variables: %w", err)
		}
		vars[strings.ToLower(name)] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read source variables: %w", err)
	}
	return vars, nil
}

// CheckRequirements verifies that the server is configured the way binwave needs.
// It returns a *RequirementsError when it is not.
func CheckRequirements(ctx context.Context, db *sql.DB) error {
	vars, err := readVariables(ctx, db)
	if err != nil {
		return err
	}
	if _, ok := vars["version"]; !ok {
		return &RequirementsError{Problems: []string{"cannot read the server version: " + supportedVersions}}
	}
	return checkVariables(vars)
}
