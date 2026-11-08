// Package preflight inspects the source catalog against the configuration and reports what would
// stop or silently damage the replication of a table: no usable key, a typo in an override, a
// column ClickHouse cannot take, two tables for one target, foreign keys whose cascades never
// reach the binlog. The checks are plain functions over schema.Catalog, so they need no database.
//
// They are not part of starting the service: how long they take depends on the number of tables.
// The engine runs them right after the service is ready, and again for a table that appears later.
package preflight

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/errcode"
	"github.com/thealexjo/binwave/internal/schema"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
)

// Severity tells whether a finding stops the service.
type Severity int

// Severities.
const (
	SevWarning Severity = iota
	SevError
)

func (s Severity) String() string {
	if s == SevError {
		return "error"
	}
	return "warning"
}

// Scope says whose problem a finding is, which decides what follows from it.
type Scope int

// Scopes.
const (
	// ScopeConfig is a mistake in the configuration or a clash between settings: no single table
	// can be blamed, and the service cannot do what it was asked to. It stops the service.
	ScopeConfig Scope = iota
	// ScopeTable is a problem of one table, such as a missing key. The table can be set aside
	// (tables.on_replication_error: quarantine) while the others go on.
	ScopeTable
)

// Problem is one finding. Table is empty when it concerns the whole configuration.
type Problem struct {
	Severity Severity
	Scope    Scope
	Table    string
	// Code identifies the kind of the problem (docs/reference/error-codes.md).
	Code    errcode.Code
	Message string
}

func (p Problem) String() string {
	if p.Table == "" {
		return p.Message
	}
	return p.Table + ": " + p.Message
}

// Report is the outcome of the checks.
type Report struct {
	// Tables is the number of tables that were checked.
	Tables   int
	Problems []Problem
}

// Errors returns the findings that stop the service.
func (r *Report) Errors() []Problem { return r.bySeverity(SevError) }

// Warnings returns the findings that are only reported.
func (r *Report) Warnings() []Problem { return r.bySeverity(SevWarning) }

func (r *Report) bySeverity(s Severity) []Problem {
	var out []Problem
	for _, p := range r.Problems {
		if p.Severity == s {
			out = append(out, p)
		}
	}
	return out
}

// Log writes every finding and a summary to the log.
func (r *Report) Log(log *slog.Logger) {
	r.LogProblems(log)
	log.Info("preflight finished", "tables", r.Tables, "errors", len(r.Errors()), "warnings", len(r.Warnings()))
}

// LogProblems writes the findings without the summary, for a check of a single table.
func (r *Report) LogProblems(log *slog.Logger) {
	for _, p := range r.Problems {
		level := slog.LevelWarn
		if p.Severity == SevError {
			level = slog.LevelError
		}
		log.Log(context.Background(), level, "["+string(p.Code)+"] preflight", "table", p.Table, "code", p.Code, "detail", p.Message)
	}
}

// ConfigProblems returns the errors that are mistakes of the configuration.
func (r *Report) ConfigProblems() []Problem { return r.errorsOfScope(ScopeConfig) }

// TableProblems returns the errors that belong to one table each.
func (r *Report) TableProblems() []Problem { return r.errorsOfScope(ScopeTable) }

func (r *Report) errorsOfScope(scope Scope) []Problem {
	var out []Problem
	for _, p := range r.Problems {
		if p.Severity == SevError && p.Scope == scope {
			out = append(out, p)
		}
	}
	return out
}

// ConfigErr is Err for the configuration problems only.
func (r *Report) ConfigErr() error {
	if p := r.ConfigProblems(); len(p) > 0 {
		return &Error{Problems: p}
	}
	return nil
}

// Err returns a *Error listing every error of the report, or nil when there are none.
func (r *Report) Err() error {
	errs := r.Errors()
	if len(errs) == 0 {
		return nil
	}
	return &Error{Problems: errs}
}

// Error lists the problems that make the configuration unusable for the source. Restarting does
// not help, so callers treat it as a configuration error.
type Error struct{ Problems []Problem }

func (e *Error) Error() string {
	lines := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		lines[i] = "  - " + p.String()
	}
	return fmt.Sprintf("the source does not fit the configuration (%d problems):\n%s", len(e.Problems), strings.Join(lines, "\n"))
}

// Check runs every check over the catalog: the per-table ones and those that need to see all
// tables at once.
func Check(cat *schema.Catalog, cfg *config.Config) *Report {
	r := &Report{Tables: len(cat.Tables)}
	fks := foreignKeysByTable(cat.ForeignKeys)

	targets := map[string][]string{}
	seen := map[string]bool{}
	for _, t := range cat.Tables {
		seen[t.FullName()] = true
		r.Problems = append(r.Problems, CheckTable(t, fks[t.FullName()], cfg)...)

		db, name := chsink.TargetName(cfg, t.Database, t.Name)
		key := db + "." + name
		targets[key] = append(targets[key], t.FullName())
	}

	for target, sources := range targets {
		if len(sources) > 1 {
			sort.Strings(sources)
			r.Problems = append(r.Problems, Problem{Severity: SevError, Scope: ScopeConfig, Code: errcode.TBL401, Message: fmt.Sprintf(
				"tables %s would all be replicated into ClickHouse table %s; give them different targets with tables.overrides.<table>.target_table",
				strings.Join(sources, ", "), target)})
		}
	}

	switch {
	case len(cat.Tables) == 0:
		r.Problems = append(r.Problems, Problem{Severity: SevWarning, Message: "no existing table matches tables.include; " +
			"this is fine if the tables are created later, otherwise check the patterns and that the user has SELECT on the tables (the catalog shows nothing else)"})
	default:
		for name := range cfg.Tables.Overrides {
			if !seen[name] {
				r.Problems = append(r.Problems, Problem{Severity: SevWarning, Table: name, Code: errcode.TBL203, Message: "tables.overrides names a table that does not exist or is not replicated"})
			}
		}
	}

	sortProblems(r.Problems)
	return r
}

// CheckTable runs the checks that concern one table, given its foreign keys.
func CheckTable(t schema.CatalogTable, fks []schema.ForeignKey, cfg *config.Config) []Problem {
	full := t.FullName()
	ov := cfg.Tables.Overrides[full]
	var out []Problem
	add := func(sev Severity, scope Scope, code errcode.Code, format string, args ...any) {
		out = append(out, Problem{Severity: sev, Scope: scope, Table: full, Code: code, Message: fmt.Sprintf(format, args...)})
	}

	excluded := map[string]bool{}
	for _, c := range ov.ExcludeColumns {
		excluded[c] = true
		if !slices.Contains(t.Columns, c) {
			// A typo here would publish a column that was meant to stay private.
			add(SevError, ScopeConfig, errcode.TBL203, "tables.overrides.%s.exclude_columns names unknown column %q", full, c)
		}
	}
	for c := range ov.Columns {
		if !slices.Contains(t.Columns, c) {
			add(SevError, ScopeConfig, errcode.TBL203, "tables.overrides.%s.columns names unknown column %q", full, c)
		}
	}

	for _, c := range t.Columns {
		if excluded[c] {
			continue
		}
		if chsink.IsServiceColumn(c) {
			add(SevError, ScopeTable, errcode.TBL201, "column %q collides with a binwave service column; exclude it or rename it in the source", c)
		}
		if !chsink.Insertable(c) {
			add(SevError, ScopeTable, errcode.TBL202, "column %q cannot be inserted by the ClickHouse client library (a name with a comma, a backtick or a double quote at either end); exclude it with tables.overrides.%s.exclude_columns", c, full)
		}
	}
	if len(t.Columns) > 0 && len(excluded) >= len(t.Columns) && allExcluded(t.Columns, excluded) {
		add(SevError, ScopeConfig, errcode.TBL204, "every column is excluded")
	}

	out = append(out, checkKey(t, ov, excluded)...)
	out = append(out, checkForeignKeys(full, fks, cfg.Tables.ForeignKeyCascades)...)
	return out
}

func allExcluded(columns []string, excluded map[string]bool) bool {
	for _, c := range columns {
		if !excluded[c] {
			return false
		}
	}
	return true
}

// checkKey verifies that rows of the table can be told apart in ClickHouse.
func checkKey(t schema.CatalogTable, ov config.TableOverride, excluded map[string]bool) []Problem {
	full := t.FullName()
	problem := func(scope Scope, code errcode.Code, format string, args ...any) []Problem {
		return []Problem{{Severity: SevError, Scope: scope, Table: full, Code: code, Message: fmt.Sprintf(format, args...)}}
	}

	for _, name := range ov.OrderBy {
		switch {
		case !slices.Contains(t.Columns, name):
			return problem(ScopeConfig, errcode.TBL104, "tables.overrides.%s.order_by names unknown column %q", full, name)
		case excluded[name]:
			return problem(ScopeConfig, errcode.TBL102, "column %q is excluded but is part of the sort key; set order_by to other columns", name)
		}
	}

	if len(t.PrimaryKey) > 0 {
		// A sort key that leaves out a primary key column would merge distinct rows.
		for _, pk := range t.PrimaryKey {
			if excluded[pk] {
				return problem(ScopeConfig, errcode.TBL102, "primary key column %q is excluded, so the table cannot be replicated without it", pk)
			}
			if len(ov.OrderBy) > 0 && !slices.Contains(ov.OrderBy, pk) {
				return problem(ScopeConfig, errcode.TBL103, "order_by must include every primary key column, missing %q: a shorter or different key would merge distinct rows", pk)
			}
		}
		return nil
	}
	if len(ov.OrderBy) > 0 || t.HasKey() {
		return nil
	}
	return problem(ScopeTable, errcode.TBL101, "the table has no primary key and no unique key over NOT NULL columns, so the binlog carries no key for its rows and "+
		"ClickHouse could not tell which row an update or a delete means; add a key in the source or set tables.overrides.%s.order_by", full)
}

func checkForeignKeys(full string, fks []schema.ForeignKey, policy config.CascadePolicy) []Problem {
	if policy == config.CascadesIgnore {
		return nil
	}
	sev := SevError
	if policy == config.CascadesWarn {
		sev = SevWarning
	}
	var out []Problem
	for _, fk := range fks {
		if !fk.Cascades() {
			continue
		}
		out = append(out, Problem{Severity: sev, Scope: ScopeTable, Table: full, Code: errcode.TBL301, Message: fmt.Sprintf(
			"foreign key %s to %s has ON UPDATE %s ON DELETE %s: MySQL applies these changes inside the storage engine and does not write them to the binlog, "+
				"so ClickHouse keeps rows MySQL has deleted or changed (tables.foreign_key_cascades: %s)",
			fk.Name, fk.ReferencedTable, fk.UpdateRule, fk.DeleteRule, policy)})
	}
	return out
}

func foreignKeysByTable(all []schema.ForeignKey) map[string][]schema.ForeignKey {
	out := map[string][]schema.ForeignKey{}
	for _, fk := range all {
		key := fk.Database + "." + fk.Table
		out[key] = append(out[key], fk)
	}
	return out
}

func sortProblems(ps []Problem) {
	sort.SliceStable(ps, func(i, j int) bool {
		if ps[i].Table != ps[j].Table {
			return ps[i].Table < ps[j].Table
		}
		return ps[i].Message < ps[j].Message
	})
}
