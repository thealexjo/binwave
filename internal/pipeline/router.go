package pipeline

import (
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/thealexjo/binwave/internal/config"
)

// Router decides which source tables are replicated, from the include and exclude patterns and
// the shadow tables of online schema changes of the tables they select.
type Router struct {
	include, exclude []string
}

// NewRouter builds a router from the table selection of the configuration.
func NewRouter(t config.Tables) *Router {
	return &Router{include: t.Include, exclude: t.Exclude}
}

// Match reports whether changes of database.table are replicated: the table is selected by the
// patterns, or it is the shadow of a table that is.
func (r *Router) Match(database, table string) bool {
	return r.Selected(database, table) || r.IsShadow(database, table)
}

// Selected reports whether tables.include and tables.exclude select database.table: the name must
// match an include pattern and no exclude pattern. Patterns are path.Match globs over
// "database.table"; the configuration validates them, so a malformed one simply matches nothing here.
func (r *Router) Selected(database, table string) bool {
	name := database + "." + table
	if !matchAny(r.include, name) {
		return false
	}
	return !matchAny(r.exclude, name)
}

// SelectsDatabase reports whether an include pattern can select a table of the database.
func (r *Router) SelectsDatabase(database string) bool {
	for _, p := range r.include {
		dbPattern, _, ok := strings.Cut(p, ".")
		if !ok {
			continue
		}
		if matched, err := path.Match(dbPattern, database); err == nil && matched {
			return true
		}
	}
	return false
}

// IsShadow reports whether database.table is replicated only as the temporary table of an online
// schema change: it is not selected itself, no exclude pattern names it, and its name is one the
// tools give to a table that is selected.
func (r *Router) IsShadow(database, table string) bool {
	if r.Selected(database, table) || matchAny(r.exclude, database+"."+table) {
		return false
	}
	for _, base := range shadowBases(table) {
		if r.Selected(database, base) {
			return true
		}
	}
	return false
}

// maxShadowUnderscores is how many underscores pt-online-schema-change puts before the name of
// its new table: it starts with one and adds another for every name that is taken, up to ten.
const maxShadowUnderscores = 10

// reTimestampedOld is the old table of gh-ost --timestamp-old-table.
var reTimestampedOld = regexp.MustCompile(`^(.+)_\d{14}$`)

// shadowBases lists the names of the tables that the tools' default temporary tables could be
// shadows of: _t_new with more underscores in front (pt-online-schema-change), _t_old
// (pt-online-schema-change), _t_gho and _t_del (gh-ost), and _t_<timestamp>_del (gh-ost with
// --timestamp-old-table). The name of the tool's service tables (_t_ghc, _t_ghk) is not among them.
func shadowBases(name string) []string {
	var bases []string
	add := func(base string) {
		if base != "" {
			bases = append(bases, base)
		}
	}
	if rest, ok := strings.CutSuffix(name, "_new"); ok {
		for k := 1; k <= maxShadowUnderscores && strings.HasPrefix(rest, strings.Repeat("_", k)); k++ {
			add(rest[k:])
		}
	}
	for _, suffix := range []string{"_old", "_gho", "_del"} {
		rest, ok := strings.CutSuffix(name, suffix)
		if !ok || !strings.HasPrefix(rest, "_") {
			continue
		}
		rest = rest[1:]
		if suffix == "_del" {
			if m := reTimestampedOld.FindStringSubmatch(rest); m != nil {
				add(m[1])
			}
		}
		add(rest)
	}
	return bases
}

func matchAny(patterns []string, name string) bool {
	for _, p := range patterns {
		if ok, err := path.Match(p, name); err == nil && ok {
			return true
		}
	}
	return false
}

// Databases returns the databases the include patterns name literally, sorted: "shop.*" names
// shop, while "sh*.orders" names no database. They are what must exist and be visible at start.
func (r *Router) Databases() []string {
	set := map[string]bool{}
	for _, p := range r.include {
		db, _, ok := strings.Cut(p, ".")
		if ok && db != "" && !strings.ContainsAny(db, `*?[\`) {
			set[db] = true
		}
	}
	out := make([]string, 0, len(set))
	for db := range set {
		out = append(out, db)
	}
	sort.Strings(out)
	return out
}
