package pipeline

import (
	"strings"

	"github.com/thealexjo/binwave/internal/ddl"
	"github.com/thealexjo/binwave/internal/event"
)

// sourceTables is the set of source tables the configuration selects, as far as the pipeline
// knows: the ones the check after the start found, the ones bound since, and the creations, drops
// and renames of the stream. It counts the tables for the observer; it decides nothing.
type sourceTables struct {
	names map[string]bool
}

// add records the selected tables of names and reports whether the set changed.
func (s *sourceTables) add(r *Router, names ...string) bool {
	changed := false
	for _, name := range names {
		db, table, ok := cutQualified(name)
		if !ok || !r.Selected(db, table) || s.names[name] {
			continue
		}
		if s.names == nil {
			s.names = map[string]bool{}
		}
		s.names[name] = true
		changed = true
	}
	return changed
}

// remove forgets names and reports whether the set changed.
func (s *sourceTables) remove(names ...string) bool {
	changed := false
	for _, name := range names {
		if s.names[name] {
			delete(s.names, name)
			changed = true
		}
	}
	return changed
}

// removeDatabase forgets every table of database and reports whether the set changed.
func (s *sourceTables) removeDatabase(database string) bool {
	changed := false
	for name := range s.names {
		if strings.HasPrefix(name, database+".") {
			delete(s.names, name)
			changed = true
		}
	}
	return changed
}

// TablesFound adds the source tables the check after the start read from the catalog; those the
// configuration does not select are left out. It may be called from any goroutine.
func (p *Pipeline) TablesFound(names []string) {
	p.tablesMu.Lock()
	defer p.tablesMu.Unlock()
	if p.tables.add(p.router, names...) {
		p.d.Observer.TablesSelected(len(p.tables.names))
	}
}

// tableBound counts a table the pipeline bound.
func (p *Pipeline) tableBound(name string) {
	p.tablesMu.Lock()
	defer p.tablesMu.Unlock()
	if p.tables.add(p.router, name) {
		p.d.Observer.TablesSelected(len(p.tables.names))
	}
}

// countTables follows a statement that creates, drops or renames tables.
func (p *Pipeline) countTables(e event.DDL) {
	op, err := ddl.Parse(e.Query, e.Database)
	if err != nil {
		return
	}
	p.tablesMu.Lock()
	defer p.tablesMu.Unlock()
	changed := false
	switch o := op.(type) {
	case ddl.CreateTable:
		if !o.Temporary {
			changed = p.tables.add(p.router, o.Table.String())
		}
	case ddl.DropTable:
		if !o.Temporary {
			for _, t := range o.Tables {
				changed = p.tables.remove(t.String()) || changed
			}
		}
	case ddl.DropDatabase:
		changed = p.tables.removeDatabase(o.Database)
	case ddl.RenameTable:
		for _, pair := range o.Pairs {
			if p.tables.remove(pair.From.String()) {
				changed = true
			}
			changed = p.tables.add(p.router, pair.To.String()) || changed
		}
	case ddl.AlterTable:
		for _, spec := range o.Specs {
			if r, ok := spec.(ddl.RenameTo); ok {
				changed = p.tables.remove(o.Table.String()) || changed
				changed = p.tables.add(p.router, r.Table.String()) || changed
			}
		}
	}
	if changed {
		p.d.Observer.TablesSelected(len(p.tables.names))
	}
}
