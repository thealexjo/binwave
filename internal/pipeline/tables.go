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
	// While the catalog is read, the changes of the stream are kept in their order, so that they are
	// applied again over the catalog, whenever it was read (see TablesReading).
	reading bool
	journal []tableChange
}

// tableChange is a change of the set made while the catalog is read: a table added or removed, or,
// with database, every table of a database removed.
type tableChange struct {
	add      bool
	name     string
	database string
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
	if s.reading {
		for _, name := range names {
			s.journal = append(s.journal, tableChange{add: true, name: name})
		}
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
		if s.reading {
			s.journal = append(s.journal, tableChange{name: name})
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
	if s.reading {
		s.journal = append(s.journal, tableChange{database: database})
	}
	return changed
}

// found adds the tables of a catalog, then applies again every change the stream made while it was
// read: the last change of a table is what holds, whether the catalog saw it or not.
func (s *sourceTables) found(r *Router, names []string) bool {
	journal := s.journal
	s.reading, s.journal = false, nil
	before := len(s.names)
	changed := s.add(r, names...)
	for _, c := range journal {
		switch {
		case c.database != "":
			s.removeDatabase(c.database)
		case c.add:
			s.add(r, c.name)
		default:
			s.remove(c.name)
		}
	}
	return changed || len(s.names) != before
}

// TablesReading tells the pipeline that the check after the start begins to read the catalog, before
// it does; TablesFound ends the reading. It may be called from any goroutine.
func (p *Pipeline) TablesReading() {
	p.tablesMu.Lock()
	defer p.tablesMu.Unlock()
	p.tables.reading, p.tables.journal = true, nil
}

// TablesFound adds the source tables the check after the start read from the catalog (none, when it
// could not be read); those the configuration does not select are left out. It may be called from
// any goroutine.
func (p *Pipeline) TablesFound(names []string) {
	p.tablesMu.Lock()
	defer p.tablesMu.Unlock()
	if p.tables.found(p.router, names) {
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
