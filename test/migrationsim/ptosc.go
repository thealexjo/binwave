package migrationsim

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

var reCreateHead = regexp.MustCompile(`(?m)\ACREATE TABLE .*?\($`)

// PtOSC runs what pt-online-schema-change does to migrate the table with the ALTER given (its
// --alter, without "ALTER TABLE"): a new table created from SHOW CREATE TABLE and altered,
// three triggers that carry the changes of the original into it, a chunked copy, ANALYZE, the
// swap, and the clean-up.
//
// The name of the new table is _<table>_new, with another underscore in front for every name that
// is taken, up to ten, as the tool does.
func (r *Runner) PtOSC(ctx context.Context, alter string, o PtOSCOptions) (err error) {
	m, err := r.begin(ctx, o.Options)
	if err != nil {
		return err
	}
	defer m.end()

	for k := 1; k <= 10; k++ {
		m.shadow = strings.Repeat("_", k) + r.table + "_new"
		taken, err := m.exists(m.shadow)
		if err != nil {
			return err
		}
		if !taken {
			break
		}
		if k == 10 {
			return fmt.Errorf("no free name for the new table of %s", r.table)
		}
	}
	oldName := "_" + r.table + "_old"
	trig := func(kind string) string { return fmt.Sprintf("pt_osc_%s_%s_%s", r.schema, r.table, kind) }

	created := false
	defer func() {
		if err != nil && created {
			// The tool cleans up after a failure: the triggers and the new table.
			for _, k := range []string{"del", "upd", "ins"} {
				_ = m.exec(true, "DROP TRIGGER IF EXISTS "+m.q(trig(k)))
			}
			_ = m.exec(true, "DROP TABLE IF EXISTS "+m.q(m.shadow))
		}
	}()

	var showName, showSQL string
	if err := m.conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+m.q(r.table)).Scan(&showName, &showSQL); err != nil {
		return fmt.Errorf("show create table: %w", err)
	}
	create := reCreateHead.ReplaceAllString(showSQL, "CREATE TABLE "+m.q(m.shadow)+" (")
	// Constraint names are unique in a database, so the new table's are prefixed.
	create = strings.ReplaceAll(create, "CONSTRAINT `", "CONSTRAINT `_")
	if err := m.exec(true, create); err != nil {
		return err
	}
	created = true
	if err := m.exec(true, "ALTER TABLE "+m.q(m.shadow)+" "+alter); err != nil {
		return err
	}
	if err := m.learn(alter); err != nil {
		return err
	}

	var user string
	if err := m.conn.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&user); err != nil {
		return err
	}
	at := strings.LastIndex(user, "@")
	definer := "`" + user[:at] + "`@`" + user[at+1:] + "`"
	key, keyNew := ident(m.keyOld), ident(m.keyNew)
	newTable, oldTable := m.q(m.shadow), m.q(r.table)
	values := func() string {
		var v []string
		for _, p := range m.pairs {
			v = append(v, "NEW."+ident(p.old))
		}
		return strings.Join(v, ", ")
	}
	replace := fmt.Sprintf("REPLACE INTO %s (%s) VALUES (%s);", newTable, m.cols(true), values())
	const handler = "DECLARE CONTINUE HANDLER FOR 1146 begin end;"
	for _, t := range []struct{ kind, event, body string }{
		{"del", "DELETE", fmt.Sprintf("DELETE IGNORE FROM %s WHERE %s.%s <=> OLD.%s; END", newTable, newTable, keyNew, key)},
		{"upd", "UPDATE", fmt.Sprintf("DELETE IGNORE FROM %s WHERE !(OLD.%s <=> NEW.%s) AND %s.%s <=> OLD.%s; %s END", newTable, key, key, newTable, keyNew, key, replace)},
		{"ins", "INSERT", replace + "END"},
	} {
		stmt := fmt.Sprintf("CREATE DEFINER=%s TRIGGER %s AFTER %s ON %s FOR EACH ROW BEGIN %s %s", definer, ident(trig(t.kind)), t.event, oldTable, handler, t.body)
		if err := m.exec(true, stmt); err != nil {
			return err
		}
	}
	if o.AfterCreate != nil {
		o.AfterCreate()
	}

	insert := func() string {
		return fmt.Sprintf("INSERT LOW_PRIORITY IGNORE INTO %s (%s) SELECT %s FROM %s FORCE INDEX (`PRIMARY`) WHERE ((%s >= ?)) AND ((%s < ?)) LOCK IN SHARE MODE /*pt-online-schema-change 1 copy table*/",
			newTable, m.cols(true), m.cols(false), oldTable, key, key)
	}
	if err := m.copyChunks(insert, nil); err != nil {
		return err
	}
	if err := m.exec(true, "ANALYZE TABLE "+newTable+" /* pt-online-schema-change */"); err != nil {
		return err
	}
	if o.BeforeCutover != nil {
		o.BeforeCutover()
	}

	if o.DropSwap {
		if err := m.exec(false, "SET SESSION foreign_key_checks = 0"); err != nil {
			return err
		}
		if err := m.exec(true, "DROP TABLE IF EXISTS "+oldTable); err != nil {
			return err
		}
		if err := m.exec(true, "RENAME TABLE "+newTable+" TO "+oldTable); err != nil {
			return err
		}
	} else if err := m.exec(true, fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s", oldTable, m.q(oldName), newTable, oldTable)); err != nil {
		return err
	}
	created = false // swapped: nothing of the new table is left to clean up
	if o.AfterCutover != nil {
		o.AfterCutover()
	}

	if !o.DropSwap && !o.KeepOld {
		if err := m.exec(true, "DROP TABLE IF EXISTS "+m.q(oldName)); err != nil {
			return err
		}
	}
	for _, k := range []string{"del", "upd", "ins"} {
		if err := m.exec(true, "DROP TRIGGER IF EXISTS "+m.q(trig(k))); err != nil {
			return err
		}
	}
	return nil
}
