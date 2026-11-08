package migrationsim

import (
	"context"
	"fmt"
	"time"
)

// Ghost runs what gh-ost does to migrate the table with the ALTER given (its --alter): the
// changelog table, the ghost table created LIKE the original and altered, a chunked copy, the
// cut-over that swaps the tables, and the clean-up.
//
// gh-ost carries the changes of the original into the ghost table by reading the binlog itself
// and applying them; that work is row events on the ghost table, and the simulation reaches the
// same state with a catch-up under a table lock at the cut-over, which is also what makes the
// swap lose no change.
func (r *Runner) Ghost(ctx context.Context, alter string, o GhostOptions) (err error) {
	m, err := r.begin(ctx, o.Options)
	if err != nil {
		return err
	}
	defer m.end()

	m.shadow = "_" + r.table + "_gho"
	del, ghc, ghk := "_"+r.table+"_del", "_"+r.table+"_ghc", "_"+r.table+"_ghk"
	orig, gho := m.q(r.table), m.q(m.shadow)

	if !o.NoInitialDrops {
		for _, t := range []string{m.shadow, del, ghc} {
			if err := m.exec(true, "DROP TABLE IF EXISTS "+m.q(t)); err != nil {
				return err
			}
		}
	}
	created := false
	defer func() {
		if err != nil && created {
			_ = m.exec(true, "DROP TABLE IF EXISTS "+gho)
			_ = m.exec(true, "DROP TABLE IF EXISTS "+m.q(ghc))
		}
	}()

	if err := m.exec(true, fmt.Sprintf("create /* gh-ost */ table %s (\n\t\t\tid bigint unsigned auto_increment,\n\t\t\tlast_update timestamp not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n\t\t\thint varchar(64) charset ascii not null,\n\t\t\tvalue varchar(4096) charset ascii not null,\n\t\t\tprimary key(id),\n\t\t\tunique key hint_uidx(hint)\n\t\t) auto_increment=256 comment='gh-ost changelog'", m.q(ghc))); err != nil {
		return err
	}
	if err := m.exec(true, fmt.Sprintf("create /* gh-ost */ table %s like %s", gho, orig)); err != nil {
		return err
	}
	created = true
	if err := m.exec(true, fmt.Sprintf("alter /* gh-ost */ table %s %s", gho, alter)); err != nil {
		return err
	}
	if err := m.learn(alter); err != nil {
		return err
	}
	if o.AfterCreate != nil {
		o.AfterCreate()
	}

	key := ident(m.keyOld)
	insert := func() string {
		return fmt.Sprintf("insert /* gh-ost %s.%s */ ignore into %s (%s) (select %s from %s force index (`PRIMARY`) where (((%s >= ?)) and ((%s < ?))) lock in share mode)",
			ident(r.schema), ident(r.table), gho, m.cols(true), m.cols(false), orig, key, key)
	}
	heartbeat := func(int) error {
		// gh-ost writes to its changelog table all the time; the table is not replicated.
		return m.exec(false, fmt.Sprintf("insert /* gh-ost */ into %s (id, hint, value) values (NULLIF(2, 0), 'heartbeat', '%s') on duplicate key update last_update=NOW(), value=VALUES(value)",
			m.q(ghc), time.Now().UTC().Format(time.RFC3339Nano)))
	}
	if err := m.copyChunks(insert, heartbeat); err != nil {
		return err
	}
	if o.BeforeCutover != nil {
		o.BeforeCutover()
	}

	catchUp := func() error {
		if err := m.exec(false, fmt.Sprintf("delete /* gh-ost */ from %s where %s not in (select %s from %s)", gho, ident(m.keyNew), key, orig)); err != nil {
			return err
		}
		return m.exec(false, fmt.Sprintf("replace /* gh-ost */ into %s (%s) select %s from %s", gho, m.cols(true), m.cols(false), orig))
	}

	if o.TwoStep {
		if err := m.exec(false, fmt.Sprintf("LOCK TABLES %s WRITE, %s WRITE", orig, gho)); err != nil {
			return err
		}
		if err := catchUp(); err != nil {
			_ = m.exec(false, "UNLOCK TABLES")
			return err
		}
		if err := m.exec(true, fmt.Sprintf("alter /* gh-ost */ table %s rename %s", orig, ident(del))); err != nil {
			_ = m.exec(false, "UNLOCK TABLES")
			return err
		}
		// The table does not exist between the two statements, but the lock keeps the writers out.
		if err := m.exec(true, fmt.Sprintf("alter /* gh-ost */ table %s rename %s", gho, ident(r.table))); err != nil {
			_ = m.exec(false, "UNLOCK TABLES")
			return err
		}
		if err := m.exec(false, "UNLOCK TABLES"); err != nil {
			return err
		}
	} else if err := m.atomicCutOver(gho, orig, del, catchUp); err != nil {
		return err
	}
	created = false
	if o.AfterCutover != nil {
		o.AfterCutover()
	}

	if err := m.exec(true, "DROP TABLE IF EXISTS "+m.q(ghc)); err != nil {
		return err
	}
	if !o.KeepOld {
		if err := m.exec(true, "DROP TABLE IF EXISTS "+m.q(del)); err != nil {
			return err
		}
		if err := m.exec(true, "DROP TABLE IF EXISTS "+m.q(ghk)); err != nil {
			return err
		}
	}
	return nil
}

// atomicCutOver is gh-ost's default cut-over: a sentry table is created, the original and the
// ghost table are locked, and the RENAME of both is issued from another session, where it waits
// for the lock. Once it waits, the sentry table is dropped and the lock released, and the rename
// goes through before any writer that queued behind the lock. The lock makes the catch-up final.
func (m *run) atomicCutOver(gho, orig, del string, catchUp func() error) error {
	sentry := m.q(del)
	if err := m.exec(true, fmt.Sprintf("create /* gh-ost */ table %s ( id int auto_increment primary key ) engine=InnoDB comment='ghost-cut-over-sentry'", sentry)); err != nil {
		return err
	}
	if err := m.exec(false, fmt.Sprintf("LOCK TABLES %s WRITE, %s WRITE, %s WRITE", orig, gho, sentry)); err != nil {
		return err
	}
	unlock := func() { _ = m.exec(false, "UNLOCK TABLES") }
	if err := catchUp(); err != nil {
		unlock()
		return err
	}

	other, err := m.db.Conn(m.ctx)
	if err != nil {
		unlock()
		return err
	}
	defer func() { _ = other.Close() }()
	if _, err := other.ExecContext(m.ctx, "USE "+ident(m.schema)); err != nil {
		unlock()
		return err
	}
	renameSQL := fmt.Sprintf("rename /* gh-ost */ table %s to %s, %s to %s", orig, sentry, gho, orig)
	renamed := make(chan error, 1)
	go func() {
		_, err := other.ExecContext(m.ctx, renameSQL)
		renamed <- err
	}()

	// Wait until the rename is queued behind the lock; only then is it safe to release it.
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		err := m.conn.QueryRowContext(m.ctx, "SELECT COUNT(*) FROM performance_schema.processlist WHERE INFO LIKE 'rename /* gh-ost */%' AND STATE LIKE 'Waiting for%metadata lock'").Scan(&n)
		if err != nil {
			unlock()
			return fmt.Errorf("wait for the rename: %w", err)
		}
		if n > 0 {
			break
		}
		select {
		case err := <-renamed:
			unlock()
			return fmt.Errorf("the rename ended before it was queued: %w", err)
		default:
		}
		if time.Now().After(deadline) {
			unlock()
			return fmt.Errorf("the rename did not queue behind the lock")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := m.exec(true, "DROP TABLE IF EXISTS "+sentry); err != nil {
		unlock()
		return err
	}
	m.mu.Lock()
	m.log = append(m.log, Statement{SQL: renameSQL, DDL: true})
	m.mu.Unlock()
	unlock()
	if err := <-renamed; err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}
