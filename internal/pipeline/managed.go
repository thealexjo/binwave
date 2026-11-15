package pipeline

import (
	"fmt"

	"github.com/thealexjo/binwave/internal/checkpoint"
)

// The tables binwave manages are the ClickHouse tables it created or found and wrote to, by source
// table. DROP DATABASE of the source drops the ones of the dropped database and nothing else: a table
// of the same shape that binwave never wrote to is not binwave's to drop. The list is saved with the
// position (checkpoint.Position.Tables), and every change of it is safe to repeat after a crash: a
// table bound or swapped in again is added again, one dropped or renamed again is removed again. A new
// installation starts with an empty list.

// loadManaged takes the list of tables from the saved position. A position without a list was saved by
// a pre-release that kept none, and the tables it wrote cannot be told from the tables of ClickHouse
// now: the source catalog misses what the stream has yet to drop, and a shape proves nothing. So such
// a position is refused instead of guessed at.
func (p *Pipeline) loadManaged(pos checkpoint.Position) error {
	tables, known, err := checkpoint.DecodeTables(pos.Tables)
	if err != nil {
		return err
	}
	if !known {
		return fmt.Errorf("%w: the saved position was written by a pre-release of binwave, which kept no list of the ClickHouse tables it writes to; "+
			"replicate again from replication.initial_position: remove the saved position (checkpoint.file_path, or the state table checkpoint.table) and the replicated tables in ClickHouse", ErrUnsupported)
	}
	p.managed, p.managedStale = tables, false
	return nil
}

// manage records that binwave writes the source table name into the ClickHouse table t.
func (p *Pipeline) manage(name string, t checkpoint.Target) {
	if cur, ok := p.managed[name]; ok && cur == t {
		return
	}
	p.managed[name] = t
	p.managedStale = true
}

// unmanage records that the source table name has no ClickHouse table of binwave's any more.
func (p *Pipeline) unmanage(name string) {
	if _, ok := p.managed[name]; !ok {
		return
	}
	delete(p.managed, name)
	p.managedStale = true
}

// materializeManaged puts the list of tables into the position about to be saved, if it changed. The
// list goes with the next save of the position, which every change of it is followed by: the commit of
// the transaction that bound, swapped in, renamed or dropped the table.
func (p *Pipeline) materializeManaged() {
	if p.managedStale {
		p.managedStale = false
		p.committed.Tables = checkpoint.EncodeTables(p.managed)
	}
}
