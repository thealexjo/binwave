package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/ddl"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/typemap"
)

// rebuildTimeout bounds one attempt of a rebuild, which copies the whole table and is not cut by
// retry.attempt_timeout, an insert's limit. It is a guard against a hang, not a budget: a table
// that takes longer than a day to copy inside ClickHouse is a different problem.
const rebuildTimeout = 24 * time.Hour

// pkSpec is what the specifications of an ALTER TABLE say about the primary key.
type pkSpec struct {
	// set is true when the statement drops or adds a primary key.
	set bool
	// add are the columns of the key the statement adds; empty when it only drops the key.
	add []string
}

// planKey decides what a statement means for the sort key of the ClickHouse table, and takes the
// operations on key columns out of the change request: ClickHouse changes none of them in place, so
// they become a rebuild of the table (chsink.Client.Rebuild). It returns nil when nothing about the
// key needs doing:
//
//   - a new primary key that is the sort key the table has (or, with tables.overrides.order_by, that
//     the override contains, since the override is the sort key then) needs nothing;
//   - a column of the key that is renamed, dropped or given a wider type, or any other new primary
//     key, is a rebuild under the key the source now has. A sort key that is only finer than the new
//     primary key would identify the rows correctly, but binwave expects the sort key to be the
//     primary key (or the override), and keeping a finer one would need remembering it across
//     restarts.
//
// What cannot be rebuilt safely is a *TableError (quarantine): a type that could lose values, a
// key set by tables.overrides that does not cover the new primary key, no key at all.
func planKey(cfg *config.Config, source ddl.Name, shape chsink.TableShape, req *chsink.ChangeRequest, pk pkSpec, oldKind func(column string) (typemap.Column, bool), canRebuild bool) (*chsink.RebuildRequest, error) {
	name := source.String()
	fail := func(format string, args ...any) error {
		return &chsink.TableError{Table: name, Err: fmt.Errorf("ALTER TABLE %s: "+format, append([]any{name}, args...)...)}
	}
	inKey := map[string]bool{}
	for _, k := range shape.Key {
		inKey[k] = true
	}
	renamedTo := map[string]string{}
	renamedFrom := map[string]string{}
	for _, r := range req.Rename {
		renamedTo[r.Old], renamedFrom[r.New] = r.New, r.Old
	}

	rb := &chsink.RebuildRequest{Source: req.Source, Database: req.Database, Table: req.Table}
	var keepRename []chsink.ColumnRename
	for _, r := range req.Rename {
		if inKey[r.Old] {
			rb.Rename = append(rb.Rename, r)
		} else {
			keepRename = append(keepRename, r)
		}
	}
	dropped := map[string]bool{}
	var keepDrop []string
	for _, d := range req.Drop {
		if inKey[d] {
			rb.Drop = append(rb.Drop, d)
			dropped[d] = true
		} else {
			keepDrop = append(keepDrop, d)
		}
	}

	// The key as it stands after the renames and drops.
	var mapped []string
	now := map[string]bool{}
	for _, k := range shape.Key {
		if dropped[k] {
			continue
		}
		if to, ok := renamedTo[k]; ok {
			k = to
		}
		mapped = append(mapped, k)
		now[k] = true
	}

	var keepModify []chsink.ColumnPlan
	for _, m := range req.Modify {
		old := m.Name
		if from, ok := renamedFrom[m.Name]; ok {
			old = from
		}
		current, inClickHouse := shape.Columns[old]
		if !inClickHouse {
			keepModify = append(keepModify, m) // not a column of ClickHouse (excluded): nothing to do
			continue
		}
		kind, known := oldKind(old)
		conv := chsink.Classify(kind, known, current, m.Kind, m.Type)
		switch {
		case conv.Action == chsink.ActionRefuse:
			// Unlike the other refusals in this function, a resync fixes this on its own: it converts
			// against the type the column is now, not against what an ALTER's own arithmetic would be.
			return nil, &chsink.TableError{Table: name, Err: fmt.Errorf(
				"ALTER TABLE %s: column %s changes from %s to %s: %s; the table is set aside until it is resynchronized (binwave tables resync)",
				name, m.Name, current, m.Type, conv.Reason), Recoverable: true}
		case conv.Action == chsink.ActionNothing:
			keepModify = append(keepModify, m) // the comment that says what the column is now still changes
		case conv.Action == chsink.ActionInPlace && !now[m.Name]:
			keepModify = append(keepModify, m)
		default:
			// A conversion that needs a copy, or a column of the sort key, which ClickHouse does not change in place.
			if !canRebuild {
				return nil, fail("column %s changes from %s to %s, which needs the table to be rebuilt, and ddl.mode %s does not rebuild tables (ddl.mode full does)", m.Name, current, m.Type, cfg.DDL.Mode)
			}
			if conv.Action == chsink.ActionInPlace {
				conv = chsink.Classify(kind, known, current, m.Kind, m.Type)
				conv.Expr = "accurateCast({col}, '" + m.Type + "')"
			}
			m.Convert = conv.Expr
			rb.Modify = append(rb.Modify, m)
		}
	}
	req.Rename, req.Drop, req.Modify = keepRename, keepDrop, keepModify

	rebuild := len(rb.Rename)+len(rb.Drop)+len(rb.Modify) > 0
	key := mapped
	if pk.set {
		if override := cfg.Tables.Overrides[name].OrderBy; len(override) > 0 {
			// The sort key is the override, whatever the primary key is, as long as it has every column of it.
			for _, c := range pk.add {
				if !contains(override, c) {
					return nil, fail("the new primary key (%v) is not covered by tables.overrides.%s.order_by; add its columns to it", pk.add, name)
				}
			}
		} else if !sameOrder(pk.add, mapped) {
			key, rebuild = pk.add, true
		}
	}
	if !rebuild {
		return nil, nil
	}
	if !canRebuild {
		return nil, fail("the primary key or a column of it changes, which needs the table to be rebuilt, and ddl.mode %s does not rebuild tables (ddl.mode full does)", cfg.DDL.Mode)
	}
	if len(key) == 0 {
		return nil, fail("the table would have no primary key, and binwave has nothing to tell its rows apart with: set tables.overrides.%s.order_by", name)
	}
	rb.Key = key
	return rb, nil
}

// rebuildTable rebuilds the ClickHouse table under the new sort key. The stream is not read while
// it runs (the DDL event is a barrier, and everything before it is written), so nothing changes the
// table meanwhile and the swap is exact; the source keeps its binary logs for the time it takes.
// The copy is done inside ClickHouse in one statement, and a transient failure repeats the rebuild,
// which is safe to repeat at every step.
func (p *Pipeline) rebuildTable(ctx context.Context, name string, req chsink.RebuildRequest) error {
	req.DryRun = p.cfg.DDL.DryRun
	req.Progress = func(uint64) { p.progress() }
	p.log.Warn("sort key changed, rebuilding table; replication waits",
		"table", name, "sort_key", req.Key)
	began := p.d.Now()
	p.d.Observer.RebuildStarted(name)
	err := p.withRetryTimeout(ctx, "rebuild table "+name, rebuildTimeout, func(actx context.Context) error {
		return p.d.Sink.Rebuild(actx, req)
	})
	p.d.Observer.RebuildFinished(name, err == nil)
	if err != nil {
		return err
	}
	p.forgetTable(name) // the structure and the key changed: the next rows bind the table again
	p.log.Info("table rebuilt", "table", name, "took", p.d.Now().Sub(began).Round(time.Millisecond))
	return nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func sameOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
