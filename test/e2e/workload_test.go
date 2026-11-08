//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

const accountDDL = `CREATE TABLE binwave.acct (
	id         BIGINT UNSIGNED NOT NULL,
	owner      VARCHAR(32) NOT NULL,
	qty        INT NOT NULL,
	balance    DECIMAL(10,2) NOT NULL,
	note       TEXT NULL,
	updated_at DATETIME(3) NOT NULL,
	PRIMARY KEY (id)
) ENGINE=InnoDB`

// accounts renders binwave.acct in one canonical text form on both sides.
var accounts = comparison{
	name:  "acct",
	mysql: "SELECT id, owner, qty, CAST(balance AS CHAR), note, CAST(updated_at AS CHAR) FROM binwave.acct ORDER BY id",
	ch:    "SELECT id, owner, qty, toDecimalString(balance, 2), note, toString(updated_at) FROM replica.acct FINAL ORDER BY id",
}

// workload applies a random mix of changes to binwave.acct from several goroutines: upserts,
// updates, deletes, primary key changes, delete-and-reinsert of one key in a transaction,
// multi-row transactions, and large or NULL text. The small key space makes changes collide.
type workload struct {
	db     *sql.DB
	keys   int
	seed   int64
	ops    atomic.Int64
	failed atomic.Int64
}

// workloadKeys is the size of the key space; it is small so that changes collide.
const workloadKeys = 2000

// workloadWorkers is how many goroutines write at once.
const workloadWorkers = 4

func newWorkload(db *sql.DB, seed int64) *workload {
	return &workload{db: db, keys: workloadKeys, seed: seed}
}

// run applies changes from several workers until ctx ends.
func (w *workload) run(ctx context.Context) {
	done := make(chan struct{}, workloadWorkers)
	for i := range workloadWorkers {
		go func() {
			defer func() { done <- struct{}{} }()
			w.worker(ctx, rand.New(rand.NewSource(w.seed+int64(i))))
		}()
	}
	for range workloadWorkers {
		<-done
	}
}

func (w *workload) worker(ctx context.Context, r *rand.Rand) {
	for ctx.Err() == nil {
		if err := w.one(ctx, r); err != nil && ctx.Err() == nil {
			w.failed.Add(1) // duplicate keys and deadlocks are part of the game
		}
		w.ops.Add(1)
		time.Sleep(time.Duration(1+r.Intn(4)) * time.Millisecond)
	}
}

func (w *workload) key(r *rand.Rand) int { return 1 + r.Intn(w.keys) }

var owners = []string{"ann", "bob", "cy", "di", "ed", "Zoë", "李雷", "o'brien", `back\slash`}

func (w *workload) upsertSQL() string {
	return `INSERT INTO binwave.acct (id, owner, qty, balance, note, updated_at) VALUES (?, ?, ?, ?, ?, NOW(3)) AS new
		ON DUPLICATE KEY UPDATE owner = new.owner, qty = new.qty, balance = new.balance, note = new.note, updated_at = new.updated_at`
}

func (w *workload) note(r *rand.Rand) any {
	switch r.Intn(6) {
	case 0:
		return nil
	case 1:
		return strings.Repeat("x", 1+r.Intn(20000))
	default:
		return fmt.Sprintf("note %d", r.Intn(1000))
	}
}

func (w *workload) balance(r *rand.Rand) string {
	return fmt.Sprintf("%d.%02d", r.Intn(90000), r.Intn(100))
}

func (w *workload) one(ctx context.Context, r *rand.Rand) error {
	switch p := r.Intn(100); {
	case p < 30:
		_, err := w.db.ExecContext(ctx, w.upsertSQL(), w.key(r), owners[r.Intn(len(owners))], r.Intn(1000), w.balance(r), w.note(r))
		return err
	case p < 55:
		_, err := w.db.ExecContext(ctx, "UPDATE binwave.acct SET qty = qty + 1, balance = balance + 0.25, updated_at = NOW(3) WHERE id = ?", w.key(r))
		return err
	case p < 70:
		_, err := w.db.ExecContext(ctx, "DELETE FROM binwave.acct WHERE id = ?", w.key(r))
		return err
	case p < 80:
		_, err := w.db.ExecContext(ctx, "UPDATE binwave.acct SET id = ? WHERE id = ?", w.key(r), w.key(r))
		return err
	case p < 90:
		return w.deleteAndReinsert(ctx, r)
	default:
		return w.multiRow(ctx, r)
	}
}

// deleteAndReinsert removes a row and writes it again in one transaction, like a nightly refresh.
func (w *workload) deleteAndReinsert(ctx context.Context, r *rand.Rand) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	id := w.key(r)
	if _, err := tx.ExecContext(ctx, "DELETE FROM binwave.acct WHERE id = ?", id); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, w.upsertSQL(), id, owners[r.Intn(len(owners))], r.Intn(1000), w.balance(r), w.note(r)); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (w *workload) multiRow(ctx context.Context, r *rand.Rand) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for range 5 {
		if _, err := tx.ExecContext(ctx, w.upsertSQL(), w.key(r), owners[r.Intn(len(owners))], r.Intn(1000), w.balance(r), w.note(r)); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
