package mysql

import (
	"container/list"
	"sync"
)

// stmtCache keeps a bounded set of prepared statements, so that reading a table in chunks does not
// prepare and close one statement per chunk (two round-trips each, which is most of the cost of a
// small chunk). It is bounded because a prepared statement is prepared again on every pooled
// connection it runs on and the server counts them all against max_prepared_stmt_count (16382 by
// default): the cache holds the working set — the tables being read right now — and not one
// statement per table of a source that has thousands.
//
// The least recently used statement is closed when the cache is full. A statement is moved to the
// front before it is used, so the one being evicted is the one that has not been used while the
// whole cache was used past it; if that ever races with a query on it, the query fails and is
// retried like any other read.
type stmtCache[T any] struct {
	mu      sync.Mutex
	limit   int
	order   *list.List // front is the most recently used; values are *cachedStmt[T]
	byKey   map[string]*list.Element
	release func(T) error
}

type cachedStmt[T any] struct {
	key  string
	stmt T
}

// newStmtCache returns a cache of at most limit statements, closed with release when evicted.
func newStmtCache[T any](limit int, release func(T) error) *stmtCache[T] {
	return &stmtCache[T]{limit: limit, order: list.New(), byKey: map[string]*list.Element{}, release: release}
}

// get returns the statement for key, preparing it with prepare when the cache does not have it.
func (c *stmtCache[T]) get(key string, prepare func() (T, error)) (T, error) {
	c.mu.Lock()
	if el, ok := c.byKey[key]; ok {
		c.order.MoveToFront(el)
		stmt := el.Value.(*cachedStmt[T]).stmt
		c.mu.Unlock()
		return stmt, nil
	}
	c.mu.Unlock()

	// Prepared outside the lock: it is a round-trip to the server, and another table must not wait
	// for it. Two goroutines preparing the same key at once is possible and harmless — the second
	// one to finish replaces the first, which is then closed.
	stmt, err := prepare()
	if err != nil {
		var zero T
		return zero, err
	}

	c.mu.Lock()
	var stale []T
	if el, ok := c.byKey[key]; ok {
		stale = append(stale, el.Value.(*cachedStmt[T]).stmt)
		c.order.Remove(el)
	}
	c.byKey[key] = c.order.PushFront(&cachedStmt[T]{key: key, stmt: stmt})
	for c.order.Len() > c.limit {
		oldest := c.order.Back()
		entry := oldest.Value.(*cachedStmt[T])
		c.order.Remove(oldest)
		delete(c.byKey, entry.key)
		stale = append(stale, entry.stmt)
	}
	c.mu.Unlock()

	for _, s := range stale {
		_ = c.release(s)
	}
	return stmt, nil
}

// closeAll closes every statement the cache holds and empties it. The first error is returned, after
// every statement was closed.
func (c *stmtCache[T]) closeAll() error {
	c.mu.Lock()
	stmts := make([]T, 0, c.order.Len())
	for el := c.order.Front(); el != nil; el = el.Next() {
		stmts = append(stmts, el.Value.(*cachedStmt[T]).stmt)
	}
	c.order.Init()
	c.byKey = map[string]*list.Element{}
	c.mu.Unlock()

	var first error
	for _, s := range stmts {
		if err := c.release(s); err != nil && first == nil {
			first = err
		}
	}
	return first
}
