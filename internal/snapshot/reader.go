package snapshot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thealexjo/binwave/internal/schema"
)

// ErrPartitionGone is what ReadChunk returns when the partition it was asked for does not exist any
// more: it was dropped or reorganized while the table was being read. It is not a failure of the read —
// the rows of that partition are gone from the source too, and the statement that removed them comes
// through the stream — so the reader moves on to the next partition instead of giving up.
var ErrPartitionGone = errors.New("the partition does not exist any more")

// Source is what the reader needs of the MySQL source.
type Source interface {
	// ExecutedGTIDSet returns @@gtid_executed.
	ExecutedGTIDSet(ctx context.Context) (string, error)
	// ReadChunk returns the rows of one chunk, in key order, and their size in bytes.
	ReadChunk(ctx context.Context, req ChunkRequest) (rows [][]any, size int, err error)
}

// ChunkRequest is one chunk to read: the rows of Table past the key After (nil: from the start of the
// scope), at most Limit of them. Partition, when set, restricts the read to that partition of the
// table, which is its own key space; it is a partition the table declares (schema.Table.Partitions).
type ChunkRequest struct {
	Table     *schema.Table
	Partition string
	After     []any
	Limit     int
}

// Ranger is what a Source may also offer, to say how far along the table a read is.
type Ranger interface {
	// KeyRange returns the smallest and the largest key of a table with a single integer primary key;
	// ok is false for any other key. It is cheap: it reads two entries of the index.
	KeyRange(ctx context.Context, tbl *schema.Table) (lo, hi int64, ok bool, err error)
	// EstimatedRows is the number of rows the server believes the table has: a statistic, not a count.
	EstimatedRows(ctx context.Context, tbl *schema.Table) (int64, error)
}

// Chunk is a piece of a table.
type Chunk struct {
	// Rows are the rows in key order, as ReadChunk returns them.
	Rows [][]any
	// Watermark is the GTID set the source had executed just before the chunk was read.
	Watermark string
	// Last is the key of the last row, the cursor to continue from; nil for an empty chunk.
	Last []any
	// Done is true for the last chunk: the table has no more rows after it.
	Done bool
	// Took is how long the read took, and Limit how many rows were asked for.
	Took  time.Duration
	Limit int
	// Partition is the partition this chunk was read from, empty for a read that is not partitioned;
	// NextPartition is the one the next chunk comes from, empty when the table is read. PartitionDone
	// is set for the last chunk of a partition, which for a read that is not partitioned is the last
	// chunk of the table.
	Partition     string
	NextPartition string
	PartitionDone bool
}

// Options tune a Reader.
type Options struct {
	Sizer SizerOptions
	// Resume is where a previous read of this table stopped, as it was saved. The zero value starts a
	// read that nothing precedes.
	Resume Scope
	// MaxRowsPerSecond limits how fast this table alone is read on average; 0 is unlimited.
	MaxRowsPerSecond int
	// Budget, when set, additionally paces this Reader together with others sharing the same Budget.
	Budget *Budget
	// Now and Sleep are injectable so tests are deterministic.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}

// Reader reads one table chunk by chunk. It is not safe for concurrent use.
type Reader struct {
	src   Source
	tbl   *schema.Table
	sizer *Sizer
	opts  Options

	cursor   []any
	rows     int64
	started  time.Time
	finished bool

	// plan says how the table is read; it is decided once, in NewReader, and never per chunk.
	plan Plan
	// part is the partition being read, an index into plan.Partitions; 0 for a read that is not
	// partitioned.
	part int

	lo, hi     int64
	rangeKnown bool
	estimate   int64
	resumed    bool
}

// NewReader reads tbl from after the key cursor (nil: from the start). Options.Resume continues a read
// that was interrupted; see Resumed for whether it could be continued.
func NewReader(src Source, tbl *schema.Table, cursor []any, o Options) *Reader {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Sleep == nil {
		o.Sleep = func(ctx context.Context, d time.Duration) error {
			select {
			case <-time.After(d):
				return nil
			case <-ctx.Done():
				return fmt.Errorf("wait between chunks: %w", ctx.Err())
			}
		}
	}
	r := &Reader{src: src, tbl: tbl, sizer: NewSizer(o.Sizer), opts: o, cursor: cursor, plan: PlanFor(tbl)}
	if cursor != nil || o.Resume.Partition != "" {
		index, ok := r.plan.resume(o.Resume.Strategy, o.Resume.Partition)
		if !ok {
			// The saved cursor belongs to a scope this read does not have: another strategy, or a
			// partition that is gone. Continuing from it would leave whatever it does not cover
			// unread, so the read starts over instead. Resumed() tells the caller.
			r.cursor, r.resumed = nil, false
		} else {
			r.part, r.resumed = index, true
		}
	}
	return r
}

// Resumed reports whether the cursor NewReader was given is being continued. It is false when the
// saved position could not be continued and the read starts from the beginning of the table: the rows
// read before are in the shadow table already and are written again with the same versions, so this
// costs time, not correctness.
func (r *Reader) Resumed() bool { return r.resumed }

// Scope is the part of the table being read, for a caller that saves where a read stopped.
func (r *Reader) Scope() Scope {
	return Scope{Strategy: r.plan.Strategy, Partition: r.partition()}
}

// partition is the partition the next chunk is read from, empty when the read is not partitioned.
func (r *Reader) partition() string {
	if r.plan.Strategy != StrategyPartitioned || r.part >= len(r.plan.Partitions) {
		return ""
	}
	return r.plan.Partitions[r.part]
}

// Cursor is the key to continue from: the key of the last row of the last chunk, nil before the first.
func (r *Reader) Cursor() []any { return r.cursor }

// Next reads the next chunk. After the chunk with Done set there is nothing more to read.
func (r *Reader) Next(ctx context.Context) (Chunk, error) {
	if r.finished {
		return Chunk{Done: true}, nil
	}
	if r.started.IsZero() {
		r.started = r.opts.Now()
		r.learnRange(ctx)
	}
	if err := r.throttle(ctx); err != nil {
		return Chunk{}, err
	}
	if err := r.opts.Budget.wait(ctx); err != nil {
		return Chunk{}, err
	}

	watermark, err := r.src.ExecutedGTIDSet(ctx)
	if err != nil {
		return Chunk{}, fmt.Errorf("read the watermark: %w", err)
	}
	limit := r.sizer.Limit()
	began := r.opts.Now()
	partition := r.partition()
	rows, size, err := r.src.ReadChunk(ctx, ChunkRequest{Table: r.tbl, Partition: partition, After: r.cursor, Limit: limit})
	if errors.Is(err, ErrPartitionGone) {
		// Nothing to read: the rows of that partition are not in the source any more, and the
		// statement that dropped them is in the stream, where it discards the resync and begins it again.
		chunk := Chunk{Watermark: watermark, Limit: limit, Partition: partition}
		r.endOfScope(&chunk)
		return chunk, nil
	}
	if err != nil {
		return Chunk{}, err
	}
	took := r.opts.Now().Sub(began)
	r.sizer.Observe(len(rows), size, took)

	r.opts.Budget.add(len(rows))
	chunk := Chunk{Rows: rows, Watermark: watermark, Took: took, Limit: limit, Partition: partition}
	if len(rows) > 0 {
		chunk.Last = keyOf(r.tbl, rows[len(rows)-1])
		r.cursor = chunk.Last
		r.rows += int64(len(rows))
	}
	if len(rows) < limit {
		r.endOfScope(&chunk)
	} else {
		chunk.NextPartition = r.partition()
	}
	return chunk, nil
}

// endOfScope records that there is nothing more to read where the reader was reading. For a read by key
// that is the end of the table; for a partitioned read it is the end of one partition, and the next one
// is a key space of its own, so the cursor starts over with it and the table is read only once the last
// partition is done.
func (r *Reader) endOfScope(chunk *Chunk) {
	r.part++
	r.cursor = nil
	r.finished = r.part >= len(r.plan.Partitions)
	chunk.PartitionDone = true
	chunk.Done = r.finished
	chunk.NextPartition = r.partition()
}

func keyOf(tbl *schema.Table, row []any) []any {
	key := make([]any, len(tbl.PrimaryKey))
	for i, idx := range tbl.PrimaryKey {
		key[i] = row[idx]
	}
	return key
}

// throttle waits so that the average speed stays under MaxRowsPerSecond.
func (r *Reader) throttle(ctx context.Context) error {
	if r.opts.MaxRowsPerSecond <= 0 || r.rows == 0 {
		return nil
	}
	due := time.Duration(float64(r.rows) / float64(r.opts.MaxRowsPerSecond) * float64(time.Second))
	if wait := due - r.opts.Now().Sub(r.started); wait > 0 {
		return r.opts.Sleep(ctx, wait)
	}
	return nil
}

// learnRange reads what says how big the table is, when the source can tell. It is for progress
// only, so a failure is not one.
func (r *Reader) learnRange(ctx context.Context) {
	rg, ok := r.src.(Ranger)
	if !ok {
		return
	}
	if lo, hi, known, err := rg.KeyRange(ctx, r.tbl); err == nil && known {
		r.lo, r.hi, r.rangeKnown = lo, hi, true
	}
	if n, err := rg.EstimatedRows(ctx, r.tbl); err == nil {
		r.estimate = n
	}
}

// unfinished caps the share reported for a table that is still being read: both ways of telling how
// far a read is are approximate, and neither may claim a table is complete before it is.
const unfinished = 0.99

// Progress says how far the read is.
type Progress struct {
	// Rows read so far.
	Rows int64
	// Fraction is the share of the table that is behind the cursor, 0 to 1, or -1 when it cannot be
	// told. It is the rows read against the server's row estimate, and where the cursor is in the key
	// range only when there is no estimate (see Progress). Both are approximate, so it stays below
	// one until the last chunk is in: a table is reported done because it is done, not because a
	// number says so.
	Fraction float64
	// RowsPerSecond is the average speed of the read.
	RowsPerSecond float64
	// Remaining is the estimated time left, or 0 when it cannot be told.
	Remaining time.Duration
	// ChunkSize is the size of the next chunk.
	ChunkSize int
	// Partitions is how many partitions the read covers, and PartitionsDone how many of them are read;
	// both 0 for a table that is not partitioned. For such a table this is the honest unit of progress,
	// and where the cursor is inside the key space of one partition says nothing about the whole table.
	Partitions     int
	PartitionsDone int
}

// Progress reports how far along the table the reader is.
func (r *Reader) Progress() Progress {
	p := Progress{Rows: r.rows, Fraction: -1, ChunkSize: r.sizer.Limit()}
	if r.plan.Strategy == StrategyPartitioned {
		p.Partitions, p.PartitionsDone = len(r.plan.Partitions), min(r.part, len(r.plan.Partitions))
	}
	elapsed := r.opts.Now().Sub(r.started).Seconds()
	if elapsed > 0 && !r.started.IsZero() {
		p.RowsPerSecond = float64(r.rows) / elapsed
	}
	switch {
	case r.finished:
		p.Fraction = 1
	case r.plan.Strategy == StrategyPartitioned && r.estimate <= 0 && len(r.plan.Partitions) > 0:
		// No row estimate: partitions read against partitions to read is coarse, but it is real.
		p.Fraction = min(float64(p.PartitionsDone)/float64(p.Partitions), unfinished)
	case r.estimate > 0:
		// Rows read against what the server believes the table holds. The estimate is off by tens of
		// percent (it is a statistic of InnoDB, not a count), but it moves with the rows and cannot be
		// fooled by the shape of the key, which is why it comes first.
		p.Fraction = min(float64(r.rows)/float64(r.estimate), unfinished)
	// The key range is a fallback for a table read as one key space only: in a partitioned read the
	// cursor lives in the key space of one partition, and comparing it to the range of the whole table
	// would say anything at all.
	case r.plan.Strategy != StrategyPartitioned && r.rangeKnown && r.hi > r.lo && r.cursor != nil:
		// Where the cursor is in the key range: only right when the rows are spread like the keys. A
		// key with gaps (an AUTO_INCREMENT that deletes leave holes in) puts the cursor at the end of
		// the range with plenty of rows still to read, or at the start of a huge range with plenty read
		// — which is why this is the fallback, for a table the server has no row estimate for.
		if k, ok := asInt64(r.cursor[0]); ok {
			p.Fraction = min(max(float64(k-r.lo)/float64(r.hi-r.lo), 0), unfinished)
		}
	}
	if p.Fraction > 0 && p.Fraction < 1 && elapsed > 0 {
		p.Remaining = time.Duration(elapsed * (1 - p.Fraction) / p.Fraction * float64(time.Second))
	}
	return p
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case uint64:
		if x <= 1<<63-1 {
			return int64(x), true
		}
	case int:
		return int64(x), true
	}
	return 0, false
}

// Status is how one resync or initial load is going, for the operator.
type Status struct {
	Table string `json:"table"`
	// Kind is "resync" (binwave tables resync) or "initial_load" (replication.initial_position: snapshot,
	// on the first run).
	Kind string `json:"kind"`
	// Rows read so far, over every restart of the resync.
	Rows int64 `json:"rows"`
	// Fraction of the table that is behind the cursor, 0 to 1; -1 when it cannot be told.
	Fraction float64 `json:"fraction"`
	// RowsPerSecond is the speed of the read.
	RowsPerSecond float64 `json:"rows_per_second"`
	// RemainingSeconds is the estimated time left; 0 when it cannot be told.
	RemainingSeconds float64 `json:"remaining_seconds"`
	// ChunkSize is the size of the next chunk.
	ChunkSize int `json:"chunk_size"`
	// Partitions and PartitionsDone say how a partitioned table is read (see Progress); both 0 for a
	// table that is read as one key space.
	Partitions     int `json:"partitions,omitempty"`
	PartitionsDone int `json:"partitions_done,omitempty"`
	// Restarts counts how often a change of the table's structure sent the resync back to the start.
	Restarts int `json:"restarts"`
	// Reading is set while a reader hands chunks over: the table has one of the slots
	// (replication.snapshot.parallelism) rather than waiting for one.
	Reading bool `json:"reading"`
	// Done is set once the whole table is read: what is left is the stream reaching the point the last
	// chunk was read at, and the swap.
	Done    bool      `json:"done"`
	Started time.Time `json:"started"`
}
