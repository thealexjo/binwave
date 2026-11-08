package snapshot

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/typemap"
)

// clock is a model clock that a fake source advances by the cost of what it does.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func (c *clock) Sleep(_ context.Context, d time.Duration) error {
	c.now = c.now.Add(d)
	return nil
}

// fakeSource is a table of int keys with a cost per row, on a model clock.
type fakeSource struct {
	clock   *clock
	keys    []int64 // the keys of the rows, ascending
	perRow  func(rows int) time.Duration
	gtid    int
	failAt  int // the n-th read fails, 1-based
	reads   int
	limits  []int
	rowSize int
	// estimate overrides what the server believes the table holds; zero means the true row count.
	estimate int64
	// byPartition holds the keys of each partition when the table is partitioned; keys is then the
	// union of them, as the server would return for a read of the whole table.
	byPartition map[string][]int64
	// scopes records the partition of every read, in order.
	scopes []string
	// gone are the partitions the server no longer has.
	gone map[string]bool
}

func (s *fakeSource) ExecutedGTIDSet(context.Context) (string, error) {
	s.gtid++
	return fmt.Sprintf("u:1-%d", s.gtid), nil
}

func (s *fakeSource) ReadChunk(_ context.Context, req ChunkRequest) ([][]any, int, error) {
	after, limit := req.After, req.Limit
	s.reads++
	s.limits = append(s.limits, limit)
	s.scopes = append(s.scopes, req.Partition)
	if s.reads == s.failAt {
		return nil, 0, errors.New("connection lost")
	}
	if s.gone[req.Partition] {
		return nil, 0, fmt.Errorf("read a chunk: %w: Error 1735", ErrPartitionGone)
	}
	keys := s.keys
	if req.Partition != "" {
		keys = s.byPartition[req.Partition]
	}
	var rows [][]any
	for _, k := range keys {
		if after != nil && k <= after[0].(int64) {
			continue
		}
		if len(rows) == limit {
			break
		}
		rows = append(rows, []any{k})
	}
	s.clock.now = s.clock.now.Add(s.perRow(len(rows)))
	return rows, len(rows) * s.rowSize, nil
}

func (s *fakeSource) KeyRange(context.Context, *schema.Table) (int64, int64, bool, error) {
	if len(s.keys) == 0 {
		return 0, 0, false, nil
	}
	return s.keys[0], s.keys[len(s.keys)-1], true, nil
}

func (s *fakeSource) EstimatedRows(context.Context, *schema.Table) (int64, error) {
	if s.estimate > 0 {
		return s.estimate, nil // a statistic that disagrees with the table, as InnoDB's does
	}
	return int64(len(s.keys)), nil
}

var idTable = &schema.Table{Database: "shop", Name: "t", Columns: []typemap.Column{{Name: "id", Kind: typemap.KindBigInt}}, PrimaryKey: []int{0}}

func sequence(n int, step int64) []int64 {
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = int64(i+1) * step
	}
	return keys
}

func readAll(t *testing.T, r *Reader) (rows int, chunks []Chunk) {
	t.Helper()
	for {
		c, err := r.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, c)
		rows += len(c.Rows)
		if c.Done {
			return rows, chunks
		}
	}
}

func TestReaderReadsEveryRowOnceInKeyOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		keys []int64
	}{
		{"dense", sequence(2500, 1)},
		{"sparse: every thousandth id", sequence(2500, 1000)},
		{"one row", []int64{7}},
		{"empty", nil},
		{"exactly a chunk", sequence(1000, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &clock{now: time.Unix(0, 0)}
			src := &fakeSource{clock: c, keys: tc.keys, perRow: func(n int) time.Duration { return time.Duration(n) * 50 * time.Microsecond }, rowSize: 100}
			r := NewReader(src, idTable, nil, Options{Now: c.Now, Sleep: c.Sleep})

			total, chunks := readAll(t, r)

			if total != len(tc.keys) {
				t.Fatalf("read %d rows, the table has %d", total, len(tc.keys))
			}
			var last int64
			for _, ch := range chunks {
				for _, row := range ch.Rows {
					if k := row[0].(int64); k <= last {
						t.Fatalf("key %d after %d: rows repeat or are out of order", k, last)
					}
					last = row[0].(int64)
				}
			}
			if !chunks[len(chunks)-1].Done {
				t.Error("the last chunk is not marked done")
			}
			// Reading on after the end stays at the end.
			if again, err := r.Next(context.Background()); err != nil || !again.Done || len(again.Rows) != 0 {
				t.Errorf("Next after the end = %+v, %v", again, err)
			}
		})
	}
}

func TestEveryChunkCarriesTheWatermarkOfBeforeItsRead(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	src := &fakeSource{clock: c, keys: sequence(3000, 1), perRow: func(int) time.Duration { return time.Millisecond }, rowSize: 10}
	r := NewReader(src, idTable, nil, Options{Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 1000, Max: 1000}})

	_, chunks := readAll(t, r)

	seen := map[string]bool{}
	for i, ch := range chunks {
		if ch.Watermark == "" || seen[ch.Watermark] {
			t.Errorf("chunk %d has watermark %q: each chunk needs its own, read before it", i, ch.Watermark)
		}
		seen[ch.Watermark] = true
	}
}

func TestReaderResumesFromACursor(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	src := &fakeSource{clock: c, keys: sequence(2500, 1), perRow: func(int) time.Duration { return time.Millisecond }, rowSize: 10}
	first := NewReader(src, idTable, nil, Options{Now: c.Now, Sleep: c.Sleep})
	one, err := first.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	resumed := NewReader(src, idTable, first.Cursor(), Options{Now: c.Now, Sleep: c.Sleep})
	rest, _ := readAll(t, resumed)

	if len(one.Rows)+rest != 2500 {
		t.Errorf("%d rows before the cursor and %d after it, want 2500 together", len(one.Rows), rest)
	}
}

func TestReaderFailureLeavesTheCursorWhereItWas(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	src := &fakeSource{clock: c, keys: sequence(5000, 1), perRow: func(int) time.Duration { return time.Millisecond }, rowSize: 10, failAt: 2}
	r := NewReader(src, idTable, nil, Options{Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 1000, Max: 1000}})
	if _, err := r.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	cursor := r.Cursor()

	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("the failing read did not fail")
	}
	if r.Cursor()[0] != cursor[0] {
		t.Errorf("the cursor moved to %v although the chunk was not read", r.Cursor())
	}
	if c, err := r.Next(context.Background()); err != nil || len(c.Rows) != 1000 || c.Rows[0][0].(int64) != 1001 {
		t.Errorf("the retry read %d rows from %v (%v), want the chunk that failed", len(c.Rows), c.Rows[0][0], err)
	}
}

func TestThrottleKeepsTheAverageSpeedUnderTheLimit(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	src := &fakeSource{clock: c, keys: sequence(10_000, 1), perRow: func(int) time.Duration { return time.Millisecond }, rowSize: 10}
	r := NewReader(src, idTable, nil, Options{Now: c.Now, Sleep: c.Sleep, MaxRowsPerSecond: 5000, Sizer: SizerOptions{Initial: 1000, Max: 1000}})

	total, _ := readAll(t, r)

	took := c.now.Sub(time.Unix(0, 0)).Seconds()
	if speed := float64(total) / took; speed > 5100 {
		t.Errorf("read %d rows in %.1fs = %.0f a second, the limit is 5000", total, took, speed)
	}
}

func TestProgress(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	// Keys 1000..1000000 in steps of 1000: the cursor after 250 rows is a quarter of the way in key range and in rows.
	src := &fakeSource{clock: c, keys: sequence(1000, 1000), perRow: func(n int) time.Duration { return time.Duration(n) * time.Millisecond }, rowSize: 10}
	r := NewReader(src, idTable, nil, Options{Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 250, Min: 250, Max: 250}})
	if p := r.Progress(); p.Fraction != -1 {
		t.Errorf("fraction %v before the first chunk, want unknown", p.Fraction)
	}

	if _, err := r.Next(context.Background()); err != nil {
		t.Fatal(err)
	}

	p := r.Progress()
	if p.Rows != 250 || p.Fraction < 0.24 || p.Fraction > 0.26 {
		t.Errorf("progress %+v, want a quarter", p)
	}
	if p.Remaining < 700*time.Millisecond || p.Remaining > 800*time.Millisecond {
		t.Errorf("remaining %v: 250 rows took 250ms, the other 750 should take about 750ms", p.Remaining)
	}
	if p.RowsPerSecond < 900 || p.RowsPerSecond > 1100 {
		t.Errorf("speed %v", p.RowsPerSecond)
	}
}

// TestProgressOnAKeyWithGapsGoesByRowsNotByTheKeyRange: an AUTO_INCREMENT key that deletes left
// holes in says nothing about how much of the table is read. Going by the key range reported a table
// as good as done while most of its rows were still to come, and a table with plenty of rows read as
// untouched — both seen on a real source. The rows against the server's estimate move with the read.
func TestProgressOnAKeyWithGapsGoesByRowsNotByTheKeyRange(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	// 1000 rows whose keys are all at the far end of a range that reaches 1 000 000: reading the first
	// 250 of them puts the cursor past 99% of the key range and a quarter of the rows.
	keys := make([]int64, 1000)
	for i := range keys {
		keys[i] = 999_000 + int64(i)
	}
	keys[0] = 1 // one row at the very beginning, so the range is the whole million
	src := &fakeSource{clock: c, keys: keys, perRow: func(int) time.Duration { return time.Millisecond }, rowSize: 10}
	r := NewReader(src, idTable, nil, Options{Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 250, Min: 250, Max: 250}})

	if _, err := r.Next(context.Background()); err != nil {
		t.Fatal(err)
	}

	if p := r.Progress(); p.Fraction < 0.24 || p.Fraction > 0.26 {
		t.Errorf("fraction %.2f after 250 of 1000 rows, want about a quarter: the key range says 99%%", p.Fraction)
	}
}

// TestProgressNeverClaimsAReadIsCompleteBeforeItIs: both ways of telling how far a read is are
// approximate, so neither may report a table as fully read until the last chunk is in.
func TestProgressNeverClaimsAReadIsCompleteBeforeItIs(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	// The server underestimates the table: 100 rows by its statistic, 1000 in fact.
	src := &fakeSource{clock: c, keys: sequence(1000, 1), perRow: func(int) time.Duration { return time.Millisecond }, rowSize: 10, estimate: 100}
	r := NewReader(src, idTable, nil, Options{Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 500, Min: 500, Max: 500}})

	if _, err := r.Next(context.Background()); err != nil {
		t.Fatal(err) // 500 rows read, five times what the estimate said
	}
	if p := r.Progress(); p.Fraction >= 1 {
		t.Errorf("fraction %v with rows still to read, want under 1", p.Fraction)
	}

	readAll(t, r)
	if p := r.Progress(); p.Fraction != 1 {
		t.Errorf("fraction %v once the table is read, want 1", p.Fraction)
	}
}

func TestSizerGrowsWhenChunksAreFastAndShrinksWhenTheyAreSlow(t *testing.T) {
	s := NewSizer(SizerOptions{Initial: 1000, Target: 500 * time.Millisecond, Max: 1_000_000})

	// 10 microseconds a row: 1000 rows take 10ms, the target is 500ms, so it grows, by at most twice a step.
	prev := s.Limit()
	for i := 0; i < 5; i++ {
		s.Observe(s.Limit(), s.Limit()*100, time.Duration(s.Limit())*10*time.Microsecond)
		if got := s.Limit(); got <= prev || got > prev*2 {
			t.Fatalf("step %d: %d after %d, want growth by at most a factor of two", i, got, prev)
		}
		prev = s.Limit()
	}
	for i := 0; i < 20; i++ {
		s.Observe(s.Limit(), s.Limit()*100, time.Duration(s.Limit())*10*time.Microsecond)
	}
	if got := s.Limit(); got < 45_000 || got > 55_000 {
		t.Errorf("settled at %d rows, want about 50000 (500ms at 10µs a row)", got)
	}

	// The source becomes 100 times slower: 1ms a row. The size follows, at least a quarter a step.
	for i := 0; i < 20; i++ {
		s.Observe(s.Limit(), s.Limit()*100, time.Duration(s.Limit())*time.Millisecond)
	}
	if got := s.Limit(); got < 400 || got > 600 {
		t.Errorf("settled at %d rows after the slowdown, want about 500 (500ms at 1ms a row)", got)
	}
}

func TestSizerRespectsItsBounds(t *testing.T) {
	fast := NewSizer(SizerOptions{Initial: 500, Min: 200, Max: 3000})
	for i := 0; i < 30; i++ {
		fast.Observe(fast.Limit(), fast.Limit(), time.Microsecond)
	}
	if fast.Limit() != 3000 {
		t.Errorf("a source that is always fast: %d, want the maximum 3000", fast.Limit())
	}
	slow := NewSizer(SizerOptions{Initial: 500, Min: 200, Max: 3000})
	for i := 0; i < 30; i++ {
		slow.Observe(slow.Limit(), slow.Limit(), 10*time.Second)
	}
	if slow.Limit() != 200 {
		t.Errorf("a source that is always slow: %d, want the minimum 200", slow.Limit())
	}
}

func TestSizerKeepsAChunkWithinItsByteBudget(t *testing.T) {
	s := NewSizer(SizerOptions{Initial: 1000, Max: 1_000_000, MaxBytes: 1 << 20})
	// Fast rows of 10 KB each: time allows a great many, but 1 MiB holds about 100.
	for i := 0; i < 30; i++ {
		s.Observe(s.Limit(), s.Limit()*10_000, time.Duration(s.Limit())*time.Microsecond)
	}
	if got := s.Limit(); got < 100 || got > 110 {
		t.Errorf("%d rows of 10 KB in a 1 MiB budget, want about 105 (the minimum is 100)", got)
	}
}

func TestOneSlowChunkDoesNotSendTheSizeFar(t *testing.T) {
	s := NewSizer(SizerOptions{Initial: 10_000, Target: 500 * time.Millisecond})
	for i := 0; i < 10; i++ {
		s.Observe(s.Limit(), s.Limit()*100, 500*time.Millisecond) // right on target
	}
	before := s.Limit()

	s.Observe(before, before*100, 5*time.Second) // one hiccup, ten times the target

	if got := s.Limit(); got < before/4 || got >= before {
		t.Errorf("%d after a hiccup, was %d: it must go down, by at most a quarter", got, before)
	}
}

// TestFixedSizerKeepsEverySizeWhateverTheChunksCost: a source where the cost of a chunk is in the
// round-trip and not in the rows (a read of 1000 rows takes as long as one of 100) is read far faster
// by a size an operator measured than by the rule, which would settle on the floor.
func TestFixedSizerKeepsEverySizeWhateverTheChunksCost(t *testing.T) {
	s := NewSizer(SizerOptions{Initial: 20_000, Fixed: true})
	for i := range 30 {
		// Half a second a chunk whatever its size: the rule would halve the size again and again.
		s.Observe(s.Limit(), s.Limit()*100, 500*time.Millisecond)
		if got := s.Limit(); got != 20_000 {
			t.Fatalf("chunk %d asks for %d rows, want the 20000 it was told to", i, got)
		}
	}
}

// TestFixedSizerIsStillBoundedByTheWeightOfAChunk: a chunk is held in memory whole before it is handed
// over, so the bound on its weight holds whatever size was asked for — 20000 rows of 10 KB is 200 MB.
func TestFixedSizerIsStillBoundedByTheWeightOfAChunk(t *testing.T) {
	s := NewSizer(SizerOptions{Initial: 20_000, Fixed: true, MaxBytes: 1 << 20})
	s.Observe(20_000, 20_000*10_000, time.Second)
	if got := s.Limit(); got < 90 || got > 110 {
		t.Errorf("asks for %d rows, want about 100: 1 MiB of rows of 10 KB", got)
	}

	// Without the bound the size is what was asked for, and the memory is the operator's business.
	unbounded := NewSizer(SizerOptions{Initial: 20_000, Fixed: true, MaxBytes: -1})
	unbounded.Observe(20_000, 20_000*10_000, time.Second)
	if got := unbounded.Limit(); got != 20_000 {
		t.Errorf("asks for %d rows with the bound removed, want 20000", got)
	}
}

// TestChunkSizeOverridesTheConfiguredSizeBothWays: `binwave replication chunk-size` wins over the
// configuration while the reads run, in both directions, and takes effect on the next chunk.
func TestChunkSizeOverridesTheConfiguredSizeBothWays(t *testing.T) {
	override := NewChunkSize(0)
	adaptive := NewSizer(SizerOptions{Initial: 1000, Target: 500 * time.Millisecond, Override: override})
	fixed := NewSizer(SizerOptions{Initial: 1000, Fixed: true, Override: override})

	// Nothing asked for: each sizer lives by its configuration. The chunks are slow, so the rule shrinks.
	for range 10 {
		adaptive.Observe(adaptive.Limit(), adaptive.Limit()*100, time.Second)
		fixed.Observe(fixed.Limit(), fixed.Limit()*100, time.Second)
	}
	if adaptive.Limit() >= 1000 {
		t.Errorf("the adaptive sizer asks for %d rows, want less than 1000 after slow chunks", adaptive.Limit())
	}
	if fixed.Limit() != 1000 {
		t.Errorf("the fixed sizer asks for %d rows, want 1000", fixed.Limit())
	}

	override.Set(7500)
	if adaptive.Limit() != 7500 || fixed.Limit() != 7500 {
		t.Errorf("asked for 7500 rows, got %d and %d", adaptive.Limit(), fixed.Limit())
	}

	// Back to adaptive: both follow the rule now, the one configured fixed included.
	override.Set(0)
	for range 10 {
		fixed.Observe(fixed.Limit(), fixed.Limit()*100, time.Second)
	}
	if fixed.Limit() >= 1000 {
		t.Errorf("the sizer asks for %d rows after adaptive was asked for, want less than 1000 after slow chunks", fixed.Limit())
	}
}

func TestPlanForDecidesFromTheStructureAlone(t *testing.T) {
	plain := &schema.Table{Database: "shop", Name: "t", Columns: idTable.Columns, PrimaryKey: []int{0}}
	if got := PlanFor(plain); got.Strategy != StrategyKey || len(got.Partitions) != 0 {
		t.Errorf("a table without partitions: %+v, want the key strategy", got)
	}
	parts := &schema.Table{Database: "shop", Name: "t", Columns: idTable.Columns, PrimaryKey: []int{0}, Partitions: []string{"p1", "p2"}}
	got := PlanFor(parts)
	if got.Strategy != StrategyPartitioned || !reflect.DeepEqual(got.Partitions, []string{"p1", "p2"}) {
		t.Errorf("a partitioned table: %+v, want the partitioned strategy over p1, p2", got)
	}
}

// TestAPartitionedTableIsReadPartitionByPartition: a partitioned table has an index per partition and
// no global one, so it is read one partition at a time, each as its own key space — every row exactly
// once, the cursor starting over with each partition.
func TestAPartitionedTableIsReadPartitionByPartition(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	// Keys overlap between partitions, as they do when a table is partitioned by date and not by id.
	src := &fakeSource{
		clock: c, perRow: func(int) time.Duration { return 0 }, rowSize: 8,
		byPartition: map[string][]int64{
			"p1": {1, 2, 3, 4, 5},
			"p2": {},
			"p3": {2, 3, 100},
		},
	}
	tbl := &schema.Table{
		Database: "shop", Name: "t", Columns: idTable.Columns, PrimaryKey: []int{0},
		Partitions: []string{"p1", "p2", "p3"},
	}

	r := NewReader(src, tbl, nil, Options{Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 2, Fixed: true}})
	var got []int64
	var chunks []string
	for {
		chunk, err := r.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range chunk.Rows {
			got = append(got, row[0].(int64))
		}
		chunks = append(chunks, fmt.Sprintf("%s:%d", chunk.Partition, len(chunk.Rows)))
		if chunk.Done {
			break
		}
	}

	if want := []int64{1, 2, 3, 4, 5, 2, 3, 100}; !reflect.DeepEqual(got, want) {
		t.Errorf("read %v, want %v: every row of every partition, once", got, want)
	}
	// p1 in chunks of 2, 2, 1 (the short one ends it), p2 empty, p3 in 2 and 1.
	if want := []string{"p1:2", "p1:2", "p1:1", "p2:0", "p3:2", "p3:1"}; !reflect.DeepEqual(chunks, want) {
		t.Errorf("chunks %v, want %v", chunks, want)
	}
	if p := r.Progress(); p.Partitions != 3 || p.PartitionsDone != 3 {
		t.Errorf("progress = %d/%d partitions, want 3/3", p.PartitionsDone, p.Partitions)
	}
}

// TestAPartitionedReadContinuesInThePartitionItStopped: after a restart the read goes on where it
// stopped, in the partition the cursor belongs to, and does not start the table over.
func TestAPartitionedReadContinuesInThePartitionItStopped(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	newSource := func() *fakeSource {
		return &fakeSource{
			clock: c, perRow: func(int) time.Duration { return 0 }, rowSize: 8,
			byPartition: map[string][]int64{"p1": {1, 2}, "p2": {7, 8, 9}},
		}
	}
	tbl := &schema.Table{
		Database: "shop", Name: "t", Columns: idTable.Columns, PrimaryKey: []int{0},
		Partitions: []string{"p1", "p2"},
	}

	src := newSource()
	first := NewReader(src, tbl, nil, Options{Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 1, Fixed: true}})
	for range 4 { // p1: 1, 2, the short chunk that ends it, then the first row of p2
		if _, err := first.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	scope, cursor := first.Scope(), first.Cursor()
	if scope.Partition != "p2" || len(cursor) != 1 || cursor[0].(int64) != 7 {
		t.Fatalf("stopped at %+v cursor %v, want p2 after 7", scope, cursor)
	}

	resumed := NewReader(newSource(), tbl, cursor, Options{
		Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 10, Fixed: true}, Resume: scope,
	})
	if !resumed.Resumed() {
		t.Fatal("the saved place was not continued")
	}
	chunk, err := resumed.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for _, row := range chunk.Rows {
		got = append(got, row[0].(int64))
	}
	if want := []int64{8, 9}; !reflect.DeepEqual(got, want) {
		t.Errorf("read %v after resuming, want %v: the rest of p2 and nothing of p1", got, want)
	}
}

// TestAReadStartsOverWhenItsSavedPlaceIsGone: the partition the cursor belongs to was dropped, or the
// table is read another way now. Continuing from the cursor would leave what it does not cover unread,
// so the read starts over; reading rows again is safe, skipping them is not.
func TestAReadStartsOverWhenItsSavedPlaceIsGone(t *testing.T) {
	tbl := &schema.Table{
		Database: "shop", Name: "t", Columns: idTable.Columns, PrimaryKey: []int{0},
		Partitions: []string{"p1", "p2"},
	}
	cursor := []any{int64(5)}
	tests := map[string]Scope{
		"the partition is gone":        {Strategy: StrategyPartitioned, Partition: "p_dropped"},
		"another strategy":             {Strategy: StrategyKey},
		"a partitioned read with none": {Strategy: StrategyPartitioned},
	}
	for name, resume := range tests {
		t.Run(name, func(t *testing.T) {
			r := NewReader(&fakeSource{}, tbl, cursor, Options{Resume: resume})
			if r.Resumed() {
				t.Error("the read claims to continue a place it cannot")
			}
			if r.Cursor() != nil {
				t.Errorf("cursor = %v, want none: the read starts over", r.Cursor())
			}
			if got := r.Scope(); got.Strategy != StrategyPartitioned || got.Partition != "p1" {
				t.Errorf("scope = %+v, want the first partition", got)
			}
		})
	}
}

// TestAKeyReadResumesWhateverTheSavedStateSays: state written before the strategies existed has no
// strategy, and the table it belongs to is still read as one key space. It must continue, not restart.
func TestAKeyReadResumesWhateverTheSavedStateSays(t *testing.T) {
	r := NewReader(&fakeSource{}, idTable, []any{int64(5)}, Options{})
	if !r.Resumed() {
		t.Error("a saved cursor without a strategy did not continue a read by key")
	}
	if got := r.Scope(); got.Strategy != StrategyKey || got.Partition != "" {
		t.Errorf("scope = %+v, want the key strategy", got)
	}
}

// TestAPartitionGoneIsSkippedNotAFailure: a partition dropped while the table is being read (the
// monthly rotation of a table partitioned by date) must not end the read: its rows are gone from the
// source, the statement that dropped them comes through the stream, and the rest of the table is still
// to be read.
func TestAPartitionGoneIsSkippedNotAFailure(t *testing.T) {
	c := &clock{now: time.Unix(0, 0)}
	src := &fakeSource{
		clock: c, perRow: func(int) time.Duration { return 0 }, rowSize: 8,
		byPartition: map[string][]int64{"p1": {1, 2}, "p3": {3}},
		gone:        map[string]bool{"p2": true},
	}
	tbl := &schema.Table{
		Database: "shop", Name: "t", Columns: idTable.Columns, PrimaryKey: []int{0},
		Partitions: []string{"p1", "p2", "p3"},
	}

	r := NewReader(src, tbl, nil, Options{Now: c.Now, Sleep: c.Sleep, Sizer: SizerOptions{Initial: 10, Fixed: true}})
	var got []int64
	for {
		chunk, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("the read failed on a partition that is gone: %v", err)
		}
		for _, row := range chunk.Rows {
			got = append(got, row[0].(int64))
		}
		if chunk.Done {
			break
		}
	}
	if want := []int64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("read %v, want %v: the partitions that are there", got, want)
	}
}
