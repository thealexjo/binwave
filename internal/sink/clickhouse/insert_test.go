package clickhouse

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/schema"
	"github.com/thealexjo/binwave/internal/typemap"
)

// fakeConn is the ClickHouse connection as Insert uses it: a prepared batch and nothing else.
type fakeConn struct {
	driver.Conn
	batch *fakeBatch
}

func (c *fakeConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.batch, nil
}

// fakeBatch records how the rows were handed over: how many went into each block.
type fakeBatch struct {
	driver.Batch
	rows    int
	blocks  []int
	sent    bool
	flushed error
}

func (b *fakeBatch) Append(...any) error { b.rows++; return nil }

func (b *fakeBatch) Flush() error {
	if b.flushed != nil {
		return b.flushed
	}
	b.blocks = append(b.blocks, b.rows)
	b.rows = 0
	return nil
}

func (b *fakeBatch) Send() error {
	b.sent = true
	if b.rows > 0 {
		b.blocks = append(b.blocks, b.rows)
		b.rows = 0
	}
	return nil
}

func (b *fakeBatch) Abort() error { return nil }

func wideTable(t *testing.T) *Table {
	t.Helper()
	src := &schema.Table{
		Database: "shop", Name: "templates",
		Columns: []typemap.Column{
			{Name: "id", Kind: typemap.KindInt},
			{Name: "body", Kind: typemap.KindText, Charset: "utf8mb4"},
		},
		PrimaryKey: []int{0},
	}
	cfg := &config.Config{}
	cfg.Tables.TargetDatabase = "replica"
	tbl, err := Resolve(src, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func rowsOfSize(n, size int) []Row {
	rows := make([]Row, n)
	body := strings.Repeat("x", size)
	for i := range rows {
		rows[i] = Row{Values: []any{int32(i), body}, Version: uint64(i + 1)}
	}
	return rows
}

// TestAnInsertIsSentInBlocksThatFitTheServer: ClickHouse refuses a compressed block whose decompressed
// size is about a gigabyte ("Too large size_decompressed ... Most likely corrupted data") and closes
// the connection, which the client sees only as a reset. A table of wide text rows read in chunks of
// tens of thousands of rows makes a block over a gigabyte, and its resync cannot proceed at all. However
// many rows an insert carries, each block has to stay under the bound.
func TestAnInsertIsSentInBlocksThatFitTheServer(t *testing.T) {
	const rowSize = 1 << 20 // a megabyte per row, as a wide text column can be
	perBlock := maxBlockBytes / RowBytes(Row{Values: []any{int32(0), strings.Repeat("x", rowSize)}})
	batch := &fakeBatch{}
	c := &Client{conn: &fakeConn{batch: batch}, log: slog.New(slog.DiscardHandler), now: time.Now}

	rows := rowsOfSize(perBlock*3+7, rowSize) // three full blocks and a remainder
	if err := c.Insert(context.Background(), wideTable(t), rows); err != nil {
		t.Fatal(err)
	}

	if len(batch.blocks) < 4 {
		t.Fatalf("the insert went as %d blocks (%v), want a block per %d rows and a remainder", len(batch.blocks), batch.blocks, perBlock)
	}
	total := 0
	for i, n := range batch.blocks {
		total += n
		if n > perBlock {
			t.Errorf("block %d holds %d rows, more than the %d that fit the bound", i, n, perBlock)
		}
	}
	if total != len(rows) {
		t.Errorf("%d rows were handed over of %d", total, len(rows))
	}
	if !batch.sent {
		t.Error("the insert was never finished with Send")
	}
}

// TestASmallInsertIsOneBlock: the bound must not chop up the ordinary case — a flush of the stream is a
// handful of rows, and it goes as one block with no extra round-trip.
func TestASmallInsertIsOneBlock(t *testing.T) {
	batch := &fakeBatch{}
	c := &Client{conn: &fakeConn{batch: batch}, log: slog.New(slog.DiscardHandler), now: time.Now}

	if err := c.Insert(context.Background(), wideTable(t), rowsOfSize(100, 1000)); err != nil {
		t.Fatal(err)
	}
	if len(batch.blocks) != 1 || batch.blocks[0] != 100 {
		t.Errorf("blocks = %v, want one of 100 rows", batch.blocks)
	}
}

// TestAFailedBlockFailsTheInsert: a block that the server did not take is not silently left behind.
func TestAFailedBlockFailsTheInsert(t *testing.T) {
	boom := errors.New("connection reset by peer")
	batch := &fakeBatch{flushed: boom}
	c := &Client{conn: &fakeConn{batch: batch}, log: slog.New(slog.DiscardHandler), now: time.Now}

	const rowSize = 1 << 20
	perBlock := maxBlockBytes / RowBytes(Row{Values: []any{int32(0), strings.Repeat("x", rowSize)}})
	err := c.Insert(context.Background(), wideTable(t), rowsOfSize(perBlock*2, rowSize))
	if !errors.Is(err, boom) {
		t.Errorf("Insert() = %v, want the failure of the block", err)
	}
	if batch.sent {
		t.Error("the insert was finished although a block failed")
	}
}
