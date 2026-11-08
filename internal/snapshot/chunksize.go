package snapshot

import "sync/atomic"

// ChunkSize is the size of a chunk, shared by every Reader and changeable while they read
// (`binwave replication chunk-size`, Pipeline.SetChunkSize). A Reader asks it on every chunk, so a
// change takes effect at once, including on the tables that are already being read.
//
// Its zero value, and NewChunkSize(0), mean "whatever the configuration says": the Sizer then adapts
// or keeps its fixed size as SizerOptions was built. Set overrides that until the process restarts;
// the override is deliberately not saved, like the rate limit.
type ChunkSize struct {
	// rows is 0 for "the configuration decides", adaptive for "adapt whatever it says", and a
	// positive number for that many rows in every chunk.
	rows atomic.Int64
}

// adaptive is the stored value of an override that asks for adaptive sizing.
const adaptive = -1

// NewChunkSize returns a ChunkSize overriding the configured size with rows rows; 0 overrides nothing.
func NewChunkSize(rows int) *ChunkSize {
	c := &ChunkSize{}
	if rows > 0 {
		c.rows.Store(int64(rows))
	}
	return c
}

// Set makes every chunk from now on rows rows; 0 or less asks for adaptive sizing instead.
func (c *ChunkSize) Set(rows int) {
	if rows <= 0 {
		c.rows.Store(adaptive)
		return
	}
	c.rows.Store(int64(rows))
}

// Fixed is the size every chunk must have, 0 when the size is not fixed here.
func (c *ChunkSize) Fixed() int {
	if c == nil {
		return 0
	}
	if n := c.rows.Load(); n > 0 {
		return int(n)
	}
	return 0
}

// Adapts reports whether this override asks for adaptive sizing, whatever the configuration says.
func (c *ChunkSize) Adapts() bool { return c != nil && c.rows.Load() == adaptive }
