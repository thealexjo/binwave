package snapshot

import "time"

// SizerOptions bound and tune the size of the chunks.
type SizerOptions struct {
	// Initial is the size of the first chunk, when nothing is known about the table; 1000 by default.
	Initial int
	// Min and Max bound the size of a chunk in rows; 100 and 100000 by default.
	Min, Max int
	// Target is how long a chunk should take to read; 500ms by default. A slow source or a table
	// with wide rows gets smaller chunks, a fast one bigger.
	Target time.Duration
	// MaxBytes bounds a chunk in bytes, as the rows were measured; 32 MiB by default, a negative
	// value removes the bound. It holds whatever the size asks for, fixed or adaptive: a chunk is
	// held in memory whole before it is handed over, so a size that is right for narrow rows is
	// gigabytes on wide ones.
	MaxBytes int
	// Fixed keeps every chunk at Initial instead of sizing it from how long the chunks before it
	// took. A source where the cost of a chunk is in the round-trip rather than in the rows is read
	// far faster by a size an operator measured than by the rule.
	Fixed bool
	// Override, when set, is the size an operator asked for while the read runs; it wins over Fixed
	// and Initial. Shared by every Reader.
	Override *ChunkSize
}

func (o SizerOptions) withDefaults() SizerOptions {
	if o.Min <= 0 {
		o.Min = 100
	}
	if o.Max <= 0 {
		o.Max = 100_000
	}
	if o.Max < o.Min {
		o.Max = o.Min
	}
	if o.Initial <= 0 {
		o.Initial = 1000
	}
	if !o.Fixed {
		// A fixed size is what the operator asked for, not a starting point, so Min and Max do not
		// hold it: they are the bounds the rule may move between.
		o.Initial = min(max(o.Initial, o.Min), o.Max)
	}
	if o.Target <= 0 {
		o.Target = 500 * time.Millisecond
	}
	switch {
	case o.MaxBytes < 0:
		o.MaxBytes = 0 // asked for: no bound
	case o.MaxBytes == 0:
		o.MaxBytes = 32 << 20
	}
	return o
}

// Sizer decides how many rows the next chunk has. It keeps a moving average of what a row costs, in
// time and in bytes, over the chunks read so far, and sizes the next chunk to take Target and to fit
// MaxBytes. The size moves at most by a factor of two upwards and a quarter downwards from one chunk
// to the next, so one odd chunk (a slow moment of the source, a run of wide rows) does not send it far,
// and a source that becomes slow is answered at once.
//
// How sparse the keys are does not matter to it: chunks are cut by row count (WHERE key > cursor
// ORDER BY key LIMIT n), so a chunk of 10000 rows costs the same whether they are the ids 1..10000 or
// every thousandth id of a table of ten billion.
type Sizer struct {
	opts    SizerOptions
	limit   int
	perRow  float64 // seconds per row, moving average
	perByte float64 // bytes per row, moving average
	seen    bool
}

// NewSizer returns a Sizer.
func NewSizer(o SizerOptions) *Sizer {
	o = o.withDefaults()
	return &Sizer{opts: o, limit: o.Initial}
}

// Limit is the size, in rows, of the next chunk.
func (s *Sizer) Limit() int {
	n := s.limit
	if fixed := s.fixed(); fixed > 0 {
		n = fixed
	}
	if s.opts.MaxBytes > 0 && s.perByte > 0 {
		// The bound on the weight of a chunk holds whatever the size asks for; it can only lower it.
		n = min(n, max(int(float64(s.opts.MaxBytes)/s.perByte), 1))
	}
	return max(n, 1)
}

// fixed is the size every chunk must have, 0 when the size follows the rule. An override asked for
// while the read runs wins over the configuration, in both directions.
func (s *Sizer) fixed() int {
	if n := s.opts.Override.Fixed(); n > 0 {
		return n
	}
	if s.opts.Override.Adapts() {
		return 0
	}
	if s.opts.Fixed {
		return s.opts.Initial
	}
	return 0
}

// smoothing is the weight of the latest chunk in the moving averages.
const smoothing = 0.3

// Observe records a chunk that had rows rows and size bytes and took took to read, and sizes the next.
// The averages are kept even while the size is fixed: they bound the weight of a chunk (MaxBytes), and
// they are what the rule continues from if an operator turns it back on.
func (s *Sizer) Observe(rows, size int, took time.Duration) {
	if rows <= 0 {
		return
	}
	perRow := took.Seconds() / float64(rows)
	bytesPerRow := float64(size) / float64(rows)
	if !s.seen {
		s.perRow, s.perByte, s.seen = perRow, bytesPerRow, true
	} else {
		s.perRow += smoothing * (perRow - s.perRow)
		s.perByte += smoothing * (bytesPerRow - s.perByte)
	}

	var next float64
	if s.perRow > 0 {
		next = s.opts.Target.Seconds() / s.perRow
	} else {
		next = float64(s.limit) * 2 // too quick to measure
	}
	if s.opts.MaxBytes > 0 && s.perByte > 0 {
		next = min(next, float64(s.opts.MaxBytes)/s.perByte)
	}
	// A chunk that took far longer than the target is answered by more than the average says.
	if took > 2*s.opts.Target {
		next = min(next, float64(s.limit)*s.opts.Target.Seconds()/took.Seconds())
	}
	next = min(max(next, float64(s.limit)/4), float64(s.limit)*2)
	s.limit = min(max(int(next), s.opts.Min), s.opts.Max)
}
