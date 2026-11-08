// Package snapshot reads a table of the source in chunks by primary key, for the initial load and
// for the resynchronization of a table. It reads with plain SELECT statements, one chunk at a time,
// without a long transaction and without writing anything to the source.
//
// Each chunk comes with a watermark: the GTID set the source had executed just before the chunk was
// read. The chunk holds at least what the source had committed by then, so once the change stream
// has passed the watermark, the rows of the chunk can be written with the version of that point
// (the pipeline does this, not this package): changes after it have higher versions and win, and
// changes before it are already in the chunk. The whole algorithm is described in the pipeline
// package (internal/pipeline/resync.go).
//
// The size of a chunk is not fixed. A chunk of the wrong size is expensive either way: too small
// and the source answers thousands of statements, too big and one statement holds the server and a
// buffer of the service for long. The Sizer follows how long chunks take, and keeps each near a
// target time.
package snapshot
