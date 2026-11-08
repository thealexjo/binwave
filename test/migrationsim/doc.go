// Package migrationsim reproduces, statement by statement, what the online schema change tools
// pt-online-schema-change and gh-ost do to a MySQL server: the shadow table, the triggers or the
// changelog table, the chunked copy, the swap of the tables (or gh-ost's two-step cut-over) and
// the clean-up. The tests of binwave use it instead of the real tools, which would have to be
// installed in every pipeline and are slow to run: what binwave sees of a migration is the binlog,
// and the binlog of the simulation carries the same DDL, in the same order, as the tool's.
//
// The statement sequences were captured from the real tools (pt-online-schema-change 3.x and
// gh-ost 1.1.x against MySQL 8.4) and are kept as golden files in testdata; a test compares the
// simulation with them, so the simulation cannot drift away from the tools unnoticed. Refresh the
// files with the real tools when a new version changes its statements (see testdata/README.md).
//
// The simulation is faithful to what reaches the binlog, not to how a tool schedules its work: the
// rows of the copy arrive in chunks, changes made to the original table while it runs reach the
// shadow table (through the triggers, for pt-online-schema-change, and through a final catch-up
// under a table lock, for gh-ost), and hooks let a test act between the phases: write to the table,
// kill the service, inspect the target. The tables need a single-column integer primary key.
package migrationsim
