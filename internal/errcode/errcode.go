// Package errcode names the stable codes attached to binwave's log lines and to the reasons it
// saves (a quarantined table, a failed transaction). A code identifies the kind of a problem
// independently of the wording of its message, so an operator can grep, alert or dashboard on it
// without parsing prose; the prose lives once, in docs/reference/error-codes.md, not repeated in
// every log line.
//
// Codes are a wide family per kind of cause, not one per exact variant: the primary key having an
// incompatible type is TBL106 whether the type is ENUM or FLOAT, and what varies (which type, which
// column) is a "detail" attribute next to the code, not a new code. This keeps the list short and
// keeps old dashboards working when a new variant of an existing cause is recognized.
//
// Codes are stable across releases: a retired code is never reused for something else. All is the
// complete list, checked against docs/reference/error-codes.md by TestEveryCodeIsDocumented.
package errcode

// Code identifies the kind of a problem. String, not an int: it reads in a log line without a
// lookup, and its value is exactly what appears in docs/reference/error-codes.md.
type Code string

// SRC — the MySQL source: requirements checked at start, connectivity, liveness. Mostly ends the
// process with exit code 78 (unmet requirement, missing privilege) or is retried at start
// (unreachable database).
const (
	SRC101 Code = "SRC101" // binlog_format is not ROW
	SRC102 Code = "SRC102" // binlog_row_image is not FULL
	SRC103 Code = "SRC103" // binlog_row_metadata is not FULL
	SRC104 Code = "SRC104" // gtid_mode is not ON
	SRC105 Code = "SRC105" // enforce_gtid_consistency is not ON
	SRC106 Code = "SRC106" // binlog_row_value_options contains PARTIAL_JSON
	SRC107 Code = "SRC107" // log_bin is not ON
	SRC201 Code = "SRC201" // unsupported MySQL version, or MariaDB
	SRC301 Code = "SRC301" // a required privilege is missing (REPLICATION SLAVE/CLIENT, SELECT)
	SRC401 Code = "SRC401" // the binlog position needed is already purged (error 1236)
	SRC501 Code = "SRC501" // the source has been silent past the heartbeat timeout
	SRC502 Code = "SRC502" // binlog content binwave does not support (XA, partial JSON update, an incomplete row image)
)

// TGT — the ClickHouse target: connectivity, privileges, capacity, at start and while running.
const (
	TGT101 Code = "TGT101" // connection or login to ClickHouse failed
	TGT102 Code = "TGT102" // a required privilege is missing (CHECK GRANT)
	TGT201 Code = "TGT201" // target.clickhouse.cluster (ON CLUSTER, Replicated engines) is not supported yet
	TGT301 Code = "TGT301" // ClickHouse is overloaded; retried inside retry.overload_budget
	TGT302 Code = "TGT302" // ClickHouse stayed overloaded past retry.overload_budget; the service stopped
)

// TBL — structural problems of one table: it cannot be admitted for replication as configured, or
// its key cannot be read chunk by chunk for a resync or the initial load. Ends in a quarantine
// (tables.on_replication_error: quarantine, the default) or a config error (stop) — never partial
// replication of the table.
const (
	TBL101 Code = "TBL101" // no primary key and no usable unique key over NOT NULL columns, and no tables.overrides.order_by
	TBL102 Code = "TBL102" // a primary key column is excluded (exclude_columns)
	TBL103 Code = "TBL103" // tables.overrides.order_by omits a primary key column
	TBL104 Code = "TBL104" // tables.overrides.order_by names a column the table does not have
	TBL105 Code = "TBL105" // the primary key is on a prefix of a column, so it cannot resume a chunked read
	TBL106 Code = "TBL106" // the primary key's shape or type blocks reading it in chunks (a resync or the initial load): no primary key, a prefix key, or a column type that cannot be compared exactly (FLOAT, DOUBLE, ENUM, SET, BIT, JSON, GEOMETRY). Unlike TBL101, this check cannot take tables.overrides.order_by into account.
	TBL201 Code = "TBL201" // a column's name collides with a binwave service column (_version and the like)
	TBL202 Code = "TBL202" // a column's name cannot be inserted by the ClickHouse client library (a comma, a backtick or a double quote at an edge)
	TBL203 Code = "TBL203" // tables.overrides names a column the table does not have
	TBL204 Code = "TBL204" // every column of the table is excluded
	TBL301 Code = "TBL301" // a foreign key has ON DELETE/UPDATE CASCADE, SET NULL or SET DEFAULT, which never reaches the binlog
	TBL401 Code = "TBL401" // two source tables would be replicated into the same ClickHouse table
)

// DDL — schema changes read from the binlog and applied (or deliberately not applied) to ClickHouse.
const (
	DDL101 Code = "DDL101" // a DDL statement could not be parsed
	DDL102 Code = "DDL102" // an ALTER TABLE specification is not understood
	DDL201 Code = "DDL201" // a destructive statement (DROP TABLE, TRUNCATE, DROP DATABASE) was not applied: ddl.mode is additive
	DDL202 Code = "DDL202" // a partition operation was not applied: ClickHouse has no matching partitions
	DDL301 Code = "DDL301" // a quarantined table was renamed in the source; its quarantine now follows the new name
	DDL401 Code = "DDL401" // the ClickHouse structure a write needs is missing (ddl.on_missing_structure)
)

// CONV — converting a source value to its ClickHouse representation.
const (
	CONV101 Code = "CONV101" // a value cannot be converted exactly (typemap.ConversionError)
	CONV201 Code = "CONV201" // a value was replaced by rule (a zero date and the like); informational, not an error
)

// REPL — a specific transaction's write was refused by ClickHouse for a reason that repeating it
// will not fix by itself: the "failed" state. The service stays up;
// `binwave replication resume` retries or skips the transaction.
const (
	REPL101 Code = "REPL101" // ClickHouse refused the write with a conflict that does not resolve by retrying (chsink.ConflictError)
	REPL102 Code = "REPL102" // the resolved table no longer matches what is in ClickHouse (chsink.SchemaMismatchError)
	REPL103 Code = "REPL103" // a buffered batch of rows was refused for a reason of the table itself (it is gone, a column is missing)
)

// SYNC — the mechanics of a resync (`binwave tables resync`) or the initial load
// (replication.initial_position: snapshot): reading a table again, chunk by chunk, into a shadow.
const (
	SYNC101 Code = "SYNC101" // reading a chunk failed repeatedly; the resync or initial load gave up
	SYNC102 Code = "SYNC102" // the shadow table is gone; the resync starts again from the first row
	SYNC103 Code = "SYNC103" // the table's structure changed while the service was down; the resync starts again from the first row
	SYNC104 Code = "SYNC104" // the shadow table could not be dropped; it needs a manual DROP before the table can be resynchronized again
	SYNC105 Code = "SYNC105" // a DDL statement reached the table during its resync or initial load; progress is discarded and it starts again
	SYNC201 Code = "SYNC201" // tables.autoresync started resynchronizing a quarantined table on its own
	SYNC202 Code = "SYNC202" // tables.autoresync could not read or start resynchronizing the table; it stays quarantined
	SYNC203 Code = "SYNC203" // tables.autoresync exhausted its attempts; the table stays quarantined
)

// CTL — the control channel and operator commands.
const (
	CTL101 Code = "CTL101" // the control channel (control.socket) failed
	CTL201 Code = "CTL201" // a transaction was skipped on the operator's request (--skip-transactions, --gtid-next)
)

// CFG — the configuration file: loading (CFG0xx) and semantic validation (CFG1xx). Validation
// codes are wide on purpose (CFG101 "required field missing" covers every field), so a new
// setting almost never needs a new code: docs/reference/error-codes.md explains each one and the
// `field` attribute of the log line names exactly which setting is the problem.
const (
	CFG001 Code = "CFG001" // the config file could not be read (missing, no permission)
	CFG002 Code = "CFG002" // the file is not valid YAML
	CFG003 Code = "CFG003" // a ${VAR} reference is malformed, or the environment variable is not set
	CFG004 Code = "CFG004" // an unknown (or removed) configuration key
	CFG005 Code = "CFG005" // a value has the wrong type for its field

	CFG101 Code = "CFG101" // a required field is empty
	CFG102 Code = "CFG102" // a value is out of its allowed range or format
	CFG103 Code = "CFG103" // a value must be one of a fixed set and is not
	CFG104 Code = "CFG104" // a value must be a plain identifier (no quoting needed in MySQL or ClickHouse)
	CFG105 Code = "CFG105" // a pattern must be "database.table"
	CFG106 Code = "CFG106" // a glob pattern is invalid
	CFG107 Code = "CFG107" // a "host:port" address is invalid
	CFG108 Code = "CFG108" // two settings are mutually exclusive
	CFG109 Code = "CFG109" // related fields are set to an inconsistent combination
	CFG110 Code = "CFG110" // the setting is not supported yet
)

// All lists every code, for TestEveryCodeIsDocumented to check against docs/reference/error-codes.md.
var All = []Code{
	SRC101, SRC102, SRC103, SRC104, SRC105, SRC106, SRC107, SRC201, SRC301, SRC401, SRC501, SRC502,
	TGT101, TGT102, TGT201, TGT301, TGT302,
	TBL101, TBL102, TBL103, TBL104, TBL105, TBL106, TBL201, TBL202, TBL203, TBL204, TBL301, TBL401,
	DDL101, DDL102, DDL201, DDL202, DDL301, DDL401,
	CONV101, CONV201,
	REPL101, REPL102, REPL103,
	SYNC101, SYNC102, SYNC103, SYNC104, SYNC105, SYNC201, SYNC202, SYNC203,
	CTL101, CTL201,
	CFG001, CFG002, CFG003, CFG004, CFG005,
	CFG101, CFG102, CFG103, CFG104, CFG105, CFG106, CFG107, CFG108, CFG109, CFG110,
}
