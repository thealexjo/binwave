# Error and warning codes

A reference of the codes that binwave writes to the log and stores together with the reason for a quarantine or failure (`code`). The goal is a short, stable, greppable and alertable identifier in the log line itself, instead of repeating the full explanation every time. The full explanation is here, once.

A code names the **kind** of reason, not the exact variant: "the primary key type is not compatible with chunking" is one code (`TBL106`) for `ENUM` and for `FLOAT` alike; which type and which column exactly is the `detail` next to the code in the log, not a separate code. This keeps the list of codes short, and it does not grow with every new special case.

The source of truth for the list of codes itself is `internal/errcode`; the table below must match it (checked by the test `TestEveryCodeIsDocumented`).

## `SRC` — MySQL source

The requirements are checked at start (`internal/source/mysql/requirements.go`). A violation means exit code 78; a retry will not help.

| Code | Reason | What to do |
|---|---|---|
| `SRC101` | `binlog_format` is not `ROW` | set `binlog_format=ROW` on the source |
| `SRC102` | `binlog_row_image` is not `FULL` | set `binlog_row_image=FULL` |
| `SRC103` | `binlog_row_metadata` is not `FULL` | set `binlog_row_metadata=FULL` |
| `SRC104` | `gtid_mode` is not `ON` | enable GTID replication on the source |
| `SRC105` | `enforce_gtid_consistency` is not `ON` | enable it |
| `SRC106` | `binlog_row_value_options` contains `PARTIAL_JSON` | remove `PARTIAL_JSON` — otherwise the binlog carries JSON diffs instead of values |
| `SRC107` | `log_bin` is not `ON` | enable the binlog on the source itself (for a replica this is not the same as replication from the primary, see `docs/deployment.md`) |
| `SRC108` | `binlog_order_commits` is not `ON` on MySQL before 8.0.33 | set `binlog_order_commits=ON` (the default): before 8.0.33 unordered commits can leave a transaction out of the GTID set of the next binary log, and a reconnect would skip it |
| `SRC109` | `log_replica_updates` is not `ON` (`log_slave_updates` before 8.0.26) | set it to `ON` (the default): a replica that does not write what it applies into its own binlog moves its GTID set past changes binwave never sees |
| `SRC201` | unsupported MySQL version or MariaDB | MySQL 8.0.23+ (the 8.0 line, except the withdrawn 8.0.29) or 8.4 LTS is required |
| `SRC301` | `REPLICATION SLAVE`/`REPLICATION CLIENT`/`SELECT` is missing | grant the privileges to the binwave user |
| `SRC401` | the required binlog position is already purged (error 1236) | a resync from scratch (`initial_position`) or a longer `binlog_expire_logs_seconds` on the source is needed |
| `SRC501` | the source has been silent for longer than three heartbeat periods | network or a hung server — check the source; the process restarts by itself |
| `SRC502` | the binlog contains something unsupported (XA, partial JSON update, incomplete row image) | not supported; replication from this source is impossible as it is |

## `TGT` — ClickHouse sink

| Code | Reason | What to do |
|---|---|---|
| `TGT101` | connection or login to ClickHouse failed | check the address, user, password, TLS |
| `TGT102` | privileges are missing (`CHECK GRANT`) | grant the required privileges (`docs/deployment.md`, step 3) |
| `TGT201` | `target.clickhouse.cluster` — not supported yet | remove `cluster` from the config |
| `TGT301` | ClickHouse is overloaded, retrying within `retry.overload_budget` | it usually passes by itself; watch the retry metric |
| `TGT302` | the overload budget is exhausted, the service is stopped | sort out ClickHouse (too many partitions and the like), then restart |

## `TBL` — structural table problems

They lead to a quarantine (`tables.on_replication_error: quarantine`, the default) or to a service stop (`stop`) — never to partial replication of a table.

| Code | Reason | Recoverable by autoresync? | What to do |
|---|---|---|---|
| `TBL101` | no PK, no unique key over NOT NULL columns, and no `order_by` set | no | set `tables.overrides.<t>.order_by` or add a key in the source |
| `TBL102` | an excluded column conflicts with the key: it is a PK column itself, or it is named in `order_by` | no | do not exclude this column, or fix `order_by` |
| `TBL103` | `order_by` does not include a PK column | no | fix `order_by` |
| `TBL104` | `order_by` names a column that does not exist | no | fix `order_by` |
| `TBL105` | PK on a column prefix — reading cannot continue from where it stopped | no | not supported for chunking; exclude the table or change the key in the source |
| `TBL106` | the PK cannot be read in chunks during a resync or initial load: no PK, PK on a prefix, or a type that does not compare exactly (`FLOAT`, `DOUBLE`, `ENUM`, `SET`, `BIT`, `JSON`, `GEOMETRY`). Unlike `TBL101`, it does not take `order_by` into account | no | `binwave tables release --accept-data-gap` if the history is not needed; otherwise a key that allows chunking is needed |
| `TBL201` | a column name matches a service column (`_version` and the like) | no | rename the column in the source or exclude it (`exclude_columns`) |
| `TBL202` | the ClickHouse client cannot insert the column name (a comma, backtick or double quote at the edge) | no | exclude the column |
| `TBL203` | `overrides` names a column that does not exist | — (configuration error) | fix the config |
| `TBL204` | all columns are excluded | — (configuration error) | fix `exclude_columns` |
| `TBL301` | a foreign key with `ON DELETE`/`ON UPDATE CASCADE`/`SET NULL`/`SET DEFAULT` | no | set `tables.foreign_key_cascades: warn` deliberately, or remove the cascade in the source |
| `TBL401` | two source tables point to one ClickHouse table, or one to the shadow table (`<table>__resync`) of the other | — (configuration error) | separate them with `tables.overrides.<t>.target_table` or `tables.database_map` |
| `TBL501` | the source server may write wrong values of the column into the binlog, by a known bug of its version: a `COLUMN_FORMAT COMPRESSED` column of a partitioned table on Percona Server before 8.0.35 (PS-7806: a statement that reads the table in index order across partitions, such as a `DELETE`) | no | upgrade the source to Percona Server 8.0.35-27 or newer, remove the compression (`MODIFY ... COLUMN_FORMAT DEFAULT`) or exclude the column (`exclude_columns`); then `binwave tables resync` |

## `DDL` — schema changes in the stream

| Code | Reason | What to do |
|---|---|---|
| `DDL101` | a DDL statement was not parsed | report the unsupported syntax; the table does not change until manual intervention |
| `DDL102` | an `ALTER TABLE` construct was not recognized | check whether the table structure has diverged; the next row write re-checks it |
| `DDL201` | `DROP TABLE`/`TRUNCATE`/`DROP DATABASE` were not applied (`ddl.mode: additive`) | expected in `additive`; mirroring deletions needs `ddl.mode: full` |
| `DDL202` | an operation on a partition was not applied | expected (partitions are not mirrored yet) |
| `DDL301` | a quarantined table was renamed in the source, the quarantine followed the name | informational, nothing to do |
| `DDL401` | the required structure is missing in ClickHouse (`ddl.on_missing_structure`) | create the structure manually (`stop`) or wait for `resync` (`quarantine`) |

## `CONV` — value conversion

| Code | Reason | What to do |
|---|---|---|
| `CONV101` | a value does not convert exactly | depends on `tables.on_replication_error`: quarantine of the table or a service stop |
| `CONV201` | a value was replaced by an explicit rule (zero date → the type minimum and the like) | informational, not an error |

## `REPL` — failure of a specific transaction (state `failed`)

The process is alive, `binwave replication status` shows the GTID and the reason, `replication resume` retries or skips.

| Code | Reason | What to do |
|---|---|---|
| `REPL101` | ClickHouse rejected the write with an unresolvable conflict | look at `replication status`; most often a `resync` is needed |
| `REPL102` | the table schema has diverged from what is in ClickHouse now | fix the structure in ClickHouse manually or wait for it to be resolved through DDL |
| `REPL103` | a batch of rows was rejected for a reason in the table itself (missing column or table) | create the missing structure, then `replication resume` |

## `SYNC` — resync / initial load mechanics

| Code | Reason | Recoverable by autoresync? | What to do |
|---|---|---|---|
| `SYNC101` | reading a chunk failed five times in a row with an error that does not go away by itself (an unavailable source does not count: the resync waits it out) — the resync or load gave up | depends on the reason (see `recoverable` of the record) | `binwave tables resync` once more, after sorting out the reason; with `on_replication_error: stop` the resync is kept and replication stops: `binwave replication resume` |
| `SYNC102` | the shadow table disappeared — starting over | — | informational, nothing to do |
| `SYNC103` | the structure changed while the service was not running — starting over | — | informational |
| `SYNC104` | the shadow table could not be dropped | — | drop it manually (`DROP TABLE`); it is the table marked as a binwave shadow (comment) |
| `SYNC105` | DDL during a resync or initial load — progress is reset, starting over | — | informational, it will be retried automatically |
| `SYNC201` | `tables.autoresync` started a resync by itself | — | informational |
| `SYNC202` | `tables.autoresync` could not read or start — the table stays in quarantine | — | look at `tables list`, sort it out manually |
| `SYNC203` | `tables.autoresync` ran out of attempts — the table stays in quarantine | — | `binwave tables resync` manually after the reason is fixed |

## `CTL` — control and operator commands

| Code | Reason | What to do |
|---|---|---|
| `CTL101` | the control channel (`control.socket`) went down | `binwave replication`/`tables` work with the stored state directly until the channel is back up |
| `CTL201` | a transaction was skipped by an operator command (`--skip-transactions`/`--gtid-next`) | informational — this was intended |

## `CFG` — configuration

Loading (`CFG0xx`) happens before any attempt to understand the contents; validation (`CFG1xx`) is the semantic checks of an already parsed config. The validation codes are broad on purpose: `field` in the log names the exact field, the code is only the kind of problem, so a new setting almost never needs a new code.

| Code | Reason |
|---|---|
| `CFG001` | the config file cannot be read (no file, no permissions) |
| `CFG002` | the file is not valid YAML |
| `CFG003` | a `${VAR}` reference is broken, or the variable is not set in the environment |
| `CFG004` | unknown (or removed) config key |
| `CFG005` | the value is not of the type the field expects |
| `CFG101` | a required field is not set |
| `CFG102` | the value is out of the allowed range or format |
| `CFG103` | the value must be one of a fixed set |
| `CFG104` | the value must be a plain identifier (one that needs no quoting in MySQL/ClickHouse) |
| `CFG105` | the pattern must be `database.table` |
| `CFG106` | invalid glob pattern |
| `CFG107` | invalid `host:port` address |
| `CFG108` | the settings are mutually exclusive |
| `CFG109` | inconsistent combination of related fields |
| `CFG110` | the setting is not supported yet |
