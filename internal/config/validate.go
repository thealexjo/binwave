package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ValidationError lists every problem found in a configuration.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid config:\n  - " + strings.Join(e.Problems, "\n  - ")
}

type validator struct {
	problems []string
}

func (v *validator) addf(field, format string, args ...any) {
	v.problems = append(v.problems, field+": "+fmt.Sprintf(format, args...))
}

func (v *validator) required(field, value string) {
	if strings.TrimSpace(value) == "" {
		v.addf(field, "is required")
	}
}

func oneOf[T ~string](v *validator, field string, value T, allowed ...T) {
	if !slices.Contains(allowed, value) {
		names := make([]string, len(allowed))
		for i, a := range allowed {
			names[i] = string(a)
		}
		v.addf(field, "must be one of %s, got %q", strings.Join(names, ", "), value)
	}
}

// Validate checks the configuration and reports all problems at once.
func (c *Config) Validate() error {
	v := &validator{}

	c.validateMySQL(v)
	c.validateClickHouse(v)
	c.validateReplication(v)
	c.validateCheckpoint(v)
	c.validateTables(v)
	c.validateBuffer(v)
	c.validateRetry(v)
	c.validateControl(v)
	c.validateDDL(v)
	c.validateObservability(v)

	if len(v.problems) == 0 {
		return nil
	}
	return &ValidationError{Problems: v.problems}
}

func (c *Config) validateMySQL(v *validator) {
	m := c.Source.MySQL
	v.required("source.mysql.host", m.Host)
	v.required("source.mysql.user", m.User)
	if m.Port < 1 || m.Port > 65535 {
		v.addf("source.mysql.port", "must be between 1 and 65535, got %d", m.Port)
	}
	if m.ServerID == 0 {
		v.addf("source.mysql.server_id", "is required and must be > 0 (unique among the source's replicas)")
	}
	validateTLS(v, "source.mysql.tls", m.TLS)
	if time.Duration(m.HeartbeatPeriod) < 100*time.Millisecond {
		v.addf("source.mysql.heartbeat_period", "must be at least 100ms, got %s", time.Duration(m.HeartbeatPeriod))
	}
}

func (c *Config) validateClickHouse(v *validator) {
	ch := c.Target.ClickHouse
	if len(ch.Addresses) == 0 {
		v.addf("target.clickhouse.addresses", "at least one address is required")
	}
	for i, addr := range ch.Addresses {
		if err := validateHostPort(addr); err != nil {
			v.addf(fmt.Sprintf("target.clickhouse.addresses[%d]", i), "%v", err)
		}
	}
	v.required("target.clickhouse.database", ch.Database)
	if ch.Cluster != "" {
		v.addf("target.clickhouse.cluster", "ON CLUSTER and replicated engines are not supported yet")
	}
	validateTLS(v, "target.clickhouse.tls", ch.TLS)
	if ch.TLS.Mode == TLSPreferred {
		v.addf("target.clickhouse.tls.mode", "preferred is not supported for ClickHouse (the native protocol has separate plain and TLS ports); use disabled, required or verify-full")
	}
}

func validateTLS(v *validator, field string, t TLS) {
	oneOf(v, field+".mode", t.Mode, TLSDisabled, TLSPreferred, TLSRequired, TLSVerifyFull)
	if (t.CertFile == "") != (t.KeyFile == "") {
		v.addf(field, "cert_file and key_file must be set together")
	}
	if t.Mode == TLSDisabled && (t.CAFile != "" || t.CertFile != "") {
		v.addf(field, "ca_file/cert_file are set but mode is disabled")
	}
}

func (c *Config) validateReplication(v *validator) {
	r := c.Replication
	oneOf(v, "replication.initial_position", r.InitialPosition, StartFromSnapshot, StartFromNow, StartFromGTID)
	if r.Snapshot.Parallelism < 1 {
		v.addf("replication.snapshot.parallelism", "must be at least 1, got %d", r.Snapshot.Parallelism)
	}
	if r.Snapshot.MaxRowsPerSecond < 0 {
		v.addf("replication.snapshot.max_rows_per_second", "must not be negative, got %d", r.Snapshot.MaxRowsPerSecond)
	}
	if r.Snapshot.ChunkSize < 1 {
		v.addf("replication.snapshot.chunk_size", "must be at least 1 row, got %d", r.Snapshot.ChunkSize)
	}
	switch {
	case r.InitialPosition == StartFromGTID && strings.TrimSpace(r.GTID) == "":
		v.addf("replication.gtid", "is required when initial_position is gtid")
	case r.InitialPosition != StartFromGTID && r.GTID != "":
		v.addf("replication.gtid", "is only allowed when initial_position is gtid")
	}
}

func (c *Config) validateCheckpoint(v *validator) {
	cp := c.Checkpoint
	oneOf(v, "checkpoint.store", cp.Store, CheckpointClickHouse, CheckpointFile)
	switch cp.Store {
	case CheckpointClickHouse:
		if !isIdentifier(cp.Table) {
			v.addf("checkpoint.table", "must be a plain identifier, got %q", cp.Table)
		}
	case CheckpointFile:
		if cp.FilePath == "" {
			v.addf("checkpoint.file_path", "is required for the file store when $%s is not set", StateDirectoryEnv)
		}
	}
}

func (c *Config) validateTables(v *validator) {
	t := c.Tables
	if len(t.Include) == 0 {
		v.addf("tables.include", "at least one pattern is required")
	}
	validatePatterns(v, "tables.include", t.Include)
	validatePatterns(v, "tables.exclude", t.Exclude)

	if t.TargetDatabase != "" && !isIdentifier(t.TargetDatabase) {
		v.addf("tables.target_database", "must be a plain identifier, got %q", t.TargetDatabase)
	}
	if t.TargetDatabase != "" && len(t.DatabaseMap) > 0 {
		v.addf("tables.database_map", "cannot be set together with tables.target_database, which already collects every table into one database")
	}

	dbKeys := make([]string, 0, len(t.DatabaseMap))
	for db := range t.DatabaseMap {
		dbKeys = append(dbKeys, db)
	}
	sort.Strings(dbKeys)
	for _, db := range dbKeys {
		target := t.DatabaseMap[db]
		if !isIdentifier(db) {
			v.addf("tables.database_map", "key must be a plain database name, got %q", db)
		}
		if !isIdentifier(target) {
			v.addf("tables.database_map."+db, "must be a plain identifier, got %q", target)
		}
	}

	oneOf(v, "tables.on_replication_error", t.OnReplicationError, OnErrorQuarantine, OnErrorStop)
	oneOf(v, "tables.foreign_key_cascades", t.ForeignKeyCascades, CascadesError, CascadesWarn, CascadesIgnore)

	keys := make([]string, 0, len(t.Overrides))
	for k := range t.Overrides {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		o := t.Overrides[key]
		field := "tables.overrides." + key
		db, table, ok := strings.Cut(key, ".")
		if !ok || !isIdentifier(db) || !isIdentifier(table) {
			v.addf(field, "key must be \"database.table\"")
		}
		if o.TargetTable != "" && !isIdentifier(o.TargetTable) {
			v.addf(field+".target_table", "must be a plain identifier, got %q", o.TargetTable)
		}
		for _, col := range o.ExcludeColumns {
			if _, overridden := o.Columns[col]; overridden {
				v.addf(field+".columns."+col, "column is both excluded and overridden")
			}
		}
		for _, col := range o.OrderBy {
			if slices.Contains(o.ExcludeColumns, col) {
				v.addf(field+".order_by", "column %q is excluded", col)
			}
		}
		for col, co := range o.Columns {
			if strings.TrimSpace(co.Type) == "" {
				v.addf(field+".columns."+col+".type", "is required")
			}
		}
	}
}

func validatePatterns(v *validator, field string, patterns []string) {
	for i, p := range patterns {
		f := fmt.Sprintf("%s[%d]", field, i)
		if strings.Count(p, ".") != 1 {
			v.addf(f, "pattern must be \"database.table\", got %q", p)
			continue
		}
		if _, err := path.Match(p, ""); err != nil {
			v.addf(f, "invalid pattern %q: %v", p, err)
		}
	}
}

func (c *Config) validateBuffer(v *validator) {
	b := c.Buffer
	if b.MaxRows < 1 {
		v.addf("buffer.max_rows", "must be > 0")
	}
	if b.MaxBytes < 1 {
		v.addf("buffer.max_bytes", "must be > 0")
	}
	if b.FlushInterval <= 0 {
		v.addf("buffer.flush_interval", "must be > 0")
	}
	if b.CatchupFlushInterval <= 0 {
		v.addf("buffer.catchup_flush_interval", "must be > 0")
	}
	if b.CatchupFlushInterval < b.FlushInterval {
		// It is the interval used while catching up, where batches matter more than the delay, and it
		// is the threshold for "behind": shorter than the ordinary interval it would mean neither.
		v.addf("buffer.catchup_flush_interval", "must be >= buffer.flush_interval (%s), got %s", b.FlushInterval, b.CatchupFlushInterval)
	}
	if b.MaxMemory < b.MaxBytes {
		v.addf("buffer.max_memory", "must be >= buffer.max_bytes (%s), got %s", b.MaxBytes, b.MaxMemory)
	}
	if b.InsertParallelism < 1 {
		v.addf("buffer.insert_parallelism", "must be at least 1, got %d", b.InsertParallelism)
	}
	if limit := 64; b.InsertParallelism > limit {
		// Every one of them is a connection to ClickHouse while a flush runs, and a flush that needs
		// dozens at once is a sign of a different problem than the number of connections.
		v.addf("buffer.insert_parallelism", "must not be above %d, got %d", limit, b.InsertParallelism)
	}
}

// maxSocketPath is what fits in a Unix socket address on the supported systems (108 bytes on
// Linux, 104 on macOS, counting the terminator).
const maxSocketPath = 100

func (c *Config) validateControl(v *validator) {
	validateSocket(v, c.Control.Socket)
}

func validateSocket(v *validator, path string) {
	if !filepath.IsAbs(path) {
		v.addf("control.socket", "must be an absolute path, got %q", path)
	}
	if len(path) > maxSocketPath {
		v.addf("control.socket", "is %d bytes long; a Unix socket path can hold at most %d", len(path), maxSocketPath)
	}
}

func (c *Config) validateRetry(v *validator) {
	if c.Retry.OverloadBudget <= 0 {
		v.addf("retry.overload_budget", "must be > 0")
	}
	if c.Retry.StartupBudget <= 0 {
		v.addf("retry.startup_budget", "must be > 0")
	}
	if c.Retry.InitialDelay <= 0 {
		v.addf("retry.initial_delay", "must be > 0")
	}
	if c.Retry.MaxDelay < c.Retry.InitialDelay {
		v.addf("retry.max_delay", "must be >= retry.initial_delay (%s), got %s", time.Duration(c.Retry.InitialDelay), time.Duration(c.Retry.MaxDelay))
	}
	if c.Retry.AttemptTimeout <= 0 {
		v.addf("retry.attempt_timeout", "must be > 0")
	}
}

func (c *Config) validateDDL(v *validator) {
	oneOf(v, "ddl.mode", c.DDL.Mode, DDLAdditive, DDLFull)
	oneOf(v, "ddl.on_missing_structure", c.DDL.OnMissingStructure, OnMissingStop, OnMissingQuarantine)
}

func (c *Config) validateObservability(v *validator) {
	o := c.Observability
	if err := validateHostPort(o.Listen); err != nil {
		v.addf("observability.listen", "%v", err)
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(o.LogLevel)); err != nil {
		v.addf("observability.log_level", "must be one of debug, info, warn, error, got %q", o.LogLevel)
	}
	oneOf(v, "observability.log_format", o.LogFormat, LogJSON, LogText)
}

func validateHostPort(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address %q: want host:port", addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return errors.New("invalid port in address " + strconv.Quote(addr))
	}
	return nil
}

// isIdentifier accepts names that need no quoting in MySQL or ClickHouse.
func isIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
