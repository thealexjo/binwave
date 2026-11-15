package config

// Config is the complete binwave configuration for one MySQL source → ClickHouse pipeline.
type Config struct {
	Source        Source        `yaml:"source"`
	Target        Target        `yaml:"target"`
	Replication   Replication   `yaml:"replication"`
	Control       Control       `yaml:"control"`
	Checkpoint    Checkpoint    `yaml:"checkpoint"`
	Tables        Tables        `yaml:"tables"`
	Buffer        Buffer        `yaml:"buffer"`
	Retry         Retry         `yaml:"retry"`
	DDL           DDL           `yaml:"ddl"`
	Observability Observability `yaml:"observability"`
}

// Source describes where changes are read from.
type Source struct {
	MySQL MySQL `yaml:"mysql"`
}

// MySQL is the replication source connection.
type MySQL struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	// ServerID identifies binwave as a replica; it must be unique among the source's replicas.
	ServerID uint32 `yaml:"server_id"`
	TLS      TLS    `yaml:"tls"`
	// HeartbeatPeriod is how often the source must send a heartbeat while it has nothing else
	// to send. A source that stays silent for three periods is declared frozen and binwave
	// exits, so that the supervisor restarts it. A shorter period finds a frozen source
	// sooner and costs a few tiny packets per second.
	HeartbeatPeriod Duration `yaml:"heartbeat_period"`
	// Compression compresses the result sets of the SELECT connections — the chunks of a snapshot,
	// which is where nearly all the bytes of a long initial load are. It trades CPU on the source and
	// on binwave for bandwidth, which is the right trade when the two are not in the same rack: rows
	// of text compress several times over. On by default; turn it off when the source has no CPU to
	// spare or the link is wide enough not to matter. The binlog connection is not covered: the client
	// library negotiates compression during the handshake and offers no hook for it.
	Compression *bool `yaml:"compression"`
}

// CompressionEnabled reports whether the SELECT connections compress their result sets.
func (m MySQL) CompressionEnabled() bool { return m.Compression == nil || *m.Compression }

// Target describes where changes are written to.
type Target struct {
	ClickHouse ClickHouse `yaml:"clickhouse"`
}

// ClickHouse is the destination connection.
type ClickHouse struct {
	Addresses []string `yaml:"addresses"`
	Database  string   `yaml:"database"`
	User      string   `yaml:"user"`
	Password  string   `yaml:"password"`
	TLS       TLS      `yaml:"tls"`
	// Cluster enables ON CLUSTER DDL and Replicated* engines when non-empty.
	Cluster string `yaml:"cluster"`
}

// TLSMode controls transport encryption.
type TLSMode string

// TLS modes.
const (
	TLSDisabled   TLSMode = "disabled"
	TLSPreferred  TLSMode = "preferred"
	TLSRequired   TLSMode = "required"
	TLSVerifyFull TLSMode = "verify-full"
)

// TLS configures transport encryption for a connection.
type TLS struct {
	Mode     TLSMode `yaml:"mode"`
	CAFile   string  `yaml:"ca_file"`
	CertFile string  `yaml:"cert_file"`
	KeyFile  string  `yaml:"key_file"`
}

// InitialPosition selects where replication starts when no checkpoint exists yet.
type InitialPosition string

// Initial positions.
const (
	StartFromSnapshot InitialPosition = "snapshot"
	StartFromNow      InitialPosition = "now"
	StartFromGTID     InitialPosition = "gtid"
)

// Control configures the channel an operator uses to talk to a running service.
type Control struct {
	// Socket is the path of the Unix socket the service listens on for the commands of
	// `binwave replication` and `binwave tables`. The socket is private to the service's user.
	// The default is /run/binwave/binwave.sock, the RuntimeDirectory of binwave.service; without
	// that directory the service runs with no channel.
	Socket string `yaml:"socket"`
}

// Replication controls the replication start point.
type Replication struct {
	// Autostart says whether replication starts by itself when the process starts, like MySQL
	// without skip_replica_start. With false the service comes up and checks both databases but
	// reads nothing until `binwave replication resume`. Nil means the default, true.
	Autostart       *bool           `yaml:"autostart"`
	InitialPosition InitialPosition `yaml:"initial_position"`
	// GTID is the executed GTID set to start after; used only with initial_position: gtid.
	GTID string `yaml:"gtid"`
	// Snapshot tunes how the initial load (initial_position: snapshot) reads the source; it is also
	// what a resync of a table (`binwave tables resync`) reads through.
	Snapshot Snapshot `yaml:"snapshot"`
}

// Snapshot controls how many tables are read in chunks at once, and how fast, for the initial load.
type Snapshot struct {
	// Parallelism is how many tables the initial load reads in chunks at the same time. A table
	// being resynchronized on its own (`binwave tables resync`) is not held to this: an operator who
	// asks for one does not expect it queued behind an unrelated initial load.
	Parallelism int `yaml:"parallelism"`
	// MaxRowsPerSecond limits the combined speed of every table the initial load is reading at once,
	// averaged from when it started; 0 is unlimited. It does not apply to `binwave tables resync`.
	MaxRowsPerSecond int `yaml:"max_rows_per_second"`
	// ChunkSize is how many rows one chunk of a snapshot read asks for: the size of every chunk when
	// AdaptiveChunks is off, and the size of the first one when it is on.
	ChunkSize int `yaml:"chunk_size"`
	// AdaptiveChunks sizes each chunk from how long the ones before it took, aiming at half a second
	// per chunk, between 100 rows and 100000. Off, every chunk is ChunkSize: a source where the cost
	// of a chunk is in the round-trip rather than in the rows (a read of 1000 rows takes as long as
	// one of 100) is read far faster by a size an operator measured than by the rule.
	AdaptiveChunks *bool `yaml:"adaptive_chunks"`
	// LargestFirst reads the tables of the initial load biggest first, by what the server believes
	// each holds (information_schema.TABLES.TABLE_ROWS). The order decides when the load ends: the
	// slots are few and the sizes differ by orders of magnitude, so a table of a hundred million rows
	// that starts last leaves everything else waiting for it alone, while the same table started first
	// is read while hundreds of small ones come and go beside it. Off, the order is the order of the
	// names, which is arbitrary with respect to size.
	LargestFirst *bool `yaml:"largest_first"`
	// ChunkMaxBytes bounds a chunk by the size of the rows it read, whatever ChunkSize and
	// AdaptiveChunks ask for: a chunk is held in memory whole before it is handed to the pipeline, so
	// a size that is right for narrow rows is gigabytes on wide ones. 0 removes the bound, and then
	// nothing but ChunkSize stands between wide rows and the memory of the process.
	ChunkMaxBytes *ByteSize `yaml:"chunk_max_bytes"`
}

// LargestFirstEnabled reports whether the initial load reads the biggest tables first.
func (s Snapshot) LargestFirstEnabled() bool { return s.LargestFirst == nil || *s.LargestFirst }

// AdaptiveChunksEnabled reports whether the size of a chunk follows how long the chunks before it took.
func (s Snapshot) AdaptiveChunksEnabled() bool { return s.AdaptiveChunks == nil || *s.AdaptiveChunks }

// MaxChunkBytes is the bound on the size of a chunk in bytes; 0 is no bound.
func (s Snapshot) MaxChunkBytes() ByteSize {
	if s.ChunkMaxBytes == nil {
		return DefaultSnapshotChunkMaxBytes
	}
	return *s.ChunkMaxBytes
}

// AutostartEnabled reports whether replication starts by itself.
func (r Replication) AutostartEnabled() bool { return r.Autostart == nil || *r.Autostart }

// CheckpointStore selects where replication positions are persisted.
type CheckpointStore string

// Checkpoint stores.
const (
	CheckpointClickHouse CheckpointStore = "clickhouse"
	CheckpointFile       CheckpointStore = "file"
)

// Checkpoint configures position persistence.
type Checkpoint struct {
	Store CheckpointStore `yaml:"store"`
	// Table is the ClickHouse table used by the clickhouse store.
	Table string `yaml:"table"`
	// FilePath is used by the file store; defaults to $STATE_DIRECTORY/checkpoint.json.
	FilePath string `yaml:"file_path"`
}

// Tables selects and maps replicated tables.
type Tables struct {
	// Include and Exclude are "database.table" glob patterns (path.Match syntax).
	Include []string `yaml:"include"`
	Exclude []string `yaml:"exclude"`
	// TargetDatabase is the ClickHouse database every replicated table goes to. Empty, the default,
	// puts each table in a database named like its MySQL database: shop.orders stays shop.orders.
	// Mutually exclusive with DatabaseMap: it already ignores the source database's identity, which
	// DatabaseMap exists to rename, not erase.
	TargetDatabase string `yaml:"target_database"`
	// DatabaseMap renames a source database on its way to ClickHouse: {shop: shop_mirror} sends
	// shop.orders to shop_mirror.orders. A source database not named here keeps its own name, as
	// without DatabaseMap at all. Keyed by the source database name (not a glob, an exact match);
	// tables.overrides stays keyed by the source "database.table" regardless of this map, the same
	// way tables.include and tables.exclude do.
	DatabaseMap map[string]string `yaml:"database_map"`
	// OnReplicationError is what happens when one table cannot be replicated: a value that cannot
	// be converted, a structure change nobody can apply, a table without a key. With quarantine
	// the table is set aside, loudly, and the other tables go on; with stop the whole service
	// stops. Problems of the service as a whole, such as an unreachable database, always stop it.
	OnReplicationError ReplicationErrorPolicy `yaml:"on_replication_error"`
	// ForeignKeyCascades is the reaction to a replicated table that has a foreign key with
	// ON DELETE or ON UPDATE CASCADE / SET NULL / SET DEFAULT. MySQL applies these changes inside
	// the storage engine and never writes them to the binlog, so ClickHouse keeps the child rows
	// the source has already deleted or changed.
	ForeignKeyCascades CascadePolicy `yaml:"foreign_key_cascades"`
	// Autoresync resynchronizes a quarantined table by itself (binwave tables resync, without an
	// operator) when its reason is one a resync fixes on its own: a resync rereads the table from the
	// structure it has now, so it needs no guessing about what a schema statement's own reasoning
	// would have been for existing rows. It never touches a table quarantined for a reason a resync
	// cannot fix (no key at all, for instance). Off by default.
	Autoresync bool `yaml:"autoresync"`
	// Overrides are keyed by "database.table".
	Overrides map[string]TableOverride `yaml:"overrides"`
}

// ReplicationErrorPolicy is the reaction to a problem of one table.
type ReplicationErrorPolicy string

// Replication error policies.
const (
	// OnErrorQuarantine sets the table aside and goes on with the others (the default). The table
	// stays behind the source until it is resynchronized and released.
	OnErrorQuarantine ReplicationErrorPolicy = "quarantine"
	// OnErrorStop stops the service, as MySQL's own replication does on an error.
	OnErrorStop ReplicationErrorPolicy = "stop"
)

// CascadePolicy is the reaction to foreign keys whose changes never reach the binlog.
type CascadePolicy string

// Cascade policies.
const (
	// CascadesError stops the service: the operator must acknowledge that the copy will diverge.
	CascadesError CascadePolicy = "error"
	// CascadesWarn logs every such key and replicates anyway.
	CascadesWarn CascadePolicy = "warn"
	// CascadesIgnore replicates without a word.
	CascadesIgnore CascadePolicy = "ignore"
)

// TableOverride adjusts replication of a single table.
type TableOverride struct {
	TargetTable    string                    `yaml:"target_table"`
	ExcludeColumns []string                  `yaml:"exclude_columns"`
	OrderBy        []string                  `yaml:"order_by"`
	Columns        map[string]ColumnOverride `yaml:"columns"`
}

// ColumnOverride adjusts mapping of a single column.
type ColumnOverride struct {
	// Type is the ClickHouse base type (without Nullable); compatibility is checked against the source column.
	Type string `yaml:"type"`
}

// Buffer limits batching and memory use.
type Buffer struct {
	MaxRows  int      `yaml:"max_rows"`
	MaxBytes ByteSize `yaml:"max_bytes"`
	// FlushInterval is the longest a change waits in memory while replication is at the head of the
	// binlog, which is where it spends its life: it is the delay an operator sees as the lag, so it is
	// short. CatchupFlushInterval replaces it while there is a backlog to work through.
	FlushInterval Duration `yaml:"flush_interval"`
	// CatchupFlushInterval is the interval used while the stream is further behind the source than
	// this same interval: an initial load, a long outage, a restart after one. There, batches are what
	// matters and not the delay — the changes are hours old anyway, and a batch of a thousand rows
	// costs the same round-trip and the same fsync as a batch of two. It is also the threshold: past
	// it the pipeline batches, under it it writes promptly (see Pipeline.atHead).
	CatchupFlushInterval Duration `yaml:"catchup_flush_interval"`
	MaxMemory            ByteSize `yaml:"max_memory"`
	// InsertParallelism is how many tables one flush writes at the same time; 1 writes them one after
	// another. A flush of a source whose changes are spread over many tables is a row or two per
	// table, and the cost of such an insert is the round-trip and the durability of the server
	// (fsync_after_insert), not the rows: writing them one by one makes a flush as long as the number
	// of tables it touches. The tables are independent — the order that matters is the order within
	// one table, and the version decides anyway — so they are written together. The position is still
	// saved only after every one of them is in.
	InsertParallelism int `yaml:"insert_parallelism"`
}

// Retry controls how long binwave keeps retrying ClickHouse.
type Retry struct {
	// OverloadBudget is how long ClickHouse may keep answering "overloaded" (too many parts, memory
	// limit, too many queries) before binwave stops with an error. Retrying is right while the
	// server catches up, but an overload that lasts is a problem an operator must see, not one
	// to hide behind an endless retry loop that also holds back replication of every table.
	// Network failures are retried without limit: an outage ends by itself.
	OverloadBudget Duration `yaml:"overload_budget"`
	// StartupBudget is how long binwave keeps retrying to reach MySQL and ClickHouse when it
	// starts (or restarts) while one of them is down. Only errors that may pass by themselves are
	// retried: a wrong password, a missing privilege or purged binary logs stop the service at
	// once with the configuration exit code. After the budget the service exits with an error and
	// the supervisor decides what happens next.
	StartupBudget Duration `yaml:"startup_budget"`
	// InitialDelay and MaxDelay bound the wait between attempts, which doubles from the first to
	// the second; they apply to ClickHouse requests and to the attempts at start.
	InitialDelay Duration `yaml:"initial_delay"`
	MaxDelay     Duration `yaml:"max_delay"`
	// AttemptTimeout cuts one ClickHouse request that does not answer, so that a hung
	// connection is retried on a new one. It must exceed the longest insert.
	AttemptTimeout Duration `yaml:"attempt_timeout"`
}

// DDLMode controls which schema changes binwave applies to ClickHouse automatically. With
// ddl.dry_run it applies none and logs what it would execute, which is also how a schema that is
// kept by hand is used: the log shows the statements to run.
type DDLMode string

// DDL modes.
const (
	DDLAdditive DDLMode = "additive"
	DDLFull     DDLMode = "full"
)

// DDL configures automatic schema management.
type DDL struct {
	Mode   DDLMode `yaml:"mode"`
	DryRun bool    `yaml:"dry_run"`
	// OnMissingStructure is what happens when a table or a column that has to be written is
	// missing in ClickHouse: binwave does not create it (ddl.dry_run, which only logs the DDL to
	// run by hand) or it was dropped after replication started. With stop, the default, the service
	// stops and nothing is lost: the operator creates the table and restarts, and every change
	// is still waiting in the binlog. With quarantine the table is set aside and the rest goes
	// on, and the changes made until the table is created and released are NOT in ClickHouse.
	// It only takes effect with tables.on_replication_error: quarantine.
	OnMissingStructure MissingStructurePolicy `yaml:"on_missing_structure"`
}

// MissingStructurePolicy is the reaction to a table or column that is missing in ClickHouse.
type MissingStructurePolicy string

// Missing table policies.
const (
	// OnMissingStop stops the service (the default): nothing is lost.
	OnMissingStop MissingStructurePolicy = "stop"
	// OnMissingQuarantine sets the table aside and goes on: the table's changes are lost until
	// it is resynchronized.
	OnMissingQuarantine MissingStructurePolicy = "quarantine"
)

// LogFormat selects the log encoding.
type LogFormat string

// Log formats.
const (
	LogJSON LogFormat = "json"
	LogText LogFormat = "text"
)

// Observability configures metrics, health endpoints and logging.
type Observability struct {
	Listen    string    `yaml:"listen"`
	LogLevel  string    `yaml:"log_level"`
	LogFormat LogFormat `yaml:"log_format"`
	// Pprof serves the Go profiles of the process under /debug/pprof on the same address as the
	// metrics. Off by default: the profiles say a good deal about what the process is doing, and the
	// address is meant to be reachable by whoever scrapes the metrics. On, one command answers "where
	// is the time going" (go tool pprof http://host:port/debug/pprof/profile?seconds=30), which is
	// otherwise a rebuild and a restart away.
	Pprof bool `yaml:"pprof"`
}
