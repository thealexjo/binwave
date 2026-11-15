package config

import (
	"path/filepath"
	"time"
)

// Default values. Every default lives here and is covered by tests.
const (
	DefaultMySQLPort           = 3306
	DefaultMySQLTLSMode        = TLSPreferred
	DefaultClickHouseUser      = "default"
	DefaultClickHouseTLSMode   = TLSDisabled
	DefaultInitialPosition     = StartFromNow
	DefaultSnapshotParallelism = 4
	// DefaultSnapshotChunkSize and DefaultSnapshotChunkMaxBytes are the size of a chunk of a snapshot
	// read and the bound on its weight; see internal/snapshot.SizerOptions.
	DefaultSnapshotChunkSize     = 1000
	DefaultSnapshotChunkMaxBytes = 32 * MiB
	DefaultCheckpointStore       = CheckpointClickHouse
	DefaultCheckpointTable       = "binwave_state"
	DefaultCheckpointFileName    = "checkpoint.json"
	DefaultControlSocket         = "/run/binwave/binwave.sock"
	DefaultBufferMaxRows         = 100_000
	DefaultBufferMaxBytes        = 64 * MiB
	DefaultBufferFlush           = Duration(250 * time.Millisecond)
	DefaultBufferCatchupFlush    = Duration(5 * time.Second)
	DefaultBufferMaxMemory       = 1 * GiB
	// DefaultBufferInsertParallelism is how many tables one flush writes at the same time; see
	// config.Buffer.
	DefaultBufferInsertParallelism = 4
	DefaultOverloadBudget          = Duration(10 * time.Minute)
	DefaultStartupBudget           = Duration(10 * time.Minute)
	DefaultRetryInitialDelay       = Duration(500 * time.Millisecond)
	DefaultRetryMaxDelay           = Duration(30 * time.Second)
	DefaultAttemptTimeout          = Duration(2 * time.Minute)
	DefaultHeartbeatPeriod         = Duration(5 * time.Second)
	DefaultForeignKeyCascades      = CascadesError
	DefaultOnReplicationError      = OnErrorQuarantine
	DefaultOnMissingStructure      = OnMissingStop
	DefaultDDLMode                 = DDLFull
	DefaultListen                  = "127.0.0.1:9187"
	DefaultLogLevel                = "info"
	DefaultLogFormat               = LogText

	// StateDirectoryEnv is set by systemd (StateDirectory=) and by the Docker image.
	StateDirectoryEnv = "STATE_DIRECTORY"
)

func (c *Config) applyDefaults(env EnvLookup) {
	m := &c.Source.MySQL
	setDefault(&m.Port, DefaultMySQLPort)
	setDefault(&m.TLS.Mode, DefaultMySQLTLSMode)
	setDefault(&m.HeartbeatPeriod, DefaultHeartbeatPeriod)
	if m.Compression == nil {
		compression := true
		m.Compression = &compression
	}

	ch := &c.Target.ClickHouse
	setDefault(&ch.User, DefaultClickHouseUser)
	setDefault(&ch.TLS.Mode, DefaultClickHouseTLSMode)

	setDefault(&c.Replication.InitialPosition, DefaultInitialPosition)
	setDefault(&c.Replication.Snapshot.Parallelism, DefaultSnapshotParallelism)
	setDefault(&c.Replication.Snapshot.ChunkSize, DefaultSnapshotChunkSize)
	if c.Replication.Snapshot.LargestFirst == nil {
		largestFirst := true
		c.Replication.Snapshot.LargestFirst = &largestFirst
	}
	if c.Replication.Snapshot.AdaptiveChunks == nil {
		adaptive := true
		c.Replication.Snapshot.AdaptiveChunks = &adaptive
	}
	if c.Replication.Snapshot.ChunkMaxBytes == nil {
		maxBytes := DefaultSnapshotChunkMaxBytes
		c.Replication.Snapshot.ChunkMaxBytes = &maxBytes
	}
	if c.Replication.Autostart == nil {
		autostart := true
		c.Replication.Autostart = &autostart
	}
	setDefault(&c.Control.Socket, DefaultControlSocket)

	cp := &c.Checkpoint
	setDefault(&cp.Store, DefaultCheckpointStore)
	setDefault(&cp.Table, DefaultCheckpointTable)
	if cp.Store == CheckpointFile && cp.FilePath == "" {
		// systemd may pass several colon-separated directories; binwave uses one.
		if dir, ok := env(StateDirectoryEnv); ok && dir != "" {
			cp.FilePath = filepath.Join(filepath.SplitList(dir)[0], DefaultCheckpointFileName)
		}
	}

	b := &c.Buffer
	setDefault(&b.MaxRows, DefaultBufferMaxRows)
	setDefault(&b.MaxBytes, DefaultBufferMaxBytes)
	setDefault(&b.FlushInterval, DefaultBufferFlush)
	setDefault(&b.CatchupFlushInterval, DefaultBufferCatchupFlush)
	setDefault(&b.MaxMemory, DefaultBufferMaxMemory)
	setDefault(&b.InsertParallelism, DefaultBufferInsertParallelism)

	setDefault(&c.Retry.OverloadBudget, DefaultOverloadBudget)
	setDefault(&c.Retry.StartupBudget, DefaultStartupBudget)
	setDefault(&c.Retry.InitialDelay, DefaultRetryInitialDelay)
	setDefault(&c.Retry.MaxDelay, DefaultRetryMaxDelay)
	setDefault(&c.Retry.AttemptTimeout, DefaultAttemptTimeout)
	setDefault(&c.Tables.ForeignKeyCascades, DefaultForeignKeyCascades)
	setDefault(&c.Tables.OnReplicationError, DefaultOnReplicationError)

	setDefault(&c.DDL.Mode, DefaultDDLMode)
	setDefault(&c.DDL.OnMissingStructure, DefaultOnMissingStructure)

	o := &c.Observability
	setDefault(&o.Listen, DefaultListen)
	setDefault(&o.LogLevel, DefaultLogLevel)
	setDefault(&o.LogFormat, DefaultLogFormat)
}

func setDefault[T comparable](field *T, value T) {
	var zero T
	if *field == zero {
		*field = value
	}
}
