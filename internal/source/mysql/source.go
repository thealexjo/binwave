// Package mysql reads change events from a MySQL source: binlog streaming (GTID-based),
// source requirement checks and, later, table snapshots. It never writes to the source.
package mysql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	sqldriver "github.com/go-sql-driver/mysql"

	"github.com/thealexjo/binwave/internal/config"
	"github.com/thealexjo/binwave/internal/event"
	"github.com/thealexjo/binwave/internal/tlsutil"
)

var heartbeat event.Event = event.Heartbeat{}

const (
	defaultIdlePeriod      = 5 * time.Second
	defaultHeartbeatPeriod = 5 * time.Second
	// silentPeriods is how many missed heartbeat periods make the source "silent".
	silentPeriods     = 3
	connectTimeout    = 10 * time.Second
	maxReconnectTries = 5
	// preparedChunkStatements is how many statements that read chunks are kept ready. The working set
	// is one or two per table being read at once (with and without a cursor), so a few dozen covers
	// every reader and every `tables resync` with room to spare, while a source with thousands of
	// tables never accumulates a statement per table against max_prepared_stmt_count. See stmtCache.
	preparedChunkStatements = 64
)

// Options tune a Source.
type Options struct {
	// Include reports whether changes of a table are emitted; nil means all tables. Transactions
	// are always reported so the position advances even when nothing is replicated.
	Include func(database, table string) bool
	// Databases are the databases named literally in the table selection. Open verifies that the
	// user can see them.
	Databases []string
	// IdlePeriod is how long the stream waits for an event before emitting a Heartbeat.
	IdlePeriod time.Duration
	// HeartbeatPeriod is how often the server must send a heartbeat while it has nothing else to
	// send. The stream fails with ErrSourceSilent when nothing arrives for three periods.
	HeartbeatPeriod time.Duration
	// OnReconnect, if set, is called each time go-mysql re-establishes a broken binlog connection.
	OnReconnect func()
	// Now is the clock for liveness tracking; it defaults to time.Now.
	Now func() time.Time
	// SnapshotPoolSize sizes the connection pool a snapshot reads through (TableDefinition,
	// ReadChunk, KeyRange, EstimatedRows), separate from the small pool behind everything else:
	// several tables read in chunks at once must not queue behind one another, or behind a
	// heartbeat or a status query. 0 or less uses defaultSnapshotPoolSize.
	SnapshotPoolSize int
}

// Source is a MySQL replication source.
type Source struct {
	cfg  config.MySQL
	opts Options
	log  *slog.Logger

	db         *sql.DB
	snapshotDB *sql.DB // a snapshot's reads; see Options.SnapshotPoolSize
	// chunkStmts keeps the statements that read chunks ready; see stmtCache and chunkStatement.
	chunkStmts *stmtCache[*sql.Stmt]
	streamTLS  *tls.Config
	collations collations
	server     server
}

// defaultSnapshotPoolSize is used when Options.SnapshotPoolSize is not set: enough for the default
// parallelism of the initial load (config.DefaultSnapshotParallelism) plus a `tables resync` or two
// alongside it.
const defaultSnapshotPoolSize = 6

// readDB is the pool a snapshot reads through.
func (s *Source) readDB() *sql.DB { return s.snapshotDB }

// New creates a Source. Call Open before anything else.
func New(cfg config.MySQL, opts Options, log *slog.Logger) *Source {
	if opts.IdlePeriod <= 0 {
		opts.IdlePeriod = defaultIdlePeriod
	}
	if opts.HeartbeatPeriod <= 0 {
		opts.HeartbeatPeriod = defaultHeartbeatPeriod
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Source{
		cfg: cfg, opts: opts, log: log,
		chunkStmts: newStmtCache(preparedChunkStatements, (*sql.Stmt).Close),
	}
}

// maxIdleTime is how long a connection may wait in a pool unused. A restart of the source leaves every
// idle connection dead, and each costs a failed read to find out; between chunks of an initial load
// and the reads of the watermark, a connection waiting longer than this is rarely worth keeping.
const maxIdleTime = time.Minute

// Open connects, verifies the source requirements and loads character set metadata.
// An unmet requirement is reported as *RequirementsError.
func (s *Source) Open(ctx context.Context) error {
	tlsCfg, err := tlsutil.Build(s.cfg.TLS, s.cfg.Host)
	if err != nil {
		return fmt.Errorf("source TLS: %w", err)
	}

	drv := driverConfig(s.cfg, tlsCfg)

	connector, err := sqldriver.NewConnector(drv)
	if err != nil {
		return fmt.Errorf("source connection settings: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(maxIdleTime)

	if err := s.initialize(ctx, db, tlsCfg); err != nil {
		_ = db.Close()
		return err
	}
	s.db = db

	poolSize := s.opts.SnapshotPoolSize
	if poolSize <= 0 {
		poolSize = defaultSnapshotPoolSize
	}
	snapshotDB := sql.OpenDB(connector) // a separate pool from the same connector: its own connections
	snapshotDB.SetMaxOpenConns(poolSize)
	snapshotDB.SetConnMaxLifetime(30 * time.Minute)
	snapshotDB.SetConnMaxIdleTime(maxIdleTime)
	s.snapshotDB = snapshotDB
	return nil
}

// driverConfig builds the SQL connection settings. Sessions are read-only: binwave must never
// write to the source, so the server itself refuses writes on our connections.
func driverConfig(cfg config.MySQL, tlsCfg *tls.Config) *sqldriver.Config {
	drv := sqldriver.NewConfig()
	drv.User = cfg.User
	drv.Passwd = cfg.Password
	drv.Net = "tcp"
	drv.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	drv.Timeout = connectTimeout
	drv.TLS = tlsCfg
	drv.AllowFallbackToPlaintext = cfg.TLS.Mode == config.TLSPreferred
	// time_zone: a TIMESTAMP read with SELECT comes in the time zone of the session, and the binlog carries
	// it as UTC, so the two are the same only when the session is UTC.
	// character_set_results: text comes as the bytes the column stores, in its own character set, as
	// the binlog carries it; the server would convert it to the character set of the connection, and
	// the conversion of the values that follow would then be applied twice.
	drv.Params = map[string]string{"transaction_read_only": "1", "time_zone": "'+00:00'", "character_set_results": "NULL"}
	if cfg.CompressionEnabled() {
		// The chunks of a snapshot are nearly all the bytes binwave reads; rows of text compress
		// several times over, and both ends have CPU to spare more often than bandwidth. Apply only
		// fails on an option that fails, and this one cannot.
		_ = drv.Apply(sqldriver.EnableCompression(true))
	}
	return drv
}

func (s *Source) initialize(ctx context.Context, db *sql.DB, tlsCfg *tls.Config) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to source %s:%d: %w", s.cfg.Host, s.cfg.Port, err)
	}
	server, err := checkRequirements(ctx, db)
	if err != nil {
		return err
	}
	s.server = server
	if err := checkDatabases(ctx, db, s.cfg.User, s.opts.Databases); err != nil {
		return err
	}
	cs, err := loadCollations(ctx, db)
	if err != nil {
		return err
	}
	s.collations = cs

	s.streamTLS = tlsCfg
	if s.cfg.TLS.Mode == config.TLSPreferred && tlsCfg != nil {
		// Preferred means "TLS if the server has it": the binlog connection follows what this one negotiated.
		var name, cipher string
		if err := db.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); err != nil || cipher == "" {
			s.streamTLS = nil
		}
	}
	return nil
}

// Close releases the SQL connections.
func (s *Source) Close() error {
	if s.chunkStmts != nil {
		// Before the pools: a prepared statement outlives its connection only as long as the pool does.
		if err := s.chunkStmts.closeAll(); err != nil {
			s.log.Warn("closing snapshot statements failed", "error", err)
		}
	}
	if s.snapshotDB != nil {
		if err := s.snapshotDB.Close(); err != nil {
			return fmt.Errorf("close source snapshot connection: %w", err)
		}
	}
	if s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close source connection: %w", err)
	}
	return nil
}

// ExecutedGTIDSet returns the GTID set the server has executed so far, the starting point for
// "replicate from now".
func (s *Source) ExecutedGTIDSet(ctx context.Context) (string, error) {
	if s.db == nil {
		return "", errors.New("source is not open")
	}
	var set string
	if err := s.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&set); err != nil {
		return "", fmt.Errorf("read gtid_executed: %w", err)
	}
	// The server wraps long sets over several lines.
	return strings.NewReplacer("\n", "", "\r", "", " ", "").Replace(set), nil
}

// Stream reads the binlog from the position after the GTID set from and passes events to handle
// until ctx is cancelled (returns ctx's error) or something fails. It reports Heartbeat while idle.
// started, if not nil, is called once the server has proved it accepts the stream by sending
// its first event: it does so right after it accepts the dump request, so a missing privilege or
// a position that is gone (purged binary logs) shows up as an error before started, not after.
func (s *Source) Stream(ctx context.Context, from string, started func(), handle Handler) error {
	if s.db == nil {
		return errors.New("source is not open")
	}
	st, err := newStream(from, s.collations, s.opts.Include, handle, s.log)
	if err != nil {
		return err
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:  s.cfg.ServerID,
		Flavor:    gomysql.MySQLFlavor,
		Host:      s.cfg.Host,
		Port:      uint16(s.cfg.Port),
		User:      s.cfg.User,
		Password:  s.cfg.Password,
		TLSConfig: s.streamTLS,

		// Values must reach typemap exactly: DECIMAL as decimal.Decimal (never float), temporal
		// values as text, TIMESTAMP text in UTC, JSON in MySQL's text form.
		UseDecimal:              true,
		ParseTime:               false,
		TimestampStringLocation: time.UTC,
		RenderJSONAsMySQLText:   true,
		VerifyChecksum:          true,

		// While it has nothing to send, the server sends a heartbeat every period. That is what
		// tells an idle server from a frozen one. go-mysql's own read timeout is not used: it
		// would fire after any long pause of the consumer, when the reader simply was not
		// allowed to read.
		HeartbeatPeriod: s.opts.HeartbeatPeriod,

		MaxReconnectAttempts: maxReconnectTries,
		Logger:               syncerLogger(s.log, s.opts.OnReconnect),
	})
	defer s.closeSyncer(syncer)

	gset, err := gomysql.ParseMysqlGTIDSet(from)
	if err != nil {
		return fmt.Errorf("parse GTID set %q: %w", from, err)
	}
	streamer, err := syncer.StartSyncGTID(gset)
	if err != nil {
		return fmt.Errorf("start binlog stream from %q: %w", from, err)
	}

	alive := newLiveness(silentPeriods*s.opts.HeartbeatPeriod, s.opts.Now)
	for {
		waitCtx, cancel := context.WithTimeout(ctx, s.opts.IdlePeriod)
		be, err := streamer.GetEvent(waitCtx)
		cancel()

		switch {
		case err == nil:
			if ctx.Err() != nil {
				// Asked to stop: events that were already buffered are left for the next run.
				return fmt.Errorf("binlog stream stopped: %w", ctx.Err())
			}
			alive.touch()
			if started != nil {
				started()
				started = nil
			}
			if err := st.process(ctx, be); err != nil {
				return fmt.Errorf("binlog position %s: %w", position(be), err)
			}
			// Time spent in the handler is the consumer's, not the source's.
			alive.touch()
		case ctx.Err() != nil:
			return fmt.Errorf("binlog stream stopped: %w", ctx.Err())
		case errors.Is(err, context.DeadlineExceeded):
			if d, silent := alive.silence(); silent {
				return fmt.Errorf("%w: no event or heartbeat for %s", ErrSourceSilent, d.Round(time.Second))
			}
			// Nothing arrived, but not yet for long enough to call the source silent: report the
			// idle period, so the caller can flush by time. This is not a sign of life from the
			// source, so it must not reset the liveness clock; only the time the caller spends
			// handling it is excused.
			began := s.opts.Now()
			if err := handle(ctx, heartbeat); err != nil {
				return err
			}
			alive.credit(s.opts.Now().Sub(began))
		default:
			return fmt.Errorf("read binlog: %w", err)
		}
	}
}

// closeGrace bounds how long closing a stream may take.
const closeGrace = 5 * time.Second

// closeSyncer closes the binlog connection without waiting for a server that cannot answer.
// Closing opens a second connection to kill the dump thread on the server; when the server is
// frozen, which is exactly when a stream is being abandoned for silence, that connection would
// block for good and take the caller's shutdown with it. The process exits right after such a
// failure, so a close that is still stuck after the grace period is left behind.
func (s *Source) closeSyncer(syncer *replication.BinlogSyncer) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		syncer.Close()
	}()
	select {
	case <-done:
	case <-time.After(closeGrace):
		s.log.Warn("binlog connection close timed out", "grace", closeGrace)
	}
}

func position(be *replication.BinlogEvent) string {
	return fmt.Sprintf("pos %d (%s)", be.Header.LogPos, be.Header.EventType)
}
