package observability

import (
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	snap "github.com/thealexjo/binwave/internal/snapshot"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/failure"
	"github.com/thealexjo/binwave/internal/typemap"
)

// State is where the process is in its life.
type State string

// States.
const (
	StateStarting State = "starting"
	StateReady    State = "ready"
	// StatePaused is a running service that reads nothing: replication was paused by an
	// operator or the service was told not to start it.
	StatePaused State = "paused"
	// StateFailed is a running service that reads nothing because a transaction could not be
	// replicated: the failure is in the status, and the operator resumes replication (retrying,
	// or skipping the transaction) once it is understood.
	StateFailed State = "failed"
	// StateDegraded is a running service whose data path keeps failing: requests to ClickHouse are
	// being retried and nothing is getting through. It is not the same as failed — nobody has given
	// up, and it clears by itself the moment a request succeeds — but it must not look like ready
	// either: the position stands still, and the only other sign of it is the lag growing.
	StateDegraded State = "degraded"
	StateStopping State = "stopping"
)

// degradedAfter is how long the data path must keep failing before the state says degraded. A restart
// of ClickHouse takes seconds, and a state that flips on every hiccup is noise rather than a signal.
const degradedAfter = 15 * time.Second

// maxErrorLength bounds the last error kept for /status.
const maxErrorLength = 500

// Options configure a Hub.
type Options struct {
	// Now is the clock; it defaults to time.Now.
	Now func() time.Time
	// Version and Commit are exposed as binwave_build_info and in /status.
	Version, Commit string
	// Pprof adds /debug/pprof to the handler; see config.Observability.Pprof.
	Pprof bool
}

// LastError is the most recent failure binwave survived (or died of): a request to ClickHouse
// that is being retried, or an attempt to start.
//
// Ongoing says whether it is still happening. Without it the record is unreadable: it is written on
// every retry and kept afterwards, so an operator looking at the status cannot tell a request that is
// failing right now from a hiccup that passed an hour ago.
type LastError struct {
	Time    time.Time `json:"time"`
	Kind    string    `json:"kind"`
	Message string    `json:"message"`
	// Ongoing is set while the operation is still failing; Since is when it started failing and
	// Attempts how many attempts have failed. All three are about the current run of failures, so they
	// are cleared when something succeeds (ResolvedAt then says when that was).
	Ongoing    bool       `json:"ongoing"`
	Since      time.Time  `json:"since,omitempty"`
	Attempts   int        `json:"attempts,omitempty"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
}

// Hub collects what the process reports about itself and serves it as metrics, health and status.
// It implements pipeline.Observer, so the pipeline needs to know nothing else about it, and the
// process-state hooks of the app package. All methods are safe for concurrent use.
//
// The metrics have no per-table labels yet: a service with thousands of tables would make
// thousands of series for every one of them. Observer methods receive the table name, so adding
// per-table series later needs no change outside this package.
type Hub struct {
	now  func() time.Time
	opts Options

	registry *prometheus.Registry

	transactions, rowsRead, rowsWritten, inserts                                          prometheus.Counter
	startupFailures, sourceReconnects, sourceReadRetries, ddlSkipped, skippedTx, rebuilds prometheus.Counter
	quarantineSkipped, resyncs, resyncRows                                                prometheus.Counter
	autoresyncs, autoresyncFailures                                                       prometheus.Counter
	retries, adjustments                                                                  *prometheus.CounterVec
	insertSeconds                                                                         prometheus.Histogram
	bufferRows, bufferBytes, tables                                                       prometheus.Gauge

	mu             sync.Mutex
	pausedSince    time.Time
	state          State
	started        time.Time
	lastProgress   time.Time
	lag            lagTracker
	position       checkpoint.Position
	positionSaved  time.Time
	lastCommitUnix float64
	tableCount     int
	lastError      *LastError
	failure        *failure.Failure
	rebuilding     map[string]bool
	quarantined    []checkpoint.Quarantined
	resyncing      []snap.Status
}

// NewHub creates a Hub in the starting state.
func NewHub(opts Options) *Hub {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	h := &Hub{now: opts.Now, opts: opts, state: StateStarting, registry: prometheus.NewRegistry()}
	h.started = h.now()
	h.lastProgress = h.started

	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "binwave", Name: name, Help: help})
	}
	h.transactions = counter("transactions_total", "Transactions read from the source binlog.")
	h.rowsRead = counter("rows_read_total", "ClickHouse rows produced from the source's row changes (an update produces two).")
	h.rowsWritten = counter("rows_written_total", "Rows accepted by ClickHouse, after duplicates within a batch were collapsed.")
	h.inserts = counter("inserts_total", "Batches accepted by ClickHouse.")
	h.startupFailures = counter("startup_failures_total", "Failed attempts to start, before the service became ready.")
	h.sourceReconnects = counter("source_reconnects_total", "Times the binlog connection was re-established after it broke.")
	h.sourceReadRetries = counter("source_read_retries_total", "Reads of the source for a resync or the initial load that failed and are retried: "+
		"a source that is down is waited out, any other failure gives the resync up after a few attempts.")
	h.quarantineSkipped = counter("quarantine_skipped_rows_total", "Rows of changes dropped because their table is quarantined; the table falls behind the source by that much.")
	h.rebuilds = counter("table_rebuilds_total", "Tables rebuilt under a new sort key because the source changed its primary key or a column of it.")
	h.resyncs = counter("table_resyncs_total", "Tables resynchronized: read again from the source and replaced by the copy.")
	h.resyncRows = counter("resync_rows_read_total", "Rows read from the source by resyncs.")
	h.autoresyncs = counter("autoresync_total", "Tables tables.autoresync resynchronized on its own, without an operator: a quarantine that resolved itself. Off by default.")
	h.autoresyncFailures = counter("autoresync_failed_total", "Automatic resyncs tables.autoresync tried that failed; the table stays quarantined for an operator after enough attempts.")
	h.skippedTx = counter("skipped_transactions_total", "Transactions dropped on the operator's request (binwave replication resume --skip-transactions or --gtid-next): their changes are not in ClickHouse.")
	h.ddlSkipped = counter("destructive_ddl_skipped_total", "DROP, TRUNCATE and partition statements of the source that were left unapplied: ddl.mode additive applies none, and partition operations are not applied in any mode.")
	h.retries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "binwave", Name: "clickhouse_retries_total",
		Help: "Requests to ClickHouse that failed and were retried, by reason: overload (too many parts, memory limit) or transient (network and the like).",
	}, []string{"reason"})
	h.adjustments = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "binwave", Name: "value_adjustments_total",
		Help: "Values replaced instead of converted exactly (zero dates and the like), by rule; each is also logged.",
	}, []string{"kind"})
	h.insertSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "binwave", Name: "insert_duration_seconds",
		Help:    "How long ClickHouse took to accept a batch, retries included.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2.5, 10),
	})
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "binwave", Name: name, Help: help})
	}
	h.bufferRows = gauge("buffer_rows", "Rows waiting in memory to be written to ClickHouse.")
	h.bufferBytes = gauge("buffer_bytes", "Estimated size of the rows waiting in memory.")
	h.tables = gauge("tables", "Source tables being replicated.")

	fn := func(name, help string, f func() float64) prometheus.GaugeFunc {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "binwave", Name: name, Help: help}, f)
	}
	info := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "binwave", Name: "build_info", Help: "Build information; the value is always 1."}, []string{"version", "commit"})
	info.WithLabelValues(opts.Version, opts.Commit).Set(1)

	// One series per state of a table of the initial load. They are separate collectors that share a
	// name and differ by a constant label, which is how a function-backed gauge gets a label.
	var loadStates []prometheus.Collector
	for _, state := range []struct {
		name string
		of   func(InitialLoad) int
	}{
		{"loaded", func(l InitialLoad) int { return l.Loaded }},
		{"reading", func(l InitialLoad) int { return l.Reading }},
		{"waiting_for_stream", func(l InitialLoad) int { return l.WaitingForStream }},
		{"waiting_for_slot", func(l InitialLoad) int { return l.WaitingForSlot }},
	} {
		loadStates = append(loadStates, prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "binwave", Name: "initial_load_tables",
			Help: "Tables of the initial load by state: loaded (replaced, visible in ClickHouse), reading (a slot reads its chunks), " +
				"waiting_for_stream (read in full, replaced once the stream reaches the point it was read at), " +
				"waiting_for_slot (not started yet, see replication.snapshot.parallelism). All 0 when there is no initial load.",
			ConstLabels: prometheus.Labels{"state": state.name},
		}, func() float64 { return float64(state.of(h.initialLoad())) }))
	}

	h.registry.MustRegister(loadStates...)
	h.registry.MustRegister(
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		info,
		h.transactions, h.rowsRead, h.rowsWritten, h.inserts, h.startupFailures, h.sourceReconnects, h.sourceReadRetries, h.ddlSkipped, h.skippedTx, h.rebuilds, h.quarantineSkipped, h.resyncs, h.resyncRows,
		h.autoresyncs, h.autoresyncFailures,
		h.retries, h.adjustments, h.insertSeconds, h.bufferRows, h.bufferBytes, h.tables,
		fn("lag_seconds", "How far ClickHouse is behind the source. 0 while the source has nothing more to send; otherwise the age of the oldest change not yet written and saved. "+
			"A quiet source shows 0, not the time since its last change.", func() float64 { return h.snapshot().lag.Seconds() }),
		fn("quarantined_tables", "Tables that are not being replicated because they cannot be: each is logged, listed in /status and stays behind the source until it is resynchronized and released.",
			func() float64 { return float64(len(h.snapshot().quarantined)) }),
		fn("paused", "1 while replication is paused: the service is up and reads nothing. The lag then grows from where it stood when the pause began.", func() float64 {
			if h.snapshot().state == StatePaused {
				return 1
			}
			return 0
		}),
		fn("replication_failed", "1 while replication is stopped by a transaction that could not be replicated: the service is up, /status and `binwave replication status` describe the failure.", func() float64 {
			if h.snapshot().state == StateFailed {
				return 1
			}
			return 0
		}),
		fn("rebuilding_tables", "Tables being rebuilt under a new sort key right now; while it is above 0 no table is replicated and the lag grows.",
			func() float64 { return float64(len(h.snapshot().rebuilding)) }),
		fn("resyncing_tables", "Tables being resynchronized right now because an operator asked (binwave tables resync) or tables.autoresync did: "+
			"read again from the source into a shadow table while replication goes on. The tables of the initial load are counted by binwave_initial_load_tables.",
			func() float64 {
				n := 0
				for _, r := range h.snapshot().resyncing {
					if r.Kind != "initial_load" {
						n++
					}
				}
				return float64(n)
			}),
		fn("initial_load_covered_tables", "Tables the initial load covers (replication.initial_position: snapshot); 0 when there is no initial load, "+
			"or when the saved position was written before the total was recorded.",
			func() float64 { return float64(h.initialLoad().Total) }),
		fn("ready", "1 while the service is replicating; 0 while starting and stopping, while paused, while stopped by a failure, and while degraded (the data path keeps failing).", func() float64 {
			if h.snapshot().state == StateReady {
				return 1
			}
			return 0
		}),
		fn("degraded", "1 while requests to ClickHouse keep failing and are being retried: the service is up, nobody has given up, and nothing is getting through. "+
			"The position stands still and the lag grows while this is 1.", func() float64 {
			if h.snapshot().state == StateDegraded {
				return 1
			}
			return 0
		}),
		fn("degraded_seconds", "How long requests to ClickHouse have been failing without a success; 0 when they are not.", func() float64 {
			return h.snapshot().degradedFor.Seconds()
		}),
		fn("start_time_seconds", "Start time of the process, in Unix seconds.", func() float64 { return float64(h.started.Unix()) }),
		optionalGauge{
			desc: prometheus.NewDesc("binwave_position_saved_age_seconds", "Seconds since the replication position was last saved; it grows on a quiet source too. "+
				"Absent until this process saves it for the first time.", nil, nil),
			value: func() (float64, bool) {
				s := h.snapshot()
				return s.now.Sub(s.positionSaved).Seconds(), !s.positionSaved.IsZero()
			},
		},
		fn("position_commit_timestamp_seconds", "When the source committed the last transaction covered by the saved position, in Unix seconds; 0 before the first save.", func() float64 {
			return h.snapshot().lastCommitUnix
		}),
		fn("position_version", "Version (the ReplacingMergeTree version) of the last saved position.", func() float64 { return float64(h.snapshot().position.Version) }),
	)
	return h
}

// optionalGauge is a gauge that has no series while its value is not known.
type optionalGauge struct {
	desc  *prometheus.Desc
	value func() (float64, bool)
}

func (g optionalGauge) Describe(ch chan<- *prometheus.Desc) { ch <- g.desc }

func (g optionalGauge) Collect(ch chan<- prometheus.Metric) {
	if v, ok := g.value(); ok {
		ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, v)
	}
}

// Registry is the Prometheus registry behind /metrics.
func (h *Hub) Registry() *prometheus.Registry { return h.registry }

// snapshot is a consistent copy of the state, for gauges and status.
type snapshot struct {
	now            time.Time
	state          State
	started        time.Time
	lastProgress   time.Time
	lag            time.Duration
	idle           bool
	position       checkpoint.Position
	positionSaved  time.Time
	lastCommitUnix float64
	tables         int
	lastError      *LastError
	failure        *failure.Failure
	rebuilding     []string
	quarantined    []checkpoint.Quarantined
	resyncing      []snap.Status
	pausedSince    time.Time
	// degradedFor is how long the data path has been failing; it is 0 unless the state is degraded.
	degradedFor time.Duration
}

func (h *Hub) snapshot() snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	lag, idle := h.lag.lag(now), h.lag.idle
	state := h.state
	var degradedFor time.Duration
	if state == StateReady {
		// Only a service that would otherwise be ready can be degraded: paused, failed, starting and
		// stopping say something an operator has to know first.
		if failing, degraded := h.degradedFor(now); degraded {
			state, degradedFor = StateDegraded, failing
		}
	}
	if h.state == StatePaused || h.state == StateFailed {
		// Nothing is read, so how far behind the source really is cannot be known; it is at
		// least the backlog it stopped with, grown by the pause, and at least as much as the pause
		// has lasted on a source that keeps changing. A paused service must not look caught up.
		lag, idle = max(lag, now.Sub(h.pausedSince)), false
	}
	return snapshot{
		now: now, state: state, started: h.started, lastProgress: h.lastProgress, degradedFor: degradedFor,
		lag: lag, idle: idle, pausedSince: h.pausedSince,
		position: h.position, positionSaved: h.positionSaved, lastCommitUnix: h.lastCommitUnix,
		tables: h.tableCount, lastError: h.lastError, failure: h.failure, quarantined: h.quarantined, rebuilding: h.rebuildingTables(), resyncing: h.resyncing,
	}
}

// ---- process state (app hooks) ----

// Ready marks the service as started.
func (h *Hub) Ready() {
	h.setState(StateReady)
	// The failed attempts to start were recorded as they happened; being ready is their end. An error
	// of another kind, of the data path, is not over because of it.
	h.resolve("startup")
}

// Stopping marks the beginning of the shutdown.
func (h *Hub) Stopping() { h.setState(StateStopping) }

// Paused marks replication as paused: the service is up and reads nothing.
func (h *Hub) Paused() {
	h.mu.Lock()
	if h.state != StatePaused {
		h.pausedSince = h.now()
	}
	h.state = StatePaused
	h.lastProgress = h.now()
	h.mu.Unlock()
}

// Failed marks replication as failed: the service is up and reads nothing until the operator
// resumes it. f is kept whole; the metrics port serves it redacted.
func (h *Hub) Failed(f failure.Failure) {
	h.mu.Lock()
	h.pausedSince = h.now()
	h.state = StateFailed
	h.failure = &f
	h.lastProgress = h.now()
	h.mu.Unlock()
}

// Resumed marks replication as running again.
func (h *Hub) Resumed() {
	// What stopped replication is not failing until it fails again: the transaction is retried, and
	// its first failure is recorded anew. Kept ongoing, it would hold the state at degraded.
	h.resolve("")
	h.mu.Lock()
	h.failure = nil
	h.pausedSince = time.Time{}
	h.state = StateReady
	h.lastProgress = h.now()
	h.mu.Unlock()
}

func (h *Hub) setState(s State) {
	h.mu.Lock()
	h.state = s
	h.lastProgress = h.now()
	h.mu.Unlock()
}

// Progress records that the engine is alive and doing its job.
func (h *Hub) Progress() {
	h.mu.Lock()
	h.lastProgress = h.now()
	h.mu.Unlock()
}

// RecordError remembers a failure for /status. kind says what failed ("startup", "clickhouse").
func (h *Hub) RecordError(kind string, err error) {
	if err == nil {
		return
	}
	msg := err.Error()
	if len(msg) > maxErrorLength {
		msg = msg[:maxErrorLength]
		for !utf8.ValidString(msg) {
			msg = msg[:len(msg)-1]
		}
		msg += "..."
	}
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	// A run of failures of the same kind keeps its start and counts its attempts; anything else starts
	// a new run. Only the data path (ClickHouse) makes a service degraded: a failed attempt to start is
	// reported by the state machine and by binwave_startup_failures_total.
	e := &LastError{Time: now, Kind: kind, Message: msg, Ongoing: true, Since: now, Attempts: 1}
	if prev := h.lastError; prev != nil && prev.Ongoing && prev.Kind == kind {
		e.Since, e.Attempts = prev.Since, prev.Attempts+1
	}
	h.lastError = e
}

// Recovered records that an operation that had been failing succeeded. The record of the failure stays
// for an operator to see what happened, but it stops claiming to be happening.
func (h *Hub) Recovered() { h.resolve("") }

// resolve marks the last error as no longer happening, if it is of kind (any kind when empty).
func (h *Hub) resolve(kind string) {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.lastError; e != nil && e.Ongoing && (kind == "" || e.Kind == kind) {
		resolved := *e
		resolved.Ongoing, resolved.ResolvedAt = false, &now
		h.lastError = &resolved
	}
}

// initialLoad is the summary of the initial load, the same /status shows; the zero value when there is
// none. It is computed per scrape from the statuses of the resyncs, which is a pass over one entry per
// table — cheap next to everything else a scrape does.
func (h *Hub) initialLoad() InitialLoad {
	s := h.snapshot()
	if _, load := summarizeResyncs(s.resyncing, s.position.LoadTotal); load != nil {
		return *load
	}
	return InitialLoad{}
}

// degraded reports whether the data path has been failing long enough to call the service degraded, and
// for how long it has been failing. It is read under the lock the caller holds.
func (h *Hub) degradedFor(now time.Time) (time.Duration, bool) {
	e := h.lastError
	if e == nil || !e.Ongoing || e.Kind != "clickhouse" {
		return 0, false
	}
	failing := now.Sub(e.Since)
	return failing, failing >= degradedAfter
}

// StartupFailed records a failed attempt to start.
func (h *Hub) StartupFailed(err error) {
	h.startupFailures.Inc()
	h.RecordError("startup", err)
}

// SourceReconnected counts a re-established binlog connection.
func (h *Hub) SourceReconnected() { h.sourceReconnects.Inc() }

// ---- pipeline.Observer ----

// TransactionBegan implements pipeline.Observer.
func (h *Hub) TransactionBegan(commit time.Time) {
	h.mu.Lock()
	h.lag.began(commit)
	h.mu.Unlock()
}

// TransactionCommitted implements pipeline.Observer.
func (h *Hub) TransactionCommitted() { h.transactions.Inc() }

// RebuildStarted implements pipeline.Observer.
func (h *Hub) RebuildStarted(table string) {
	h.mu.Lock()
	if h.rebuilding == nil {
		h.rebuilding = map[string]bool{}
	}
	h.rebuilding[table] = true
	h.mu.Unlock()
}

// RebuildFinished implements pipeline.Observer.
func (h *Hub) RebuildFinished(table string, ok bool) {
	h.mu.Lock()
	delete(h.rebuilding, table)
	h.mu.Unlock()
	if ok {
		h.rebuilds.Inc()
	}
}

// rebuildingTables lists the tables being rebuilt, sorted. mu is held.
func (h *Hub) rebuildingTables() []string {
	out := make([]string, 0, len(h.rebuilding))
	for t := range h.rebuilding {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// TransactionSkipped implements pipeline.Observer.
func (h *Hub) TransactionSkipped() { h.skippedTx.Inc() }

// RowsRead implements pipeline.Observer.
func (h *Hub) RowsRead(_ string, rows int) { h.rowsRead.Add(float64(rows)) }

// BufferChanged implements pipeline.Observer.
func (h *Hub) BufferChanged(rows, bytes int) {
	h.bufferRows.Set(float64(rows))
	h.bufferBytes.Set(float64(bytes))
}

// Inserted implements pipeline.Observer.
func (h *Hub) Inserted(_ string, rows int, took time.Duration) {
	h.inserts.Inc()
	h.rowsWritten.Add(float64(rows))
	h.insertSeconds.Observe(took.Seconds())
}

// PositionSaved implements pipeline.Observer.
func (h *Hub) PositionSaved(pos checkpoint.Position, applied, open time.Time) {
	h.mu.Lock()
	h.lag.saved(applied, open)
	h.position = pos
	h.positionSaved = h.now()
	if !applied.IsZero() {
		h.lastCommitUnix = float64(applied.UnixNano()) / 1e9
	}
	h.mu.Unlock()
}

// PositionLoaded implements pipeline.Observer. When the position was saved, and when the source
// committed what it covers, is not known until the next save.
func (h *Hub) PositionLoaded(pos checkpoint.Position) {
	h.mu.Lock()
	h.position = pos
	h.mu.Unlock()
}

// Idle implements pipeline.Observer.
func (h *Hub) Idle() {
	h.mu.Lock()
	h.lag.quiet()
	h.mu.Unlock()
}

// Retry implements pipeline.Observer.
func (h *Hub) Retry(_, reason string, err error) {
	h.retries.WithLabelValues(reason).Inc()
	h.RecordError("clickhouse", err)
}

// SourceRetry implements pipeline.Observer. It does not touch the last error: that is the one of the
// data path, which a success of ClickHouse resolves, and a read of the source is not part of it.
func (h *Hub) SourceRetry(string, error) { h.sourceReadRetries.Inc() }

// TablesBound implements pipeline.Observer.
func (h *Hub) TablesBound(n int) {
	h.tables.Set(float64(n))
	h.mu.Lock()
	h.tableCount = n
	h.mu.Unlock()
}

// RowsSkipped implements pipeline.Observer.
func (h *Hub) RowsSkipped(_ string, rows int) { h.quarantineSkipped.Add(float64(rows)) }

// QuarantineChanged implements pipeline.Observer.
func (h *Hub) QuarantineChanged(entries []checkpoint.Quarantined) {
	sorted := append([]checkpoint.Quarantined(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Table < sorted[j].Table })
	h.mu.Lock()
	h.quarantined = sorted
	h.mu.Unlock()
}

// ResyncChanged implements pipeline.Observer.
func (h *Hub) ResyncChanged(statuses []snap.Status) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resyncing = append([]snap.Status(nil), statuses...)
}

// ResyncRowsRead implements pipeline.Observer.
// The rows are counted here, chunk by chunk, and not from the statuses: those are published at most
// so often, and a table read between two publications would not be counted at all.
func (h *Hub) ResyncRowsRead(_ string, rows int) { h.resyncRows.Add(float64(rows)) }

// ResyncFinished implements pipeline.Observer.
func (h *Hub) ResyncFinished(_ string, ok bool) {
	if ok {
		h.resyncs.Inc()
	}
}

// AutoresyncResolved implements pipeline.Observer.
func (h *Hub) AutoresyncResolved(_ string, ok bool, _ int) {
	if ok {
		h.autoresyncs.Inc()
	} else {
		h.autoresyncFailures.Inc()
	}
}

// DestructiveDDLSkipped implements pipeline.Observer.
func (h *Hub) DestructiveDDLSkipped() { h.ddlSkipped.Inc() }

// ---- typemap.Reporter ----

// countingReporter counts value adjustments by kind and passes them on.
type countingReporter struct {
	next typemap.Reporter
	hub  *Hub
}

func (r countingReporter) Report(a typemap.Adjustment) {
	r.hub.adjustments.WithLabelValues(string(a.Kind)).Inc()
	if r.next != nil {
		r.next.Report(a)
	}
}

// WrapReporter returns a typemap.Reporter that counts every adjustment and then hands it to next.
func (h *Hub) WrapReporter(next typemap.Reporter) typemap.Reporter {
	return countingReporter{next: next, hub: h}
}
