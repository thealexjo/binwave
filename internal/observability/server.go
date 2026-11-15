package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/failure"
	snap "github.com/thealexjo/binwave/internal/snapshot"
)

// healthStaleAfter is how long the engine may go without reporting progress before /healthz says
// it is stuck. Progress is reported at least once a second while anything is happening, and at
// every idle poll (a flush interval, at most five seconds), so half a minute is far from normal.
const healthStaleAfter = 30 * time.Second

// Status is the body of /status.
type Status struct {
	State         State   `json:"state"`
	Version       string  `json:"version"`
	Commit        string  `json:"commit"`
	UptimeSeconds float64 `json:"uptime_seconds"`
	LagSeconds    float64 `json:"lag_seconds"`
	CaughtUp      bool    `json:"caught_up"`
	Position      string  `json:"position"`
	Tables        int     `json:"tables"` // source tables the configuration selects; see binwave_tables

	LastError *LastError `json:"last_error"`
	// PausedSince is when replication was paused; null while it runs.
	PausedSince *time.Time `json:"paused_since"`
	// Quarantined lists the tables that are not being replicated, with the reason.
	Quarantined []checkpoint.Quarantined `json:"quarantined"`
	// Rebuilding lists the tables that are being rebuilt under a new sort key; replication of every
	// table waits until they are done.
	Rebuilding []string `json:"rebuilding"`
	// Resyncing lists the resyncs worth naming one by one: the ones an operator asked for
	// (binwave tables resync, tables.autoresync) and the tables of the initial load that are being
	// read right now. The tables of the initial load that are only waiting are counted in
	// InitialLoad instead: a source with thousands of tables would otherwise make a list nobody can
	// read, and a status document of megabytes.
	Resyncing []snap.Status `json:"resyncing"`
	// InitialLoad summarizes the initial load (replication.initial_position: snapshot); null when
	// none is in flight.
	InitialLoad *InitialLoad `json:"initial_load"`
	// Failure is what stopped replication, while the state is failed. The full form, with the
	// key values of the failing row, is only for the control socket: see Redacted.
	Failure *failure.Failure `json:"failure"`
}

// InitialLoad is how far the initial load has got, as counts rather than a line per table.
type InitialLoad struct {
	// Total is how many tables the load covered when it began, and Loaded how many of those are
	// done. Both are 0 when the saved position does not say (it was written before the total was
	// recorded): then only Left is known.
	Total  int `json:"total"`
	Loaded int `json:"loaded"`
	// Left is how many tables are still to be loaded, and the three counts below say what each of
	// them is doing: reading now, read and waiting for the stream to reach the point it was read at,
	// or waiting for a slot (replication.snapshot.parallelism).
	Left             int `json:"left"`
	Reading          int `json:"reading"`
	WaitingForStream int `json:"waiting_for_stream"`
	WaitingForSlot   int `json:"waiting_for_slot"`
	// RowsPerSecond is what the tables being read add up to.
	RowsPerSecond float64 `json:"rows_per_second"`
}

// summarizeResyncs splits the resyncs into the ones to name one by one and the summary of the
// initial load. loadTotal is checkpoint.Position.LoadTotal.
func summarizeResyncs(statuses []snap.Status, loadTotal int) ([]snap.Status, *InitialLoad) {
	var named []snap.Status
	var load InitialLoad
	for _, r := range statuses {
		if r.Kind != "initial_load" {
			named = append(named, r) // an operator is watching this one
			continue
		}
		load.Left++
		switch {
		case r.Done:
			load.WaitingForStream++
		case r.Reading:
			load.Reading++
			load.RowsPerSecond += r.RowsPerSecond
			named = append(named, r)
		default:
			load.WaitingForSlot++
		}
	}
	if load.Left == 0 {
		return named, nil
	}
	if loadTotal >= load.Left {
		load.Total, load.Loaded = loadTotal, loadTotal-load.Left
	}
	return named, &load
}

// Redacted is the status without personal data, for the metrics port.
func (s Status) Redacted() Status {
	if s.Failure != nil {
		r := s.Failure.Redacted()
		s.Failure = &r
	}
	return s
}

// Status returns the current status.
func (h *Hub) Status() Status {
	s := h.snapshot()
	named, load := summarizeResyncs(s.resyncing, s.position.LoadTotal)
	return Status{
		State: s.state, Version: h.opts.Version, Commit: h.opts.Commit,
		UptimeSeconds: s.now.Sub(s.started).Seconds(),
		LagSeconds:    s.lag.Seconds(), CaughtUp: s.idle,
		Position: s.position.GTIDSet, Tables: s.tables, LastError: s.lastError,
		Quarantined: append([]checkpoint.Quarantined{}, s.quarantined...),
		PausedSince: pausedSince(s.pausedSince),
		Failure:     s.failure,
		Rebuilding:  s.rebuilding,
		Resyncing:   named,
		InitialLoad: load,
	}
}

// Healthy reports whether the process is alive and doing its job, with a reason when it is not.
// While starting it is healthy: a database that is down is retried, and the service manager's
// start timeout is what ends a start that goes on too long. Afterwards the engine must keep
// reporting progress, the same signal the systemd watchdog relies on.
func (h *Hub) Healthy() (bool, string) {
	s := h.snapshot()
	if s.state == StateStarting {
		return true, "starting"
	}
	if idle := s.now.Sub(s.lastProgress); idle > healthStaleAfter {
		return false, fmt.Sprintf("no progress for %s", idle.Round(time.Second))
	}
	return true, "ok"
}

// Handler serves /metrics, /healthz, /readyz and /status, and /debug/pprof when it is asked for.
//
//   - /metrics: Prometheus metrics.
//   - /healthz: 200 while the process is alive and doing its job, 503 when it is stuck.
//   - /readyz: 200 once replication has started, 503 while starting and stopping.
//   - /status: the state, the position, the lag and the last error, as JSON.
//   - /debug/pprof: the Go profiles, only with Options.Pprof (observability.pprof).
func (h *Hub) Handler() http.Handler {
	mux := http.NewServeMux()
	if h.opts.Pprof {
		// The profiles of the running process: which is the only way to answer "where is the time
		// going" without rebuilding and restarting it.
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	mux.Handle("GET /metrics", promhttp.HandlerFor(h.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		ok, why := h.Healthy()
		writeText(w, ok, why)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		state := h.snapshot().state
		writeText(w, state == StateReady, string(state))
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(h.Status().Redacted())
	})
	return mux
}

func writeText(w http.ResponseWriter, ok bool, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_, _ = fmt.Fprintln(w, body)
}

// Serve listens on addr and serves handler until the returned stop function is called. Listening
// happens before Serve returns, so an address that is taken is an error at start, not a log line.
// stop shuts the server down, giving requests in flight a few seconds.
func Serve(addr string, handler http.Handler, log *slog.Logger) (stop func(), err error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on observability.listen %s: %w", addr, err)
	}
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("the metrics server failed", "error", err)
		}
	}()
	log.Info("metrics and health are served", "address", ln.Addr().String())

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}, nil
}

func pausedSince(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
