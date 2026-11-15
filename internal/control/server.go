package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
)

// umaskMu serializes the change of the process umask, which is global, around creating the
// socket: the socket must never exist with a mode wider than 0600, not even for a moment.
var umaskMu sync.Mutex

// Serve listens on the Unix socket at path and serves the control API until the returned stop
// function is called. A socket left by a process that died is replaced; a socket that another
// live service answers on is an error. The socket is private to the user of the process.
//
// The server holds an exclusive lock on path + ".lock" for its whole life, so of the processes
// that start at once on the same path only one replaces, creates and later removes the socket.
func Serve(path string, b Backend, log *slog.Logger) (stop func(), err error) {
	lock, err := lockSocket(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	if err := removeStale(path); err != nil {
		return nil, err
	}

	umaskMu.Lock()
	old := syscall.Umask(0o177)
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "unix", path)
	syscall.Umask(old)
	umaskMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("listen on control.socket %s: %w", path, err)
	}

	srv := &http.Server{Handler: handler(b), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("the control channel failed", "error", err)
		}
	}()
	log.Info("control channel is served", "socket", path)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx) // closing the listener removes the socket
		<-done
		_ = lock.Close()
	}, nil
}

// lockSocket takes the lock that makes a process the owner of the socket at path. The lock file is
// left in place: removing it would let two processes hold locks on two different files.
func lockSocket(path string) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("control.socket %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("control.socket %s is served by another process", path)
		}
		return nil, fmt.Errorf("lock control.socket %s: %w", path, err)
	}
	return f, nil
}

// removeStale removes the socket that a process which died left at path, and nothing else: not a
// file that is not a socket, and not a socket that may still be served.
func removeStale(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("control.socket %s: %w", path, err)
	}
	if info.Mode().Type() != os.ModeSocket {
		return fmt.Errorf("control.socket %s is not a socket, and binwave does not remove it: set control.socket to a path of its own", path)
	}
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(context.Background(), "unix", path)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("control.socket %s is served by another process", path)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("control.socket %s: cannot tell whether another process serves it: %w", path, err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove the stale control socket %s: %w", path, err)
	}
	return nil
}

func handler(b Backend) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/pause", func(w http.ResponseWriter, r *http.Request) {
		reply(w, b.Pause(r.Context()), map[string]string{"state": "paused"})
	})
	mux.HandleFunc("POST /v1/resume", func(w http.ResponseWriter, r *http.Request) {
		var req ResumeRequest
		if r.ContentLength != 0 {
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
				reply(w, fmt.Errorf("the request is not valid JSON: %w", err), nil)
				return
			}
		}
		reply(w, b.Resume(r.Context(), req), map[string]string{"state": "running"})
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		st, err := b.Status()
		reply(w, err, st)
	})
	mux.HandleFunc("GET /v1/tables", func(w http.ResponseWriter, r *http.Request) {
		entries, err := b.Quarantined(r.Context())
		if entries == nil {
			entries = []checkpoint.Quarantined{}
		}
		reply(w, err, entries)
	})
	mux.HandleFunc("POST /v1/tables/release", func(w http.ResponseWriter, r *http.Request) {
		var req releaseRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			reply(w, fmt.Errorf("the request is not valid JSON: %w", err), nil)
			return
		}
		if !req.AcceptDataGap {
			reply(w, ErrDataGapNotAccepted, nil)
			return
		}
		released, unknown, err := b.Release(r.Context(), req.Tables)
		reply(w, err, releaseResponse{Released: nonNil(released), Unknown: nonNil(unknown)})
	})
	mux.HandleFunc("POST /v1/tables/resync", func(w http.ResponseWriter, r *http.Request) {
		var req resyncRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			reply(w, fmt.Errorf("the request is not valid JSON: %w", err), nil)
			return
		}
		results, err := b.Resync(r.Context(), req.Tables)
		if results == nil {
			results = []ResyncResult{}
		}
		reply(w, err, resyncResponse{Results: results})
	})
	mux.HandleFunc("POST /v1/replication/snapshot-rate", func(w http.ResponseWriter, r *http.Request) {
		var req snapshotRateRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			reply(w, fmt.Errorf("the request is not valid JSON: %w", err), nil)
			return
		}
		reply(w, b.SnapshotRate(r.Context(), req.RowsPerSecond), map[string]int{"rows_per_second": req.RowsPerSecond})
	})
	mux.HandleFunc("POST /v1/replication/chunk-size", func(w http.ResponseWriter, r *http.Request) {
		var req chunkSizeRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			reply(w, fmt.Errorf("the request is not valid JSON: %w", err), nil)
			return
		}
		reply(w, b.ChunkSize(r.Context(), req.Rows), map[string]int{"rows": req.Rows})
	})
	return mux
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// reply writes the result of an operation: the body on success, an error body otherwise.
func reply(w http.ResponseWriter, err error, body any) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, ErrStarting) {
			code = http.StatusServiceUnavailable
		}
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(errorBody{Error: err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}
