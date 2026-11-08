package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/thealexjo/binwave/internal/checkpoint"
	"github.com/thealexjo/binwave/internal/observability"
)

// ErrNotRunning means nothing answers on the socket: the service is not running, or does not
// have a control channel.
var ErrNotRunning = errors.New("no service answers on the control socket")

// Client talks to a service over its control socket.
type Client struct {
	Socket string
}

func (c Client) http() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Minute, // a pause waits for the final flush
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "unix", c.Socket)
			},
		},
	}
}

// do sends a request and decodes the answer into out (if not nil).
func (c Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode the request: %w", err)
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://binwave"+path, body)
	if err != nil {
		return fmt.Errorf("build the request: %w", err)
	}
	resp, err := c.http().Do(req)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOENT) {
			return fmt.Errorf("%w (%s)", ErrNotRunning, c.Socket)
		}
		return fmt.Errorf("talk to the service on %s: %w", c.Socket, err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read the answer: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		var eb errorBody
		if json.Unmarshal(data, &eb) == nil && eb.Error != "" {
			if resp.StatusCode == http.StatusServiceUnavailable {
				return fmt.Errorf("%w: %s", ErrStarting, eb.Error)
			}
			return errors.New(eb.Error)
		}
		return fmt.Errorf("the service answered %s", resp.Status)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("the answer is not valid JSON: %w", err)
		}
	}
	return nil
}

// Pause stops replication; it returns when it has stopped.
func (c Client) Pause(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/pause", nil, nil)
}

// Resume starts replication again, dropping the transactions the request names.
func (c Client) Resume(ctx context.Context, req ResumeRequest) error {
	return c.do(ctx, http.MethodPost, "/v1/resume", req, nil)
}

// Status returns the status of the service.
func (c Client) Status(ctx context.Context) (observability.Status, error) {
	var st observability.Status
	err := c.do(ctx, http.MethodGet, "/v1/status", nil, &st)
	return st, err
}

// Quarantined lists the tables that are set aside.
func (c Client) Quarantined(ctx context.Context) ([]checkpoint.Quarantined, error) {
	var entries []checkpoint.Quarantined
	err := c.do(ctx, http.MethodGet, "/v1/tables", nil, &entries)
	return entries, err
}

// Release takes tables out of quarantine. The caller has acknowledged the data gap.
func (c Client) Release(ctx context.Context, tables []string) (released, unknown []string, err error) {
	var resp releaseResponse
	if err := c.do(ctx, http.MethodPost, "/v1/tables/release", releaseRequest{Tables: tables, AcceptDataGap: true}, &resp); err != nil {
		return nil, nil, err
	}
	return resp.Released, resp.Unknown, nil
}

// Resync asks the service to read tables again from the source. It returns when each resync has begun,
// or with the reason it could not.
func (c Client) Resync(ctx context.Context, tables []string) ([]ResyncResult, error) {
	var resp resyncResponse
	if err := c.do(ctx, http.MethodPost, "/v1/tables/resync", resyncRequest{Tables: tables}, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// SnapshotRate changes the combined speed limit of the initial load while it runs; 0 removes it. The
// change is not saved: after a restart the limit is the configured
// replication.snapshot.max_rows_per_second again.
func (c Client) SnapshotRate(ctx context.Context, rowsPerSecond int) error {
	return c.do(ctx, http.MethodPost, "/v1/replication/snapshot-rate", snapshotRateRequest{RowsPerSecond: rowsPerSecond}, nil)
}

// ChunkSize fixes how many rows a chunk of a snapshot read asks for, while the reads run; 0 returns to
// adaptive sizing. The change is not saved: after a restart the configuration decides again.
func (c Client) ChunkSize(ctx context.Context, rows int) error {
	return c.do(ctx, http.MethodPost, "/v1/replication/chunk-size", chunkSizeRequest{Rows: rows}, nil)
}
