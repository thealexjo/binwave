package observability

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/thealexjo/binwave/internal/config"
)

// NewLogger builds the process logger from the observability settings. Output goes to w
// (stderr in production, so journald and Docker collect it); binwave never writes log files.
//
// No timestamp: journald and Docker's logging driver already stamp every line, and repeating it
// inside the line as well only lengthens it. A line binwave itself needs to time (a failure, a
// resync's progress) carries that time as its own named attribute, not the record's timestamp.
func NewLogger(w io.Writer, o config.Observability) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(o.LogLevel)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", o.LogLevel, err)
	}
	opts := &slog.HandlerOptions{Level: level, ReplaceAttr: dropTime}

	switch o.LogFormat {
	case config.LogJSON:
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	case config.LogText:
		return slog.New(slog.NewTextHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("log format %q: unknown", o.LogFormat)
	}
}

// dropTime removes the record's own timestamp attribute (see NewLogger).
func dropTime(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}
