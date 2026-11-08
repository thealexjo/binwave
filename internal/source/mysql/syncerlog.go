package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// droppedSyncerMessages are the go-mysql messages that dump its configuration or internal
// structures as log attributes. The configuration holds the account password, and the dumps do
// not even serialize (the log shows an encoding error), so they are dropped whatever the level:
// a secret must not depend on a formatter failing to print it.
var droppedSyncerMessages = map[string]bool{
	"create BinlogSyncer":                true,
	"begin to sync binlog from GTID set": true,
}

// syncerLogger returns the logger handed to go-mysql: ours, minus the messages above.
// onReconnect, if not nil, is called whenever go-mysql reports that it re-establishes the connection.
func syncerLogger(log *slog.Logger, onReconnect func()) *slog.Logger {
	return slog.New(filteringHandler{next: log.Handler(), onReconnect: onReconnect})
}

// reconnectMessage is what go-mysql logs when it starts over after a broken connection.
const reconnectMessage = "begin to re-sync"

// retryMessage is what go-mysql logs, at the error level, before each new attempt to reconnect; the
// attempt that gives up logs another message, which stays an error.
const retryMessage = "retry sync err, wait 1s and retry again"

// killMessage is what go-mysql logs when, after reconnecting, it cannot kill the connection it had.
const killMessage = "kill connection"

// unknownThread is the answer of a server that does not know the connection any more: it was
// restarted, or the connection died with the network. That is what the kill was for.
const unknownThread = "Unknown thread id"

// quieted is the record as binwave logs it: go-mysql prints the stack of every error it traced, a few
// kilobytes on each attempt to reconnect while MySQL restarts, so the message and the attributes keep
// their first line, which says what failed. Waiting to try again, and the old connection being gone
// after a reconnect, are what is meant to happen, so they are logged lower.
func quieted(r slog.Record) slog.Record {
	level := r.Level
	var attrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		a = firstLines(a)
		if r.Message == killMessage && a.Key == "error" && strings.Contains(a.Value.String(), unknownThread) {
			level = slog.LevelDebug
		}
		attrs = append(attrs, a)
		return true
	})
	if r.Message == retryMessage && level > slog.LevelWarn {
		level = slog.LevelWarn
	}
	out := slog.NewRecord(r.Time, level, firstLine(r.Message), r.PC)
	out.AddAttrs(attrs...)
	return out
}

// firstLines cuts every value of a, an error among them, to its first line.
func firstLines(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindGroup:
		group := v.Group()
		cut := make([]slog.Attr, len(group))
		for i, g := range group {
			cut[i] = firstLines(g)
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(cut...)}
	case slog.KindString:
		return slog.String(a.Key, firstLine(v.String()))
	case slog.KindAny:
		if err, ok := v.Any().(error); ok {
			return slog.String(a.Key, firstLine(err.Error()))
		}
	}
	return slog.Attr{Key: a.Key, Value: v}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

type filteringHandler struct {
	next        slog.Handler
	onReconnect func()
}

// Enabled says yes to everything when a reconnect callback is set: the reconnect message is
// counted whatever the log level is, and Handle applies the level before writing.
func (h filteringHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.onReconnect != nil || h.next.Enabled(ctx, l)
}

func (h filteringHandler) Handle(ctx context.Context, r slog.Record) error {
	if droppedSyncerMessages[r.Message] {
		return nil
	}
	if r.Message == reconnectMessage && h.onReconnect != nil {
		h.onReconnect()
	}
	// quieted only ever lowers the level, so what is not enabled before it is not after it either.
	if !h.next.Enabled(ctx, r.Level) {
		return nil
	}
	if r = quieted(r); !h.next.Enabled(ctx, r.Level) {
		return nil
	}
	if err := h.next.Handle(ctx, r); err != nil {
		return fmt.Errorf("log a go-mysql message: %w", err)
	}
	return nil
}

func (h filteringHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return filteringHandler{next: h.next.WithAttrs(attrs), onReconnect: h.onReconnect}
}

func (h filteringHandler) WithGroup(name string) slog.Handler {
	return filteringHandler{next: h.next.WithGroup(name), onReconnect: h.onReconnect}
}
