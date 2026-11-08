// Package sdnotify implements the systemd notification protocol (sd_notify) over NOTIFY_SOCKET
// without cgo or libsystemd. Without NOTIFY_SOCKET (Docker, a terminal) every call is a no-op.
package sdnotify

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Environment variables of the protocol.
const (
	socketEnv       = "NOTIFY_SOCKET"
	watchdogUsecEnv = "WATCHDOG_USEC"
	watchdogPIDEnv  = "WATCHDOG_PID"
)

// EnvLookup resolves environment variables; os.LookupEnv in production.
type EnvLookup func(name string) (string, bool)

// Notifier sends state notifications to systemd. The zero value and a nil *Notifier are no-ops.
type Notifier struct {
	addr *net.UnixAddr
}

// New creates a Notifier from the environment. An unset or empty NOTIFY_SOCKET gives a no-op
// Notifier; an unsupported value is an error.
func New(env EnvLookup) (*Notifier, error) {
	path, ok := env(socketEnv)
	if !ok || path == "" {
		return &Notifier{}, nil
	}

	switch {
	case strings.HasPrefix(path, "/"):
	case strings.HasPrefix(path, "@"):
		path = "\x00" + path[1:] // abstract socket namespace
	default:
		return nil, fmt.Errorf("%s=%q: only absolute paths and abstract sockets (@name) are supported", socketEnv, path)
	}
	return &Notifier{addr: &net.UnixAddr{Name: path, Net: "unixgram"}}, nil
}

// Enabled reports whether notifications reach systemd.
func (n *Notifier) Enabled() bool { return n != nil && n.addr != nil }

// Ready reports that startup is complete (READY=1).
func (n *Notifier) Ready() error { return n.send("READY=1") }

// Stopping reports that shutdown has begun (STOPPING=1).
func (n *Notifier) Stopping() error { return n.send("STOPPING=1") }

// Watchdog pings the service watchdog (WATCHDOG=1).
func (n *Notifier) Watchdog() error { return n.send("WATCHDOG=1") }

// Status sets the free-form status line shown by systemctl status.
func (n *Notifier) Status(text string) error {
	// A newline would let the text inject extra assignments.
	return n.send("STATUS=" + strings.ReplaceAll(text, "\n", " "))
}

func (n *Notifier) send(state string) error {
	if !n.Enabled() {
		return nil
	}
	conn, err := net.DialUnix("unixgram", nil, n.addr)
	if err != nil {
		return fmt.Errorf("sd_notify: connect: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte(state)); err != nil {
		return fmt.Errorf("sd_notify: send %q: %w", state, err)
	}
	return nil
}

// WatchdogTimeout returns the watchdog timeout configured by systemd (WatchdogSec=), or zero
// when the watchdog is disabled or addressed to another process. pid is the current process ID.
func WatchdogTimeout(env EnvLookup, pid int) (time.Duration, error) {
	raw, ok := env(watchdogUsecEnv)
	if !ok || raw == "" {
		return 0, nil
	}
	usec, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || usec <= 0 {
		return 0, fmt.Errorf("%s=%q: want a positive number of microseconds", watchdogUsecEnv, raw)
	}

	if rawPID, ok := env(watchdogPIDEnv); ok && rawPID != "" {
		target, err := strconv.Atoi(rawPID)
		if err != nil {
			return 0, errors.New(watchdogPIDEnv + ": not a number: " + strconv.Quote(rawPID))
		}
		if target != pid {
			return 0, nil
		}
	}
	return time.Duration(usec) * time.Microsecond, nil
}
