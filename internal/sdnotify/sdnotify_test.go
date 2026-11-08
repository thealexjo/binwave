package sdnotify

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func mapEnv(m map[string]string) EnvLookup {
	return func(name string) (string, bool) {
		v, ok := m[name]
		return v, ok
	}
}

// listen starts a unixgram socket like systemd's. The path is short because sockaddr_un is limited to ~104 bytes.
func listen(t *testing.T) (path string, recv func() string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "sd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	path = filepath.Join(dir, "n")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return path, func() string {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 256)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("no notification received: %v", err)
		}
		return string(buf[:n])
	}
}

func TestNotificationsReachSocket(t *testing.T) {
	path, recv := listen(t)
	n, err := New(mapEnv(map[string]string{"NOTIFY_SOCKET": path}))
	if err != nil {
		t.Fatal(err)
	}
	if !n.Enabled() {
		t.Fatal("Enabled() = false with NOTIFY_SOCKET set")
	}

	tests := []struct {
		name string
		send func() error
		want string
	}{
		{"ready", n.Ready, "READY=1"},
		{"stopping", n.Stopping, "STOPPING=1"},
		{"watchdog", n.Watchdog, "WATCHDOG=1"},
		{"status", func() error { return n.Status("flushing") }, "STATUS=flushing"},
		{"status newline is neutralized", func() error { return n.Status("a\nREADY=1") }, "STATUS=a READY=1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.send(); err != nil {
				t.Fatalf("send: %v", err)
			}
			if got := recv(); got != tt.want {
				t.Errorf("received %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNoSocketIsNoop(t *testing.T) {
	for name, env := range map[string]map[string]string{"unset": {}, "empty": {"NOTIFY_SOCKET": ""}} {
		t.Run(name, func(t *testing.T) {
			n, err := New(mapEnv(env))
			if err != nil {
				t.Fatal(err)
			}
			if n.Enabled() {
				t.Error("Enabled() = true without NOTIFY_SOCKET")
			}
			if err := n.Ready(); err != nil {
				t.Errorf("Ready() on a no-op notifier: %v", err)
			}
		})
	}

	var nilNotifier *Notifier
	if err := nilNotifier.Ready(); err != nil {
		t.Errorf("Ready() on a nil notifier: %v", err)
	}
}

func TestUnsupportedSocket(t *testing.T) {
	if _, err := New(mapEnv(map[string]string{"NOTIFY_SOCKET": "vsock:2:1234"})); err == nil {
		t.Error("New() accepted an unsupported socket address")
	}
}

func TestAbstractSocketAddress(t *testing.T) {
	n, err := New(mapEnv(map[string]string{"NOTIFY_SOCKET": "@binwave-test"}))
	if err != nil {
		t.Fatal(err)
	}
	if want := "\x00binwave-test"; n.addr.Name != want {
		t.Errorf("address = %q, want %q", n.addr.Name, want)
	}
}

func TestSendToMissingSocketFails(t *testing.T) {
	n, err := New(mapEnv(map[string]string{"NOTIFY_SOCKET": filepath.Join(t.TempDir(), "gone")}))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Ready(); err == nil {
		t.Error("Ready() succeeded without a listening socket")
	}
}

func TestWatchdogTimeout(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    time.Duration
		wantErr bool
	}{
		{"disabled", map[string]string{}, 0, false},
		{"enabled", map[string]string{"WATCHDOG_USEC": "60000000"}, time.Minute, false},
		{"our pid", map[string]string{"WATCHDOG_USEC": "1000000", "WATCHDOG_PID": "42"}, time.Second, false},
		{"other pid", map[string]string{"WATCHDOG_USEC": "1000000", "WATCHDOG_PID": "7"}, 0, false},
		{"not a number", map[string]string{"WATCHDOG_USEC": "soon"}, 0, true},
		{"zero", map[string]string{"WATCHDOG_USEC": "0"}, 0, true},
		{"bad pid", map[string]string{"WATCHDOG_USEC": "1000000", "WATCHDOG_PID": "me"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := WatchdogTimeout(mapEnv(tt.env), 42)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("timeout = %v, want %v", got, tt.want)
			}
		})
	}
}
