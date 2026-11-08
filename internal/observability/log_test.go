package observability

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/config"
)

func TestNewLogger(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		var buf bytes.Buffer
		log, err := NewLogger(&buf, config.Observability{LogLevel: "info", LogFormat: config.LogJSON})
		if err != nil {
			t.Fatal(err)
		}
		log.Info("hello", "k", "v")

		var rec map[string]any
		if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
			t.Fatalf("output is not JSON: %q", buf.String())
		}
		if rec["msg"] != "hello" || rec["k"] != "v" {
			t.Errorf("record = %v", rec)
		}
		if _, ok := rec["time"]; ok {
			t.Errorf("record = %v, want no time: journald/Docker already stamp every line", rec)
		}
	})

	t.Run("text", func(t *testing.T) {
		var buf bytes.Buffer
		log, err := NewLogger(&buf, config.Observability{LogLevel: "info", LogFormat: config.LogText})
		if err != nil {
			t.Fatal(err)
		}
		log.Info("hello")
		out := buf.String()
		if !strings.Contains(out, "msg=hello") {
			t.Errorf("output = %q", out)
		}
		if strings.Contains(out, "time=") {
			t.Errorf("output = %q, want no time attribute: journald/Docker already stamp every line", out)
		}
	})

	t.Run("level filters", func(t *testing.T) {
		var buf bytes.Buffer
		log, err := NewLogger(&buf, config.Observability{LogLevel: "warn", LogFormat: config.LogJSON})
		if err != nil {
			t.Fatal(err)
		}
		log.Info("dropped")
		log.Warn("kept")
		if out := buf.String(); strings.Contains(out, "dropped") || !strings.Contains(out, "kept") {
			t.Errorf("output = %q", out)
		}
	})
}

func TestNewLoggerErrors(t *testing.T) {
	if _, err := NewLogger(&bytes.Buffer{}, config.Observability{LogLevel: "loud", LogFormat: config.LogJSON}); err == nil {
		t.Error("accepted an unknown level")
	}
	if _, err := NewLogger(&bytes.Buffer{}, config.Observability{LogLevel: "info", LogFormat: "xml"}); err == nil {
		t.Error("accepted an unknown format")
	}
}
