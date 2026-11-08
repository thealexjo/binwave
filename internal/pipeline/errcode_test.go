package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/thealexjo/binwave/internal/errcode"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	"github.com/thealexjo/binwave/internal/typemap"
)

// TestCauseCodeMapsKnownErrorTypes: the code identifies the kind of a data-path error from its Go
// type, the same way whether it ends up quarantining a table or failing one transaction (see
// annotate in failure.go), so an operator sees the same code in either state.
func TestCauseCodeMapsKnownErrorTypes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want errcode.Code
	}{
		{"conflict", &chsink.ConflictError{Err: errors.New("x")}, errcode.REPL101},
		{"schema mismatch", &chsink.SchemaMismatchError{}, errcode.REPL102},
		{"table error", &chsink.TableError{Table: "shop.items", Err: errors.New("x")}, errcode.REPL103},
		{"write error", &WriteError{Table: "shop.items", Err: errors.New("x")}, errcode.REPL103},
		{"conversion error", &typemap.ConversionError{}, errcode.CONV101},
		{"unrecognized", errors.New("some other problem"), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := causeCode(tt.err); got != tt.want {
				t.Errorf("causeCode(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// TestQuarantineLogLineIsShortAndDoesNotRepeatTheTableName: the code names the kind of the problem
// once, in both the message and its own attribute; the detail is only the variable part (which
// column, which type) and must not restate the table name, which is already a separate attribute.
func TestQuarantineLogLineIsShortAndDoesNotRepeatTheTableName(t *testing.T) {
	log, buf, mu := captureLogger()
	h := newHarness(t)
	p := h.pipeline(t)
	p.log = log
	p.q.setSince(fence(0))

	if !p.Quarantine("shop.weird", errcode.TBL106, "a column of type ENUM cannot be compared exactly") {
		t.Fatal("Quarantine() = false")
	}

	mu.Lock()
	line := buf.String()
	mu.Unlock()

	for _, want := range []string{"[TBL106]", "code=TBL106", "table=shop.weird"} {
		if !strings.Contains(line, want) {
			t.Errorf("log line = %q, want it to contain %q", line, want)
		}
	}
	if strings.Count(line, "shop.weird") != 1 {
		t.Errorf("log line = %q, the table name must appear exactly once, not repeated in the detail", line)
	}
	// The old, paragraph-length message must be gone.
	if strings.Contains(line, "resynchronized (binwave tables resync)") {
		t.Errorf("log line = %q, the explanation belongs in docs/reference/error-codes.md, not repeated on every line", line)
	}
}
