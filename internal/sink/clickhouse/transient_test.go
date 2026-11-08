package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},

		// Conditions that clear up by themselves: retrying is right.
		{"too many parts, the server asks to slow down", &ch.Exception{Code: 252}, true},
		{"memory limit", &ch.Exception{Code: 241}, true},
		{"too many queries", &ch.Exception{Code: 202}, true},
		{"read only replica", &ch.Exception{Code: 164}, true},
		{"unknown status of insert", &ch.Exception{Code: 319}, true},
		{"network error code", &ch.Exception{Code: 210}, true},
		{"wrapped exception", fmt.Errorf("insert 5 rows: %w", &ch.Exception{Code: 252}), true},
		{"connection reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, true},
		{"broken pipe", &net.OpError{Op: "write", Err: syscall.EPIPE}, true},
		{"connection refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, true},
		{"eof", io.EOF, true},
		{"unexpected eof", fmt.Errorf("read block: %w", io.ErrUnexpectedEOF), true},
		{"attempt timed out", context.DeadlineExceeded, true},
		{"no connection available", ch.ErrAcquireConnTimeout, true},

		// Conditions a retry cannot fix: the same statement fails the same way.
		{"unknown table", &ch.Exception{Code: 60}, false},
		{"type mismatch", &ch.Exception{Code: 53}, false},
		{"cannot parse", &ch.Exception{Code: 27}, false},
		{"authentication failed", &ch.Exception{Code: 516}, false},
		{"access denied", &ch.Exception{Code: 497}, false},
		{"syntax error", &ch.Exception{Code: 62}, false},
		{"client side conversion", errors.New("clickhouse [AppendRow]: (5 x) converting string to Int32"), false},
		{"cancelled by the caller", context.Canceled, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransient(tt.err); got != tt.want {
				t.Errorf("IsTransient(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsOverload(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"too many parts", &ch.Exception{Code: 252}, true},
		{"memory limit", &ch.Exception{Code: 241}, true},
		{"too many queries", &ch.Exception{Code: 202}, true},
		{"timeout exceeded", &ch.Exception{Code: 159}, true},
		{"wrapped", fmt.Errorf("insert: %w", &ch.Exception{Code: 252}), true},
		{"network error is not overload", &ch.Exception{Code: 210}, false},
		{"connection refused is not overload", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, false},
		{"unknown table is not overload", &ch.Exception{Code: 60}, false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsOverload(tt.err); got != tt.want {
				t.Errorf("IsOverload(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestPermanentErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"authentication failed", &ch.Exception{Code: 516}, true},
		{"wrapped access denied", fmt.Errorf("insert: %w", &ch.Exception{Code: 497}), true},
		{"wrong password", &ch.Exception{Code: 193}, true},
		{"missing privilege found by CHECK GRANT", fmt.Errorf("start: %w", &AccessError{Database: "shop", Privilege: "INSERT"}), true},
		{"too many parts passes by itself", &ch.Exception{Code: 252}, false},
		{"unknown table", &ch.Exception{Code: 60}, false},
		{"connection refused", errors.New("dial tcp: connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hint, got := Permanent(tt.err)
			if got != tt.want {
				t.Fatalf("Permanent(%v) = %v, want %v", tt.err, got, tt.want)
			}
			if got && hint == "" {
				t.Error("a permanent error must come with a hint for the operator")
			}
			if got && IsTransient(tt.err) {
				t.Error("an error cannot be both permanent and transient")
			}
		})
	}
}

func TestIsTableProblem(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"table error", &TableError{Table: "shop.t", Err: errors.New("no key")}, true},
		{"wrapped table error", fmt.Errorf("bind: %w", &TableError{Table: "shop.t", Err: errors.New("x")}), true},
		{"schema mismatch", &SchemaMismatchError{Table: "t", Problems: []string{"column a is missing"}}, true},
		{"network", syscall.ECONNRESET, false},
		{"overload", &ch.Exception{Code: 252}, false},
		{"permission", &ch.Exception{Code: 497}, false},
	}
	for _, tt := range tests {
		if got := IsTableProblem(tt.err); got != tt.want {
			t.Errorf("%s: IsTableProblem = %v, want %v", tt.name, got, tt.want)
		}
	}
	if !isTableException(&ch.Exception{Code: 60}) || !isTableException(fmt.Errorf("prepare: %w", &ch.Exception{Code: 16})) || isTableException(&ch.Exception{Code: 241}) {
		t.Error("only a missing table or column is a table exception: a memory limit is the server's problem, not the table's")
	}
}

func TestOnlyAMissingTableIsMarkedMissing(t *testing.T) {
	if !isMissingTableException(&ch.Exception{Code: 60}) || isMissingTableException(&ch.Exception{Code: 16}) {
		t.Error("code 60 is a missing table; code 16 is a missing column, which is another problem")
	}
	if !IsMissingTable(&TableError{Table: "t", Missing: true, Err: errors.New("x")}) ||
		IsMissingTable(&TableError{Table: "t", Err: errors.New("x")}) ||
		IsMissingTable(&SchemaMismatchError{Table: "t"}) || IsMissingTable(nil) {
		t.Error("IsMissingTable must be true only for a TableError that says the table is missing")
	}
}
