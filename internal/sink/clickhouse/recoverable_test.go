package clickhouse

import (
	"errors"
	"fmt"
	"testing"
)

// TestRecoverable: what tables.autoresync acts on. A *SchemaMismatchError is always recoverable (a
// resync rereads the structure the table has now, whatever changed it); a *TableError only when it
// says so; anything else is not.
func TestRecoverable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"schema mismatch, always recoverable", &SchemaMismatchError{Table: "shop.t"}, true},
		{"table error marked recoverable", &TableError{Table: "shop.t", Err: errors.New("x"), Recoverable: true}, true},
		{"table error not marked", &TableError{Table: "shop.t", Err: errors.New("x")}, false},
		{"wrapped recoverable table error", fmt.Errorf("resync of shop.t: %w", &TableError{Err: errors.New("x"), Recoverable: true}), true},
		{"an unrelated error", errors.New("connection refused"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Recoverable(tt.err); got != tt.want {
				t.Errorf("Recoverable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
