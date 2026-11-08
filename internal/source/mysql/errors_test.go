package mysql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	sqldriver "github.com/go-sql-driver/mysql"
)

func TestPermanentErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"access denied from the SQL driver", &sqldriver.MySQLError{Number: 1045, Message: "Access denied"}, true},
		{"wrapped like Open wraps it", fmt.Errorf("connect to source: %w", &sqldriver.MySQLError{Number: 1045}), true},
		{"missing privilege on the binlog stream", &gomysql.MyError{Code: 1227, Message: "Access denied; you need REPLICATION SLAVE"}, true},
		{"purged binlog on the binlog stream", fmt.Errorf("read binlog: %w", &gomysql.MyError{Code: 1236, Message: "purged"}), true},
		{"too many connections passes by itself", &sqldriver.MySQLError{Number: 1040}, false},
		{"lock wait timeout", &gomysql.MyError{Code: 1205}, false},
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
		})
	}
}

func TestTransientErrors(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection refused while the server is down", fmt.Errorf("read gtid_executed: %w", refused), true},
		{"a connection the server closed", fmt.Errorf("read the watermark: %w", sqldriver.ErrInvalidConn), true},
		{"bad connection after the driver gave up on the pool", driver.ErrBadConn, true},
		{"a connection reset", fmt.Errorf("read a chunk: %w", syscall.ECONNRESET), true},
		{"the stream of rows cut short", io.ErrUnexpectedEOF, true},
		{"server shutdown in progress", &sqldriver.MySQLError{Number: 1053, Message: "Server shutdown in progress"}, true},
		{"query interrupted by the shutdown", &sqldriver.MySQLError{Number: 1317}, true},
		{"too many connections", &sqldriver.MySQLError{Number: 1040}, true},
		{"disconnected for inactivity", &sqldriver.MySQLError{Number: 4031}, true},
		{"a missing privilege does not pass", &sqldriver.MySQLError{Number: 1142}, false},
		{"an unknown column does not pass", &sqldriver.MySQLError{Number: 1054}, false},
		{"a table that is gone does not pass", &sqldriver.MySQLError{Number: 1146}, false},
		{"an error nobody knows", errors.New("scan a value"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Transient(tt.err); got != tt.want {
				t.Fatalf("Transient(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
