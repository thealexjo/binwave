package mysql

import (
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"syscall"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	sqldriver "github.com/go-sql-driver/mysql"
)

// permanentCodes are the server error codes that repeat until someone changes the server or the
// configuration. Retrying them only delays the message the operator needs.
var permanentCodes = map[uint16]string{
	1044: "the user has no access to the database",
	1045: "access denied: check source.mysql.user and its password, and that the user holds the privileges REPLICATION SLAVE, REPLICATION CLIENT and SELECT (the server also answers this way when the replication privilege is missing)",
	1130: "the server does not accept connections from this host for this user",
	1142: "the user lacks a privilege on a table",
	1227: "the user lacks a required privilege (REPLICATION SLAVE and REPLICATION CLIENT are needed)",
	1236: "the server cannot serve the binlog from the saved position, for example because the binary logs holding it were purged; set replication.initial_position and the checkpoint deliberately",
	1698: "access denied: the account has no password or uses an authentication plugin that ignores it",
	1862: "the password of the account has expired",
	3159: "the server requires a secure connection: set source.mysql.tls.mode",
}

// partitionGoneCodes are the answers that mean "the partition named in the statement is not there":
// it was dropped or reorganized (1735), or the table is not partitioned any more (1747).
var partitionGoneCodes = map[uint16]bool{1735: true, 1747: true}

// partitionGone reports whether err is the server saying the partition of the statement does not exist.
func partitionGone(err error) bool {
	var driverErr *sqldriver.MySQLError
	if errors.As(err, &driverErr) {
		return partitionGoneCodes[driverErr.Number]
	}
	return false
}

// Permanent reports whether err is a server answer that a retry cannot change, with an
// explanation for the operator. Network failures and everything unknown are not permanent.
func Permanent(err error) (hint string, ok bool) {
	var driverErr *sqldriver.MySQLError
	if errors.As(err, &driverErr) {
		hint, ok = permanentCodes[driverErr.Number]
		return hint, ok
	}
	var streamErr *gomysql.MyError
	if errors.As(err, &streamErr) {
		hint, ok = permanentCodes[streamErr.Code]
		return hint, ok
	}
	return "", false
}

// transientCodes are the server answers that pass by themselves: the server is shutting down or
// restarting (1053, 1317, 1927), it has no connection to spare right now (1040), it closed an idle
// connection (4031), the network broke under a statement (1152, 1158-1161), or the statement lost a
// race for a lock (1205, 1213).
var transientCodes = map[uint16]bool{
	1040: true, 1053: true, 1152: true, 1158: true, 1159: true, 1160: true, 1161: true,
	1205: true, 1213: true, 1317: true, 1927: true, 4031: true,
}

// Transient reports whether err is a failure of a read that passes with time, without anyone
// changing anything: the source is down or restarting, or the network between it and binwave is.
// Everything else, the unknown included, is not: retrying it forever would hide it.
func Transient(err error) bool {
	if err == nil {
		return false
	}
	var driverErr *sqldriver.MySQLError
	if errors.As(err, &driverErr) {
		return transientCodes[driverErr.Number]
	}
	var netErr net.Error
	switch {
	case errors.As(err, &netErr),
		errors.Is(err, sqldriver.ErrInvalidConn), errors.Is(err, driver.ErrBadConn),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return true
	}
	return false
}
