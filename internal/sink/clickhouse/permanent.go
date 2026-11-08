package clickhouse

import (
	"context"
	"errors"
	"fmt"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// permanentCodes are the server exception codes that repeat until someone changes the server or
// the configuration.
var permanentCodes = map[int32]string{
	192: "the ClickHouse user does not exist",
	193: "wrong ClickHouse password",
	194: "the ClickHouse user requires a password",
	195: "the ClickHouse user may not connect from this host",
	497: "the ClickHouse user lacks a required privilege (binwave needs CREATE TABLE, INSERT, SELECT and ALTER TABLE on the target databases, DROP TABLE and TRUNCATE as well with ddl.mode full, which mirrors what the source does to its tables, and CREATE TABLE, INSERT and SELECT on its state database)",
	516: "ClickHouse rejected the login: check target.clickhouse.user and password",
}

// AccessError reports a privilege the ClickHouse user lacks on a database.
type AccessError struct {
	Database  string
	Privilege string
}

func (e *AccessError) Error() string {
	return fmt.Sprintf("the ClickHouse user has no %s privilege on %s.*", e.Privilege, e.Database)
}

// CheckAccess verifies with CHECK GRANT that the user holds each privilege on every table of the
// database, and returns an *AccessError for the first one it lacks. Nothing is created or
// changed, and the database need not exist: a grant may be given ahead of it. A user granted
// privileges table by table, not on the database, is reported as lacking them: binwave needs
// them on the whole database because it creates tables in it.
func (c *Client) CheckAccess(ctx context.Context, database string, privileges ...string) error {
	for _, p := range privileges {
		var granted uint8
		if err := c.conn.QueryRow(ctx, "CHECK GRANT "+p+" ON "+quote(database)+".*").Scan(&granted); err != nil {
			return fmt.Errorf("check the %s privilege on %s.*: %w", p, database, err)
		}
		if granted == 0 {
			return &AccessError{Database: database, Privilege: p}
		}
	}
	return nil
}

// Permanent reports whether err is a server answer that a retry cannot change, with an
// explanation for the operator. Network failures and everything unknown are not permanent.
func Permanent(err error) (hint string, ok bool) {
	var access *AccessError
	if errors.As(err, &access) {
		return "grant the privilege to the ClickHouse user of target.clickhouse.user", true
	}
	var exception *ch.Exception
	if errors.As(err, &exception) {
		hint, ok = permanentCodes[exception.Code]
		return hint, ok
	}
	return "", false
}
