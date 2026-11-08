// Package gtid answers questions about MySQL GTID sets that the pipeline needs.
package gtid

import (
	"fmt"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// Set is a parsed GTID set, such as "uuid:1-10:15-20,uuid2:1-5".
type Set struct{ set gomysql.GTIDSet }

// Parse reads a GTID set in MySQL's text form.
func Parse(text string) (*Set, error) {
	set, err := gomysql.ParseMysqlGTIDSet(text)
	if err != nil {
		return nil, fmt.Errorf("parse GTID set %q: %w", text, err)
	}
	return &Set{set: set}, nil
}

// Contains reports whether the transaction (a single GTID, "uuid:sequence") is in the set.
func (s *Set) Contains(gtid string) (bool, error) {
	one, err := gomysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		return false, fmt.Errorf("parse GTID %q: %w", gtid, err)
	}
	return s.set.Contain(one), nil
}

// Covers reports whether every transaction of other is in the set. A nil other is in every set.
//
// This is the form to use where the question is asked often: parsing a set from text allocates (tens
// of allocations for a set of a few intervals), and asking it for thousands of sets on every
// transaction of the stream is how a service ends up spending its time in the garbage collector
// rather than on the data. Parse once, ask many times.
func (s *Set) Covers(other *Set) bool {
	if other == nil {
		return true
	}
	if s == nil {
		return false
	}
	return s.set.Contain(other.set)
}

// Count returns the number of transactions in the set.
func (s *Set) Count() int64 {
	if s == nil {
		return 0
	}
	mysqlSet, ok := s.set.(*gomysql.MysqlGTIDSet)
	if !ok {
		return 0
	}
	var n int64
	for _, tags := range *mysqlSet {
		for _, intervals := range tags {
			for _, iv := range intervals {
				n += iv.Stop - iv.Start // the interval is half open
			}
		}
	}
	return n
}

// Count returns the number of transactions in a GTID set.
func Count(text string) (int64, error) {
	set, err := Parse(text)
	if err != nil {
		return 0, err
	}
	return set.Count(), nil
}

// Subset reports whether every transaction of sub is in super. An empty sub is in every set.
func Subset(sub, super string) (bool, error) {
	if sub == "" {
		return true, nil
	}
	a, err := gomysql.ParseMysqlGTIDSet(sub)
	if err != nil {
		return false, fmt.Errorf("parse GTID set %q: %w", sub, err)
	}
	if super == "" {
		return false, nil
	}
	b, err := gomysql.ParseMysqlGTIDSet(super)
	if err != nil {
		return false, fmt.Errorf("parse GTID set %q: %w", super, err)
	}
	return b.Contain(a), nil
}
