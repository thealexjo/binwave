package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	sqldriver "github.com/go-sql-driver/mysql"

	"github.com/thealexjo/binwave/internal/app"
	"github.com/thealexjo/binwave/internal/pipeline"
	chsink "github.com/thealexjo/binwave/internal/sink/clickhouse"
	mysqlsrc "github.com/thealexjo/binwave/internal/source/mysql"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantConfig bool
	}{
		{"nil", nil, false},
		{"unmet source requirements", &mysqlsrc.RequirementsError{Problems: []string{"binlog_format must be ROW"}}, true},
		{"wrapped requirements", fmt.Errorf("open: %w", &mysqlsrc.RequirementsError{Problems: []string{"x"}}), true},
		{"unsupported configuration", fmt.Errorf("%w: snapshot start", pipeline.ErrUnsupported), true},
		{"wrong MySQL password", fmt.Errorf("connect to source: %w", &sqldriver.MySQLError{Number: 1045}), true},
		{"purged binlog while streaming", fmt.Errorf("read binlog: %w", &gomysql.MyError{Code: 1236}), true},
		{"wrong ClickHouse password", fmt.Errorf("open ClickHouse: %w", &ch.Exception{Code: 516}), true},
		{"ClickHouse denies an insert", fmt.Errorf("flush: %w", &ch.Exception{Code: 497}), true},
		{"ClickHouse disagrees with the source", fmt.Errorf("rename: %w", &chsink.ConflictError{Err: errors.New("both tables exist")}), true},
		{"ClickHouse overloaded", &ch.Exception{Code: 252}, false},
		{"connection failure", errors.New("connect to source: refused"), false},
		{"data error", errors.New("convert shop.t.c: bad value"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.err)

			var cfgErr *app.ConfigError
			if isConfig := errors.As(got, &cfgErr); isConfig != tt.wantConfig {
				t.Errorf("classify(%v) is a ConfigError = %v, want %v", tt.err, isConfig, tt.wantConfig)
			}
			if tt.err != nil && !errors.Is(got, tt.err) {
				t.Errorf("classify lost the original error: %v", got)
			}
			if tt.err == nil && got != nil {
				t.Errorf("classify(nil) = %v", got)
			}
		})
	}
}

func TestIdlePeriodIsTheFlushIntervalWithinBounds(t *testing.T) {
	tests := map[time.Duration]time.Duration{
		time.Millisecond:       100 * time.Millisecond,
		time.Second:            time.Second,
		time.Minute:            5 * time.Second,
		250 * time.Millisecond: 250 * time.Millisecond,
	}
	for in, want := range tests {
		if got := idlePeriod(in); got != want {
			t.Errorf("idlePeriod(%v) = %v, want %v", in, got, want)
		}
	}
}
