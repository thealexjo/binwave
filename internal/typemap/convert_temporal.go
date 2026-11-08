package typemap

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ClickHouse Date32 and DateTime64 support 1900-01-01 … 2299-12-31.
var (
	MinDate = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	MaxDate = time.Date(2299, time.December, 31, 0, 0, 0, 0, time.UTC)
	// MaxDateTime is the last microsecond representable for every supported fsp.
	MaxDateTime = time.Date(2299, time.December, 31, 23, 59, 59, 999_999_000, time.UTC)
)

const (
	dateLayout     = "2006-01-02"
	dateTimeLayout = "2006-01-02 15:04:05.999999"
)

// wallClock is a MySQL date/datetime as written, before validation.
type wallClock struct {
	year, month, day     int
	hour, minute, second int
	nanos                int
}

func (w wallClock) isZero() bool {
	return w == wallClock{}
}

func (w wallClock) hasZeroDatePart() bool {
	return w.year == 0 || w.month == 0 || w.day == 0
}

// time returns the UTC time for the wall clock, or false if the date does not exist.
func (w wallClock) time() (time.Time, bool) {
	t := time.Date(w.year, time.Month(w.month), w.day, w.hour, w.minute, w.second, w.nanos, time.UTC)
	ok := t.Year() == w.year && int(t.Month()) == w.month && t.Day() == w.day
	return t, ok
}

func convertDate(m *Mapping, v any) (any, error) {
	w, original, err := toWallClock(v, m.Column.Kind)
	if err != nil {
		return nil, err
	}
	w.hour, w.minute, w.second, w.nanos = 0, 0, 0, 0
	return m.fitTemporal(w, original, MinDate, MaxDate, dateLayout), nil
}

func convertDateTime(m *Mapping, v any) (any, error) {
	w, original, err := toWallClock(v, m.Column.Kind)
	if err != nil {
		return nil, err
	}
	return m.fitTemporal(w, original, MinDate, MaxDateTime, dateTimeLayout), nil
}

// fitTemporal replaces zero/invalid dates with the minimum and clamps out-of-range values,
// reporting every replacement.
func (m *Mapping) fitTemporal(w wallClock, original string, lo, hi time.Time, layout string) time.Time {
	switch {
	case w.isZero():
		m.report(AdjustZeroDate, original, lo.Format(layout))
		return lo
	case w.hasZeroDatePart():
		m.report(AdjustInvalidDate, original, lo.Format(layout))
		return lo
	}

	t, ok := w.time()
	switch {
	case !ok:
		m.report(AdjustInvalidDate, original, lo.Format(layout))
		return lo
	case t.Before(lo):
		m.report(AdjustOutOfRange, original, lo.Format(layout))
		return lo
	case t.After(hi):
		m.report(AdjustOutOfRange, original, hi.Format(layout))
		return hi
	}
	return t
}

func toWallClock(v any, kind Kind) (wallClock, string, error) {
	switch x := v.(type) {
	case string:
		w, err := parseWallClock(x)
		return w, x, err
	case []byte:
		w, err := parseWallClock(string(x))
		return w, string(x), err
	case time.Time:
		if kind == KindTimestamp {
			// TIMESTAMP is an instant.
			x = x.UTC()
		}
		// DATETIME keeps its wall clock regardless of the time.Time location.
		w := wallClock{x.Year(), int(x.Month()), x.Day(), x.Hour(), x.Minute(), x.Second(), x.Nanosecond()}
		return w, x.Format(dateTimeLayout), nil
	}
	return wallClock{}, "", fmt.Errorf("want MySQL date text or time.Time, got %T", v)
}

var errDateSyntax = errors.New("want YYYY-MM-DD[ HH:MM:SS[.ffffff]]")

// parseWallClock parses MySQL date/datetime text without normalizing, so zero and
// invalid parts stay detectable.
func parseWallClock(s string) (wallClock, error) {
	var w wallClock

	datePart, timePart, hasTime := strings.Cut(strings.TrimSpace(s), " ")
	if !hasTime {
		datePart, timePart, hasTime = strings.Cut(datePart, "T")
	}

	fields := strings.Split(datePart, "-")
	if len(fields) != 3 || len(fields[0]) != 4 || len(fields[1]) != 2 || len(fields[2]) != 2 {
		return w, fmt.Errorf("invalid date %q: %w", s, errDateSyntax)
	}
	var err error
	if w.year, err = digits(fields[0]); err != nil {
		return w, fmt.Errorf("invalid date %q: %w", s, errDateSyntax)
	}
	if w.month, err = digits(fields[1]); err != nil || w.month > 12 {
		return w, fmt.Errorf("invalid month in %q", s)
	}
	if w.day, err = digits(fields[2]); err != nil || w.day > 31 {
		return w, fmt.Errorf("invalid day in %q", s)
	}

	if !hasTime {
		return w, nil
	}

	clock, frac, hasFrac := strings.Cut(timePart, ".")
	parts := strings.Split(clock, ":")
	if len(parts) != 3 || len(parts[0]) != 2 || len(parts[1]) != 2 || len(parts[2]) != 2 {
		return w, fmt.Errorf("invalid time in %q: %w", s, errDateSyntax)
	}
	if w.hour, err = digits(parts[0]); err != nil || w.hour > 23 {
		return w, fmt.Errorf("invalid hour in %q", s)
	}
	if w.minute, err = digits(parts[1]); err != nil || w.minute > 59 {
		return w, fmt.Errorf("invalid minute in %q", s)
	}
	if w.second, err = digits(parts[2]); err != nil || w.second > 59 {
		return w, fmt.Errorf("invalid second in %q", s)
	}
	if hasFrac {
		if w.nanos, err = fraction(frac); err != nil {
			return w, fmt.Errorf("invalid fractional seconds in %q", s)
		}
	}
	return w, nil
}

// convertTime stores MySQL TIME (-838:59:59 … 838:59:59) as signed microseconds.
func convertTime(_ *Mapping, v any) (any, error) {
	switch x := v.(type) {
	case time.Duration:
		return x.Microseconds(), nil
	case []byte:
		return parseTime(string(x))
	case string:
		return parseTime(x)
	}
	return nil, fmt.Errorf("want MySQL time text or time.Duration, got %T", v)
}

func parseTime(s string) (int64, error) {
	text := strings.TrimSpace(s)
	neg := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(text, "-")

	clock, frac, hasFrac := strings.Cut(text, ".")
	parts := strings.Split(clock, ":")
	if len(parts) != 3 || len(parts[0]) < 2 || len(parts[0]) > 3 || len(parts[1]) != 2 || len(parts[2]) != 2 {
		return 0, fmt.Errorf("invalid time %q: want [-]HH:MM:SS[.ffffff]", s)
	}
	hours, err1 := digits(parts[0])
	minutes, err2 := digits(parts[1])
	seconds, err3 := digits(parts[2])
	if err := errors.Join(err1, err2, err3); err != nil || minutes > 59 || seconds > 59 || hours > 838 {
		return 0, fmt.Errorf("invalid time %q", s)
	}
	nanos := 0
	if hasFrac {
		var err error
		if nanos, err = fraction(frac); err != nil {
			return 0, fmt.Errorf("invalid fractional seconds in %q", s)
		}
	}

	micros := (int64(hours)*3600+int64(minutes)*60+int64(seconds))*1_000_000 + int64(nanos/1000)
	if micros > 838*3600*1_000_000+59*60*1_000_000+59*1_000_000 {
		return 0, fmt.Errorf("time %q out of MySQL TIME range", s)
	}
	if neg {
		micros = -micros
	}
	return micros, nil
}

// digits parses a short unsigned decimal field (at most 9 digits).
func digits(s string) (int, error) {
	if s == "" || len(s) > 9 {
		return 0, strconv.ErrSyntax
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, strconv.ErrSyntax
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}

// fraction parses up to 9 fractional digits into nanoseconds.
func fraction(s string) (int, error) {
	n, err := digits(s)
	if err != nil {
		return 0, err
	}
	for range 9 - len(s) {
		n *= 10
	}
	return n, nil
}
