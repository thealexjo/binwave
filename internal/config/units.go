package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// ByteSize is a size in bytes, written in YAML as "64MiB", "1GB" or a plain integer.
type ByteSize int64

// Binary and decimal byte units.
const (
	KiB ByteSize = 1 << 10
	MiB ByteSize = 1 << 20
	GiB ByteSize = 1 << 30
)

var byteUnits = []struct {
	suffix string
	factor int64
}{
	// Longer suffixes first so "MiB" is not matched as "B".
	{"KiB", 1 << 10},
	{"MiB", 1 << 20},
	{"GiB", 1 << 30},
	{"TiB", 1 << 40},
	{"KB", 1e3},
	{"MB", 1e6},
	{"GB", 1e9},
	{"TB", 1e12},
	{"B", 1},
}

// ParseByteSize parses a human-readable size such as "64MiB".
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty size")
	}

	number, factor := s, int64(1)
	for _, u := range byteUnits {
		if strings.HasSuffix(s, u.suffix) {
			number, factor = strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), u.factor
			break
		}
	}

	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: want an integer with an optional unit (B, KiB, MiB, GiB, TiB, KB, MB, GB, TB)", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid size %q: must not be negative", s)
	}
	if n > (1<<63-1)/factor {
		return 0, fmt.Errorf("invalid size %q: too large", s)
	}
	return ByteSize(n * factor), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	v, err := ParseByteSize(node.Value)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (b ByteSize) MarshalYAML() (any, error) {
	return b.String(), nil
}

// String renders the size with the largest exact binary unit.
func (b ByteSize) String() string {
	for _, u := range []struct {
		suffix string
		factor ByteSize
	}{{"GiB", GiB}, {"MiB", MiB}, {"KiB", KiB}} {
		if b != 0 && b%u.factor == 0 {
			return strconv.FormatInt(int64(b/u.factor), 10) + u.suffix
		}
	}
	return strconv.FormatInt(int64(b), 10) + "B"
}

// Duration is a time.Duration written in YAML as "1s", "500ms".
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	v, err := time.ParseDuration(strings.TrimSpace(node.Value))
	if err != nil {
		return fmt.Errorf("invalid duration %q: want a value like 500ms, 1s, 2m", node.Value)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}

// String renders the duration in Go syntax.
func (d Duration) String() string {
	return time.Duration(d).String()
}
