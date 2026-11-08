package typemap

import (
	"errors"
	"fmt"
)

// ErrUnsupported marks a column/override combination binwave cannot map.
var ErrUnsupported = errors.New("unsupported mapping")

// ConversionError describes a value that could not be converted. The pipeline stops on it;
// the replication position is added by the caller.
type ConversionError struct {
	Table      string
	Column     string
	SourceType string
	TargetType string
	Value      any
	Err        error
}

const maxValueRepr = 128

// Error describes the failure without the value: messages end up in logs and in the status that
// the metrics port serves, and a value of a row may be personal data. The reason and the value
// are Detail, which only the operator's own status command shows.
func (e *ConversionError) Error() string {
	return fmt.Sprintf("convert %s.%s (%s → %s): the value cannot be converted (the reason and the value are shown by `binwave replication status`)",
		e.Table, e.Column, e.SourceType, e.TargetType)
}

// Detail is the whole description, with the reason and the value. Do not log it.
func (e *ConversionError) Detail() string {
	repr := fmt.Sprintf("%#v", e.Value)
	if len(repr) > maxValueRepr {
		repr = repr[:maxValueRepr] + "…"
	}
	return fmt.Sprintf("convert %s.%s (%s → %s): %v (value: %s)",
		e.Table, e.Column, e.SourceType, e.TargetType, e.Err, repr)
}

func (e *ConversionError) Unwrap() error { return e.Err }
