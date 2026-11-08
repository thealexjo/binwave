package checkpoint

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Quarantined is a table whose replication was stopped because it cannot be replicated (a value
// that cannot be converted, a structure change nobody can apply, ...), while the rest goes on.
// The record is saved together with the position, so a restart keeps the table quarantined.
type Quarantined struct {
	// Table is the source table, "database.table".
	Table string `json:"table"`
	// Since is the GTID set the pipeline had committed when the table was quarantined; changes
	// of the table after it are not in ClickHouse.
	Since string `json:"since"`
	// Code identifies the kind of the reason (docs/reference/error-codes.md); Reason is the detail
	// next to it (which column, which type). Empty for an entry saved before codes existed.
	Code string `json:"code,omitempty"`
	// Reason is the detail of what made the table unreplicable, next to Code.
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
	// Recoverable reports whether the reason is one a resync fixes on its own: what
	// tables.autoresync (off by default) acts on. Set once, when the table was quarantined.
	Recoverable bool `json:"recoverable,omitempty"`
	// Until is set for a table that was released: its changes are replicated again from the
	// first transaction that is not in this GTID set, the source's position when it was
	// released. Changes up to it stay dropped, so a table is never replayed with the structure
	// or the data of a time before its cause was fixed. Empty while the table is quarantined.
	Until string `json:"until,omitempty"`
}

// Released reports whether the table was released and only waits for the position to pass Until.
func (q Quarantined) Released() bool { return q.Until != "" }

// EncodeQuarantine renders the quarantined tables as the text stored in Position.Quarantine:
// sorted by table, so equal sets give equal text, and empty for none.
func EncodeQuarantine(entries []Quarantined) string {
	if len(entries) == 0 {
		return ""
	}
	sorted := append([]Quarantined(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Table < sorted[j].Table })
	data, err := json.Marshal(sorted)
	if err != nil {
		// Strings and times always encode.
		panic(fmt.Sprintf("encode quarantine: %v", err))
	}
	return string(data)
}

// DecodeQuarantine reads what EncodeQuarantine wrote.
func DecodeQuarantine(text string) ([]Quarantined, error) {
	if text == "" {
		return nil, nil
	}
	var entries []Quarantined
	if err := json.Unmarshal([]byte(text), &entries); err != nil {
		return nil, fmt.Errorf("the quarantine list of the saved state is damaged: %w", err)
	}
	return entries, nil
}
