package checkpoint

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Resync is a table that is being read again from the source, chunk by chunk, into a shadow table next
// to the stream of its changes, which replaces the table when the read is complete: either one asked
// for by the operator (`binwave tables resync`), or one of the initial load of a table on the first run
// (replication.initial_position: snapshot, InitialLoad), which uses the exact same mechanism for every
// table the catalog shows at that moment — InitialLoad only says "don't hold a `tables resync` up
// behind this, and don't count it against the same parallelism budget". The record is saved together
// with the position, so a restart continues where the read stopped.
type Resync struct {
	// Table is the source table, "database.table".
	Table string `json:"table"`
	// Shadow is the name of the ClickHouse table that is filled, in the database of the table.
	Shadow string `json:"shadow"`
	// ShadowUUID is the UUID binwave makes the shadow table with, saved before the table is made (or with
	// the resync that made it): only a table of that UUID is the shadow table. A shadow table made again
	// gets a new one. Empty in state written before it was kept, where the marker alone tells.
	ShadowUUID string `json:"shadow_uuid,omitempty"`
	// Version is the row version of the snapshot: that of the last transaction before the shadow
	// table began to receive the changes of the table. Every change that reaches the shadow table
	// has a higher version, so it wins over the row the snapshot read, whatever the order.
	Version uint64 `json:"version"`
	// Since is the GTID set the pipeline had committed when the resync began.
	Since string `json:"since"`
	// Strategy is how the table is read (internal/snapshot.Strategy: "key", "partitioned"), and
	// Partition is the partition the cursor belongs to when the strategy has partitions. A cursor only
	// means something together with its scope: a read that continues under another strategy, or in a
	// partition that is gone, would leave what the cursor does not cover unread, so it starts over
	// instead. Empty in state written before the strategies existed, which is read as "key".
	Strategy  string `json:"strategy,omitempty"`
	Partition string `json:"partition,omitempty"`
	// Cursor is the primary key of the last row the snapshot has handed over, within the scope above;
	// empty before the first chunk of it. It advances only past rows that are written.
	Cursor []CursorValue `json:"cursor,omitempty"`
	// Rows is how many rows the snapshot has handed over, over all restarts of the resync.
	Rows int64 `json:"rows"`
	// Estimate is what the server believed the table held when the load was composed
	// (information_schema.TABLES.TABLE_ROWS). It is saved so that a restart keeps reading the biggest
	// tables first without asking the source again (replication.snapshot.largest_first); it is a
	// statistic, never used for anything but the order.
	Estimate int64 `json:"estimate,omitempty"`
	// Done is set when the last chunk was read; Final is the GTID set the source had executed before it.
	// The table is replaced once the stream has passed Final: only then does the shadow table hold what
	// the source held at that point.
	Done  bool   `json:"done,omitempty"`
	Final string `json:"final,omitempty"`
	// Quarantined is the reason the table was in quarantine when the resync began, so that a resync
	// that cannot finish puts it back.
	Quarantined string `json:"quarantined,omitempty"`
	// Restarts counts how often a change of the table's structure sent the resync back to the start.
	Restarts int       `json:"restarts,omitempty"`
	Started  time.Time `json:"started"`
	// InitialLoad marks a resync that the initial load began (replication.initial_position: snapshot),
	// as opposed to one the operator asked for with `binwave tables resync`. It changes nothing about
	// how the resync itself works, only how the pipeline schedules it: see internal/pipeline/resync.go.
	InitialLoad bool `json:"initial_load,omitempty"`
	// Autoresync marks a resync that tables.autoresync began on its own, from a quarantine whose
	// reason a resync fixes (Quarantined.Recoverable), without an operator. Same note as InitialLoad.
	Autoresync bool `json:"autoresync,omitempty"`
	// Pending marks a resync that has to begin again from the start, with every check of a new one: a
	// statement discarded it, and beginning it again failed with tables.on_replication_error: stop.
	Pending bool `json:"pending,omitempty"`
}

// CursorValue is one value of a primary key, in a form that survives JSON: the driver returns
// integers, text and bytes, and the key must come back as it was to continue the read.
type CursorValue struct {
	// Type is "i" (int64), "u" (uint64), "b" (bytes) or "s" (string).
	Type  string `json:"t"`
	Value string `json:"v"`
}

// EncodeCursor turns the values of a key into what is saved.
func EncodeCursor(key []any) ([]CursorValue, error) {
	out := make([]CursorValue, len(key))
	for i, v := range key {
		switch x := v.(type) {
		case int64:
			out[i] = CursorValue{"i", strconv.FormatInt(x, 10)}
		case int:
			out[i] = CursorValue{"i", strconv.Itoa(x)}
		case uint64:
			out[i] = CursorValue{"u", strconv.FormatUint(x, 10)}
		case []byte:
			out[i] = CursorValue{"b", base64.StdEncoding.EncodeToString(x)}
		case string:
			out[i] = CursorValue{"s", x}
		default:
			return nil, fmt.Errorf("a key value of type %T cannot be saved", v)
		}
	}
	return out, nil
}

// DecodeCursor is the reverse of EncodeCursor; an empty cursor is nil.
func DecodeCursor(values []CursorValue) ([]any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]any, len(values))
	for i, c := range values {
		switch c.Type {
		case "i":
			n, err := strconv.ParseInt(c.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("the saved cursor is damaged: %w", err)
			}
			out[i] = n
		case "u":
			n, err := strconv.ParseUint(c.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("the saved cursor is damaged: %w", err)
			}
			out[i] = n
		case "b":
			b, err := base64.StdEncoding.DecodeString(c.Value)
			if err != nil {
				return nil, fmt.Errorf("the saved cursor is damaged: %w", err)
			}
			out[i] = b
		case "s":
			out[i] = c.Value
		default:
			return nil, fmt.Errorf("the saved cursor has a value of type %q", c.Type)
		}
	}
	return out, nil
}

// resyncState is what Position.Resync holds: the entries plus the pool of GTID sets they refer to.
// The initial load of a source with thousands of tables creates all its entries at one position, so
// Since is the same text in every one of them, and Final repeats as well; written out in full, the
// GTID set of a busy source is by far the largest part of the state, which is saved as one row.
type resyncState struct {
	GTIDs   []string `json:"gtids,omitempty"`
	Resyncs []Resync `json:"resyncs"`
}

// gtidRef marks a GTID set replaced by its index in resyncState.GTIDs. A real GTID set is a list of
// UUIDs and intervals, so it can never begin with this.
const gtidRef = "#"

// EncodeResyncs renders the resyncs as the text stored in Position.Resync: sorted by table, empty for none.
func EncodeResyncs(entries []Resync) string {
	if len(entries) == 0 {
		return ""
	}
	state := resyncState{Resyncs: append([]Resync(nil), entries...)}
	sort.Slice(state.Resyncs, func(i, j int) bool { return state.Resyncs[i].Table < state.Resyncs[j].Table })
	pool := make(map[string]int)
	intern := func(set *string) {
		if *set == "" {
			return
		}
		i, ok := pool[*set]
		if !ok {
			i = len(state.GTIDs)
			pool[*set] = i
			state.GTIDs = append(state.GTIDs, *set)
		}
		*set = gtidRef + strconv.Itoa(i)
	}
	for i := range state.Resyncs {
		intern(&state.Resyncs[i].Since)
		intern(&state.Resyncs[i].Final)
	}
	data, err := json.Marshal(state)
	if err != nil {
		panic(fmt.Sprintf("encode resyncs: %v", err)) // strings, numbers and times always encode
	}
	return string(data)
}

// DecodeResyncs reads what EncodeResyncs wrote, including the bare array written before the GTID pool.
func DecodeResyncs(text string) ([]Resync, error) {
	if text == "" {
		return nil, nil
	}
	if strings.HasPrefix(strings.TrimSpace(text), "[") {
		var entries []Resync
		if err := json.Unmarshal([]byte(text), &entries); err != nil {
			return nil, fmt.Errorf("the resync list of the saved state is damaged: %w", err)
		}
		return entries, nil
	}
	var state resyncState
	if err := json.Unmarshal([]byte(text), &state); err != nil {
		return nil, fmt.Errorf("the resync list of the saved state is damaged: %w", err)
	}
	resolve := func(set *string) error {
		if !strings.HasPrefix(*set, gtidRef) {
			return nil
		}
		i, err := strconv.Atoi((*set)[len(gtidRef):])
		if err != nil || i < 0 || i >= len(state.GTIDs) {
			return fmt.Errorf("the resync list of the saved state refers to the unknown position %q", *set)
		}
		*set = state.GTIDs[i]
		return nil
	}
	for i := range state.Resyncs {
		if err := resolve(&state.Resyncs[i].Since); err != nil {
			return nil, err
		}
		if err := resolve(&state.Resyncs[i].Final); err != nil {
			return nil, err
		}
	}
	return state.Resyncs, nil
}
