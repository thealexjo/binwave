package checkpoint

import (
	"encoding/json"
	"fmt"
)

// Target is a ClickHouse table binwave writes a source table into: one it created, or one it found
// and took over.
type Target struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

// EncodeTables renders the tables binwave manages, by source table ("database.table"), as the text
// stored in Position.Tables. The keys are sorted, so equal sets give equal text; no tables at all is
// "{}", which is not the empty text of a position saved before there was a list.
func EncodeTables(tables map[string]Target) string {
	if len(tables) == 0 {
		return "{}"
	}
	data, err := json.Marshal(tables)
	if err != nil {
		// Strings always encode.
		panic(fmt.Sprintf("encode tables: %v", err))
	}
	return string(data)
}

// DecodeTables reads what EncodeTables wrote. known is false for the empty text: a position saved by a
// version that kept no list.
func DecodeTables(text string) (tables map[string]Target, known bool, err error) {
	if text == "" {
		return map[string]Target{}, false, nil
	}
	tables = map[string]Target{}
	if err := json.Unmarshal([]byte(text), &tables); err != nil {
		return nil, false, fmt.Errorf("the list of tables of the saved state is damaged: %w", err)
	}
	return tables, true, nil
}
