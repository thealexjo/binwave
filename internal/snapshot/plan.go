package snapshot

import "github.com/thealexjo/binwave/internal/schema"

// Strategy is how a table is read in chunks.
type Strategy string

// The strategies. The name is saved with the resync (checkpoint.Resync.Strategy), because a cursor
// only means something together with the strategy that produced it.
const (
	// StrategyKey reads the table as one key space: chunks past the cursor in primary key order.
	StrategyKey Strategy = "key"
	// StrategyPartitioned reads one partition at a time (FROM t PARTITION (p)), each as its own key
	// space. A partitioned table has a local index per partition and no global one, so a read in
	// primary key order over the whole table makes the server merge every partition at once, and the
	// cost of a chunk grows with the number of partitions; one partition is one index range.
	StrategyPartitioned Strategy = "partitioned"
)

// Scope is the part of a table a read is in: the strategy it is read with and, for a partitioned read,
// the partition being read. A cursor only means something together with its scope, so the two are
// saved and restored together (checkpoint.Resync.Strategy and Partition).
type Scope struct {
	Strategy  Strategy
	Partition string
}

// Plan is how one table is read: the strategy and, for a partitioned table, the partitions to read in
// order. It is decided once per read, from the structure of the table alone, and kept by the Reader —
// nothing about it is decided per chunk.
type Plan struct {
	Strategy   Strategy
	Partitions []string
}

// PlanFor decides how to read tbl. There is no setting for this: the structure of the table says
// everything the choice needs, and a source where the choice is wrong is a bug to fix, not a knob to
// turn.
func PlanFor(tbl *schema.Table) Plan {
	if len(tbl.Partitions) == 0 {
		return Plan{Strategy: StrategyKey}
	}
	return Plan{Strategy: StrategyPartitioned, Partitions: tbl.Partitions}
}

// resume positions a plan at the partition a saved cursor belongs to. ok is false when the saved
// state cannot be continued under this plan — the partition is gone, or the saved strategy is not the
// one the table is read with now — and the read has to start over.
func (p Plan) resume(strategy Strategy, partition string) (index int, ok bool) {
	if strategy != "" && strategy != p.Strategy {
		return 0, false
	}
	if p.Strategy != StrategyPartitioned {
		return 0, true
	}
	if partition == "" {
		// A partitioned read always names its partition; without one there is nothing to continue.
		return 0, false
	}
	for i, name := range p.Partitions {
		if name == partition {
			return i, true
		}
	}
	return 0, false
}
