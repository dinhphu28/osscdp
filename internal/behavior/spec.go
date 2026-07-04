package behavior

import (
	"encoding/json"
	"time"
)

// Spec is the storage-layer view of a behavioral predicate, filled by the segment
// package from its DSL BehaviorSpec. It lives here (not in segment) so the store
// depends on no DSL types and there is no behavior→segment import cycle.
//
// The primary count/frequency comparison is applied by the segment package on the
// value the store returns; Op/Value are only consulted by the store for a
// correlated-absence Anchor (whose threshold the store must test to find t_a).
type Spec struct {
	EventName string        // primary event (count/frequency/recency/absence)
	Window    time.Duration // trailing window before the evaluation instant
	Op        string        // comparison op (used by the store only for an Anchor)
	Value     float64       // threshold (used by the store only for an Anchor)
	ValueProp string        // frequency-of-value: numeric property to sum
	Within    time.Duration // sequence: max gap between consecutive steps
	Steps     []string      // sequence: ordered event names A,B,...
	Anchor    *Spec         // correlated absence: the anchor behaviour (a count)

	// WhereMatch, when non-nil, filters in-window rows by their props_json using
	// the DSL comparison grammar the segment package owns (the store cannot run it
	// in SQL). Provided as a closure so the store stays DSL-agnostic.
	WhereMatch func(props json.RawMessage) bool
}
