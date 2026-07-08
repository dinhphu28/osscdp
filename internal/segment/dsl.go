// Package segment implements stateless segmentation: a JSON rule DSL, an
// evaluator over profile + event, and membership tracking.
// See docs/cdp/06-segmentation-engine.md.
package segment

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// analyzeRule derives the segment_version metadata (Phase 6) from a parsed rule:
// isStateful (has any behaviour leaf), hasStateless (has any comparison leaf, so a
// trait change may newly match — it must never be prefiltered out), the sorted set
// of event names any behaviour leaf references (for the pure-behavioural edge
// prefilter), and the largest window across leaves.
func analyzeRule(r Rule) (isStateful, hasStateless bool, events []string, maxWindow time.Duration) {
	seen := map[string]bool{}
	var walk func(Rule)
	walk = func(r Rule) {
		switch {
		case r.Behavior != nil:
			isStateful = true
			collectBehaviorMeta(r.Behavior, seen, &maxWindow)
		case r.isLogical():
			for _, c := range r.Conditions {
				walk(c)
			}
		case r.Field != "":
			hasStateless = true
		}
	}
	walk(r)
	events = []string{} // non-nil so it stores as '{}' (the column is NOT NULL)
	for e := range seen {
		events = append(events, e)
	}
	sort.Strings(events)
	return
}

// RuleAnalysis is the exported result of analyzing a rule (see AnalyzeRule). It lets
// other packages (e.g. journey, which embeds segment.Rule in condition steps) derive
// the same referenced-event / max-window metadata the segmentation engine uses.
type RuleAnalysis struct {
	IsStateful      bool
	HasStateless    bool
	ReferencedNames []string
	MaxWindow       time.Duration
}

// AnalyzeRule is the exported wrapper over analyzeRule: it returns the referenced
// event names and largest window across a rule's behavioral leaves. Used by the
// journey package to populate journey_version metadata and widen the behavioral
// retention horizon for journey conditions.
func AnalyzeRule(r Rule) RuleAnalysis {
	isStateful, hasStateless, events, maxWindow := analyzeRule(r)
	return RuleAnalysis{IsStateful: isStateful, HasStateless: hasStateless, ReferencedNames: events, MaxWindow: maxWindow}
}

func collectBehaviorMeta(b *BehaviorSpec, seen map[string]bool, maxWindow *time.Duration) {
	if b.EventName != "" {
		seen[b.EventName] = true
	}
	if w, err := ParseWindow(b.Window); err == nil && w > *maxWindow {
		*maxWindow = w
	}
	if b.Within != "" {
		if w, err := ParseWindow(b.Within); err == nil && w > *maxWindow {
			*maxWindow = w
		}
	}
	if b.Anchor != nil {
		collectBehaviorMeta(b.Anchor, seen, maxWindow)
	}
	for i := range b.Steps {
		collectBehaviorMeta(&b.Steps[i], seen, maxWindow)
	}
}

// Logical operators.
const (
	OpAnd = "and"
	OpOr  = "or"
	OpNot = "not"
)

// Comparison operators (v1).
const (
	OpEq          = "eq"
	OpNeq         = "neq"
	OpGt          = "gt"
	OpGte         = "gte"
	OpLt          = "lt"
	OpLte         = "lte"
	OpContains    = "contains"
	OpNotContains = "not_contains"
	OpIn          = "in"
	OpNotIn       = "not_in"
	OpExists      = "exists"
	OpNotExists   = "not_exists"
)

var comparisonOps = map[string]bool{
	OpEq: true, OpNeq: true, OpGt: true, OpGte: true, OpLt: true, OpLte: true,
	OpContains: true, OpNotContains: true, OpIn: true, OpNotIn: true,
	OpExists: true, OpNotExists: true,
}

// Rule is a logical node (Operator + Conditions), a leaf comparison
// (Field + Op + Value), or a windowed behavioral leaf (Behavior). Exactly one
// shape is populated. A nil Behavior leaves the node byte-for-byte identical to
// the pre-Level-3 DSL, so all stored rule_json parses and evaluates unchanged.
type Rule struct {
	Operator   string        `json:"operator,omitempty"`
	Conditions []Rule        `json:"conditions,omitempty"`
	Field      string        `json:"field,omitempty"`
	Op         string        `json:"op,omitempty"`
	Value      any           `json:"value,omitempty"`
	Behavior   *BehaviorSpec `json:"behavior,omitempty"`
}

func (r Rule) isLogical() bool {
	return r.Operator == OpAnd || r.Operator == OpOr || r.Operator == OpNot
}

// Behavior kinds for windowed stateful leaves (Level 3).
// See docs/cdp/16-stateful-segmentation.md.
const (
	BehaviorCount     = "count"
	BehaviorFrequency = "frequency"
	BehaviorRecency   = "recency"
	BehaviorAbsence   = "absence"
	BehaviorSequence  = "sequence"
)

// behaviorCountOps are the comparison operators allowed on count/frequency thresholds.
var behaviorCountOps = map[string]bool{OpGte: true, OpGt: true, OpLte: true, OpLt: true, OpEq: true}

// highFrequencyEvents names event types too hot to serve exact/sequence re-queries.
// Phase 1 ships an empty set; Phase 6 wires the real per-tenant rate list
// (docs/cdp/16-stateful-segmentation.md finding #13).
var highFrequencyEvents = map[string]bool{}

// BehaviorSpec is a windowed behavioral predicate leaf (Level 3 stateful
// segmentation). A Rule carrying a non-nil Behavior is neither a logical node nor
// a comparison leaf. Evaluation lands in a later phase; today the DSL only parses
// and validates the shape (the evaluator ignores behavior leaves).
type BehaviorSpec struct {
	Kind      string         `json:"kind"`                 // count | frequency | recency | absence | sequence
	EventName string         `json:"event_name,omitempty"` // required for non-sequence kinds
	Window    string         `json:"window,omitempty"`     // "7d","24h","30m" -> time.Duration
	Op        string         `json:"op,omitempty"`         // count/frequency: gte|gt|lte|lt|eq
	Value     *float64       `json:"value,omitempty"`      // threshold; pointer so absent is distinct from 0
	ValueProp string         `json:"value_prop,omitempty"` // frequency-of-value: numeric property to sum
	Where     *Rule          `json:"where,omitempty"`      // OPTIONAL props filter (comparison-leaf grammar)
	Steps     []BehaviorSpec `json:"steps,omitempty"`      // sequence: ordered landmarks A,B,...
	Within    string         `json:"within,omitempty"`     // sequence: max span between consecutive steps
	Anchor    *BehaviorSpec  `json:"anchor,omitempty"`     // correlated absence: "no E within Window AFTER anchor"
	Exact     bool           `json:"exact,omitempty"`      // force the behavioral_event re-query path
}

// ParseWindow parses a window such as "7d", "24h", "30m", "90s" into a positive
// Duration. Go's time.ParseDuration has no day unit, so a trailing "d" is
// expanded to hours; everything else defers to time.ParseDuration.
func ParseWindow(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty window")
	}
	var d time.Duration
	if strings.HasSuffix(s, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil {
			return 0, fmt.Errorf("invalid window %q: %w", s, err)
		}
		d = time.Duration(days) * 24 * time.Hour
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, fmt.Errorf("invalid window %q: %w", s, err)
		}
	}
	if d <= 0 {
		return 0, fmt.Errorf("window must be positive: %q", s)
	}
	return d, nil
}

// validateBehavior checks a windowed behavioral leaf and force-sets Exact for the
// shapes the bucket aggregation cannot serve honestly — sequences, per-event
// property (where) filters, and correlated absence
// (docs/cdp/16-stateful-segmentation.md findings #4/#16/#17).
func validateBehavior(b *BehaviorSpec) error {
	switch b.Kind {
	case BehaviorCount, BehaviorFrequency, BehaviorRecency, BehaviorAbsence, BehaviorSequence:
	default:
		return &ValidationError{fmt.Sprintf("unknown behavior kind %q", b.Kind)}
	}

	// Route shapes buckets aggregate away to the exact behavioral_event path.
	if b.Kind == BehaviorSequence || b.Where != nil || b.Anchor != nil {
		b.Exact = true
	}

	switch b.Kind {
	case BehaviorCount, BehaviorFrequency:
		if b.EventName == "" {
			return &ValidationError{b.Kind + " behavior requires event_name"}
		}
		if !behaviorCountOps[b.Op] {
			return &ValidationError{b.Kind + " behavior requires op in {gte,gt,lte,lt,eq}"}
		}
		if b.Value == nil {
			return &ValidationError{b.Kind + " behavior requires a value threshold"}
		}
		if _, err := ParseWindow(b.Window); err != nil {
			return &ValidationError{b.Kind + " behavior window: " + err.Error()}
		}
	case BehaviorRecency, BehaviorAbsence:
		if b.EventName == "" {
			return &ValidationError{b.Kind + " behavior requires event_name"}
		}
		if _, err := ParseWindow(b.Window); err != nil {
			return &ValidationError{b.Kind + " behavior window: " + err.Error()}
		}
	case BehaviorSequence:
		if len(b.Steps) < 2 {
			return &ValidationError{"sequence behavior requires at least two steps"}
		}
		for i := range b.Steps {
			if b.Steps[i].EventName == "" {
				return &ValidationError{"sequence step requires event_name"}
			}
		}
		if _, err := ParseWindow(b.Within); err != nil {
			return &ValidationError{"sequence behavior within: " + err.Error()}
		}
	}

	// The optional props filter reuses the comparison-leaf grammar.
	if b.Where != nil {
		if err := Validate(*b.Where); err != nil {
			return err
		}
	}
	// The correlated-absence anchor is itself a behavior.
	if b.Anchor != nil {
		if err := validateBehavior(b.Anchor); err != nil {
			return err
		}
	}

	// Guard exact/sequence re-queries against too-hot event names (no-op until Phase 6).
	if b.Exact || b.Kind == BehaviorSequence {
		names := []string{b.EventName}
		for i := range b.Steps {
			names = append(names, b.Steps[i].EventName)
		}
		if b.Anchor != nil {
			names = append(names, b.Anchor.EventName) // the anchor participates in the exact re-query
		}
		for _, n := range names {
			if highFrequencyEvents[n] {
				return &ValidationError{fmt.Sprintf("event %q is too high-frequency for an exact/sequence behavior", n)}
			}
		}
	}
	return nil
}

// ValidationError is a client-facing rule validation failure.
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

// Validate checks a rule tree against the DSL (logical, comparison, or behavioral).
func Validate(r Rule) error {
	if r.Behavior != nil {
		if r.Operator != "" || len(r.Conditions) != 0 || r.Field != "" || r.Op != "" || r.Value != nil {
			return &ValidationError{"behavior leaf must not set operator/conditions/field/op/value"}
		}
		return validateBehavior(r.Behavior)
	}
	if r.isLogical() {
		if r.Field != "" || r.Op != "" || r.Value != nil {
			return &ValidationError{fmt.Sprintf("logical node %q must not set field/op/value", r.Operator)}
		}
		switch r.Operator {
		case OpNot:
			if len(r.Conditions) != 1 {
				return &ValidationError{"not requires exactly one condition"}
			}
		default: // and / or
			if len(r.Conditions) == 0 {
				return &ValidationError{r.Operator + " requires at least one condition"}
			}
		}
		for _, c := range r.Conditions {
			if err := Validate(c); err != nil {
				return err
			}
		}
		return nil
	}

	// Leaf condition.
	if r.Operator != "" {
		return &ValidationError{fmt.Sprintf("unknown logical operator %q", r.Operator)}
	}
	if r.Field == "" {
		return &ValidationError{"condition requires a field"}
	}
	if !comparisonOps[r.Op] {
		return &ValidationError{fmt.Sprintf("unknown operator %q", r.Op)}
	}
	switch r.Op {
	case OpExists, OpNotExists:
		if r.Value != nil {
			return &ValidationError{r.Op + " must not have a value"}
		}
	case OpIn, OpNotIn:
		if _, ok := r.Value.([]any); !ok {
			return &ValidationError{r.Op + " requires an array value"}
		}
	default:
		if r.Value == nil {
			return &ValidationError{r.Op + " requires a value"}
		}
	}
	return nil
}
