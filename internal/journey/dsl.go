package journey

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/segment"
)

// ValidationError is a client-facing definition validation failure.
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

// maxSplitWeight caps a single split branch weight — keeps weight sums well within
// uint64 (no overflow in splitTarget) and rejects absurd configs.
const maxSplitWeight = 1_000_000

// forbidFields rejects a step that populates fields belonging to a different step
// shape (the Step struct carries every shape's fields on one type). when is true when
// any foreign field is set.
func forbidFields(i int, _ Step, kind string, when bool) error {
	if when {
		return &ValidationError{fmt.Sprintf("step %d %s must only set its own fields", i, kind)}
	}
	return nil
}

// referencesEventField reports whether a rule's logical/comparison tree references an
// event.* field (resolved against the triggering event, which a journey lacks).
// Behavioral leaves are not walked — they read stored behavioral_event by name, not the
// triggering event.
func referencesEventField(r segment.Rule) bool {
	if strings.HasPrefix(r.Field, "event.") {
		return true
	}
	for _, c := range r.Conditions {
		if referencesEventField(c) {
			return true
		}
	}
	return false
}

// Validate checks a journey definition: a non-empty, ordered array of
// wait|send|condition|split steps with at least one send. Branch targets
// (condition IfTrue/IfFalse, split Branches[].Next) must be FORWARD — strictly
// greater than the branching step's own index and at most len(steps) (a target equal
// to len(steps) means "complete"). Forward-only edges guarantee an acyclic DAG that
// always terminates. Unknown step types fail closed. wait durations and condition
// rules reuse the segment grammar verbatim (segment.ParseWindow / segment.Validate).
func Validate(def Definition) error {
	n := len(def.Steps)
	if n == 0 {
		return &ValidationError{"journey requires at least one step"}
	}
	// forward validates a branch target index.
	forward := func(i, target int, what string) error {
		if target <= i || target > n {
			return &ValidationError{fmt.Sprintf("step %d %s target %d must be forward (in %d..%d)", i, what, target, i+1, n)}
		}
		return nil
	}
	hasSend := false
	for i, s := range def.Steps {
		switch s.Type {
		case StepWait:
			if err := forbidFields(i, s, "wait", s.Condition != nil || s.IfTrue != 0 || s.IfFalse != 0 || len(s.Branches) > 0 || s.DestinationID != uuid.Nil); err != nil {
				return err
			}
			if _, err := segment.ParseWindow(s.Duration); err != nil {
				return &ValidationError{fmt.Sprintf("step %d wait: %s", i, err.Error())}
			}
			if s.Next != 0 {
				if err := forward(i, s.Next, "next"); err != nil {
					return err
				}
			}
		case StepSend:
			if err := forbidFields(i, s, "send", s.Condition != nil || s.IfTrue != 0 || s.IfFalse != 0 || len(s.Branches) > 0 || s.Duration != ""); err != nil {
				return err
			}
			if s.DestinationID == uuid.Nil {
				return &ValidationError{fmt.Sprintf("step %d send requires destination_id", i)}
			}
			if s.Next != 0 {
				if err := forward(i, s.Next, "next"); err != nil {
					return err
				}
			}
			hasSend = true
		case StepCondition:
			if err := forbidFields(i, s, "condition", s.Duration != "" || s.DestinationID != uuid.Nil || s.Next != 0 || len(s.Branches) > 0); err != nil {
				return err
			}
			if s.Condition == nil {
				return &ValidationError{fmt.Sprintf("step %d condition requires a rule", i)}
			}
			if err := segment.Validate(*s.Condition); err != nil {
				return &ValidationError{fmt.Sprintf("step %d condition rule: %s", i, err.Error())}
			}
			// A journey condition is evaluated with NO triggering event, so an event.*
			// field reference would silently resolve to false — reject it at authoring.
			if referencesEventField(*s.Condition) {
				return &ValidationError{fmt.Sprintf("step %d condition must not reference event.* fields (no triggering event in a journey)", i)}
			}
			if err := forward(i, s.IfTrue, "condition if_true"); err != nil {
				return err
			}
			if err := forward(i, s.IfFalse, "condition if_false"); err != nil {
				return err
			}
		case StepSplit:
			if err := forbidFields(i, s, "split", s.Duration != "" || s.DestinationID != uuid.Nil || s.Next != 0 || s.Condition != nil || s.IfTrue != 0 || s.IfFalse != 0); err != nil {
				return err
			}
			if len(s.Branches) < 2 {
				return &ValidationError{fmt.Sprintf("step %d split requires at least two branches", i)}
			}
			for bi, b := range s.Branches {
				if b.Weight <= 0 || b.Weight > maxSplitWeight {
					return &ValidationError{fmt.Sprintf("step %d split branch %d weight must be in 1..%d", i, bi, maxSplitWeight)}
				}
				if err := forward(i, b.Next, fmt.Sprintf("split branch %d", bi)); err != nil {
					return err
				}
			}
		default:
			return &ValidationError{fmt.Sprintf("step %d unknown type %q (supports wait|send|condition|split)", i, s.Type)}
		}
	}
	if !hasSend {
		return &ValidationError{"journey requires at least one send step"}
	}
	return nil
}

// analyzeDefinition derives the journey_version metadata from every condition step's
// rule: the sorted set of referenced event names and the largest behavioral window
// across all conditions. Mirrors segment.analyzeRule (via the exported AnalyzeRule);
// max_window_seconds feeds the behavioral retention horizon. events is non-nil so it
// stores as the TEXT[] '{}' literal, never NULL.
func analyzeDefinition(def Definition) (events []string, maxWindow time.Duration) {
	seen := map[string]bool{}
	for _, s := range def.Steps {
		if s.Type != StepCondition || s.Condition == nil {
			continue
		}
		a := segment.AnalyzeRule(*s.Condition)
		for _, e := range a.ReferencedNames {
			seen[e] = true
		}
		if a.MaxWindow > maxWindow {
			maxWindow = a.MaxWindow
		}
	}
	events = []string{}
	for e := range seen {
		events = append(events, e)
	}
	sort.Strings(events)
	return events, maxWindow
}
