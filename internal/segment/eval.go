package segment

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/profile"
)

// EvalContext is the data a rule evaluates against.
type EvalContext struct {
	Profile profile.Profile
	Event   events.Envelope
}

// Evaluate reports whether the rule matches the context. Level 3 behavioral
// leaves are inert until the Phase 3 evaluator lands: a behavior leaf is treated
// as unsatisfied (false), so a mixed rule's stateless leaves still work while the
// segment adds no members on the strength of a not-yet-evaluated behavior. NOT is
// guarded separately — negating an un-evaluable behavior subtree would flip false
// to a spurious whole-population match, so it stays inert.
func Evaluate(r Rule, ec EvalContext) bool {
	if r.isLogical() {
		switch r.Operator {
		case OpAnd:
			for _, c := range r.Conditions {
				if !Evaluate(c, ec) {
					return false
				}
			}
			return true
		case OpOr:
			for _, c := range r.Conditions {
				if Evaluate(c, ec) {
					return true
				}
			}
			return false
		case OpNot:
			if len(r.Conditions) != 1 || hasBehavior(r.Conditions[0]) {
				return false
			}
			return !Evaluate(r.Conditions[0], ec)
		}
	}
	if r.Behavior != nil {
		return false // inert until Phase 3
	}
	val, present := resolveField(r.Field, ec)
	return applyOp(r.Op, val, present, r.Value)
}

// hasBehavior reports whether the rule tree contains a behavioral leaf.
func hasBehavior(r Rule) bool {
	if r.Behavior != nil {
		return true
	}
	for _, c := range r.Conditions {
		if hasBehavior(c) {
			return true
		}
	}
	return false
}

func resolveField(path string, ec EvalContext) (any, bool) {
	switch {
	case strings.HasPrefix(path, "profile.traits."):
		return nestedGet(ec.Profile.Traits, strings.TrimPrefix(path, "profile.traits."))
	case strings.HasPrefix(path, "profile.computed_attributes."):
		return nestedGet(ec.Profile.ComputedAttributes, strings.TrimPrefix(path, "profile.computed_attributes."))
	case path == "profile.canonical_user_id":
		return ec.Profile.CanonicalUserID, ec.Profile.CanonicalUserID != ""
	case path == "profile.first_seen_at":
		if ec.Profile.FirstSeenAt == nil {
			return nil, false
		}
		return ec.Profile.FirstSeenAt.Format(time.RFC3339), true
	case path == "profile.last_seen_at":
		if ec.Profile.LastSeenAt == nil {
			return nil, false
		}
		return ec.Profile.LastSeenAt.Format(time.RFC3339), true
	case path == "event.event_name":
		return ec.Event.EventName, ec.Event.EventName != ""
	case path == "event.type":
		return ec.Event.Type, ec.Event.Type != ""
	case strings.HasPrefix(path, "event.properties."):
		return nestedGet(decodeJSON(ec.Event.Properties), strings.TrimPrefix(path, "event.properties."))
	case strings.HasPrefix(path, "event.context."):
		return nestedGet(decodeJSON(ec.Event.Context), strings.TrimPrefix(path, "event.context."))
	default:
		return nil, false
	}
}

func decodeJSON(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func nestedGet(m map[string]any, path string) (any, bool) {
	if m == nil {
		return nil, false
	}
	parts := strings.Split(path, ".")
	var cur any = m
	for _, p := range parts {
		asMap, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = asMap[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func applyOp(op string, val any, present bool, ruleVal any) bool {
	switch op {
	case OpExists:
		return present
	case OpNotExists:
		return !present
	}
	if !present {
		return false
	}
	switch op {
	case OpEq:
		return valuesEqual(val, ruleVal)
	case OpNeq:
		return !valuesEqual(val, ruleVal)
	case OpGt, OpGte, OpLt, OpLte:
		return numericCompare(op, val, ruleVal)
	case OpContains:
		return containsVal(val, ruleVal)
	case OpNotContains:
		return !containsVal(val, ruleVal)
	case OpIn:
		return inArray(val, ruleVal)
	case OpNotIn:
		return !inArray(val, ruleVal)
	}
	return false
}

func valuesEqual(a, b any) bool {
	if fa, ok1 := toFloat(a); ok1 {
		if fb, ok2 := toFloat(b); ok2 {
			return fa == fb
		}
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func numericCompare(op string, a, b any) bool {
	fa, ok1 := toFloat(a)
	fb, ok2 := toFloat(b)
	if !ok1 || !ok2 {
		return false
	}
	switch op {
	case OpGt:
		return fa > fb
	case OpGte:
		return fa >= fb
	case OpLt:
		return fa < fb
	case OpLte:
		return fa <= fb
	}
	return false
}

func containsVal(val, ruleVal any) bool {
	if arr, ok := val.([]any); ok {
		return inArray(ruleVal, arr) || containsAny(arr, ruleVal)
	}
	return strings.Contains(fmt.Sprint(val), fmt.Sprint(ruleVal))
}

func containsAny(arr []any, target any) bool {
	for _, e := range arr {
		if valuesEqual(e, target) {
			return true
		}
	}
	return false
}

func inArray(val, ruleArr any) bool {
	arr, ok := ruleArr.([]any)
	if !ok {
		return false
	}
	return containsAny(arr, val)
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
