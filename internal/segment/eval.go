package segment

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/profile"
)

// EvalContext is the data a rule evaluates against.
type EvalContext struct {
	Profile profile.Profile
	Event   events.Envelope
}

// BehaviorStore answers windowed behavioral questions for the evaluator (Level 3).
// A nil store makes behavior leaves inert, so pure-stateless rules evaluate
// exactly as before. Implemented by *behavior.Store; tests inject a fake.
type BehaviorStore interface {
	Count(ctx context.Context, tenantID, profileID uuid.UUID, spec behavior.Spec, at time.Time) (int64, error)
	Recent(ctx context.Context, tenantID, profileID uuid.UUID, spec behavior.Spec, at time.Time) (bool, error)
	Absent(ctx context.Context, tenantID, profileID uuid.UUID, spec behavior.Spec, at time.Time) (bool, error)
	CorrelatedAbsent(ctx context.Context, tenantID, profileID uuid.UUID, spec behavior.Spec, at time.Time) (bool, error)
	Sequence(ctx context.Context, tenantID, profileID uuid.UUID, spec behavior.Spec, at time.Time) (bool, error)
	SumValue(ctx context.Context, tenantID, profileID uuid.UUID, spec behavior.Spec, at time.Time) (float64, error)
	// LastAt / NthNewestInWindow support due_at deadline computation (Phase 5).
	LastAt(ctx context.Context, tenantID, profileID uuid.UUID, eventName string, at time.Time) (time.Time, bool, error)
	NthNewestInWindow(ctx context.Context, tenantID, profileID uuid.UUID, eventName string, window time.Duration, n int, at time.Time) (time.Time, bool, error)
}

// Evaluate reports whether the rule matches the context. Behavioral leaves consult
// the store; a nil store leaves them inert (false), so pure-stateless rules evaluate
// unchanged and a NOT over an un-evaluable behavior subtree cannot flip to a spurious
// match. A store read error is threaded out (not swallowed to false): the caller
// fails the handler so at-least-once redelivery retries, rather than letting a
// transient error spuriously enter (via NOT) or exit a member.
func Evaluate(ctx context.Context, r Rule, ec EvalContext, store BehaviorStore, at time.Time) (bool, error) {
	if r.isLogical() {
		switch r.Operator {
		case OpAnd:
			for _, c := range r.Conditions {
				m, err := Evaluate(ctx, c, ec, store, at)
				if err != nil {
					return false, err
				}
				if !m {
					return false, nil
				}
			}
			return true, nil
		case OpOr:
			for _, c := range r.Conditions {
				m, err := Evaluate(ctx, c, ec, store, at)
				if err != nil {
					return false, err
				}
				if m {
					return true, nil
				}
			}
			return false, nil
		case OpNot:
			if len(r.Conditions) != 1 {
				return false, nil
			}
			if store == nil && hasBehavior(r.Conditions[0]) {
				return false, nil // inert without a store; NOT must not invert
			}
			m, err := Evaluate(ctx, r.Conditions[0], ec, store, at)
			if err != nil {
				return false, err
			}
			return !m, nil
		}
	}
	if r.Behavior != nil {
		return evalBehavior(ctx, r.Behavior, ec, store, at)
	}
	val, present := resolveField(r.Field, ec)
	return applyOp(r.Op, val, present, r.Value), nil
}

// evalBehavior dispatches a windowed behavioral leaf to the store. A nil store
// leaves it inert (false). A store error is returned so the caller can retry.
func evalBehavior(ctx context.Context, b *BehaviorSpec, ec EvalContext, store BehaviorStore, at time.Time) (bool, error) {
	if store == nil {
		return false, nil
	}
	spec, err := toSpec(ctx, b, ec, store, at)
	if err != nil {
		return false, err
	}
	tid, pid := ec.Profile.TenantID, ec.Profile.ID
	switch b.Kind {
	case BehaviorCount:
		n, err := store.Count(ctx, tid, pid, spec, at)
		return err == nil && b.Value != nil && matchCount(b.Op, float64(n), *b.Value), err
	case BehaviorFrequency:
		if b.ValueProp != "" {
			sum, err := store.SumValue(ctx, tid, pid, spec, at)
			return err == nil && b.Value != nil && matchCount(b.Op, sum, *b.Value), err
		}
		n, err := store.Count(ctx, tid, pid, spec, at)
		return err == nil && b.Value != nil && matchCount(b.Op, float64(n), *b.Value), err
	case BehaviorRecency:
		return store.Recent(ctx, tid, pid, spec, at)
	case BehaviorAbsence:
		if b.Anchor != nil {
			return store.CorrelatedAbsent(ctx, tid, pid, spec, at)
		}
		return store.Absent(ctx, tid, pid, spec, at)
	case BehaviorSequence:
		return store.Sequence(ctx, tid, pid, spec, at)
	}
	return false, nil
}

// toSpec translates the DSL BehaviorSpec into the storage-layer behavior.Spec. The
// where-filter closure evaluates the props filter with a NIL store (a where is a
// pure comparison over the row's props, never a store re-entrant behavior). Window
// parse failures are surfaced (validation guarantees parseability, but do not
// silently degrade to a 0-length window).
func toSpec(ctx context.Context, b *BehaviorSpec, ec EvalContext, store BehaviorStore, at time.Time) (behavior.Spec, error) {
	spec := behavior.Spec{EventName: b.EventName, ValueProp: b.ValueProp, Op: b.Op}
	// Force the exact log path for anything buckets cannot serve honestly (where /
	// anchor / sequence / value_prop); validation already sets Exact for most of these.
	spec.Exact = b.Exact || b.Where != nil || b.Anchor != nil || b.ValueProp != ""
	if b.ValueProp != "" {
		spec.DriftProps = append(spec.DriftProps, b.ValueProp)
	}
	if b.Value != nil {
		spec.Value = *b.Value
	}
	if b.Kind == BehaviorSequence {
		w, err := ParseWindow(b.Within)
		if err != nil {
			return behavior.Spec{}, fmt.Errorf("sequence within: %w", err)
		}
		spec.Within = w
	} else {
		w, err := ParseWindow(b.Window)
		if err != nil {
			return behavior.Spec{}, fmt.Errorf("behavior window: %w", err)
		}
		spec.Window = w
	}
	for i := range b.Steps {
		spec.Steps = append(spec.Steps, b.Steps[i].EventName)
	}
	if b.Where != nil {
		where := b.Where
		spec.WhereMatch = func(props json.RawMessage) bool {
			m, _ := Evaluate(ctx, *where, EvalContext{Profile: ec.Profile, Event: events.Envelope{Properties: props}}, nil, at)
			return m
		}
		spec.DriftProps = append(spec.DriftProps, referencedPropNames(where)...)
	}
	if b.Anchor != nil {
		anchor, err := toSpec(ctx, b.Anchor, ec, store, at)
		if err != nil {
			return behavior.Spec{}, err
		}
		spec.Anchor = &anchor
	}
	return spec, nil
}

// referencedPropNames returns the distinct top-level event.properties.<name> keys a
// where sub-rule reads (e.g. "event.properties.price" -> "price";
// "event.properties.a.b" -> "a"). Used to scope schema-drift detection (doc 18 §A).
// Nested paths collapse to their top-level container — a documented limitation.
func referencedPropNames(r *Rule) []string {
	seen := map[string]bool{}
	var out []string
	var walk func(n *Rule)
	walk = func(n *Rule) {
		if rest, ok := strings.CutPrefix(n.Field, "event.properties."); ok && rest != "" {
			top := rest
			if i := strings.IndexByte(rest, '.'); i >= 0 {
				top = rest[:i]
			}
			if top != "" && !seen[top] {
				seen[top] = true
				out = append(out, top)
			}
		}
		for i := range n.Conditions {
			walk(&n.Conditions[i])
		}
	}
	walk(r)
	return out
}

// matchCount applies a count/frequency comparison operator.
func matchCount(op string, got, want float64) bool {
	switch op {
	case OpGte:
		return got >= want
	case OpGt:
		return got > want
	case OpLte:
		return got <= want
	case OpLt:
		return got < want
	case OpEq:
		return got == want
	}
	return false
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
