package segment

import (
	"context"
	"strings"
	"time"
)

// nextDueAt returns the earliest instant strictly after `at` at which some
// behavioral leaf of r could flip by elapse alone (no new event) — the deadline to
// enqueue in segment_pending_eval. ok=false when no leaf has a time-based deadline
// (e.g. every referenced event is already absent, or the rule is purely stateless).
func nextDueAt(ctx context.Context, r Rule, ec EvalContext, store BehaviorStore, at time.Time) (time.Time, bool, error) {
	var best time.Time
	found := false
	var walk func(Rule) error
	walk = func(r Rule) error {
		if r.Behavior != nil {
			due, ok, err := leafDueAt(ctx, r.Behavior, ec, store, at)
			if err != nil {
				return err
			}
			if ok && due.After(at) && (!found || due.Before(best)) {
				best, found = due, true
			}
			return nil
		}
		for _, c := range r.Conditions {
			if err := walk(c); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(r); err != nil {
		return time.Time{}, false, err
	}
	return best, found, nil
}

// leafDueAt computes the elapse deadline for one behavioral leaf, or ok=false if it
// has none (already-absent event, count already below threshold, sequence, etc.).
func leafDueAt(ctx context.Context, b *BehaviorSpec, ec EvalContext, store BehaviorStore, at time.Time) (time.Time, bool, error) {
	tid, pid := ec.Profile.TenantID, ec.Profile.ID
	switch b.Kind {
	case BehaviorAbsence:
		w, err := ParseWindow(b.Window)
		if err != nil {
			return time.Time{}, false, err
		}
		// Correlated absence matures W after the anchor's latest occurrence; plain
		// absence matures W after the referenced event's latest occurrence.
		name := b.EventName
		if b.Anchor != nil {
			name = b.Anchor.EventName
		}
		last, ok, err := store.LastAt(ctx, tid, pid, name, at)
		if err != nil || !ok {
			return time.Time{}, false, err // never occurred => already absent, no deadline
		}
		return last.Add(w), true, nil
	case BehaviorRecency:
		w, err := ParseWindow(b.Window)
		if err != nil {
			return time.Time{}, false, err
		}
		last, ok, err := store.LastAt(ctx, tid, pid, b.EventName, at)
		if err != nil || !ok {
			return time.Time{}, false, err
		}
		return last.Add(w), true, nil // recency flips to false W after the last event
	case BehaviorCount:
		if b.Value == nil {
			return time.Time{}, false, nil
		}
		w, err := ParseWindow(b.Window)
		if err != nil {
			return time.Time{}, false, err
		}
		k := int(*b.Value)
		if k < 1 {
			return time.Time{}, false, nil
		}
		// Elapse only lowers the windowed count; it reaches c-1 when the c-th newest
		// event ages out (at nth(c)+W). The comparison flips at a boundary index that
		// depends on the operator: gte K / lt K cross at K; gt K / lte K cross at K+1;
		// eq K flips at both the K+1 (enter) and K (exit) boundaries. Return the
		// earliest future of the relevant boundaries.
		var indices []int
		switch b.Op {
		case OpGte, OpLt:
			indices = []int{k}
		case OpGt, OpLte:
			indices = []int{k + 1}
		case OpEq:
			indices = []int{k + 1, k}
		default:
			return time.Time{}, false, nil
		}
		var best time.Time
		found := false
		for _, idx := range indices {
			nth, ok, err := store.NthNewestInWindow(ctx, tid, pid, b.EventName, w, idx, at)
			if err != nil {
				return time.Time{}, false, err
			}
			if !ok {
				continue // fewer than idx in window: that boundary only rises via an edge event
			}
			if due := nth.Add(w); due.After(at) && (!found || due.Before(best)) {
				best, found = due, true
			}
		}
		return best, found, nil
	default:
		// sequence / frequency-of-value: no cheap elapse deadline; the safety sweep
		// re-enqueues active memberships so these self-heal.
		return time.Time{}, false, nil
	}
}

// referencesEvent reports whether the rule has a stateless leaf over event.* fields.
// The sweeper re-evaluates with NO triggering event, so such a rule cannot be swept
// safely (its event condition would spuriously read empty); we keep it edge-only and
// never enqueue a due_at for it. Behavior leaves resolve against stored events and
// are sweep-safe.
func referencesEvent(r Rule) bool {
	if r.Behavior != nil {
		return false
	}
	if strings.HasPrefix(r.Field, "event.") {
		return true
	}
	for _, c := range r.Conditions {
		if referencesEvent(c) {
			return true
		}
	}
	return false
}
