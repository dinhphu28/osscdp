package behavior

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestStore_SchemaDrift covers finding #33 (doc 18 §A): the exact scan paths fire
// OnSchemaDrift once when a rule-referenced property changes JSON type across the
// window, and never for homogeneous, out-of-window, or consent-dropped rows.
func TestStore_SchemaDrift(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	s := NewStore(pool)
	at := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	win := 7 * 24 * time.Hour
	day := 24 * time.Hour
	tid := seedTenant(t, ctx, pool)

	var drift int
	s.OnSchemaDrift = func() { drift++ }

	t.Run("ValueProp_DriftFiresOnceUnderCounts", func(t *testing.T) {
		drift = 0
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "e1", "purchase", at.Add(-2*day), `{"amount":10}`)
		seedEvent(t, ctx, pool, tid, pid, "e2", "purchase", at.Add(-1*day), `{"amount":"10"}`) // number->string
		spec := Spec{EventName: "purchase", Window: win, ValueProp: "amount", DriftProps: []string{"amount"}}
		sum, err := s.SumValue(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.Equal(t, 1, drift, "drift fires exactly once for a mixed-type window")
		require.Equal(t, float64(10), sum, "the non-numeric row degrades to 0 (graceful under-count)")
	})

	t.Run("Homogeneous_NoDrift", func(t *testing.T) {
		drift = 0
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "h1", "purchase", at.Add(-2*day), `{"amount":10}`)
		seedEvent(t, ctx, pool, tid, pid, "h2", "purchase", at.Add(-1*day), `{"amount":20}`)
		spec := Spec{EventName: "purchase", Window: win, ValueProp: "amount", DriftProps: []string{"amount"}}
		sum, err := s.SumValue(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.Zero(t, drift, "a single-type window never drifts")
		require.Equal(t, float64(30), sum)
	})

	t.Run("Where_ObjectDriftFires", func(t *testing.T) {
		drift = 0
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "w1", "add_to_cart", at.Add(-2*day), `{"price":100}`)
		seedEvent(t, ctx, pool, tid, pid, "w2", "add_to_cart", at.Add(-1*day), `{"price":{"v":1}}`) // number->object
		spec := Spec{EventName: "add_to_cart", Window: win, DriftProps: []string{"price"},
			WhereMatch: func(json.RawMessage) bool { return true }}
		_, err := s.Count(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.Equal(t, 1, drift, "a where-referenced prop changing type drifts")
	})

	t.Run("OutOfWindowIgnored", func(t *testing.T) {
		drift = 0
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "o1", "purchase", at.Add(-10*day), `{"amount":10}`)  // before window
		seedEvent(t, ctx, pool, tid, pid, "o2", "purchase", at.Add(-8*day), `{"amount":"10"}`) // before window
		seedEvent(t, ctx, pool, tid, pid, "o3", "purchase", at.Add(-1*day), `{"amount":5}`)    // in window, numeric
		spec := Spec{EventName: "purchase", Window: win, ValueProp: "amount", DriftProps: []string{"amount"}}
		_, err := s.SumValue(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.Zero(t, drift, "type change strictly before at-window is not in-window drift")
	})

	t.Run("MultiProp_LoopContinuesToLaterDrift", func(t *testing.T) {
		drift = 0
		pid := uuid.New()
		// "a" is homogeneous (number, number); "b" drifts (number → string). The probe
		// must continue past the clean "a" to detect the drift on "b".
		seedEvent(t, ctx, pool, tid, pid, "m1", "add_to_cart", at.Add(-2*day), `{"a":1,"b":10}`)
		seedEvent(t, ctx, pool, tid, pid, "m2", "add_to_cart", at.Add(-1*day), `{"a":2,"b":"x"}`)
		spec := Spec{EventName: "add_to_cart", Window: win, DriftProps: []string{"a", "b"},
			WhereMatch: func(json.RawMessage) bool { return true }}
		_, err := s.Count(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.Equal(t, 1, drift, "the loop continues past a homogeneous prop to catch a later one")
	})

	t.Run("ConsentDroppedRowsNotDrift", func(t *testing.T) {
		drift = 0
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "c1", "purchase", at.Add(-2*day), `{"amount":10}`)
		seedEvent(t, ctx, pool, tid, pid, "c2", "purchase", at.Add(-1*day), "") // props_json NULL (gate-dropped)
		spec := Spec{EventName: "purchase", Window: win, ValueProp: "amount", DriftProps: []string{"amount"}}
		_, err := s.SumValue(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.Zero(t, drift, "NULL props are excluded, never a distinct type")
	})
}
