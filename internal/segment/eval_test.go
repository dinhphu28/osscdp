package segment

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/profile"
)

func ctx() EvalContext {
	return EvalContext{
		Profile: profile.Profile{
			CanonicalUserID: "customer_1",
			Traits:          map[string]any{"country": "VN", "name": "Ann"},
			ComputedAttributes: map[string]any{
				"total_orders": float64(5),
				"tags":         []any{"gold", "vip"},
			},
		},
		Event: events.Envelope{
			Type:       events.TypeTrack,
			EventName:  "product_viewed",
			Properties: json.RawMessage(`{"category":"phone","price":300}`),
			Context:    json.RawMessage(`{"page":{"url":"https://x/p"}}`),
		},
	}
}

func TestEval_AllOperators(t *testing.T) {
	c := ctx()
	cases := []struct {
		name string
		rule Rule
		want bool
	}{
		{"eq true", leaf("profile.traits.country", OpEq, "VN"), true},
		{"eq false", leaf("profile.traits.country", OpEq, "US"), false},
		{"neq true", leaf("profile.traits.country", OpNeq, "US"), true},
		{"gt true", leaf("profile.computed_attributes.total_orders", OpGt, float64(3)), true},
		{"gt false", leaf("profile.computed_attributes.total_orders", OpGt, float64(9)), false},
		{"gte eq", leaf("profile.computed_attributes.total_orders", OpGte, float64(5)), true},
		{"lt true", leaf("profile.computed_attributes.total_orders", OpLt, float64(9)), true},
		{"lte eq", leaf("profile.computed_attributes.total_orders", OpLte, float64(5)), true},
		{"contains string", leaf("event.properties.category", OpContains, "pho"), true},
		{"contains array", leaf("profile.computed_attributes.tags", OpContains, "vip"), true},
		{"not_contains", leaf("event.properties.category", OpNotContains, "zzz"), true},
		{"in true", leaf("profile.traits.country", OpIn, []any{"VN", "US"}), true},
		{"in false", leaf("profile.traits.country", OpIn, []any{"US", "JP"}), false},
		{"not_in true", leaf("profile.traits.country", OpNotIn, []any{"US", "JP"}), true},
		{"exists true", leaf("profile.traits.name", OpExists, nil), true},
		{"exists false", leaf("profile.traits.missing", OpExists, nil), false},
		{"not_exists true", leaf("profile.traits.missing", OpNotExists, nil), true},
		{"event_name eq", leaf("event.event_name", OpEq, "product_viewed"), true},
		{"event prop numeric gt", leaf("event.properties.price", OpGt, float64(100)), true},
		{"nested context", leaf("event.context.page.url", OpContains, "https://x"), true},
		{"missing field comparison false", leaf("profile.traits.missing", OpEq, "x"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evalRule(tc.rule, c); got != tc.want {
				t.Fatalf("Evaluate = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEval_Logical(t *testing.T) {
	c := ctx()
	and := Rule{Operator: OpAnd, Conditions: []Rule{
		leaf("profile.traits.country", OpEq, "VN"),
		leaf("event.event_name", OpEq, "product_viewed"),
	}}
	if !evalRule(and, c) {
		t.Fatal("and should match")
	}
	andFail := Rule{Operator: OpAnd, Conditions: []Rule{
		leaf("profile.traits.country", OpEq, "VN"),
		leaf("event.event_name", OpEq, "checkout"),
	}}
	if evalRule(andFail, c) {
		t.Fatal("and should fail when one condition fails")
	}
	or := Rule{Operator: OpOr, Conditions: []Rule{
		leaf("profile.traits.country", OpEq, "US"),
		leaf("event.event_name", OpEq, "product_viewed"),
	}}
	if !evalRule(or, c) {
		t.Fatal("or should match when any matches")
	}
	not := Rule{Operator: OpNot, Conditions: []Rule{leaf("profile.traits.country", OpEq, "US")}}
	if !evalRule(not, c) {
		t.Fatal("not should invert")
	}
}

func TestEval_BehaviorLeafInert(t *testing.T) {
	c := ctx()
	b := Rule{Behavior: &BehaviorSpec{Kind: BehaviorCount, EventName: "product_viewed", Window: "7d", Op: OpGte}}
	if evalRule(b, c) {
		t.Fatal("behavior leaf must be inert (false) until Phase 3")
	}
	// AND with an otherwise-true stateless leaf is dragged false by the inert behavior leaf.
	and := Rule{Operator: OpAnd, Conditions: []Rule{leaf("profile.traits.country", OpEq, "VN"), b}}
	if evalRule(and, c) {
		t.Fatal("AND with an inert behavior leaf must be false")
	}
	// OR still matches on the true stateless leaf (behavior contributes nothing).
	or := Rule{Operator: OpOr, Conditions: []Rule{leaf("profile.traits.country", OpEq, "VN"), b}}
	if !evalRule(or, c) {
		t.Fatal("OR should still match on the true stateless leaf")
	}
	// NOT over a behavior leaf must NOT invert to a whole-population match: a rule
	// referencing any behavior leaf is inert as a whole.
	not := Rule{Operator: OpNot, Conditions: []Rule{b}}
	if evalRule(not, c) {
		t.Fatal("NOT over an inert behavior leaf must not match")
	}
}

// evalRule runs the (post-Phase-3) Evaluate with a nil store and zero instant —
// stateless rules are unaffected and behavior leaves stay inert.
func evalRule(r Rule, ec EvalContext) bool {
	ok, _ := Evaluate(context.Background(), r, ec, nil, time.Time{})
	return ok
}
