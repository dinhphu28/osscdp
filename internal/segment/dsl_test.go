package segment

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func leaf(field, op string, value any) Rule { return Rule{Field: field, Op: op, Value: value} }

func mustRule(t *testing.T, js string) Rule {
	t.Helper()
	var r Rule
	if err := json.Unmarshal([]byte(js), &r); err != nil {
		t.Fatalf("unmarshal %s: %v", js, err)
	}
	return r
}

func TestValidate_ValidRules(t *testing.T) {
	rules := []Rule{
		leaf("profile.traits.country", OpEq, "VN"),
		{Operator: OpAnd, Conditions: []Rule{leaf("a", OpEq, 1), leaf("b", OpGt, 2)}},
		{Operator: OpOr, Conditions: []Rule{leaf("a", OpExists, nil)}},
		{Operator: OpNot, Conditions: []Rule{leaf("a", OpEq, "x")}},
		leaf("a", OpIn, []any{"x", "y"}),
		leaf("a", OpNotExists, nil),
	}
	for i, r := range rules {
		if err := Validate(r); err != nil {
			t.Errorf("rule %d should be valid: %v", i, err)
		}
	}
}

func TestValidate_Rejects(t *testing.T) {
	bad := map[string]Rule{
		"unknown op":         leaf("a", "between", 1),
		"missing field":      {Op: OpEq, Value: 1},
		"not with 2":         {Operator: OpNot, Conditions: []Rule{leaf("a", OpEq, 1), leaf("b", OpEq, 2)}},
		"and empty":          {Operator: OpAnd},
		"in without array":   leaf("a", OpIn, "x"),
		"exists with value":  leaf("a", OpExists, "x"),
		"missing value":      leaf("a", OpEq, nil),
		"logical with field": {Operator: OpAnd, Field: "a", Conditions: []Rule{leaf("a", OpEq, 1)}},
	}
	for name, r := range bad {
		if err := Validate(r); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

// --- Level 3 behavioral DSL (Phase 1) ---

func TestValidate_BackCompatRoundTrip(t *testing.T) {
	rules := []string{
		`{"field":"profile.computed_attributes.total_orders","op":"gte","value":1}`,
		`{"operator":"and","conditions":[{"field":"profile.traits.country","op":"eq","value":"US"},{"field":"a","op":"exists"}]}`,
		`{"operator":"not","conditions":[{"field":"a","op":"eq","value":"x"}]}`,
	}
	for _, js := range rules {
		r := mustRule(t, js)
		if err := Validate(r); err != nil {
			t.Fatalf("stateless rule rejected: %s: %v", js, err)
		}
		out, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(out), "behavior") {
			t.Fatalf("stateless rule must not emit a behavior field: %s", out)
		}
	}
}

func TestValidate_BehaviorValid(t *testing.T) {
	examples := []string{
		// Flagship: viewed >=3x in 7d AND did not purchase within 24h of the qualifying views.
		`{"operator":"and","conditions":[
			{"behavior":{"kind":"count","event_name":"product_viewed","window":"7d","op":"gte","value":3}},
			{"behavior":{"kind":"absence","event_name":"order_completed","window":"24h","anchor":{"kind":"count","event_name":"product_viewed","window":"7d","op":"gte","value":3}}}
		]}`,
		// Mixed stateless + behaviour with a per-event props filter.
		`{"operator":"and","conditions":[
			{"field":"profile.traits.country","op":"eq","value":"US"},
			{"behavior":{"kind":"count","event_name":"add_to_cart","window":"3d","op":"gte","value":1,"where":{"field":"event.properties.price","op":"gte","value":100}}},
			{"behavior":{"kind":"absence","event_name":"order_completed","window":"3d"}}
		]}`,
		`{"behavior":{"kind":"recency","event_name":"login","window":"24h"}}`,
		`{"behavior":{"kind":"frequency","event_name":"order_completed","value_prop":"revenue","window":"30d","op":"gte","value":500}}`,
		`{"behavior":{"kind":"sequence","within":"1h","steps":[{"event_name":"add_to_cart"},{"event_name":"checkout_started"}]}}`,
	}
	for _, js := range examples {
		if err := Validate(mustRule(t, js)); err != nil {
			t.Fatalf("valid behavior rejected: %s: %v", js, err)
		}
	}
}

func TestValidate_BehaviorForcesExact(t *testing.T) {
	cases := map[string]string{
		"where":    `{"behavior":{"kind":"count","event_name":"add_to_cart","window":"3d","op":"gte","value":1,"where":{"field":"event.properties.price","op":"gte","value":100}}}`,
		"sequence": `{"behavior":{"kind":"sequence","within":"1h","steps":[{"event_name":"a"},{"event_name":"b"}]}}`,
		"anchor":   `{"behavior":{"kind":"absence","event_name":"order_completed","window":"24h","anchor":{"kind":"count","event_name":"product_viewed","window":"7d","op":"gte","value":3}}}`,
	}
	for name, js := range cases {
		r := mustRule(t, js)
		if err := Validate(r); err != nil {
			t.Fatalf("%s: unexpected error: %v", name, err)
		}
		if !r.Behavior.Exact {
			t.Errorf("%s: expected Exact to be force-set true", name)
		}
	}
}

func TestValidate_BehaviorRejects(t *testing.T) {
	bad := map[string]string{
		"stray field":        `{"field":"x","behavior":{"kind":"count","event_name":"e","window":"7d","op":"gte","value":1}}`,
		"unknown kind":       `{"behavior":{"kind":"nope","event_name":"e","window":"7d"}}`,
		"bad window":         `{"behavior":{"kind":"count","event_name":"e","window":"7x","op":"gte","value":1}}`,
		"count without op":   `{"behavior":{"kind":"count","event_name":"e","window":"7d","value":1}}`,
		"count no event":     `{"behavior":{"kind":"count","window":"7d","op":"gte","value":1}}`,
		"sequence one step":  `{"behavior":{"kind":"sequence","within":"1h","steps":[{"event_name":"a"}]}}`,
		"sequence no within": `{"behavior":{"kind":"sequence","steps":[{"event_name":"a"},{"event_name":"b"}]}}`,
		"absence no window":  `{"behavior":{"kind":"absence","event_name":"e"}}`,
		"step no event":      `{"behavior":{"kind":"sequence","within":"1h","steps":[{"event_name":"a"},{}]}}`,
		"count no value":     `{"behavior":{"kind":"count","event_name":"e","window":"7d","op":"gte"}}`,
		"bad where op":       `{"behavior":{"kind":"count","event_name":"e","window":"7d","op":"gte","value":1,"where":{"field":"event.properties.price","op":"between","value":1}}}`,
		"bad anchor kind":    `{"behavior":{"kind":"absence","event_name":"e","window":"24h","anchor":{"kind":"nope","event_name":"a","window":"7d"}}}`,
	}
	for name, js := range bad {
		if err := Validate(mustRule(t, js)); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestParseWindow(t *testing.T) {
	ok := map[string]time.Duration{
		"7d":  168 * time.Hour,
		"1d":  24 * time.Hour,
		"24h": 24 * time.Hour,
		"30m": 30 * time.Minute,
		"90s": 90 * time.Second,
	}
	for s, want := range ok {
		got, err := ParseWindow(s)
		if err != nil || got != want {
			t.Errorf("ParseWindow(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	for _, s := range []string{"", "7", "7x", "d", "-3d", "0d", "0s"} {
		if _, err := ParseWindow(s); err == nil {
			t.Errorf("ParseWindow(%q) should error", s)
		}
	}
}
