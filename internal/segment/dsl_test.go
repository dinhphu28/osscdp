package segment

import "testing"

func leaf(field, op string, value any) Rule { return Rule{Field: field, Op: op, Value: value} }

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
