// Package segment implements stateless segmentation: a JSON rule DSL, an
// evaluator over profile + event, and membership tracking.
// See docs/cdp/06-segmentation-engine.md.
package segment

import "fmt"

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

// Rule is either a logical node (Operator + Conditions) or a leaf comparison
// (Field + Op + Value).
type Rule struct {
	Operator   string `json:"operator,omitempty"`
	Conditions []Rule `json:"conditions,omitempty"`
	Field      string `json:"field,omitempty"`
	Op         string `json:"op,omitempty"`
	Value      any    `json:"value,omitempty"`
}

func (r Rule) isLogical() bool {
	return r.Operator == OpAnd || r.Operator == OpOr || r.Operator == OpNot
}

// ValidationError is a client-facing rule validation failure.
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

// Validate checks a rule tree against the v1 DSL.
func Validate(r Rule) error {
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
