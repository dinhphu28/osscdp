package journey

import (
	"testing"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/segment"
)

func sendStep(next int) Step {
	return Step{Type: StepSend, DestinationID: uuid.New(), Next: next}
}

func TestValidate_ConditionSplitForwardOnly(t *testing.T) {
	rule := segment.Rule{Field: "profile.traits.country", Op: segment.OpEq, Value: "VN"}

	cases := []struct {
		name string
		def  Definition
		ok   bool
	}{
		{"valid conditional send", Definition{Steps: []Step{
			{Type: StepCondition, Condition: &rule, IfTrue: 1, IfFalse: 2},
			sendStep(3), // true arm jumps past the false arm
			sendStep(0), // false arm, default -> 3 (== len, complete)
		}}, true},
		{"valid split", Definition{Steps: []Step{
			{Type: StepSplit, Branches: []SplitBranch{{Weight: 1, Next: 1}, {Weight: 1, Next: 2}}},
			sendStep(3),
			sendStep(0),
		}}, true},
		{"condition missing rule", Definition{Steps: []Step{
			{Type: StepCondition, IfTrue: 1, IfFalse: 1},
			sendStep(0),
		}}, false},
		{"backward condition target", Definition{Steps: []Step{
			sendStep(0),
			{Type: StepCondition, Condition: &rule, IfTrue: 0, IfFalse: 2}, // 0 <= 1: backward
			sendStep(0),
		}}, false},
		{"condition target past end", Definition{Steps: []Step{
			{Type: StepCondition, Condition: &rule, IfTrue: 1, IfFalse: 3}, // 3 > len(2)
			sendStep(0),
		}}, false},
		{"split one branch", Definition{Steps: []Step{
			{Type: StepSplit, Branches: []SplitBranch{{Weight: 1, Next: 1}}},
			sendStep(0),
		}}, false},
		{"split zero weight", Definition{Steps: []Step{
			{Type: StepSplit, Branches: []SplitBranch{{Weight: 0, Next: 1}, {Weight: 1, Next: 1}}},
			sendStep(0),
		}}, false},
		{"backward send next", Definition{Steps: []Step{
			sendStep(0),
			{Type: StepSend, DestinationID: uuid.New(), Next: 1}, // 1 <= 1: backward
		}}, false},
		{"no send", Definition{Steps: []Step{
			{Type: StepWait, Duration: "1h"},
		}}, false},
		{"condition referencing event field", Definition{Steps: []Step{
			{Type: StepCondition, Condition: &segment.Rule{Field: "event.event_name", Op: segment.OpEq, Value: "x"}, IfTrue: 1, IfFalse: 1},
			sendStep(0),
		}}, false},
		{"condition referencing event field nested", Definition{Steps: []Step{
			{Type: StepCondition, Condition: &segment.Rule{Operator: segment.OpAnd, Conditions: []segment.Rule{
				{Field: "profile.traits.country", Op: segment.OpEq, Value: "VN"},
				{Field: "event.properties.total", Op: segment.OpGte, Value: 1},
			}}, IfTrue: 1, IfFalse: 1},
			sendStep(0),
		}}, false},
		{"wait with condition fields (shape confusion)", Definition{Steps: []Step{
			{Type: StepWait, Duration: "1h", IfTrue: 1},
			sendStep(0),
		}}, false},
		{"send with branches (shape confusion)", Definition{Steps: []Step{
			{Type: StepSend, DestinationID: uuid.New(), Branches: []SplitBranch{{Weight: 1, Next: 1}}},
		}}, false},
		{"split weight over cap", Definition{Steps: []Step{
			{Type: StepSplit, Branches: []SplitBranch{{Weight: maxSplitWeight + 1, Next: 1}, {Weight: 1, Next: 2}}},
			sendStep(3),
			sendStep(0),
		}}, false},
	}
	for _, c := range cases {
		err := Validate(c.def)
		if c.ok && err != nil {
			t.Errorf("%s: expected valid, got %v", c.name, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%s: expected invalid, got nil", c.name)
		}
	}
}

func TestSplitTarget_DeterministicAndWeighted(t *testing.T) {
	branches := []SplitBranch{{Weight: 1, Next: 5}, {Weight: 1, Next: 9}}

	// Deterministic: the same key always routes to the same branch.
	for i := 0; i < 100; i++ {
		if splitTarget(branches, "k|abc|7") != splitTarget(branches, "k|abc|7") {
			t.Fatal("splitTarget is not deterministic for a fixed key")
		}
	}

	// Distribution: over many distinct keys, both branches are hit and every result is
	// a declared target.
	hits := map[int]int{}
	for i := 0; i < 1000; i++ {
		target := splitTarget(branches, string(rune(i))+"|split")
		hits[target]++
		if target != 5 && target != 9 {
			t.Fatalf("splitTarget returned undeclared target %d", target)
		}
	}
	if hits[5] == 0 || hits[9] == 0 {
		t.Fatalf("expected both branches hit, got %v", hits)
	}

	// Weighting: a 99:1 split lands the vast majority on the heavy branch.
	heavy := []SplitBranch{{Weight: 99, Next: 5}, {Weight: 1, Next: 9}}
	heavyHits := 0
	for i := 0; i < 1000; i++ {
		if splitTarget(heavy, string(rune(i))+"|w") == 5 {
			heavyHits++
		}
	}
	if heavyHits < 900 {
		t.Fatalf("expected ~99%% on the heavy branch, got %d/1000", heavyHits)
	}
}
