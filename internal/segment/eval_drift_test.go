package segment

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestReferencedPropNames(t *testing.T) {
	// A nested AND of event.properties.* leaves + a profile.* leaf (ignored) + a nested path.
	where := &Rule{Operator: OpAnd, Conditions: []Rule{
		{Field: "event.properties.price", Op: OpGte, Value: float64(100)},
		{Field: "event.properties.currency", Op: OpEq, Value: "USD"},
		{Field: "event.properties.meta.region", Op: OpEq, Value: "eu"}, // nested → top-level "meta"
		{Field: "profile.traits.country", Op: OpEq, Value: "VN"},       // not an event prop → ignored
		{Field: "event.properties.price", Op: OpLte, Value: float64(999)}, // duplicate → deduped
	}}
	got := referencedPropNames(where)
	sort.Strings(got)
	want := []string{"currency", "meta", "price"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("referencedPropNames = %v, want %v", got, want)
	}
}

func TestToSpec_DriftProps(t *testing.T) {
	ctx := context.Background()
	at := time.Unix(1_700_000_000, 0).UTC()

	// value_prop leaf → DriftProps carries the value prop.
	vp := &BehaviorSpec{Kind: BehaviorCount, EventName: "purchase", Window: "7d", ValueProp: "amount", Op: OpGte, Value: fptr(1)}
	spec, err := toSpec(ctx, vp, EvalContext{}, nil, at)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec.DriftProps, []string{"amount"}) {
		t.Fatalf("value_prop DriftProps = %v, want [amount]", spec.DriftProps)
	}

	// where leaf → DriftProps carries the referenced event props.
	wh := &BehaviorSpec{Kind: BehaviorCount, EventName: "add_to_cart", Window: "3d", Op: OpGte, Value: fptr(1),
		Where: &Rule{Field: "event.properties.price", Op: OpGte, Value: float64(100)}}
	spec, err = toSpec(ctx, wh, EvalContext{}, nil, at)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec.DriftProps, []string{"price"}) {
		t.Fatalf("where DriftProps = %v, want [price]", spec.DriftProps)
	}
}

func fptr(f float64) *float64 { return &f }
