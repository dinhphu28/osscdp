package segment

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/profile"
)

var errBoom = errors.New("store boom")

// fakeStore returns canned answers so the evaluator's dispatch/short-circuit
// logic is testable without a database.
type fakeStore struct {
	count                                int64
	recent, absent, corrAbsent, sequence bool
	sum                                  float64
	lastAt                               time.Time
	hasLast                              bool
	nth                                  time.Time
	hasNth                               bool
}

func (f fakeStore) Count(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (int64, error) {
	return f.count, nil
}
func (f fakeStore) Recent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return f.recent, nil
}
func (f fakeStore) Absent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return f.absent, nil
}
func (f fakeStore) CorrelatedAbsent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return f.corrAbsent, nil
}
func (f fakeStore) Sequence(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return f.sequence, nil
}
func (f fakeStore) SumValue(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (float64, error) {
	return f.sum, nil
}
func (f fakeStore) LastAt(context.Context, uuid.UUID, uuid.UUID, string, time.Time) (time.Time, bool, error) {
	return f.lastAt, f.hasLast, nil
}
func (f fakeStore) NthNewestInWindow(context.Context, uuid.UUID, uuid.UUID, string, time.Duration, int, time.Time) (time.Time, bool, error) {
	return f.nth, f.hasNth, nil
}

func evalWith(r Rule, prof profile.Profile, store BehaviorStore) bool {
	ok, _ := Evaluate(context.Background(), r, EvalContext{Profile: prof}, store, time.Now())
	return ok
}

func TestEvalBehavior_Flagship(t *testing.T) {
	rule := mustRule(t, `{"operator":"and","conditions":[
		{"behavior":{"kind":"count","event_name":"product_viewed","window":"7d","op":"gte","value":3}},
		{"behavior":{"kind":"absence","event_name":"order_completed","window":"24h"}}
	]}`)
	prof := profile.Profile{ID: uuid.New(), TenantID: uuid.New()}

	if !evalWith(rule, prof, fakeStore{count: 3, absent: true}) {
		t.Fatal("count>=3 AND absent must match")
	}
	if evalWith(rule, prof, fakeStore{count: 2, absent: true}) {
		t.Fatal("count<3 must not match")
	}
	if evalWith(rule, prof, fakeStore{count: 5, absent: false}) {
		t.Fatal("a recent purchase must not match")
	}
}

func TestEvalBehavior_MixedShortCircuit(t *testing.T) {
	rule := mustRule(t, `{"operator":"and","conditions":[
		{"field":"profile.traits.country","op":"eq","value":"US"},
		{"behavior":{"kind":"count","event_name":"add_to_cart","window":"3d","op":"gte","value":1}}
	]}`)
	us := profile.Profile{ID: uuid.New(), TenantID: uuid.New(), Traits: map[string]any{"country": "US"}}
	vn := profile.Profile{ID: uuid.New(), TenantID: uuid.New(), Traits: map[string]any{"country": "VN"}}

	if !evalWith(rule, us, fakeStore{count: 1}) {
		t.Fatal("US + count>=1 must match")
	}
	if evalWith(rule, vn, fakeStore{count: 100}) {
		t.Fatal("non-US must short-circuit false regardless of behavior")
	}
	if evalWith(rule, us, fakeStore{count: 0}) {
		t.Fatal("US but count 0 must not match")
	}
}

func TestEvalBehavior_NoStoreInert(t *testing.T) {
	us := profile.Profile{Traits: map[string]any{"country": "US"}}
	// Pure stateless rule is unaffected by a nil store.
	if !evalWith(leaf("profile.traits.country", OpEq, "US"), us, nil) {
		t.Fatal("stateless rule must match with a nil store")
	}
	// A behavior leaf is inert with a nil store.
	b := mustRule(t, `{"behavior":{"kind":"count","event_name":"x","window":"7d","op":"gte","value":1}}`)
	if evalWith(b, us, nil) {
		t.Fatal("behavior leaf must be inert with a nil store")
	}
}

func TestEvalBehavior_KindsDispatch(t *testing.T) {
	prof := profile.Profile{ID: uuid.New(), TenantID: uuid.New()}
	cases := []struct {
		name  string
		js    string
		store fakeStore
		want  bool
	}{
		{"recency true", `{"behavior":{"kind":"recency","event_name":"login","window":"24h"}}`, fakeStore{recent: true}, true},
		{"recency false", `{"behavior":{"kind":"recency","event_name":"login","window":"24h"}}`, fakeStore{recent: false}, false},
		{"sequence true", `{"behavior":{"kind":"sequence","within":"1h","steps":[{"event_name":"a"},{"event_name":"b"}]}}`, fakeStore{sequence: true}, true},
		{"correlated absent", `{"behavior":{"kind":"absence","event_name":"o","window":"24h","anchor":{"kind":"count","event_name":"v","window":"7d","op":"gte","value":3}}}`, fakeStore{corrAbsent: true}, true},
		{"frequency-of-value >=500", `{"behavior":{"kind":"frequency","event_name":"order","value_prop":"revenue","window":"30d","op":"gte","value":500}}`, fakeStore{sum: 600}, true},
		{"frequency-of-value <500", `{"behavior":{"kind":"frequency","event_name":"order","value_prop":"revenue","window":"30d","op":"gte","value":500}}`, fakeStore{sum: 400}, false},
		{"count eq 3 true", `{"behavior":{"kind":"count","event_name":"v","window":"7d","op":"eq","value":3}}`, fakeStore{count: 3}, true},
		{"count eq 3 false", `{"behavior":{"kind":"count","event_name":"v","window":"7d","op":"eq","value":3}}`, fakeStore{count: 2}, false},
		{"count lt 5 true", `{"behavior":{"kind":"count","event_name":"v","window":"7d","op":"lt","value":5}}`, fakeStore{count: 4}, true},
		{"frequency count-mode (no value_prop)", `{"behavior":{"kind":"frequency","event_name":"o","window":"7d","op":"gte","value":2}}`, fakeStore{count: 3}, true},
	}
	for _, tc := range cases {
		if got := evalWith(mustRule(t, tc.js), prof, tc.store); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

// errStore fails every read, to verify errors propagate (not swallowed to false).
type errStore struct{}

func (errStore) Count(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (int64, error) {
	return 0, errBoom
}
func (errStore) Recent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, errBoom
}
func (errStore) Absent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, errBoom
}
func (errStore) CorrelatedAbsent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, errBoom
}
func (errStore) Sequence(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, errBoom
}
func (errStore) SumValue(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (float64, error) {
	return 0, errBoom
}
func (errStore) LastAt(context.Context, uuid.UUID, uuid.UUID, string, time.Time) (time.Time, bool, error) {
	return time.Time{}, false, errBoom
}
func (errStore) NthNewestInWindow(context.Context, uuid.UUID, uuid.UUID, string, time.Duration, int, time.Time) (time.Time, bool, error) {
	return time.Time{}, false, errBoom
}

func TestEvalBehavior_StoreErrorPropagates(t *testing.T) {
	ec := EvalContext{Profile: profile.Profile{ID: uuid.New(), TenantID: uuid.New()}}
	leaf := mustRule(t, `{"behavior":{"kind":"count","event_name":"x","window":"7d","op":"gte","value":1}}`)

	ok, err := Evaluate(context.Background(), leaf, ec, errStore{}, time.Now())
	if err == nil || ok {
		t.Fatalf("errored leaf must return (false, error); got (%v, %v)", ok, err)
	}
	// Crucially, NOT over an errored leaf must NOT invert to a spurious match.
	not := Rule{Operator: OpNot, Conditions: []Rule{leaf}}
	ok, err = Evaluate(context.Background(), not, ec, errStore{}, time.Now())
	if err == nil || ok {
		t.Fatalf("NOT over an errored leaf must return (false, error); got (%v, %v)", ok, err)
	}
}
