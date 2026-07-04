package segment

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/profile"
)

// dueFake serves LastAt (by event) and NthNewestInWindow (by index n) answers for
// due_at tests; the evaluation methods are unused by nextDueAt and return zero.
type dueFake struct {
	last map[string]time.Time
	nth  map[int]time.Time // n-th newest -> occurred_at
}

func (dueFake) Count(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (int64, error) {
	return 0, nil
}
func (dueFake) Recent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, nil
}
func (dueFake) Absent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, nil
}
func (dueFake) CorrelatedAbsent(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, nil
}
func (dueFake) Sequence(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (bool, error) {
	return false, nil
}
func (dueFake) SumValue(context.Context, uuid.UUID, uuid.UUID, behavior.Spec, time.Time) (float64, error) {
	return 0, nil
}
func (f dueFake) LastAt(_ context.Context, _, _ uuid.UUID, name string, _ time.Time) (time.Time, bool, error) {
	t, ok := f.last[name]
	return t, ok, nil
}
func (f dueFake) NthNewestInWindow(_ context.Context, _, _ uuid.UUID, _ string, _ time.Duration, n int, _ time.Time) (time.Time, bool, error) {
	t, ok := f.nth[n]
	return t, ok, nil
}

func dueOf(t *testing.T, js string, f dueFake, at time.Time) (time.Time, bool) {
	t.Helper()
	due, ok, err := nextDueAt(context.Background(), mustRule(t, js), EvalContext{Profile: profile.Profile{ID: uuid.New(), TenantID: uuid.New()}}, f, at)
	if err != nil {
		t.Fatalf("nextDueAt: %v", err)
	}
	return due, ok
}

func TestNextDueAt_Formulas(t *testing.T) {
	at := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	// absence(order,24h): matures 24h after the last order.
	lastOrder := at.Add(-1 * time.Hour)
	due, ok := dueOf(t, `{"behavior":{"kind":"absence","event_name":"order","window":"24h"}}`,
		dueFake{last: map[string]time.Time{"order": lastOrder}}, at)
	if !ok || !due.Equal(lastOrder.Add(day)) {
		t.Fatalf("absence due = %v (ok=%v), want %v", due, ok, lastOrder.Add(day))
	}

	// absence with a never-occurred event: already absent, no deadline.
	if _, ok := dueOf(t, `{"behavior":{"kind":"absence","event_name":"order","window":"24h"}}`, dueFake{}, at); ok {
		t.Fatal("never-occurred absence must have no deadline")
	}

	// recency(login,24h): flips to not-recent 24h after the last login.
	lastLogin := at.Add(-2 * time.Hour)
	due, ok = dueOf(t, `{"behavior":{"kind":"recency","event_name":"login","window":"24h"}}`,
		dueFake{last: map[string]time.Time{"login": lastLogin}}, at)
	if !ok || !due.Equal(lastLogin.Add(day)) {
		t.Fatalf("recency due = %v, want %v", due, lastLogin.Add(day))
	}

	// count(view>=3,7d): drops below 3 when the 3rd-newest ages out (nth[3]+7d).
	thirdNewest := at.Add(-3 * day)
	due, ok = dueOf(t, `{"behavior":{"kind":"count","event_name":"view","window":"7d","op":"gte","value":3}}`,
		dueFake{nth: map[int]time.Time{3: thirdNewest}}, at)
	if !ok || !due.Equal(thirdNewest.Add(7*day)) {
		t.Fatalf("count due = %v, want %v", due, thirdNewest.Add(7*day))
	}
}

// The count deadline boundary depends on the operator, not a fixed K: gte/lt cross
// at the K-th newest, gt/lte at the (K+1)-th, eq at both (earliest future).
func TestNextDueAt_CountOperators(t *testing.T) {
	at := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	t3 := at.Add(-3 * day) // 3rd newest -> ages out at at+4d
	t4 := at.Add(-4 * day) // 4th newest -> ages out at at+3d (earlier)
	f := dueFake{nth: map[int]time.Time{3: t3, 4: t4}}

	for _, tc := range []struct {
		op  string
		idx int
	}{{"gte", 3}, {"lt", 3}, {"gt", 4}, {"lte", 4}} {
		js := fmt.Sprintf(`{"behavior":{"kind":"count","event_name":"view","window":"7d","op":%q,"value":3}}`, tc.op)
		due, ok := dueOf(t, js, f, at)
		want := f.nth[tc.idx].Add(7 * day)
		if !ok || !due.Equal(want) {
			t.Fatalf("op %s: due = %v, want nth[%d]+7d = %v", tc.op, due, tc.idx, want)
		}
	}

	// eq 3: earliest future of the enter boundary nth[4]+7d and the exit nth[3]+7d.
	due, ok := dueOf(t, `{"behavior":{"kind":"count","event_name":"view","window":"7d","op":"eq","value":3}}`, f, at)
	if !ok || !due.Equal(t4.Add(7*day)) {
		t.Fatalf("eq due = %v, want earliest %v", due, t4.Add(7*day))
	}
}

func TestNextDueAt_CompositeEarliestFuture(t *testing.T) {
	at := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	// AND(count(view>=3,7d), absence(order,24h)). The two leaves have different elapse
	// deadlines; nextDueAt returns the earliest FUTURE one.
	f := dueFake{
		nth:  map[int]time.Time{3: at.Add(-6 * day)},               // count due  = at + 24h
		last: map[string]time.Time{"order": at.Add(-1 * time.Hour)}, // absence due = at + 23h (earlier)
	}
	rule := `{"operator":"and","conditions":[
		{"behavior":{"kind":"count","event_name":"view","window":"7d","op":"gte","value":3}},
		{"behavior":{"kind":"absence","event_name":"order","window":"24h"}}
	]}`
	absenceDue := at.Add(-1 * time.Hour).Add(day) // at + 23h
	countDue := at.Add(-6 * day).Add(7 * day)     // at + 24h

	due, ok := dueOf(t, rule, f, at)
	if !ok || !due.Equal(absenceDue) {
		t.Fatalf("composite due = %v, want earliest %v", due, absenceDue)
	}

	// A no-op wake at the earlier (absence) deadline: it is no longer strictly future,
	// so nextDueAt re-arms to the still-future count deadline — the later deadline of a
	// composite rule is not discarded (finding #6).
	due2, ok := dueOf(t, rule, f, absenceDue)
	if !ok || !due2.Equal(countDue) {
		t.Fatalf("re-armed due = %v, want later deadline %v", due2, countDue)
	}
}

func TestReferencesEvent(t *testing.T) {
	// Pure behavioral + profile rule: sweep-safe.
	if referencesEvent(mustRule(t, `{"behavior":{"kind":"absence","event_name":"order","window":"24h"}}`)) {
		t.Fatal("pure behavior rule must be sweep-safe")
	}
	if referencesEvent(leaf("profile.traits.country", OpEq, "VN")) {
		t.Fatal("profile-only rule must be sweep-safe")
	}
	// Any stateless event.* leaf makes it edge-only.
	mixed := Rule{Operator: OpAnd, Conditions: []Rule{
		leaf("event.event_name", OpEq, "checkout"),
		mustRule(t, `{"behavior":{"kind":"absence","event_name":"order","window":"24h"}}`),
	}}
	if !referencesEvent(mixed) {
		t.Fatal("a rule with an event.* leaf must not be sweep-safe")
	}
}
