package profile

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/events"
)

func TestMergeReparent(t *testing.T) {
	early := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)

	survivor := &Profile{
		Traits:             map[string]any{"name": "Ann", "email": "keep@x.com"},
		ComputedAttributes: map[string]any{AttrTotalEvents: float64(2), AttrLastEventName: "page_viewed"},
		LastSeenAt:         &late,
	}
	loser := Profile{
		Traits:             map[string]any{"email": "other@x.com", "phone": "+8490"},
		ComputedAttributes: map[string]any{AttrTotalEvents: float64(3), AttrTotalOrders: float64(1), AttrLastProductViewed: "p001"},
		FirstSeenAt:        &early,
		LastSeenAt:         &early,
	}

	changed := mergeReparent(survivor, loser)

	// Fill-missing traits: survivor keeps its email/name; gains the loser's phone.
	if survivor.Traits["email"] != "keep@x.com" {
		t.Fatalf("survivor email overwritten: %v", survivor.Traits["email"])
	}
	if survivor.Traits["phone"] != "+8490" {
		t.Fatalf("loser phone not folded in: %v", survivor.Traits["phone"])
	}
	// Counts summed; loser-only order count carried over.
	if got := asInt(survivor.ComputedAttributes[AttrTotalEvents]); got != 5 {
		t.Fatalf("total_events = %d, want 5", got)
	}
	if got := asInt(survivor.ComputedAttributes[AttrTotalOrders]); got != 1 {
		t.Fatalf("total_orders = %d, want 1", got)
	}
	// Missing computed key filled; existing one not clobbered.
	if survivor.ComputedAttributes[AttrLastProductViewed] != "p001" {
		t.Fatalf("last_product_viewed not filled: %v", survivor.ComputedAttributes[AttrLastProductViewed])
	}
	if survivor.ComputedAttributes[AttrLastEventName] != "page_viewed" {
		t.Fatalf("last_event_name clobbered: %v", survivor.ComputedAttributes[AttrLastEventName])
	}
	// Seen window widened: first from loser, last kept from survivor.
	if survivor.FirstSeenAt == nil || !survivor.FirstSeenAt.Equal(early) {
		t.Fatalf("first_seen_at not widened: %v", survivor.FirstSeenAt)
	}
	if !survivor.LastSeenAt.Equal(late) {
		t.Fatalf("last_seen_at changed: %v", survivor.LastSeenAt)
	}
	if len(changed) == 0 {
		t.Fatal("expected changed fields")
	}
}

func TestMergeReparent_Recency(t *testing.T) {
	early := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)

	// Survivor was seen earlier; the loser was seen more recently.
	survivor := &Profile{
		Traits: map[string]any{},
		ComputedAttributes: map[string]any{
			AttrLastEventName: "page_viewed",
			AttrLastOrderAt:   early.Format(time.RFC3339),
		},
		LastSeenAt: &early,
	}
	loser := Profile{
		Traits: map[string]any{},
		ComputedAttributes: map[string]any{
			AttrLastEventName: "order_completed",
			AttrLastOrderAt:   late.Format(time.RFC3339),
		},
		LastSeenAt: &late,
	}

	mergeReparent(survivor, loser)

	// Loser seen more recently -> its last_event_name wins over the survivor's staler one.
	if survivor.ComputedAttributes[AttrLastEventName] != "order_completed" {
		t.Fatalf("last_event_name = %v, want order_completed (loser is newer)", survivor.ComputedAttributes[AttrLastEventName])
	}
	// last_order_at always takes the later timestamp.
	if survivor.ComputedAttributes[AttrLastOrderAt] != late.Format(time.RFC3339) {
		t.Fatalf("last_order_at = %v, want %v", survivor.ComputedAttributes[AttrLastOrderAt], late.Format(time.RFC3339))
	}
}

func trackEnv(name string, props, ctxJSON, traits string) events.Envelope {
	e := events.Envelope{
		Type:      events.TypeTrack,
		EventName: name,
		SourceID:  uuid.New(),
		Timestamp: time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC),
	}
	if props != "" {
		e.Properties = json.RawMessage(props)
	}
	if ctxJSON != "" {
		e.Context = json.RawMessage(ctxJSON)
	}
	if traits != "" {
		e.Traits = json.RawMessage(traits)
	}
	return e
}

func TestMergeTraits_LatestNonEmptyWins(t *testing.T) {
	cur := map[string]any{"email": "old@x.com", "name": "Old"}
	env := trackEnv("identify", "", "", `{"email":"new@x.com","country":"VN"}`)
	got, changed := MergeTraits(cur, env)

	if got["email"] != "new@x.com" {
		t.Fatalf("email = %v, want new@x.com", got["email"])
	}
	if got["country"] != "VN" {
		t.Fatalf("country = %v, want VN", got["country"])
	}
	if got["name"] != "Old" {
		t.Fatalf("name should be unchanged, got %v", got["name"])
	}
	if !contains(changed, "traits.email") || !contains(changed, "traits.country") {
		t.Fatalf("changed = %v", changed)
	}
}

func TestMergeTraits_EmptyDoesNotClobber(t *testing.T) {
	cur := map[string]any{"email": "keep@x.com"}
	got, changed := MergeTraits(cur, trackEnv("page_viewed", "", "", ""))
	if got["email"] != "keep@x.com" {
		t.Fatalf("email clobbered: %v", got["email"])
	}
	if len(changed) != 0 {
		t.Fatalf("no changes expected, got %v", changed)
	}
}

func TestMergeComputed_TotalEventsIncrements(t *testing.T) {
	cur := map[string]any{"total_events": float64(4)} // as decoded from JSONB
	got, _ := MergeComputed(cur, trackEnv("page_viewed", "", "", ""))
	if asInt(got[AttrTotalEvents]) != 5 {
		t.Fatalf("total_events = %v, want 5", got[AttrTotalEvents])
	}
}

func TestMergeComputed_ProductViewed(t *testing.T) {
	got, _ := MergeComputed(map[string]any{}, trackEnv(EventProductViewed, `{"product_id":"p001"}`, "", ""))
	if got[AttrLastProductViewed] != "p001" {
		t.Fatalf("last_product_viewed = %v", got[AttrLastProductViewed])
	}
	if got[AttrLastEventName] != EventProductViewed {
		t.Fatalf("last_event_name = %v", got[AttrLastEventName])
	}
}

func TestMergeComputed_OrderCompleted(t *testing.T) {
	got, changed := MergeComputed(map[string]any{"total_orders": float64(2)}, trackEnv(EventOrderCompleted, "", "", ""))
	if asInt(got[AttrTotalOrders]) != 3 {
		t.Fatalf("total_orders = %v, want 3", got[AttrTotalOrders])
	}
	if got[AttrLastOrderAt] == nil {
		t.Fatal("last_order_at should be set")
	}
	if !contains(changed, "computed_attributes.total_orders") {
		t.Fatalf("changed = %v", changed)
	}
}

func TestMergeComputed_LastPageURL(t *testing.T) {
	got, _ := MergeComputed(map[string]any{}, trackEnv("page_viewed", "", `{"page":{"url":"https://x/p"}}`, ""))
	if got[AttrLastPageURL] != "https://x/p" {
		t.Fatalf("last_page_url = %v", got[AttrLastPageURL])
	}
}

func TestMergeSeen_MinMax(t *testing.T) {
	mid := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	first, last, _ := MergeSeen(nil, nil, mid)
	if !first.Equal(mid) || !last.Equal(mid) {
		t.Fatal("first event sets both seen times")
	}
	earlier := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	first2, last2, changed := MergeSeen(first, last, earlier)
	if !first2.Equal(earlier) {
		t.Fatalf("first_seen should move earlier: %v", first2)
	}
	if !last2.Equal(mid) {
		t.Fatalf("last_seen should stay: %v", last2)
	}
	if !contains(changed, "first_seen_at") || contains(changed, "last_seen_at") {
		t.Fatalf("only first_seen should change: %v", changed)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
