package profile

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/events"
)

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
