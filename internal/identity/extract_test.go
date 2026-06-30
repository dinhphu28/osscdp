package identity

import (
	"testing"

	"github.com/dinhphu28/osscdp/internal/events"
)

func has(ids []Identifier, ns, v string) bool {
	for _, id := range ids {
		if id.Namespace == ns && id.Value == v {
			return true
		}
	}
	return false
}

func TestExtract_Track(t *testing.T) {
	env := events.Envelope{Type: events.TypeTrack, Identifiers: events.Identifiers{UserID: "u1", Email: "e@x.com"}}
	ids := ExtractIdentifiers(env)
	if len(ids) != 2 || !has(ids, NSUserID, "u1") || !has(ids, NSEmail, "e@x.com") {
		t.Fatalf("unexpected ids: %+v", ids)
	}
}

func TestExtract_IdentifyLinksAnonAndUser(t *testing.T) {
	env := events.Envelope{Type: events.TypeIdentify, Identifiers: events.Identifiers{UserID: "u1", AnonymousID: "a1"}}
	ids := ExtractIdentifiers(env)
	if !has(ids, NSUserID, "u1") || !has(ids, NSAnonymousID, "a1") {
		t.Fatalf("identify should yield both ids: %+v", ids)
	}
}

func TestExtract_AliasPreviousIDAsAnonymous(t *testing.T) {
	env := events.Envelope{Type: events.TypeAlias, PreviousID: "a1", Identifiers: events.Identifiers{UserID: "u1"}}
	ids := ExtractIdentifiers(env)
	if !has(ids, NSUserID, "u1") || !has(ids, NSAnonymousID, "a1") {
		t.Fatalf("alias should link user_id and previous_id(anonymous): %+v", ids)
	}
}

func TestExtract_DedupesPreviousIDMatchingAnonymous(t *testing.T) {
	env := events.Envelope{Type: events.TypeAlias, PreviousID: "a1", Identifiers: events.Identifiers{AnonymousID: "a1", UserID: "u1"}}
	ids := ExtractIdentifiers(env)
	count := 0
	for _, id := range ids {
		if id.Namespace == NSAnonymousID && id.Value == "a1" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("duplicate anonymous_id should be deduped, got %d", count)
	}
}
