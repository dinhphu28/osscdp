package events

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	testTenant = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	testSource = uuid.MustParse("22222222-2222-2222-2222-222222222222")
	fixedNow   = time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
)

func TestNormalize_GeneratesEventIDWhenMissing(t *testing.T) {
	env, err := Normalize(IncomingEvent{UserID: "u1", EventName: "x"}, testTenant, testSource, TypeTrack, fixedNow)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if env.EventID == "" {
		t.Fatal("expected generated event_id")
	}
	if len(env.EventID) < 4 || env.EventID[:4] != "evt_" {
		t.Fatalf("expected evt_ prefix, got %q", env.EventID)
	}
}

func TestNormalize_ServerSetsReceivedAt(t *testing.T) {
	env, err := Normalize(IncomingEvent{EventID: "e1", UserID: "u1", EventName: "x"}, testTenant, testSource, TypeTrack, fixedNow)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if !env.ReceivedAt.Equal(fixedNow) {
		t.Fatalf("received_at = %v, want %v", env.ReceivedAt, fixedNow)
	}
}

func TestNormalize_TimestampDefaultsToReceivedAt(t *testing.T) {
	env, _ := Normalize(IncomingEvent{EventID: "e1", UserID: "u1", EventName: "x"}, testTenant, testSource, TypeTrack, fixedNow)
	if !env.Timestamp.Equal(fixedNow) {
		t.Fatalf("timestamp = %v, want default %v", env.Timestamp, fixedNow)
	}
}

func TestNormalize_BadTimestampRejected(t *testing.T) {
	_, err := Normalize(IncomingEvent{EventID: "e1", UserID: "u1", Timestamp: "not-a-time"}, testTenant, testSource, TypeTrack, fixedNow)
	var ve *ValidationError
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !asValidation(err, &ve) || ve.Field != "timestamp" {
		t.Fatalf("expected timestamp ValidationError, got %v", err)
	}
}

func TestIdentifierKey_Priority(t *testing.T) {
	cases := []struct {
		name string
		in   Identifiers
		want string
	}{
		{"user_id wins", Identifiers{UserID: "u1", AnonymousID: "a1", Email: "e@x.com"}, "user_id:u1"},
		{"anonymous next", Identifiers{AnonymousID: "a1", Email: "e@x.com"}, "anonymous_id:a1"},
		{"email hashed", Identifiers{Email: "e@x.com"}, "email:" + hashValue("e@x.com")},
		{"phone hashed", Identifiers{Phone: "+8490"}, "phone:" + hashValue("+8490")},
		{"external_id", Identifiers{ExternalID: "x1"}, "external_id:x1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := Envelope{EventID: "e1", Identifiers: c.in}
			if got := env.IdentifierKey(); got != c.want {
				t.Fatalf("IdentifierKey = %q, want %q", got, c.want)
			}
		})
	}
}

func TestIdentifierKey_FallsBackToEventID(t *testing.T) {
	env := Envelope{EventID: "e1"}
	if got := env.IdentifierKey(); got != "event_id:e1" {
		t.Fatalf("got %q", got)
	}
}

func TestPartitionKey_TenantScoped(t *testing.T) {
	env := Envelope{EventID: "e1", TenantID: testTenant, Identifiers: Identifiers{UserID: "u1"}}
	want := testTenant.String() + "|user_id:u1"
	if got := env.PartitionKey(); got != want {
		t.Fatalf("PartitionKey = %q, want %q", got, want)
	}
}

func TestPayloadHash_ExcludesReceivedAt(t *testing.T) {
	in := IncomingEvent{EventID: "e1", UserID: "u1", EventName: "x"}
	a, _ := Normalize(in, testTenant, testSource, TypeTrack, fixedNow)
	b, _ := Normalize(in, testTenant, testSource, TypeTrack, fixedNow.Add(time.Hour))
	ha, _ := a.PayloadHash()
	hb, _ := b.PayloadHash()
	if ha != hb {
		t.Fatal("payload hash must be stable across received_at differences")
	}
}

func TestPayloadHash_ChangesWithContent(t *testing.T) {
	a, _ := Normalize(IncomingEvent{EventID: "e1", UserID: "u1", EventName: "x"}, testTenant, testSource, TypeTrack, fixedNow)
	b, _ := Normalize(IncomingEvent{EventID: "e1", UserID: "u1", EventName: "y"}, testTenant, testSource, TypeTrack, fixedNow)
	ha, _ := a.PayloadHash()
	hb, _ := b.PayloadHash()
	if ha == hb {
		t.Fatal("payload hash must change with content")
	}
}

func asValidation(err error, target **ValidationError) bool {
	ve, ok := err.(*ValidationError)
	if ok {
		*target = ve
	}
	return ok
}
