package events

import "testing"

func validEnvelope(typ string) Envelope {
	e := Envelope{Type: typ, EventID: "e1"}
	switch typ {
	case TypeTrack:
		e.EventName = "product_viewed"
		e.Identifiers.UserID = "u1"
	case TypeIdentify:
		e.Identifiers.UserID = "u1"
	case TypeAlias:
		e.PreviousID = "anon_1"
		e.Identifiers.UserID = "u1"
	}
	return e
}

func TestValidate_HappyPaths(t *testing.T) {
	for _, typ := range []string{TypeTrack, TypeIdentify, TypeAlias} {
		if err := Validate(validEnvelope(typ)); err != nil {
			t.Errorf("%s: unexpected error %v", typ, err)
		}
	}
}

func TestValidate_TrackRequiresEventName(t *testing.T) {
	e := validEnvelope(TypeTrack)
	e.EventName = ""
	if err := Validate(e); err == nil {
		t.Fatal("expected error for missing event_name")
	}
}

func TestValidate_TrackRequiresIdentifier(t *testing.T) {
	e := validEnvelope(TypeTrack)
	e.Identifiers = Identifiers{}
	if err := Validate(e); err == nil {
		t.Fatal("expected error for missing identifier")
	}
}

func TestValidate_IdentifyRequiresUserOrAnon(t *testing.T) {
	e := validEnvelope(TypeIdentify)
	e.Identifiers = Identifiers{}
	if err := Validate(e); err == nil {
		t.Fatal("expected error for identify without user_id/anonymous_id")
	}
}

func TestValidate_AliasRequiresBothIDs(t *testing.T) {
	e := validEnvelope(TypeAlias)
	e.PreviousID = ""
	if err := Validate(e); err == nil {
		t.Fatal("expected error for alias without previous_id")
	}
	e = validEnvelope(TypeAlias)
	e.Identifiers.UserID = ""
	if err := Validate(e); err == nil {
		t.Fatal("expected error for alias without user_id")
	}
}

func TestValidate_UnknownType(t *testing.T) {
	if err := Validate(Envelope{Type: "bogus", EventID: "e1"}); err == nil {
		t.Fatal("expected error for unknown type")
	}
}
