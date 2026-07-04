package governance

import "testing"

func TestMaskIdentifierValues(t *testing.T) {
	out := maskIdentifierValues(map[string][]string{
		"email":   {"user@example.com"},
		"phone":   {"+84901234567"},
		"user_id": {"user-123"},
	})
	if got := out["email"][0]; got != "u***@example.com" {
		t.Fatalf("email mask = %q", got)
	}
	if got := out["phone"][0]; got != "+8490****567" {
		t.Fatalf("phone mask = %q", got)
	}
	if got := out["user_id"][0]; got != "u***" {
		t.Fatalf("user_id mask = %q", got)
	}
	if maskIdentifierValues(nil) != nil {
		t.Fatal("nil in must give nil out")
	}
}
