package source

import "testing"

func TestGenerateAPIKey_RoundTrips(t *testing.T) {
	plaintext, hash, err := GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	if !LooksLikeAPIKey(plaintext) {
		t.Fatalf("plaintext %q missing cdp_ prefix", plaintext)
	}
	if plaintext == hash {
		t.Fatal("hash must not equal plaintext")
	}
	if !VerifyAPIKey(plaintext, hash) {
		t.Fatal("VerifyAPIKey should accept the matching plaintext")
	}
}

func TestVerifyAPIKey_RejectsWrongKey(t *testing.T) {
	_, hash, err := GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	other, _, err := GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	if VerifyAPIKey(other, hash) {
		t.Fatal("VerifyAPIKey should reject a non-matching plaintext")
	}
}

func TestGenerateAPIKey_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		pt, _, err := GenerateAPIKey()
		if err != nil {
			t.Fatalf("GenerateAPIKey: %v", err)
		}
		if _, dup := seen[pt]; dup {
			t.Fatalf("duplicate key generated: %q", pt)
		}
		seen[pt] = struct{}{}
	}
}

func TestHashAPIKey_Deterministic(t *testing.T) {
	const k = "cdp_example"
	if HashAPIKey(k) != HashAPIKey(k) {
		t.Fatal("HashAPIKey must be deterministic")
	}
}
