package crypto

import (
	"crypto/rand"
	"encoding/base64"
	"testing"
)

func testKey(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func TestCipher_RoundTrip(t *testing.T) {
	c, err := New(testKey(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const secret = "super-secret-token"
	enc, err := c.Encrypt(secret)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if enc == secret {
		t.Fatal("ciphertext must not equal plaintext")
	}
	got, err := c.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if got != secret {
		t.Fatalf("got %q, want %q", got, secret)
	}
}

func TestCipher_TamperDetected(t *testing.T) {
	c, _ := New(testKey(t))
	enc, _ := c.Encrypt("hello")
	// Flip a character in the base64.
	b := []byte(enc)
	if b[len(b)-2] == 'A' {
		b[len(b)-2] = 'B'
	} else {
		b[len(b)-2] = 'A'
	}
	if _, err := c.Decrypt(string(b)); err == nil {
		t.Fatal("expected error decrypting tampered ciphertext")
	}
}

func TestCipher_WrongKeyFails(t *testing.T) {
	c1, _ := New(testKey(t))
	c2, _ := New(testKey(t))
	enc, _ := c1.Encrypt("x")
	if _, err := c2.Decrypt(enc); err == nil {
		t.Fatal("decrypting with the wrong key should fail")
	}
}

func TestNew_BadKey(t *testing.T) {
	if _, err := New("not-base64!!!"); err == nil {
		t.Fatal("expected error for invalid base64 key")
	}
	if _, err := New(base64.StdEncoding.EncodeToString([]byte("short"))); err == nil {
		t.Fatal("expected error for non-32-byte key")
	}
}
