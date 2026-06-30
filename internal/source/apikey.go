package source

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

// keyPrefix marks CDP-issued source API keys.
const keyPrefix = "cdp_"

// GenerateAPIKey returns a new high-entropy API key plaintext and its SHA-256
// hash (hex). The plaintext is shown to the caller exactly once; only the hash
// is persisted. SHA-256 (not a password KDF) is appropriate here because the
// token carries 256 bits of entropy.
func GenerateAPIKey() (plaintext, hash string, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("read random: %w", err)
	}
	plaintext = keyPrefix + base64.RawURLEncoding.EncodeToString(buf)
	return plaintext, HashAPIKey(plaintext), nil
}

// HashAPIKey returns the hex-encoded SHA-256 of the key plaintext.
func HashAPIKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// VerifyAPIKey reports whether plaintext hashes to the stored hash, using a
// constant-time comparison.
func VerifyAPIKey(plaintext, storedHash string) bool {
	got := HashAPIKey(plaintext)
	return subtle.ConstantTimeCompare([]byte(got), []byte(storedHash)) == 1
}

// LooksLikeAPIKey reports whether s has the CDP key prefix. Used to reject
// obviously malformed credentials before hitting the database.
func LooksLikeAPIKey(s string) bool {
	return strings.HasPrefix(s, keyPrefix)
}
