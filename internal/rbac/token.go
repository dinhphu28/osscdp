package rbac

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

const tokenPrefix = "cdpadm_"

// GenerateToken returns a new admin token plaintext and its SHA-256 hash.
func GenerateToken() (plaintext, hash string, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("read random: %w", err)
	}
	plaintext = tokenPrefix + base64.RawURLEncoding.EncodeToString(buf)
	return plaintext, HashToken(plaintext), nil
}

// HashToken returns the hex SHA-256 of an admin token.
func HashToken(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// LooksLikeToken reports whether s has the admin-token prefix.
func LooksLikeToken(s string) bool { return strings.HasPrefix(s, tokenPrefix) }
