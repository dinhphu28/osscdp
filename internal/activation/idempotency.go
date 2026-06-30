package activation

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/google/uuid"
)

// IdempotencyKey composes the doc-07 activation idempotency key:
// tenant + destination + subscription + customer_profile + source_event + change.
// The same activation must never be sent twice to the same destination; retries
// reuse the same key.
func IdempotencyKey(tenantID, destinationID, subscriptionID, profileID uuid.UUID, sourceEventID, change string) string {
	parts := []string{
		tenantID.String(), destinationID.String(), subscriptionID.String(),
		profileID.String(), sourceEventID, change,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}
