package identity

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/google/uuid"
)

// ValueHash computes a tenant-scoped, deterministic hash for an identifier
// value: sha256(tenant_id|namespace|value). Including tenant_id ensures the same
// value in different tenants never collides. HMAC with a per-tenant secret is a
// Phase 9 hardening (docs/cdp/04-identity-resolution.md).
func ValueHash(tenantID uuid.UUID, namespace, value string) string {
	h := sha256.New()
	h.Write([]byte(tenantID.String()))
	h.Write([]byte("|"))
	h.Write([]byte(namespace))
	h.Write([]byte("|"))
	h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil))
}
