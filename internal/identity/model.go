// Package identity implements deterministic identity resolution: it connects
// event identifiers into tenant-scoped identity clusters with a stable
// canonical_user_id. It does not build customer profiles (that is Phase 6).
package identity

import "github.com/dinhphu28/osscdp/internal/events"

// Identifier namespaces.
const (
	NSUserID      = "user_id"
	NSAnonymousID = "anonymous_id"
	NSEmail       = "email"
	NSPhone       = "phone"
	NSExternalID  = "external_id"
	NSDeviceID    = "device_id"
)

// Cluster status values.
const (
	ClusterActive = "active"
	ClusterMerged = "merged"
)

// Identifier is a single (namespace, value) pair extracted from an event.
type Identifier struct {
	Namespace string
	Value     string
}

// ExtractIdentifiers returns the deterministic identifiers present on an event.
// For alias events, previous_id is treated as an anonymous_id (the dominant
// case) so it links with the user_id — see docs/cdp/04-identity-resolution.md.
func ExtractIdentifiers(env events.Envelope) []Identifier {
	var ids []Identifier
	add := func(ns, v string) {
		if v != "" {
			ids = append(ids, Identifier{Namespace: ns, Value: v})
		}
	}
	i := env.Identifiers
	add(NSUserID, i.UserID)
	add(NSAnonymousID, i.AnonymousID)
	add(NSEmail, i.Email)
	add(NSPhone, i.Phone)
	add(NSExternalID, i.ExternalID)
	add(NSDeviceID, i.DeviceID)
	if env.Type == events.TypeAlias {
		add(NSAnonymousID, env.PreviousID)
	}
	return dedupe(ids)
}

func dedupe(ids []Identifier) []Identifier {
	seen := make(map[Identifier]struct{}, len(ids))
	out := ids[:0]
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
