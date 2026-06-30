// Package profile builds the unified customer profile from identity_resolved
// events: explicit trait merge policy + computed attributes, idempotent by
// event_id. See docs/cdp/05-customer-profile-unification.md.
package profile

import (
	"time"

	"github.com/google/uuid"
)

// Trait keys (latest non-empty value wins).
const (
	TraitEmail   = "email"
	TraitPhone   = "phone"
	TraitName    = "name"
	TraitCountry = "country"
)

// Computed attribute keys.
const (
	AttrTotalEvents       = "total_events"
	AttrLastEventName     = "last_event_name"
	AttrLastSourceID      = "last_source_id"
	AttrLastPageURL       = "last_page_url"
	AttrLastProductViewed = "last_product_viewed"
	AttrTotalOrders       = "total_orders"
	AttrLastOrderAt       = "last_order_at"
)

// Event names with special computed-attribute handling.
const (
	EventProductViewed  = "product_viewed"
	EventOrderCompleted = "order_completed"
)

// Profile is the unified customer profile.
type Profile struct {
	ID                 uuid.UUID      `json:"id"`
	TenantID           uuid.UUID      `json:"tenant_id"`
	CanonicalUserID    string         `json:"canonical_user_id"`
	IdentityClusterID  uuid.UUID      `json:"identity_cluster_id"`
	Traits             map[string]any `json:"traits"`
	ComputedAttributes map[string]any `json:"computed_attributes"`
	FirstSeenAt        *time.Time     `json:"first_seen_at,omitempty"`
	LastSeenAt         *time.Time     `json:"last_seen_at,omitempty"`
	Version            int64          `json:"version"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}
