package activation

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/profile"
)

type customerView struct {
	ID                 string         `json:"id"`
	Traits             map[string]any `json:"traits"`
	ComputedAttributes map[string]any `json:"computed_attributes"`
}

type activationPayload struct {
	Type       string       `json:"type"`
	TenantID   uuid.UUID    `json:"tenant_id"`
	SegmentID  uuid.UUID    `json:"segment_id"`
	Customer   customerView `json:"customer"`
	Change     string       `json:"change"`
	OccurredAt time.Time    `json:"occurred_at"`
}

// BuildPayload builds the doc-07 activation payload for a membership change.
func BuildPayload(tenantID, segmentID uuid.UUID, change string, occurredAt time.Time, prof profile.Profile) ([]byte, error) {
	p := activationPayload{
		Type:      "segment_membership_changed",
		TenantID:  tenantID,
		SegmentID: segmentID,
		Customer: customerView{
			ID:                 prof.CanonicalUserID,
			Traits:             prof.Traits,
			ComputedAttributes: prof.ComputedAttributes,
		},
		Change:     change,
		OccurredAt: occurredAt.UTC(),
	}
	return json.Marshal(p)
}
