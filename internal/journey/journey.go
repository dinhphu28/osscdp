// Package journey implements customer journey orchestration: a versioned, ordered
// flow (Phase 1: wait -> send) a customer profile enters via segment membership and
// advances through, with per-profile state. It is built on the proven segment
// claim-fenced deadline-sweep substrate and delegates sends to activation.
// See docs/cdp/19-journey-orchestration.md.
package journey

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Journey status values.
const (
	StatusDraft    = "draft"
	StatusActive   = "active"
	StatusArchived = "archived"
)

// Enrollment status values.
const (
	EnrollmentActive    = "active"
	EnrollmentCompleted = "completed"
	EnrollmentExited    = "exited"
)

// Step types (Phase 1). condition/split land in Phase 3.
const (
	StepWait = "wait"
	StepSend = "send"
)

// Step is one node in a linear journey definition. Exactly one shape is used:
// a wait (Duration) or a send (DestinationID).
type Step struct {
	Type          string    `json:"type"`
	Duration      string    `json:"duration,omitempty"`       // wait: "7d","24h","30m" -> segment.ParseWindow
	DestinationID uuid.UUID `json:"destination_id,omitempty"` // send: an activation destination
}

// Definition is the ordered step array stored in journey_version.definition_json.
type Definition struct {
	Steps []Step `json:"steps"`
}

// Journey is a journey definition head (identity + active version pointer).
type Journey struct {
	ID             uuid.UUID `json:"id"`
	TenantID       uuid.UUID `json:"tenant_id"`
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	Status         string    `json:"status"`
	EntrySegmentID uuid.UUID `json:"entry_segment_id"`
	// ExitOnSegmentLeave terminates a profile's active enrollment when it leaves the
	// entry segment (Phase 2). Default false = run to completion.
	ExitOnSegmentLeave bool       `json:"exit_on_segment_leave"`
	CurrentVersion     int        `json:"current_version"`
	Definition         Definition `json:"definition"` // populated on Get (the current version)
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

// Enrollment is a claimed per-profile state row the runner must advance.
type Enrollment struct {
	TenantID          uuid.UUID
	JourneyID         uuid.UUID
	CustomerProfileID uuid.UUID
	EnrollmentSeq     int
	JourneyVersion    int
	CurrentStepIndex  int
	StepSeq           int64
	DueAt             time.Time
}

// ParkedEnrollment is one dead-lettered enrollment surfaced to an operator.
type ParkedEnrollment struct {
	JourneyID         uuid.UUID `json:"journey_id"`
	CustomerProfileID uuid.UUID `json:"customer_profile_id"`
	CurrentStepIndex  int       `json:"current_step_index"`
	LastError         string    `json:"last_error"`
	Attempts          int       `json:"attempts"`
	DueAt             time.Time `json:"due_at"`
	ParkedAt          time.Time `json:"parked_at"`
}

// BuildPayload builds the outbound payload for a journey send step. It mirrors the
// activation membership payload's customer view so the same destinations consume it.
func BuildPayload(tenantID, journeyID uuid.UUID, stepIndex int, canonicalUserID string, traits, computed map[string]any) ([]byte, error) {
	return json.Marshal(map[string]any{
		"type":       "journey_step",
		"tenant_id":  tenantID,
		"journey_id": journeyID,
		"step_index": stepIndex,
		"customer": map[string]any{
			"id":                  canonicalUserID,
			"traits":              traits,
			"computed_attributes": computed,
		},
	})
}
