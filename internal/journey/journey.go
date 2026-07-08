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

	"github.com/dinhphu28/osscdp/internal/segment"
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

// Step types.
const (
	StepWait      = "wait"
	StepSend      = "send"
	StepCondition = "condition" // Phase 3: branch on a segment.Rule
	StepSplit     = "split"     // Phase 3: deterministic weighted branch
)

// SplitBranch is one weighted arm of a split step. Next is the (forward) step index
// to jump to; Weight is its relative selection weight.
type SplitBranch struct {
	Weight int `json:"weight"`
	Next   int `json:"next"`
}

// Step is one node in a journey definition. Exactly one shape is used per Type:
//   - wait:      Duration
//   - send:      DestinationID
//   - condition: Condition (a segment.Rule) + IfTrue/IfFalse target indices
//   - split:     Branches (weighted forward targets)
//
// Branch targets (IfTrue/IfFalse/Branches[].Next) are step indices that must be
// FORWARD (> this step's index) and <= len(steps); a target equal to len(steps) means
// "complete". Forward-only edges make every definition an acyclic DAG that always
// terminates. wait/send implicitly advance to index+1.
type Step struct {
	Type          string    `json:"type"`
	Duration      string    `json:"duration,omitempty"`       // wait
	DestinationID uuid.UUID `json:"destination_id,omitempty"` // send
	// Next is the explicit forward target for a wait/send step (0 = the default,
	// index+1). Lets a branch arm jump past the other arm so two arms stay disjoint;
	// a Next equal to len(steps) completes the enrollment.
	Next      int           `json:"next,omitempty"`
	Condition *segment.Rule `json:"condition,omitempty"` // condition
	IfTrue    int           `json:"if_true,omitempty"`   // condition: target when the rule matches
	IfFalse   int           `json:"if_false,omitempty"`  // condition: target when it does not
	Branches  []SplitBranch `json:"branches,omitempty"`  // split
}

// Definition is the ordered step array stored in journey_version.definition_json.
// Indices are stable within an (immutable) version, so index-based branch targets are
// safe; in-flight enrollments pin their version.
type Definition struct {
	Steps []Step `json:"steps"`
}

// Journey is a journey definition head (identity + active version pointer). A journey
// has exactly ONE entry mode: EntrySegmentID (enter on segment membership) XOR
// EntryEventName (enter when the profile emits that event).
type Journey struct {
	ID             uuid.UUID  `json:"id"`
	TenantID       uuid.UUID  `json:"tenant_id"`
	Name           string     `json:"name"`
	Description    string     `json:"description"`
	Status         string     `json:"status"`
	EntrySegmentID *uuid.UUID `json:"entry_segment_id,omitempty"`
	EntryEventName string     `json:"entry_event_name,omitempty"`
	// MaxEnrollments caps how many times a profile may enter this journey (1 =
	// once-only; N>1 enables re-entry after a terminal enrollment). See migration 00026.
	MaxEnrollments int `json:"max_enrollments"`
	// ExitOnSegmentLeave terminates a profile's active enrollment when it leaves the
	// entry segment (Phase 2). Default false = run to completion.
	ExitOnSegmentLeave bool       `json:"exit_on_segment_leave"`
	CurrentVersion     int        `json:"current_version"`
	Definition         Definition `json:"definition"` // populated on Get (the current version)
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

// JourneySeedJob is a durable, resumable population-backfill request the seed runner
// drains: it pages the entry segment's active members and enrolls each. ClaimedAt is
// the fence captured at claim (see segment.SeedJob); EntrySegmentID + JourneyVersion
// are snapshotted at enqueue so the drain is self-contained.
type JourneySeedJob struct {
	TenantID       uuid.UUID
	JourneyID      uuid.UUID
	EntrySegmentID uuid.UUID
	JourneyVersion int
	Reason         string
	DueAt          time.Time
	Cursor         uuid.UUID
	ClaimedAt      time.Time
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
