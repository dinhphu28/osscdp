// Package events implements the CDP ingress: validate, normalize into the
// canonical event envelope, and durably + idempotently enqueue to the outbox.
//
// No identity resolution, profile update, segmentation, or activation happens in
// the request path (docs/cdp/03-event-model-and-ingress.md).
package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event types.
const (
	TypeTrack    = "track"
	TypeIdentify = "identify"
	TypeAlias    = "alias"
)

// Outbox row status.
const (
	StatusPending = "pending"
)

// Identifiers carries the customer identifiers an event may reference. At least
// one is required for track/identify (see validate.go).
type Identifiers struct {
	UserID      string `json:"user_id,omitempty"`
	AnonymousID string `json:"anonymous_id,omitempty"`
	Email       string `json:"email,omitempty"`
	Phone       string `json:"phone,omitempty"`
	ExternalID  string `json:"external_id,omitempty"`
	DeviceID    string `json:"device_id,omitempty"`
}

// Envelope is the canonical normalized CDP event. payload_json persisted to the
// outbox is the JSON encoding of this struct.
type Envelope struct {
	EventID     string          `json:"event_id"`
	TenantID    uuid.UUID       `json:"tenant_id"`
	SourceID    uuid.UUID       `json:"source_id"`
	Type        string          `json:"type"`
	EventName   string          `json:"event_name,omitempty"`
	Identifiers Identifiers     `json:"identifiers"`
	PreviousID  string          `json:"previous_id,omitempty"` // alias only
	Timestamp   time.Time       `json:"timestamp"`
	ReceivedAt  time.Time       `json:"received_at"`
	Context     json.RawMessage `json:"context,omitempty"`
	Properties  json.RawMessage `json:"properties,omitempty"`
	Traits      json.RawMessage `json:"traits,omitempty"` // identify only

	// tsProvided records whether the client supplied timestamp. When false,
	// Timestamp is the server default (received_at) and is excluded from the
	// idempotency hash so retries without a timestamp don't falsely conflict.
	tsProvided bool
}

// IncomingEvent is the raw, untrusted request body for a single event. tenant_id
// and source_id are intentionally absent — they are resolved from the API key.
type IncomingEvent struct {
	EventID     string          `json:"event_id"`
	Type        string          `json:"type"`
	EventName   string          `json:"event_name"`
	UserID      string          `json:"user_id"`
	AnonymousID string          `json:"anonymous_id"`
	Email       string          `json:"email"`
	Phone       string          `json:"phone"`
	ExternalID  string          `json:"external_id"`
	DeviceID    string          `json:"device_id"`
	PreviousID  string          `json:"previous_id"`
	Timestamp   string          `json:"timestamp"`
	Context     json.RawMessage `json:"context"`
	Properties  json.RawMessage `json:"properties"`
	Traits      json.RawMessage `json:"traits"`
}

// BatchRequest wraps a batch of events.
type BatchRequest struct {
	Events []IncomingEvent `json:"events"`
}
