// Package activation sends segment membership changes to downstream destinations
// (webhook, Kafka) with retry, delivery logging, and idempotency.
// See docs/cdp/07-activation-outgress.md.
package activation

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Destination types.
const (
	TypeWebhook = "webhook"
	TypeKafka   = "kafka"
)

// Destination / subscription status.
const (
	StatusActive   = "active"
	StatusDisabled = "disabled"
)

// Trigger types.
const (
	TriggerSegmentMembership = "segment_membership"
)

// Activation task statuses.
const (
	TaskPending         = "pending"
	TaskSending         = "sending"
	TaskSucceeded       = "succeeded"
	TaskFailedRetryable = "failed_retryable"
	TaskFailedPermanent = "failed_permanent"
	TaskSkipped         = "skipped"
)

// Delivery statuses.
const (
	DeliverySuccess = "success"
	DeliveryFailed  = "failed"
)

// Destination is a downstream system to send to.
type Destination struct {
	ID        uuid.UUID       `json:"id"`
	TenantID  uuid.UUID       `json:"tenant_id"`
	Type      string          `json:"type"`
	Name      string          `json:"name"`
	Status    string          `json:"status"`
	Config    json.RawMessage `json:"config"`
	SecretRef *string         `json:"-"` // ciphertext; never serialized to clients
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// Subscription connects a trigger (segment) to a destination.
type Subscription struct {
	ID            uuid.UUID  `json:"id"`
	TenantID      uuid.UUID  `json:"tenant_id"`
	DestinationID uuid.UUID  `json:"destination_id"`
	TriggerType   string     `json:"trigger_type"`
	SegmentID     *uuid.UUID `json:"segment_id,omitempty"`
	EventName     *string    `json:"event_name,omitempty"`
	Status        string     `json:"status"`
}

// SegmentDestination is one destination wired to a segment via a subscription,
// including disabled subscriptions, for the admin "list destinations by segment" view.
type SegmentDestination struct {
	SubscriptionID     uuid.UUID `json:"subscription_id"`
	SubscriptionStatus string    `json:"subscription_status"`
	DestinationID      uuid.UUID `json:"destination_id"`
	Name               string    `json:"name"`
	Type               string    `json:"type"`
	DestinationStatus  string    `json:"destination_status"`
}

// Task is a unit of outbound work.
type Task struct {
	ID                uuid.UUID
	TenantID          uuid.UUID
	DestinationID     uuid.UUID
	SubscriptionID    uuid.UUID
	CustomerProfileID uuid.UUID
	SourceEventID     string
	IdempotencyKey    string
	Payload           json.RawMessage
	Status            string
	AttemptCount      int
}

// WebhookConfig is the parsed config for a webhook destination.
type WebhookConfig struct {
	URL        string            `json:"url"`
	Method     string            `json:"method"`
	Headers    map[string]string `json:"headers"`
	TimeoutMS  int               `json:"timeout_ms"`
	MaxRetries int               `json:"max_retries"`
}

// KafkaConfig is the parsed config for a kafka destination.
type KafkaConfig struct {
	Topic string `json:"topic"`
}

// ConsentTarget is the consent channel + purpose an activation maps to,
// parsed from a destination's config (with defaults).
type ConsentTarget struct {
	Channel string `json:"channel"`
	Purpose string `json:"purpose"`
}

// ConsentTargetFor returns the consent channel/purpose for a destination,
// defaulting channel to the destination type and purpose to "marketing".
func ConsentTargetFor(d Destination) ConsentTarget {
	var ct ConsentTarget
	_ = json.Unmarshal(d.Config, &ct)
	if ct.Channel == "" {
		ct.Channel = d.Type
	}
	if ct.Purpose == "" {
		ct.Purpose = "marketing"
	}
	return ct
}
