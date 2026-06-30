package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Normalize builds a canonical Envelope from an untrusted request body plus the
// authenticated tenant/source. tenant_id and source_id come only from the API
// key, never the body. The server always sets received_at; event_id is generated
// when absent. forcedType, when non-empty, overrides the body's type (the
// per-endpoint handlers set it for identify/alias/track).
func Normalize(in IncomingEvent, tenantID, sourceID uuid.UUID, forcedType string, now time.Time) (Envelope, error) {
	typ := forcedType
	if typ == "" {
		typ = strings.TrimSpace(in.Type)
	}
	if typ == "" {
		typ = TypeTrack
	}

	received := now.UTC()

	ts := received
	tsProvided := false
	if s := strings.TrimSpace(in.Timestamp); s != "" {
		parsed, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return Envelope{}, &ValidationError{Field: "timestamp", Message: "must be RFC3339"}
		}
		ts = parsed.UTC()
		tsProvided = true
	}

	eventID := strings.TrimSpace(in.EventID)
	if eventID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return Envelope{}, err
		}
		eventID = "evt_" + id.String()
	}

	env := Envelope{
		EventID:   eventID,
		TenantID:  tenantID,
		SourceID:  sourceID,
		Type:      typ,
		EventName: strings.TrimSpace(in.EventName),
		Identifiers: Identifiers{
			UserID:      strings.TrimSpace(in.UserID),
			AnonymousID: strings.TrimSpace(in.AnonymousID),
			Email:       normalizeEmail(in.Email),
			Phone:       strings.TrimSpace(in.Phone),
			ExternalID:  strings.TrimSpace(in.ExternalID),
			DeviceID:    strings.TrimSpace(in.DeviceID),
		},
		PreviousID: strings.TrimSpace(in.PreviousID),
		Timestamp:  ts,
		tsProvided: tsProvided,
		ReceivedAt: received,
		Context:    in.Context,
		Properties: in.Properties,
		Traits:     in.Traits,
	}
	return env, nil
}

func normalizeEmail(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// IdentifierKey returns the stable identifier used for partitioning and lookup,
// by the doc-03 priority: user_id → anonymous_id → email_hash → phone_hash →
// external_id → event_id. email/phone are hashed so no raw PII lands in the key.
func (e Envelope) IdentifierKey() string {
	switch {
	case e.Identifiers.UserID != "":
		return "user_id:" + e.Identifiers.UserID
	case e.Identifiers.AnonymousID != "":
		return "anonymous_id:" + e.Identifiers.AnonymousID
	case e.Identifiers.Email != "":
		return "email:" + hashValue(e.Identifiers.Email)
	case e.Identifiers.Phone != "":
		return "phone:" + hashValue(e.Identifiers.Phone)
	case e.Identifiers.ExternalID != "":
		return "external_id:" + e.Identifiers.ExternalID
	default:
		return "event_id:" + e.EventID
	}
}

// PartitionKey preserves per-customer ordering within a tenant.
func (e Envelope) PartitionKey() string {
	return e.TenantID.String() + "|" + e.IdentifierKey()
}

// PayloadJSON is the canonical JSON persisted to the outbox.
func (e Envelope) PayloadJSON() ([]byte, error) {
	return json.Marshal(e)
}

// PayloadHash is a stable content hash used for idempotency conflict detection.
// It deliberately excludes server-set fields (received_at, generated event_id,
// source_id) so a legitimate retry with identical content hashes identically.
func (e Envelope) PayloadHash() (string, error) {
	stable := struct {
		Type        string          `json:"type"`
		EventName   string          `json:"event_name"`
		Identifiers Identifiers     `json:"identifiers"`
		PreviousID  string          `json:"previous_id"`
		Timestamp   time.Time       `json:"timestamp"`
		Context     json.RawMessage `json:"context"`
		Properties  json.RawMessage `json:"properties"`
		Traits      json.RawMessage `json:"traits"`
	}{
		Type:        e.Type,
		EventName:   e.EventName,
		Identifiers: e.Identifiers,
		PreviousID:  e.PreviousID,
		Timestamp:   clientTimestamp(e),
		Context:     e.Context,
		Properties:  e.Properties,
		Traits:      e.Traits,
	}
	b, err := json.Marshal(stable)
	if err != nil {
		return "", err
	}
	return hashValue(string(b)), nil
}

// clientTimestamp returns the client-supplied timestamp, or the zero time when
// the server defaulted it, so the hash reflects only client intent.
func clientTimestamp(e Envelope) time.Time {
	if e.tsProvided {
		return e.Timestamp
	}
	return time.Time{}
}

func hashValue(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
