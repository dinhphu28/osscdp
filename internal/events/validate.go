package events

import "fmt"

// Size limits (docs/cdp/03-event-model-and-ingress.md). Kept as consts here;
// they can move to per-source config later.
const (
	MaxEventBytes = 64 * 1024       // 64 KB per event
	MaxBatchSize  = 500             // events per batch
	MaxBatchBytes = 5 * 1024 * 1024 // 5 MB per batch payload
)

// ValidationError is a client-facing, field-scoped validation failure.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// Validate checks the normalized envelope against per-type rules. Structural and
// size checks that need the raw body happen in the handler/service before this.
func Validate(e Envelope) error {
	switch e.Type {
	case TypeTrack:
		if e.EventName == "" {
			return &ValidationError{Field: "event_name", Message: "required for track events"}
		}
		if !hasIdentifier(e) {
			return &ValidationError{Field: "identifiers", Message: "at least one identifier is required"}
		}
	case TypeIdentify:
		if e.Identifiers.UserID == "" && e.Identifiers.AnonymousID == "" {
			return &ValidationError{Field: "identifiers", Message: "identify requires user_id or anonymous_id"}
		}
	case TypeAlias:
		if e.PreviousID == "" {
			return &ValidationError{Field: "previous_id", Message: "required for alias events"}
		}
		if e.Identifiers.UserID == "" {
			return &ValidationError{Field: "user_id", Message: "required for alias events"}
		}
	default:
		return &ValidationError{Field: "type", Message: fmt.Sprintf("unknown type %q", e.Type)}
	}
	return nil
}

func hasIdentifier(e Envelope) bool {
	i := e.Identifiers
	return i.UserID != "" || i.AnonymousID != "" || i.Email != "" ||
		i.Phone != "" || i.ExternalID != "" || i.DeviceID != ""
}
