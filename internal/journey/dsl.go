package journey

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/segment"
)

// ValidationError is a client-facing definition validation failure.
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

// Validate checks a journey definition against the Phase 1 grammar: a non-empty,
// ordered array of wait|send steps with at least one send. Unknown step types fail
// closed (condition/split/exit are rejected until their phase). Wait durations parse
// through segment.ParseWindow, reusing the segment window grammar verbatim.
func Validate(def Definition) error {
	if len(def.Steps) == 0 {
		return &ValidationError{"journey requires at least one step"}
	}
	hasSend := false
	for i, s := range def.Steps {
		switch s.Type {
		case StepWait:
			if _, err := segment.ParseWindow(s.Duration); err != nil {
				return &ValidationError{fmt.Sprintf("step %d wait: %s", i, err.Error())}
			}
			if s.DestinationID != uuid.Nil {
				return &ValidationError{fmt.Sprintf("step %d wait must not set destination_id", i)}
			}
		case StepSend:
			if s.DestinationID == uuid.Nil {
				return &ValidationError{fmt.Sprintf("step %d send requires destination_id", i)}
			}
			if s.Duration != "" {
				return &ValidationError{fmt.Sprintf("step %d send must not set duration", i)}
			}
			hasSend = true
		default:
			return &ValidationError{fmt.Sprintf("step %d unknown type %q (phase 1 supports wait|send)", i, s.Type)}
		}
	}
	if !hasSend {
		return &ValidationError{"journey requires at least one send step"}
	}
	return nil
}
