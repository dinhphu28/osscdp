// Package dlq records events that exhaust retries during processing.
package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status values for a DLQ row.
const (
	StatusOpen      = "open"
	StatusRetried   = "retried"
	StatusDiscarded = "discarded"
)

// Entry describes a dead-lettered event with enough context to debug and retry.
type Entry struct {
	TenantID     *uuid.UUID
	SourceID     *uuid.UUID
	EventID      string
	Component    string
	ErrorCode    string
	ErrorMessage string
	Payload      []byte // original message bytes
	RetryCount   int
	FailedAt     time.Time
}

// Recorder writes DLQ entries to dlq_event.
type Recorder struct {
	pool *pgxpool.Pool
}

// NewRecorder constructs a Recorder.
func NewRecorder(pool *pgxpool.Pool) *Recorder {
	return &Recorder{pool: pool}
}

// Record inserts a dead-letter row.
func (r *Recorder) Record(ctx context.Context, e Entry) error {
	payload := toJSONB(e.Payload)
	_, err := r.pool.Exec(ctx, `
		INSERT INTO dlq_event
			(id, tenant_id, source_id, event_id, component, error_code, error_message,
			 original_payload, retry_count, status, failed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		uuid.New(), e.TenantID, e.SourceID, nullString(e.EventID), e.Component,
		e.ErrorCode, e.ErrorMessage, payload, e.RetryCount, StatusOpen, e.FailedAt,
	)
	if err != nil {
		return fmt.Errorf("insert dlq_event: %w", err)
	}
	return nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// toJSONB ensures the payload is valid JSON for the JSONB column. A poison
// message may not be valid JSON, so non-JSON bytes are stored as a JSON string.
func toJSONB(payload []byte) []byte {
	if len(payload) == 0 {
		return []byte(`null`)
	}
	if json.Valid(payload) {
		return payload
	}
	wrapped, err := json.Marshal(string(payload))
	if err != nil {
		return []byte(`null`)
	}
	return wrapped
}
