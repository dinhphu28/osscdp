// Package audit records configuration changes and sensitive operations.
package audit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Actor types.
const (
	ActorAdmin  = "admin"
	ActorSystem = "system"
)

// Entry describes a single audit record. tenant_id may be empty for
// tenant-creation events that precede the tenant's existence in context.
type Entry struct {
	TenantID     *uuid.UUID
	ActorID      string
	ActorType    string
	Action       string
	ResourceType string
	ResourceID   string
	Before       any
	After        any
}

// Recorder writes audit entries to the audit_log table.
type Recorder struct {
	pool *pgxpool.Pool
}

// NewRecorder constructs a Recorder.
func NewRecorder(pool *pgxpool.Pool) *Recorder {
	return &Recorder{pool: pool}
}

// Record inserts an audit entry. It uses the provided executor-bound pool so it
// can participate in the caller's request flow.
func (r *Recorder) Record(ctx context.Context, e Entry) error {
	beforeJSON, err := marshalNullable(e.Before)
	if err != nil {
		return fmt.Errorf("marshal before: %w", err)
	}
	afterJSON, err := marshalNullable(e.After)
	if err != nil {
		return fmt.Errorf("marshal after: %w", err)
	}

	_, err = r.pool.Exec(ctx, `
		INSERT INTO audit_log (id, tenant_id, actor_id, actor_type, action, resource_type, resource_id, before_json, after_json)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		uuid.New(), e.TenantID, nullString(e.ActorID), e.ActorType, e.Action, e.ResourceType, nullString(e.ResourceID), beforeJSON, afterJSON,
	)
	if err != nil {
		return fmt.Errorf("insert audit_log: %w", err)
	}
	return nil
}

func marshalNullable(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
