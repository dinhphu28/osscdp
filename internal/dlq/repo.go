package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a DLQ event does not exist for the tenant.
var ErrNotFound = errors.New("dlq event not found")

// Event is the read model for a dead-lettered event.
type Event struct {
	ID              uuid.UUID       `json:"id"`
	TenantID        *uuid.UUID      `json:"tenant_id,omitempty"`
	EventID         string          `json:"event_id,omitempty"`
	Component       string          `json:"component"`
	ErrorCode       string          `json:"error_code"`
	ErrorMessage    string          `json:"error_message"`
	OriginalPayload json.RawMessage `json:"original_payload"`
	RetryCount      int             `json:"retry_count"`
	Status          string          `json:"status"`
	FailedAt        time.Time       `json:"failed_at"`
}

// Repo reads and updates dlq_event rows.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

const dlqCols = `id, tenant_id, event_id, component, error_code, error_message,
	original_payload, retry_count, status, failed_at`

func scanEvent(row pgx.Row) (Event, error) {
	var e Event
	var eventID *string
	err := row.Scan(&e.ID, &e.TenantID, &eventID, &e.Component, &e.ErrorCode, &e.ErrorMessage,
		&e.OriginalPayload, &e.RetryCount, &e.Status, &e.FailedAt)
	if eventID != nil {
		e.EventID = *eventID
	}
	return e, err
}

// List returns DLQ events for a tenant, optionally filtered by status.
func (r *Repo) List(ctx context.Context, tenantID uuid.UUID, status string, limit int) ([]Event, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var (
		rows pgx.Rows
		err  error
	)
	if status != "" {
		rows, err = r.pool.Query(ctx, `SELECT `+dlqCols+` FROM dlq_event WHERE tenant_id=$1 AND status=$2 ORDER BY failed_at DESC LIMIT $3`, tenantID, status, limit)
	} else {
		rows, err = r.pool.Query(ctx, `SELECT `+dlqCols+` FROM dlq_event WHERE tenant_id=$1 ORDER BY failed_at DESC LIMIT $2`, tenantID, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list dlq: %w", err)
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Get loads one DLQ event by id, tenant-scoped.
func (r *Repo) Get(ctx context.Context, tenantID, id uuid.UUID) (Event, error) {
	e, err := scanEvent(r.pool.QueryRow(ctx,
		`SELECT `+dlqCols+` FROM dlq_event WHERE tenant_id=$1 AND id=$2`, tenantID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	if err != nil {
		return Event{}, fmt.Errorf("get dlq: %w", err)
	}
	return e, nil
}

// MarkStatus sets a DLQ event's status. Returns false if not found.
func (r *Repo) MarkStatus(ctx context.Context, tenantID, id uuid.UUID, status string) (bool, error) {
	ct, err := r.pool.Exec(ctx,
		`UPDATE dlq_event SET status=$3, updated_at=now() WHERE tenant_id=$1 AND id=$2`,
		tenantID, id, status)
	if err != nil {
		return false, fmt.Errorf("mark dlq status: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}
