package events

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EnqueueResult is the outcome of an outbox insert.
type EnqueueResult int

const (
	// Inserted means the event was newly enqueued.
	Inserted EnqueueResult = iota
	// Duplicate means the same (tenant, event_id) with identical content already exists.
	Duplicate
	// Conflict means (tenant, event_id) exists with a different payload hash.
	Conflict
)

// Repository persists events to the transactional outbox.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository constructs a Repository.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Enqueue inserts the event idempotently. On a (tenant_id, event_id) collision it
// compares payload hashes: identical → Duplicate (idempotent success), different
// → Conflict.
func (r *Repository) Enqueue(ctx context.Context, e Envelope, payloadJSON []byte, payloadHash string) (EnqueueResult, error) {
	var id uuid.UUID
	err := r.pool.QueryRow(ctx, `
		INSERT INTO event_outbox
			(id, tenant_id, source_id, event_id, type, event_name, identifier_key,
			 partition_key, payload_json, payload_hash, timestamp, received_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (tenant_id, event_id) DO NOTHING
		RETURNING id`,
		uuid.New(), e.TenantID, e.SourceID, e.EventID, e.Type, nullString(e.EventName),
		e.IdentifierKey(), e.PartitionKey(), payloadJSON, payloadHash, e.Timestamp, e.ReceivedAt, StatusPending,
	).Scan(&id)
	if err == nil {
		return Inserted, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("insert event_outbox: %w", err)
	}

	// Conflict on (tenant_id, event_id): compare content hashes.
	var existingHash string
	if err := r.pool.QueryRow(ctx,
		`SELECT payload_hash FROM event_outbox WHERE tenant_id = $1 AND event_id = $2`,
		e.TenantID, e.EventID).Scan(&existingHash); err != nil {
		return 0, fmt.Errorf("lookup existing event: %w", err)
	}
	if existingHash == payloadHash {
		return Duplicate, nil
	}
	return Conflict, nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
