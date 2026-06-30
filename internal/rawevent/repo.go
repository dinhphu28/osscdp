// Package rawevent persists consumed events to the raw event store.
package rawevent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/events"
)

// StatusStored marks a successfully persisted raw event.
const StatusStored = "stored"

// Repo persists raw events.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

// Store persists the event idempotently on (tenant_id, event_id). rawPayload is
// the exact consumed message bytes; payload_hash is their sha256, kept
// deterministic and independent of struct round-tripping.
func (r *Repo) Store(ctx context.Context, env events.Envelope, rawPayload []byte) error {
	sum := sha256.Sum256(rawPayload)
	hash := hex.EncodeToString(sum[:])

	_, err := r.pool.Exec(ctx, `
		INSERT INTO raw_event
			(id, tenant_id, source_id, event_id, type, event_name, identifier_key,
			 payload_json, payload_hash, timestamp, received_at, processing_status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (tenant_id, event_id) DO NOTHING`,
		uuid.New(), env.TenantID, env.SourceID, env.EventID, env.Type, nullString(env.EventName),
		env.IdentifierKey(), rawPayload, hash, env.Timestamp, env.ReceivedAt, StatusStored,
	)
	if err != nil {
		return fmt.Errorf("insert raw_event: %w", err)
	}
	return nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
