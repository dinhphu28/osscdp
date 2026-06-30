// Package relay drains the transactional outbox to the event bus.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/events"
)

// Publisher publishes a record to a topic.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// Relay polls pending outbox rows and publishes them, marking each published
// once the bus acknowledges. FOR UPDATE SKIP LOCKED makes concurrent relays safe.
type Relay struct {
	pool      *pgxpool.Pool
	pub       Publisher
	topic     string
	batchSize int
	interval  time.Duration
	logger    *slog.Logger

	// Metric hooks (nil-safe).
	OnPublished   func()
	OnPublishFail func()
}

// New constructs a Relay.
func New(pool *pgxpool.Pool, pub Publisher, topic string, batchSize int, interval time.Duration, logger *slog.Logger) *Relay {
	return &Relay{pool: pool, pub: pub, topic: topic, batchSize: batchSize, interval: interval, logger: logger}
}

// Run loops until ctx is canceled, draining the outbox each tick.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("relay run failed", "error", err.Error())
			}
		}
	}
}

type pendingRow struct {
	id           uuid.UUID
	partitionKey string
	payload      []byte
}

// RunOnce publishes up to batchSize pending rows and returns how many were sent.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, partition_key, payload_json
		FROM event_outbox
		WHERE status = $1
		ORDER BY created_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, events.StatusPending, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("select pending: %w", err)
	}

	var pending []pendingRow
	for rows.Next() {
		var pr pendingRow
		if err := rows.Scan(&pr.id, &pr.partitionKey, &pr.payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		pending = append(pending, pr)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate: %w", err)
	}
	if len(pending) == 0 {
		return 0, nil
	}

	published := make([]uuid.UUID, 0, len(pending))
	for _, pr := range pending {
		if err := r.pub.Publish(ctx, r.topic, pr.partitionKey, pr.payload); err != nil {
			// Leave this and the rest pending; they retry next tick.
			if r.OnPublishFail != nil {
				r.OnPublishFail()
			}
			r.logger.Error("publish failed", "error", err.Error())
			break
		}
		published = append(published, pr.id)
		if r.OnPublished != nil {
			r.OnPublished()
		}
	}

	if len(published) == 0 {
		return 0, nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE event_outbox SET status = 'published', published_at = now()
		WHERE id = ANY($1)`, published); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(published), nil
}
