package rawevent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/events"
)

// DefaultReplayCap bounds how many events a single identifier replay will send.
const DefaultReplayCap = 1000

// Publisher republishes a stored payload to the bus.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// Replayer re-drives stored events through the pipeline by republishing them.
type Replayer struct {
	repo   *Repo
	pub    Publisher
	topic  string
	logger *slog.Logger
}

// NewReplayer constructs a Replayer.
func NewReplayer(repo *Repo, pub Publisher, topic string, logger *slog.Logger) *Replayer {
	return &Replayer{repo: repo, pub: pub, topic: topic, logger: logger}
}

// ReplayOne republishes a single stored event. Returns ErrNotFound if absent.
func (r *Replayer) ReplayOne(ctx context.Context, tenantID uuid.UUID, eventID string) error {
	ev, err := r.repo.GetByEventID(ctx, tenantID, eventID)
	if err != nil {
		return err
	}
	return r.publish(ctx, ev)
}

// ReplayByIdentifier republishes up to max events for an identifier_key, newest
// first. Returns the number republished.
func (r *Replayer) ReplayByIdentifier(ctx context.Context, tenantID uuid.UUID, identifierKey string, max int) (int, error) {
	if max <= 0 || max > DefaultReplayCap {
		max = DefaultReplayCap
	}
	count := 0
	cursor := ""
	truncated := false
	for count < max {
		page, next, err := r.repo.List(ctx, ListQuery{
			TenantID:      tenantID,
			IdentifierKey: identifierKey,
			Limit:         min(MaxLimit, max-count),
			Cursor:        cursor,
		})
		if err != nil {
			return count, err
		}
		for _, ev := range page {
			if err := r.publish(ctx, ev); err != nil {
				return count, err
			}
			count++
		}
		if next == "" {
			break
		}
		cursor = next
		if count >= max {
			truncated = true
		}
	}
	if truncated {
		r.logger.Warn("replay hit cap; more events exist",
			"tenant_id", tenantID.String(), "identifier_key", identifierKey, "cap", max)
	}
	return count, nil
}

func (r *Replayer) publish(ctx context.Context, ev RawEvent) error {
	var env events.Envelope
	if err := json.Unmarshal(ev.PayloadJSON, &env); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	return r.pub.Publish(ctx, r.topic, env.PartitionKey(), ev.PayloadJSON)
}
