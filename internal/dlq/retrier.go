package dlq

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/events"
)

// Publisher republishes a DLQ payload to the bus.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// Retrier republishes dead-lettered events to the entry topic.
type Retrier struct {
	repo  *Repo
	pub   Publisher
	topic string
}

// NewRetrier constructs a Retrier (topic is the events entry topic).
func NewRetrier(repo *Repo, pub Publisher, topic string) *Retrier {
	return &Retrier{repo: repo, pub: pub, topic: topic}
}

// Retry republishes the event's original payload and marks it retried. If the
// payload is still poison it will simply re-dead-letter.
func (r *Retrier) Retry(ctx context.Context, tenantID, id uuid.UUID) error {
	e, err := r.repo.Get(ctx, tenantID, id)
	if err != nil {
		return err
	}
	key := e.EventID
	var env events.Envelope
	if json.Unmarshal(e.OriginalPayload, &env) == nil && env.EventID != "" {
		key = env.PartitionKey()
	}
	if err := r.pub.Publish(ctx, r.topic, key, e.OriginalPayload); err != nil {
		return err
	}
	ok, err := r.repo.MarkStatus(ctx, tenantID, id, StatusRetried)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("dlq event vanished during retry")
	}
	return nil
}
