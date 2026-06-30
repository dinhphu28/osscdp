package events

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Ingest result statuses.
const (
	StatusAccepted  = "accepted"
	StatusDuplicate = "duplicate"
	StatusRejected  = "rejected"
)

// ErrConflict means the event_id already exists with a different payload.
var ErrConflict = errors.New("event_id already exists with different payload")

// Result is the outcome of ingesting a single event.
type Result struct {
	EventID string `json:"event_id"`
	Status  string `json:"status"`
}

// Service implements ingress business logic over the outbox repository.
type Service struct {
	repo *Repository
	now  func() time.Time
}

// NewService constructs a Service.
func NewService(repo *Repository) *Service {
	return &Service{repo: repo, now: time.Now}
}

// Ingest normalizes, validates, and enqueues one event. forcedType (set by the
// identify/alias/track handlers) overrides the body type when non-empty.
// Returns a *ValidationError for bad input and ErrConflict on hash mismatch.
func (s *Service) Ingest(ctx context.Context, in IncomingEvent, tenantID, sourceID uuid.UUID, forcedType string) (Result, error) {
	env, err := Normalize(in, tenantID, sourceID, forcedType, s.now())
	if err != nil {
		return Result{}, err
	}
	if err := Validate(env); err != nil {
		return Result{}, err
	}
	payload, err := env.PayloadJSON()
	if err != nil {
		return Result{}, err
	}
	hash, err := env.PayloadHash()
	if err != nil {
		return Result{}, err
	}
	outcome, err := s.repo.Enqueue(ctx, env, payload, hash)
	if err != nil {
		return Result{}, err
	}
	switch outcome {
	case Inserted:
		return Result{EventID: env.EventID, Status: StatusAccepted}, nil
	case Duplicate:
		return Result{EventID: env.EventID, Status: StatusDuplicate}, nil
	default: // Conflict
		return Result{EventID: env.EventID}, ErrConflict
	}
}

// BatchItemResult is the per-event outcome within a batch.
type BatchItemResult struct {
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitempty"`
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
}

// BatchResult aggregates batch ingestion outcomes.
type BatchResult struct {
	Accepted  int               `json:"accepted"`
	Duplicate int               `json:"duplicate"`
	Rejected  int               `json:"rejected"`
	Results   []BatchItemResult `json:"results"`
}

// IngestBatch ingests each event independently; one bad event does not fail the
// others. Each event is idempotent on its own (tenant_id, event_id).
func (s *Service) IngestBatch(ctx context.Context, events []IncomingEvent, tenantID, sourceID uuid.UUID) BatchResult {
	out := BatchResult{Results: make([]BatchItemResult, 0, len(events))}
	for i, in := range events {
		res, err := s.Ingest(ctx, in, tenantID, sourceID, "")
		item := BatchItemResult{Index: i, EventID: res.EventID}
		switch {
		case err == nil && res.Status == StatusAccepted:
			item.Status = StatusAccepted
			out.Accepted++
		case err == nil && res.Status == StatusDuplicate:
			item.Status = StatusDuplicate
			out.Duplicate++
		case errors.Is(err, ErrConflict):
			item.Status = StatusRejected
			item.Error = "conflict: event_id exists with different payload"
			out.Rejected++
		default:
			item.Status = StatusRejected
			item.Error = err.Error()
			out.Rejected++
		}
		out.Results = append(out.Results, item)
	}
	return out
}
