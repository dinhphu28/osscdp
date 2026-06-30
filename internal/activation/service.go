package activation

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

// ProfileReader loads a profile by id (profile.Repo satisfies it).
type ProfileReader interface {
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (profile.Profile, error)
}

// Service turns membership changes into activation tasks.
type Service struct {
	repo     *Repo
	profiles ProfileReader
}

// NewService constructs a Service.
func NewService(pool *pgxpool.Pool, profiles ProfileReader) *Service {
	return &Service{repo: NewRepo(pool), profiles: profiles}
}

// OnMembershipChanged creates an activation task per active subscription for the
// segment. Idempotent: duplicate membership changes don't create duplicate tasks.
func (s *Service) OnMembershipChanged(ctx context.Context, mc segment.MembershipChanged) error {
	subs, err := s.repo.ActiveSubscriptionsForSegment(ctx, mc.TenantID, mc.SegmentID)
	if err != nil {
		return err
	}
	if len(subs) == 0 {
		return nil
	}
	prof, err := s.profiles.GetByID(ctx, mc.TenantID, mc.CustomerProfileID)
	if errors.Is(err, profile.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	payload, err := BuildPayload(mc.TenantID, mc.SegmentID, mc.Change, mc.ChangedAt, prof)
	if err != nil {
		return err
	}
	for _, sub := range subs {
		key := IdempotencyKey(mc.TenantID, sub.DestinationID, sub.ID, mc.CustomerProfileID, mc.ReasonEventID, mc.Change)
		if _, err := s.repo.CreateTask(ctx, Task{
			TenantID:          mc.TenantID,
			DestinationID:     sub.DestinationID,
			SubscriptionID:    sub.ID,
			CustomerProfileID: mc.CustomerProfileID,
			SourceEventID:     mc.ReasonEventID,
			IdempotencyKey:    key,
			Payload:           payload,
		}); err != nil {
			return err
		}
	}
	return nil
}
