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

// ConsentReader returns a consent status for a (channel, purpose).
type ConsentReader interface {
	Get(ctx context.Context, tenantID, profileID uuid.UUID, channel, purpose string) (string, error)
}

// consentDenied is the status that blocks activation.
const consentDenied = "denied"

// Service turns membership changes into activation tasks.
type Service struct {
	repo     *Repo
	profiles ProfileReader
	consent  ConsentReader // nil = no gate

	// OnSkipped is a metric hook (nil-safe), called when consent blocks a send.
	OnSkipped func()
}

// NewService constructs a Service. consent may be nil to disable the gate.
func NewService(pool *pgxpool.Pool, profiles ProfileReader, consent ConsentReader) *Service {
	return &Service{repo: NewRepo(pool), profiles: profiles, consent: consent}
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
		status, lastErr := TaskPending, ""
		if s.consent != nil {
			denied, err := s.consentDeniedFor(ctx, mc.TenantID, mc.CustomerProfileID, sub.DestinationID)
			if err != nil {
				return err
			}
			if denied {
				status, lastErr = TaskSkipped, "consent_denied"
			}
		}

		key := IdempotencyKey(mc.TenantID, sub.DestinationID, sub.ID, mc.CustomerProfileID, mc.ReasonEventID, mc.Change)
		created, err := s.repo.CreateTask(ctx, Task{
			TenantID:          mc.TenantID,
			DestinationID:     sub.DestinationID,
			SubscriptionID:    sub.ID,
			CustomerProfileID: mc.CustomerProfileID,
			SourceEventID:     mc.ReasonEventID,
			IdempotencyKey:    key,
			Payload:           payload,
		}, status, lastErr)
		if err != nil {
			return err
		}
		if created && status == TaskSkipped && s.OnSkipped != nil {
			s.OnSkipped()
		}
	}
	return nil
}

// consentDeniedFor reports whether consent is denied for the destination's
// channel/purpose. Errors loading the destination are treated as not-denied so a
// missing destination doesn't silently drop activations (the sender handles it).
func (s *Service) consentDeniedFor(ctx context.Context, tenantID, profileID, destinationID uuid.UUID) (bool, error) {
	dest, err := s.repo.GetDestination(ctx, tenantID, destinationID)
	if err != nil {
		return false, nil
	}
	target := ConsentTargetFor(dest)
	status, err := s.consent.Get(ctx, tenantID, profileID, target.Channel, target.Purpose)
	if err != nil {
		return false, err
	}
	return status == consentDenied, nil
}
