package journey

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

// ProfileReader loads a profile by id (profile.Repo satisfies it).
type ProfileReader interface {
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (profile.Profile, error)
}

// Sender enqueues a journey send through the activation stack, get-or-creating the
// destination's journey subscription and applying the consent gate
// (activation.Service satisfies it).
type Sender interface {
	EnqueueJourneySend(ctx context.Context, tenantID, destinationID, profileID uuid.UUID, sourceEventID, change string, payload []byte) (bool, error)
}

// Service enrolls profiles into journeys and advances their per-profile state.
type Service struct {
	repo     *Repo
	profiles ProfileReader
	sender   Sender
	now      func() time.Time

	// OnEnrolled is a metric hook (nil-safe), called on a newly created enrollment.
	OnEnrolled func()
}

// NewService constructs a Service.
func NewService(pool *pgxpool.Pool, profiles ProfileReader, sender Sender) *Service {
	return &Service{
		repo: NewRepo(pool), profiles: profiles, sender: sender,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the clock (tests advance the injected instant).
func (s *Service) WithClock(now func() time.Time) *Service { s.now = now; return s }

// Repo exposes the underlying repository (for admin handlers and the runner).
func (s *Service) Repo() *Repo { return s.repo }

// EnrollOnMembership is the entry path: a segment_membership_changed(entered) enrolls
// the profile into every active journey that enters on that segment. Idempotent under
// at-least-once redelivery (Enroll is ON CONFLICT DO NOTHING). Exit changes are
// ignored in Phase 1 (exit-on-segment-leave is Phase 2).
func (s *Service) EnrollOnMembership(ctx context.Context, mc segment.MembershipChanged) error {
	if mc.Change != segment.ChangeEntered {
		return nil
	}
	journeys, err := s.repo.JourneysEnteringOn(ctx, mc.TenantID, mc.SegmentID)
	if err != nil {
		return err
	}
	for _, j := range journeys {
		created, err := s.repo.Enroll(ctx, mc.TenantID, j.ID, mc.CustomerProfileID, j.CurrentVersion, s.now())
		if err != nil {
			return err
		}
		if created && s.OnEnrolled != nil {
			s.OnEnrolled()
		}
	}
	return nil
}

// Advance executes the current step of a claimed enrollment and moves it forward.
// A WAIT reschedules due_at to now+duration; a SEND enqueues an activation task
// (idempotent) BEFORE advancing, so a crash between the two re-runs the send (which
// dedups) and then advances — never a double-send, never a lost send. Reaching the
// last step completes the enrollment. All step transitions go through the
// claim-fenced Repo.Advance; a stale (reclaimed) runner writes nothing.
func (s *Service) Advance(ctx context.Context, e Enrollment, now time.Time) error {
	def, err := s.repo.GetVersion(ctx, e.TenantID, e.JourneyID, e.JourneyVersion)
	if err != nil {
		return err
	}
	if e.CurrentStepIndex >= len(def.Steps) {
		// Defensive: no step to run — complete the enrollment.
		_, err := s.repo.Advance(ctx, e, now, e.CurrentStepIndex, now, EnrollmentCompleted)
		return err
	}
	step := def.Steps[e.CurrentStepIndex]
	next := e.CurrentStepIndex + 1
	status := EnrollmentActive
	if next >= len(def.Steps) {
		status = EnrollmentCompleted
	}

	switch step.Type {
	case StepWait:
		dur, err := segment.ParseWindow(step.Duration)
		if err != nil {
			return err
		}
		_, err = s.repo.Advance(ctx, e, now, next, now.Add(dur), status)
		return err
	case StepSend:
		prof, err := s.profiles.GetByID(ctx, e.TenantID, e.CustomerProfileID)
		if errors.Is(err, profile.ErrNotFound) {
			// Profile erased mid-journey: exit the enrollment (no send).
			_, err := s.repo.Advance(ctx, e, now, e.CurrentStepIndex, now, EnrollmentExited)
			return err
		}
		if err != nil {
			return err
		}
		payload, err := BuildPayload(e.TenantID, e.JourneyID, e.CurrentStepIndex, prof.CanonicalUserID, prof.Traits, prof.ComputedAttributes)
		if err != nil {
			return err
		}
		// Idempotency source key namespaces the send by journey+version+run so a
		// re-authored version or a future re-entry never dedups against a prior send.
		srcKey := fmt.Sprintf("journey:%s:%d:%d", e.JourneyID, e.JourneyVersion, e.EnrollmentSeq)
		change := fmt.Sprintf("step:%d", e.CurrentStepIndex)
		if _, err := s.sender.EnqueueJourneySend(ctx, e.TenantID, step.DestinationID, e.CustomerProfileID, srcKey, change, payload); err != nil {
			return err
		}
		// due=now so a following step (if any) runs on the next tick.
		_, err = s.repo.Advance(ctx, e, now, next, now, status)
		return err
	default:
		return fmt.Errorf("journey %s: unknown step type %q", e.JourneyID, step.Type)
	}
}
