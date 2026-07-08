package journey

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	// store (nil-safe) evaluates condition steps' behavioral leaves; a nil store
	// leaves behavioral conditions inert (they evaluate false), while stateless
	// (trait) conditions still work.
	store segment.BehaviorStore
	now   func() time.Time

	// Metric hooks (nil-safe).
	OnEnrolled func() // a newly created enrollment
	OnExited   func() // one or more enrollments exited on segment leave
}

// NewService constructs a Service. store may be nil to disable behavioral condition
// evaluation.
func NewService(pool *pgxpool.Pool, profiles ProfileReader, sender Sender, store segment.BehaviorStore) *Service {
	return &Service{
		repo: NewRepo(pool), profiles: profiles, sender: sender, store: store,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// WithClock overrides the clock (tests advance the injected instant).
func (s *Service) WithClock(now func() time.Time) *Service { s.now = now; return s }

// Repo exposes the underlying repository (for admin handlers and the runner).
func (s *Service) Repo() *Repo { return s.repo }

// OnMembershipChanged dispatches a segment membership transition: an 'entered' enrolls
// the profile; an 'exited' terminates active enrollments of journeys configured to
// exit on segment leave. Both are idempotent under at-least-once redelivery.
func (s *Service) OnMembershipChanged(ctx context.Context, mc segment.MembershipChanged) error {
	switch mc.Change {
	case segment.ChangeEntered:
		return s.EnrollOnMembership(ctx, mc)
	case segment.ChangeExited:
		return s.ExitOnMembership(ctx, mc)
	default:
		return nil
	}
}

// EnrollOnMembership is the entry path: a segment_membership_changed(entered) enrolls
// the profile into every active journey that enters on that segment. Idempotent under
// at-least-once redelivery (Enroll is ON CONFLICT DO NOTHING).
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

// ExitOnMembership is the exit path: a segment_membership_changed(exited) terminates
// the profile's active enrollments in journeys that enter on that segment and are
// configured to exit on segment leave. Idempotent (a redelivered exit finds no active
// enrollment and is a no-op).
func (s *Service) ExitOnMembership(ctx context.Context, mc segment.MembershipChanged) error {
	if mc.Change != segment.ChangeExited {
		return nil
	}
	n, err := s.repo.ExitActiveEnrollmentsForSegment(ctx, mc.TenantID, mc.SegmentID, mc.CustomerProfileID)
	if err != nil {
		return err
	}
	if n > 0 && s.OnExited != nil {
		s.OnExited()
	}
	return nil
}

// Advance executes the current step of a claimed enrollment and moves it forward by
// ONE step, committing the transition through the claim-fenced Repo.Advance (a stale
// or reclaimed runner writes nothing):
//   - WAIT reschedules due_at to now+duration.
//   - SEND enqueues an activation task (idempotent) BEFORE advancing, so a crash
//     between the two re-runs the send (which dedups) then advances — never a
//     double-send, never a lost send.
//   - CONDITION evaluates its segment.Rule (no triggering event, at=now) and routes to
//     IfTrue/IfFalse. SPLIT deterministically hashes to a weighted branch.
//
// Routing steps (condition/split) have no side effect and commit their decision
// atomically, so a pre-commit re-evaluation simply re-routes on current state — a
// downstream send can only fire once the position durably rests on that send step.
// Branch targets are forward-only (see Validate), so the walk always terminates; a
// target at len(steps) completes the enrollment.
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
	statusFor := func(target int) string {
		if target >= len(def.Steps) {
			return EnrollmentCompleted
		}
		return EnrollmentActive
	}

	switch step.Type {
	case StepWait:
		dur, err := segment.ParseWindow(step.Duration)
		if err != nil {
			return err
		}
		next := s.linearNext(e, step)
		_, err = s.repo.Advance(ctx, e, now, next, now.Add(dur), statusFor(next))
		return err
	case StepSend:
		prof, err := s.loadProfileOrExit(ctx, e, now)
		if err != nil || prof == nil {
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
		next := s.linearNext(e, step)
		// due=now so a following step (if any) runs on the next tick.
		_, err = s.repo.Advance(ctx, e, now, next, now, statusFor(next))
		return err
	case StepCondition:
		if step.Condition == nil {
			return fmt.Errorf("journey %s: condition step %d has no rule", e.JourneyID, e.CurrentStepIndex)
		}
		prof, err := s.loadProfileOrExit(ctx, e, now)
		if err != nil || prof == nil {
			return err
		}
		// Anchor windowed behavioral evaluation to the STABLE step due_at, not the
		// runner clock: a reclaim at a later instant must re-evaluate the same window
		// and route the same way (determinism across at-least-once redelivery).
		matched, err := segment.Evaluate(ctx, *step.Condition, segment.EvalContext{Profile: *prof}, s.store, e.DueAt)
		if err != nil {
			return err
		}
		target := step.IfFalse
		if matched {
			target = step.IfTrue
		}
		_, err = s.repo.Advance(ctx, e, now, target, now, statusFor(target))
		return err
	case StepSplit:
		key := fmt.Sprintf("%s|%s|%s|%d|%d", e.TenantID, e.JourneyID, e.CustomerProfileID, e.EnrollmentSeq, e.CurrentStepIndex)
		target := splitTarget(step.Branches, key)
		_, err := s.repo.Advance(ctx, e, now, target, now, statusFor(target))
		return err
	default:
		return fmt.Errorf("journey %s: unknown step type %q", e.JourneyID, step.Type)
	}
}

// linearNext returns a wait/send step's forward target: its explicit Next, or the
// default index+1.
func (s *Service) linearNext(e Enrollment, step Step) int {
	if step.Next != 0 {
		return step.Next
	}
	return e.CurrentStepIndex + 1
}

// loadProfileOrExit loads the enrollment's profile, or — if it was erased mid-journey —
// exits the enrollment and returns (nil, nil) so the caller stops without a send.
func (s *Service) loadProfileOrExit(ctx context.Context, e Enrollment, now time.Time) (*profile.Profile, error) {
	prof, err := s.profiles.GetByID(ctx, e.TenantID, e.CustomerProfileID)
	if errors.Is(err, profile.ErrNotFound) {
		_, aerr := s.repo.Advance(ctx, e, now, e.CurrentStepIndex, now, EnrollmentExited)
		return nil, aerr
	}
	if err != nil {
		return nil, err
	}
	return &prof, nil
}

// splitTarget deterministically selects a split branch's forward target by hashing the
// stable key (tenant|journey|profile|enrollment_seq|step_index), so at-least-once
// redelivery or a reclaim always routes to the same branch.
func splitTarget(branches []SplitBranch, key string) int {
	// uint64 accumulation cannot overflow given the per-weight cap enforced by Validate.
	var total uint64
	for _, b := range branches {
		total += uint64(b.Weight)
	}
	if total == 0 {
		return branches[0].Next
	}
	sum := sha256.Sum256([]byte(key))
	n := binary.BigEndian.Uint64(sum[:8]) % total
	var acc uint64
	for _, b := range branches {
		acc += uint64(b.Weight)
		if n < acc {
			return b.Next
		}
	}
	return branches[len(branches)-1].Next
}
