package segment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/profile"
)

// ProfileReader loads a profile by id.
type ProfileReader interface {
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (profile.Profile, error)
}

// Membership change kinds.
const (
	ChangeEntered = "entered"
	ChangeExited  = "exited"
)

// MembershipChanged is the emitted event.
type MembershipChanged struct {
	EventType         string    `json:"event_type"`
	TenantID          uuid.UUID `json:"tenant_id"`
	SegmentID         uuid.UUID `json:"segment_id"`
	SegmentVersionID  uuid.UUID `json:"segment_version_id"`
	CustomerProfileID uuid.UUID `json:"customer_profile_id"`
	Change            string    `json:"change"`
	ReasonEventID     string    `json:"reason_event_id"`
	TransitionSeq     int64     `json:"transition_seq"`
	ChangedAt         time.Time `json:"changed_at"`
}

// Service evaluates profiles against active segments and tracks membership.
// Membership transitions and their emits are written atomically to
// segment_membership_outbox; a relay drains that table to the bus.
type Service struct {
	repo     *Repo
	profiles ProfileReader

	// store answers windowed behavioral leaves (nil => stateful leaves inert).
	store BehaviorStore

	// Metric hooks (nil-safe).
	OnEvaluated         func()
	OnMatched           func()
	OnStatefulEvaluated func()
	OnStatefulMatched   func()
}

// NewService constructs a Service. store (nil-safe) evaluates Level 3 behavioral
// leaves; a nil store leaves stateful segments inert.
func NewService(pool *pgxpool.Pool, profiles ProfileReader, store BehaviorStore) *Service {
	return &Service{repo: NewRepo(pool), profiles: profiles, store: store}
}

// Repo exposes the underlying repository (for admin handlers).
func (s *Service) Repo() *Repo { return s.repo }

// Evaluate runs all active segments against the updated profile + reason event,
// updating membership and emitting on transitions. Idempotent.
func (s *Service) Evaluate(ctx context.Context, pu profile.ProfileUpdated) error {
	prof, err := s.profiles.GetByID(ctx, pu.TenantID, pu.CustomerProfileID)
	if errors.Is(err, profile.ErrNotFound) {
		return nil // profile vanished; nothing to evaluate
	}
	if err != nil {
		return err
	}

	segs, err := s.repo.ActiveSegmentVersions(ctx, pu.TenantID)
	if err != nil {
		return err
	}
	ec := EvalContext{Profile: prof, Event: pu.Event}
	// Edge path anchors windowed reads to the event's own clamped timestamp (not
	// now()), so a redelivered profile_updated re-evaluates the same window.
	at := pu.Event.Timestamp
	if pu.Event.ReceivedAt.Before(at) {
		at = pu.Event.ReceivedAt
	}

	for _, seg := range segs {
		if s.OnEvaluated != nil {
			s.OnEvaluated()
		}
		stateful := hasBehavior(seg.Rule)
		if stateful && s.OnStatefulEvaluated != nil {
			s.OnStatefulEvaluated()
		}
		matched, err := Evaluate(ctx, seg.Rule, ec, s.store, at)
		if err != nil {
			// A behavior-store read failed; fail the handler so the at-least-once
			// consumer retries rather than persisting a spurious enter/exit.
			return fmt.Errorf("evaluate segment %s: %w", seg.SegmentID, err)
		}
		if matched && s.OnMatched != nil {
			s.OnMatched()
		}
		if matched && stateful && s.OnStatefulMatched != nil {
			s.OnStatefulMatched()
		}
		if err := s.applyMembership(ctx, pu, seg, matched); err != nil {
			return err
		}
	}
	return nil
}

// applyMembership performs one (segment, profile) transition in a single tx: a
// conditional flip (no read-then-write race) and, only when the status actually
// flipped, the outbox insert — so flip + emit commit atomically. A redundant
// transition (already active / already exited / never a member) emits nothing.
func (s *Service) applyMembership(ctx context.Context, pu profile.ProfileUpdated, seg ActiveVersion, matched bool) error {
	tx, err := s.repo.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		seq     int64
		flipped bool
		change  string
	)
	if matched {
		change = ChangeEntered
		if seq, flipped, err = s.repo.EnterTx(ctx, tx, pu.TenantID, seg.SegmentID, pu.CustomerProfileID, seg.Version); err != nil {
			return err
		}
		if !flipped {
			// Already a member: record the evaluation, emit nothing.
			if err := s.repo.TouchEvaluatedTx(ctx, tx, pu.TenantID, seg.SegmentID, pu.CustomerProfileID, seg.Version); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
	} else {
		change = ChangeExited
		if seq, flipped, err = s.repo.ExitTx(ctx, tx, pu.TenantID, seg.SegmentID, pu.CustomerProfileID); err != nil {
			return err
		}
		if !flipped {
			return tx.Commit(ctx) // not a member / already exited
		}
	}

	// ReasonEventID is a unique per-flip token (<event_id>:<seq>) so activation's
	// idempotency key is distinct per real transition (finding #27) while replays
	// of the same flip dedup.
	payload := MembershipChanged{
		EventType:         "segment_membership_changed",
		TenantID:          pu.TenantID,
		SegmentID:         seg.SegmentID,
		SegmentVersionID:  seg.VersionID,
		CustomerProfileID: pu.CustomerProfileID,
		Change:            change,
		ReasonEventID:     fmt.Sprintf("%s:%d", pu.EventID, seq),
		TransitionSeq:     seq,
		ChangedAt:         time.Now().UTC(),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal membership change: %w", err)
	}
	key := pu.TenantID.String() + "|" + pu.CanonicalUserID
	if err := s.repo.InsertMembershipOutbox(ctx, tx, pu.TenantID, key, b); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
