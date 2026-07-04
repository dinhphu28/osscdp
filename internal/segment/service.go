package segment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

	// Per-tenant cache of parsed active versions, keyed by the segments epoch so a
	// create/update in the API process invalidates the worker's cache (finding #14).
	cacheMu sync.Mutex
	cache   map[uuid.UUID]cachedVersions

	// Metric hooks (nil-safe).
	OnEvaluated         func()
	OnMatched           func()
	OnStatefulEvaluated func()
	OnStatefulMatched   func()
}

type cachedVersions struct {
	count      int64
	updated    time.Time
	versionSum int64
	versions   []ActiveVersion
}

// NewService constructs a Service. store (nil-safe) evaluates Level 3 behavioral
// leaves; a nil store leaves stateful segments inert.
func NewService(pool *pgxpool.Pool, profiles ProfileReader, store BehaviorStore) *Service {
	return &Service{repo: NewRepo(pool), profiles: profiles, store: store, cache: map[uuid.UUID]cachedVersions{}}
}

// activeVersions returns the tenant's parsed active segment versions, served from a
// cache validated against the cheap segments epoch (so the per-event fan-out avoids
// re-fetching + re-unmarshalling every rule when nothing changed).
func (s *Service) activeVersions(ctx context.Context, tenantID uuid.UUID) ([]ActiveVersion, error) {
	count, updated, versionSum, err := s.repo.SegmentsEpoch(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	s.cacheMu.Lock()
	c, ok := s.cache[tenantID]
	s.cacheMu.Unlock()
	if ok && c.count == count && c.updated.Equal(updated) && c.versionSum == versionSum {
		return c.versions, nil
	}
	versions, err := s.repo.ActiveSegmentVersions(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	s.cacheMu.Lock()
	s.cache[tenantID] = cachedVersions{count: count, updated: updated, versionSum: versionSum, versions: versions}
	s.cacheMu.Unlock()
	return versions, nil
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

	segs, err := s.activeVersions(ctx, pu.TenantID)
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
		// Prefilter: a purely-behavioural segment can only change at the edge if THIS
		// event is one it references; skip the others. A segment with any stateless
		// leaf is ALWAYS evaluated — a trait change may newly match it (findings
		// #15/#30) — so it is never gated here.
		if !seg.HasStateless && !containsString(seg.ReferencedNames, pu.Event.EventName) {
			continue
		}
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
		if err := s.applyMembership(ctx, pu, seg, ec, matched, at); err != nil {
			return err
		}
	}
	return nil
}

// coalesceGranularity: skip re-arming a deadline when the new due_at is within this
// of the stored one, to avoid write churn on repeated near-identical recomputes.
const coalesceGranularity = time.Minute

// flipAndEmit runs one conditional flip + (if it flipped) the outbox insert inside
// tx, shared by the edge and sweep paths. tokenPrefix is the ReasonEventID prefix
// ("<event_id>" on the edge, "sweep" on the sweeper); the token is "<prefix>:<seq>".
// Does not commit. Returns whether a real transition flipped.
func (s *Service) flipAndEmit(ctx context.Context, tx pgx.Tx, tenantID uuid.UUID, seg ActiveVersion, profileID uuid.UUID, canonical, tokenPrefix string, matched bool) (bool, error) {
	var (
		seq     int64
		flipped bool
		change  string
		err     error
	)
	if matched {
		change = ChangeEntered
		if seq, flipped, err = s.repo.EnterTx(ctx, tx, tenantID, seg.SegmentID, profileID, seg.Version); err != nil {
			return false, err
		}
		if !flipped {
			// Already a member: record the evaluation, emit nothing.
			return false, s.repo.TouchEvaluatedTx(ctx, tx, tenantID, seg.SegmentID, profileID, seg.Version)
		}
	} else {
		change = ChangeExited
		if seq, flipped, err = s.repo.ExitTx(ctx, tx, tenantID, seg.SegmentID, profileID); err != nil {
			return false, err
		}
		if !flipped {
			return false, nil // not a member / already exited
		}
	}

	payload := MembershipChanged{
		EventType:         "segment_membership_changed",
		TenantID:          tenantID,
		SegmentID:         seg.SegmentID,
		SegmentVersionID:  seg.VersionID,
		CustomerProfileID: profileID,
		Change:            change,
		ReasonEventID:     fmt.Sprintf("%s:%d", tokenPrefix, seq),
		TransitionSeq:     seq,
		ChangedAt:         time.Now().UTC(),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal membership change: %w", err)
	}
	return true, s.repo.InsertMembershipOutbox(ctx, tx, tenantID, tenantID.String()+"|"+canonical, b)
}

// applyMembership is the edge path: flip + emit atomically, then arm the elapse
// deadline (segment_pending_eval) for sweep-safe rules so an absence/expiry/re-entry
// transition still fires later with no inbound event. All in one tx.
func (s *Service) applyMembership(ctx context.Context, pu profile.ProfileUpdated, seg ActiveVersion, ec EvalContext, matched bool, at time.Time) error {
	due, hasDue, arm, err := s.planDeadline(ctx, seg, ec, pu.TenantID, pu.CustomerProfileID, at)
	if err != nil {
		return err
	}
	tx, err := s.repo.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := s.flipAndEmit(ctx, tx, pu.TenantID, seg, pu.CustomerProfileID, pu.CanonicalUserID, pu.EventID, matched); err != nil {
		return err
	}
	if err := s.armDeadline(ctx, tx, pu.TenantID, seg.SegmentID, pu.CustomerProfileID, due, hasDue, arm, "absence_deadline"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// planDeadline computes the next elapse deadline for a sweep-safe rule and whether
// to re-arm it (coalescing a near-identical existing due_at). Reads only; the write
// happens in the caller's tx via armDeadline.
func (s *Service) planDeadline(ctx context.Context, seg ActiveVersion, ec EvalContext, tenantID, profileID uuid.UUID, at time.Time) (due time.Time, hasDue, arm bool, err error) {
	if referencesEvent(seg.Rule) {
		return time.Time{}, false, false, nil // event-gated rule: edge-only, no deadline
	}
	due, hasDue, err = nextDueAt(ctx, seg.Rule, ec, s.store, at)
	if err != nil || !hasDue {
		return time.Time{}, false, false, err
	}
	arm = true
	if cur, ok, err := s.repo.CurrentDueAt(ctx, tenantID, seg.SegmentID, profileID); err != nil {
		return time.Time{}, false, false, err
	} else if ok && absDuration(cur.Sub(due)) < coalesceGranularity {
		arm = false // near-identical deadline already stored; leave it
	}
	return due, true, arm, nil
}

// armDeadline upserts/deletes the deadline row inside tx per the plan.
func (s *Service) armDeadline(ctx context.Context, tx pgx.Tx, tenantID, segmentID, profileID uuid.UUID, due time.Time, hasDue, arm bool, reason string) error {
	switch {
	case hasDue && arm:
		return s.repo.UpsertPendingTx(ctx, tx, tenantID, segmentID, profileID, due, reason)
	case !hasDue:
		return s.repo.DeletePendingTx(ctx, tx, tenantID, segmentID, profileID)
	default:
		return nil // coalesced: keep the existing deadline
	}
}

// SweepEvaluate re-evaluates one claimed (segment, profile) deadline at at=now()
// with NO triggering event, flips through the same atomic path (token "sweep:<seq>"),
// and unconditionally re-arms the next deadline across all leaves (finding #6). This
// is the flagship: absence/expiry transitions fire without an inbound event.
func (s *Service) SweepEvaluate(ctx context.Context, row PendingEval, at time.Time) error {
	av, ok, err := s.repo.ActiveVersionForSegment(ctx, row.TenantID, row.SegmentID)
	if err != nil {
		return err
	}
	if !ok {
		return s.dropDeadline(ctx, row) // segment inactive/deleted
	}
	if referencesEvent(av.Rule) {
		// Event-gated rule cannot be evaluated without a triggering event; a deadline
		// should never have been armed for it. Drop the stray row (defensive: the
		// safety sweep may enqueue broadly).
		return s.dropDeadline(ctx, row)
	}
	prof, err := s.profiles.GetByID(ctx, row.TenantID, row.CustomerProfileID)
	if errors.Is(err, profile.ErrNotFound) {
		return s.dropDeadline(ctx, row)
	}
	if err != nil {
		return err
	}
	ec := EvalContext{Profile: prof} // sweep has no triggering event
	matched, err := Evaluate(ctx, av.Rule, ec, s.store, at)
	if err != nil {
		return err
	}
	due, hasDue, err := nextDueAt(ctx, av.Rule, ec, s.store, at)
	if err != nil {
		return err
	}

	tx, err := s.repo.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := s.flipAndEmit(ctx, tx, row.TenantID, av, prof.ID, prof.CanonicalUserID, "sweep", matched); err != nil {
		return err
	}
	// Unconditional re-arm (no coalesce): the row was just claimed, so re-arming
	// clears claimed_at and sets the recomputed next deadline (or deletes it).
	if err := s.armDeadline(ctx, tx, row.TenantID, av.SegmentID, prof.ID, due, hasDue, true, "absence_deadline"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) dropDeadline(ctx context.Context, row PendingEval) error {
	tx, err := s.repo.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.repo.DeletePendingTx(ctx, tx, row.TenantID, row.SegmentID, row.CustomerProfileID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SeedSegment enqueues a due-now deadline for every profile of the tenant when the
// segment is a sweep-safe stateful rule, so the existing population (including
// dormant "did-not-do" profiles) is evaluated without an inbound event. No-op for
// stateless or event-gated segments. Returns rows seeded.
func (s *Service) SeedSegment(ctx context.Context, tenantID, segmentID uuid.UUID, at time.Time, reason string) (int, error) {
	av, ok, err := s.repo.ActiveVersionForSegment(ctx, tenantID, segmentID)
	if err != nil || !ok {
		return 0, err
	}
	if !hasBehavior(av.Rule) || referencesEvent(av.Rule) {
		return 0, nil
	}
	return s.repo.SeedPendingForSegment(ctx, tenantID, segmentID, at, reason)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
