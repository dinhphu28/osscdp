package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/events"
)

// BehaviorRecorder appends a behavioral_event row inside the profile-update tx
// (Level 3 stateful segmentation). Kept as an interface so the profile package
// does not import internal/behavior; the worker injects the concrete recorder.
type BehaviorRecorder interface {
	Append(ctx context.Context, tx pgx.Tx, profileID uuid.UUID, env events.Envelope) error
}

// Publisher emits profile_updated.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// ProfileUpdated is the event emitted after a profile changes.
type ProfileUpdated struct {
	EventType         string    `json:"event_type"`
	TenantID          uuid.UUID `json:"tenant_id"`
	EventID           string    `json:"event_id"`
	CustomerProfileID uuid.UUID `json:"customer_profile_id"`
	CanonicalUserID   string    `json:"canonical_user_id"`
	IdentityClusterID uuid.UUID `json:"identity_cluster_id"`
	ChangedFields     []string  `json:"changed_fields"`
	ProfileVersion    int64     `json:"profile_version"`
	UpdatedAt         time.Time `json:"updated_at"`
	// Event is the reason envelope, embedded so the segment worker can evaluate
	// event.* fields without a second lookup.
	Event events.Envelope `json:"event"`
}

// Result summarizes an update. Applied is false on an idempotent no-op.
type Result struct {
	ProfileID     uuid.UUID
	Version       int64
	ChangedFields []string
	Applied       bool
	// ReparentedIDs are the loser canonical_user_ids whose profiles were folded
	// into this survivor and deleted during a cluster merge (audited after commit).
	ReparentedIDs []string
	// CanonicalUserID / ClusterID are the *effective* survivor after any merge
	// redirect — the emit + audit use these, which may differ from the event's
	// original (retired) canonical when a late loser event was redirected.
	CanonicalUserID string
	ClusterID       uuid.UUID
}

// Service updates customer profiles and emits profile_updated.
type Service struct {
	pool  *pgxpool.Pool
	repo  *Repo
	pub   Publisher
	topic string

	// OnUpdated is a metric hook (nil-safe), called once per applied update.
	OnUpdated func()
	// Audit records reparent events (nil-safe; unset in unit tests).
	Audit *audit.Recorder
	// Logger reports best-effort failures (e.g. reparent audit); nil-safe.
	Logger *slog.Logger
	// Behavior appends behavioral_event rows in the same tx (nil-safe; Phase 2).
	Behavior BehaviorRecorder
}

// NewService constructs a Service.
func NewService(pool *pgxpool.Pool, pub Publisher, topic string) *Service {
	return &Service{pool: pool, repo: NewRepo(pool), pub: pub, topic: topic}
}

// Update merges one resolved event into the customer's profile. Transactional,
// idempotent by event_id. mergedCanonicalIDs (set when the resolution merged
// clusters) name loser profiles to reparent into this survivor.
func (s *Service) Update(ctx context.Context, canonicalUserID string, clusterID uuid.UUID, mergedCanonicalIDs []string, env events.Envelope) error {
	res, err := s.updateTx(ctx, canonicalUserID, clusterID, mergedCanonicalIDs, env)
	if err != nil {
		return err
	}
	if !res.Applied {
		return nil // already applied — no double count, no re-emit
	}
	if s.OnUpdated != nil {
		s.OnUpdated()
	}
	// Reparent audit is best-effort: identity_merge_history is the durable, in-tx
	// record of the merge, so a transient audit failure must not gate the emit
	// (which the segment/activation workers depend on). Returning an error here
	// would retry and then hit the alreadyApplied short-circuit, permanently
	// dropping profile_updated.
	if len(res.ReparentedIDs) > 0 && s.Audit != nil {
		tenantID := env.TenantID
		if err := s.Audit.Record(ctx, audit.Entry{
			TenantID: &tenantID, ActorType: audit.ActorSystem, Action: "reparent",
			ResourceType: "customer_profile", ResourceID: res.ProfileID.String(),
			After: map[string]any{"survivor": res.CanonicalUserID, "merged_from": res.ReparentedIDs},
		}); err != nil && s.Logger != nil {
			s.Logger.Warn("reparent audit failed", "error", err.Error(), "profile_id", res.ProfileID.String())
		}
	}
	return s.emit(ctx, res.CanonicalUserID, res.ClusterID, env, res)
}

func (s *Service) updateTx(ctx context.Context, canonicalUserID string, clusterID uuid.UUID, mergedCanonicalIDs []string, env events.Envelope) (Result, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	prof, found, err := s.repo.getForUpdate(ctx, tx, env.TenantID, canonicalUserID)
	if err != nil {
		return Result{}, err
	}
	if !found {
		// The canonical has no profile. If its cluster was merged away, redirect
		// the event onto the surviving profile instead of resurrecting a zombie
		// for a dead cluster (identity_resolved is partitioned by canonical, so a
		// loser event can arrive after — or be redelivered past — its merge).
		survCanonical, survCluster, redirected, err := s.repo.resolveSurvivorCluster(ctx, tx, env.TenantID, clusterID)
		if err != nil {
			return Result{}, err
		}
		if redirected {
			canonicalUserID, clusterID = survCanonical, survCluster
			if prof, found, err = s.repo.getForUpdate(ctx, tx, env.TenantID, canonicalUserID); err != nil {
				return Result{}, err
			}
		}
		if !found {
			prof = Profile{
				ID:                 uuid.New(),
				TenantID:           env.TenantID,
				CanonicalUserID:    canonicalUserID,
				IdentityClusterID:  clusterID,
				Traits:             map[string]any{},
				ComputedAttributes: map[string]any{},
				Version:            0,
			}
			if err := s.repo.create(ctx, tx, prof); err != nil {
				return Result{}, err
			}
		}
	}

	applied, err := s.repo.alreadyApplied(ctx, tx, env.TenantID, prof.ID, env.EventID)
	if err != nil {
		return Result{}, err
	}
	if applied {
		if err := tx.Commit(ctx); err != nil {
			return Result{}, err
		}
		return Result{Applied: false}, nil
	}

	before := snapshot(prof)

	// Reparent: fold any loser profiles from a cluster merge into this survivor,
	// then delete them. Idempotent — on reprocess the losers are already gone.
	var reparented []string
	var reparentChanged []string
	for _, lc := range mergedCanonicalIDs {
		if lc == canonicalUserID {
			continue
		}
		loser, found, err := s.repo.getForUpdate(ctx, tx, env.TenantID, lc)
		if err != nil {
			return Result{}, err
		}
		if !found {
			continue
		}
		reparentChanged = append(reparentChanged, mergeReparent(&prof, loser)...)
		if err := s.repo.reparentProfileChildren(ctx, tx, env.TenantID, loser.ID, prof.ID); err != nil {
			return Result{}, err
		}
		reparented = append(reparented, lc)
	}

	newTraits, ch1 := MergeTraits(prof.Traits, env)
	newComputed, ch2 := MergeComputed(prof.ComputedAttributes, env)
	newFirst, newLast, ch3 := MergeSeen(prof.FirstSeenAt, prof.LastSeenAt, env.Timestamp)
	changed := append(append(append(reparentChanged, ch1...), ch2...), ch3...)

	fromVersion := prof.Version
	prof.Traits = newTraits
	prof.ComputedAttributes = newComputed
	prof.FirstSeenAt = newFirst
	prof.LastSeenAt = newLast
	prof.Version = fromVersion + 1

	if err := s.repo.update(ctx, tx, prof, fromVersion); err != nil {
		return Result{}, err
	}
	// Append the durable behavioral_event in the same tx, behind the alreadyApplied
	// ledger — this is where exactly-once behavioral counters come for free.
	if s.Behavior != nil {
		if err := s.Behavior.Append(ctx, tx, prof.ID, env); err != nil {
			return Result{}, err
		}
	}
	after := snapshot(prof)
	if _, err := s.repo.markApplied(ctx, tx, env.TenantID, prof.ID, env.EventID, "update", before, after); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit: %w", err)
	}
	return Result{
		ProfileID: prof.ID, Version: prof.Version, ChangedFields: changed, Applied: true,
		ReparentedIDs: reparented, CanonicalUserID: canonicalUserID, ClusterID: clusterID,
	}, nil
}

func (s *Service) emit(ctx context.Context, canonicalUserID string, clusterID uuid.UUID, env events.Envelope, res Result) error {
	payload := ProfileUpdated{
		EventType:         "profile_updated",
		TenantID:          env.TenantID,
		EventID:           env.EventID,
		CustomerProfileID: res.ProfileID,
		CanonicalUserID:   canonicalUserID,
		IdentityClusterID: clusterID,
		ChangedFields:     res.ChangedFields,
		ProfileVersion:    res.Version,
		UpdatedAt:         time.Now().UTC(),
		Event:             env,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal profile_updated: %w", err)
	}
	return s.pub.Publish(ctx, s.topic, env.TenantID.String()+"|"+canonicalUserID, b)
}

func snapshot(p Profile) []byte {
	b, _ := json.Marshal(map[string]any{
		"traits":              p.Traits,
		"computed_attributes": p.ComputedAttributes,
		"first_seen_at":       p.FirstSeenAt,
		"last_seen_at":        p.LastSeenAt,
		"version":             p.Version,
	})
	return b
}
