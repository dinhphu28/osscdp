package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/events"
)

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
}

// Service updates customer profiles and emits profile_updated.
type Service struct {
	pool  *pgxpool.Pool
	repo  *Repo
	pub   Publisher
	topic string

	// OnUpdated is a metric hook (nil-safe), called once per applied update.
	OnUpdated func()
}

// NewService constructs a Service.
func NewService(pool *pgxpool.Pool, pub Publisher, topic string) *Service {
	return &Service{pool: pool, repo: NewRepo(pool), pub: pub, topic: topic}
}

// Update merges one resolved event into the customer's profile. Transactional,
// idempotent by event_id.
func (s *Service) Update(ctx context.Context, canonicalUserID string, clusterID uuid.UUID, env events.Envelope) error {
	res, err := s.updateTx(ctx, canonicalUserID, clusterID, env)
	if err != nil {
		return err
	}
	if !res.Applied {
		return nil // already applied — no double count, no re-emit
	}
	if s.OnUpdated != nil {
		s.OnUpdated()
	}
	return s.emit(ctx, canonicalUserID, clusterID, env, res)
}

func (s *Service) updateTx(ctx context.Context, canonicalUserID string, clusterID uuid.UUID, env events.Envelope) (Result, error) {
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

	newTraits, ch1 := MergeTraits(prof.Traits, env)
	newComputed, ch2 := MergeComputed(prof.ComputedAttributes, env)
	newFirst, newLast, ch3 := MergeSeen(prof.FirstSeenAt, prof.LastSeenAt, env.Timestamp)
	changed := append(append(ch1, ch2...), ch3...)

	fromVersion := prof.Version
	prof.Traits = newTraits
	prof.ComputedAttributes = newComputed
	prof.FirstSeenAt = newFirst
	prof.LastSeenAt = newLast
	prof.Version = fromVersion + 1

	if err := s.repo.update(ctx, tx, prof, fromVersion); err != nil {
		return Result{}, err
	}
	after := snapshot(prof)
	if _, err := s.repo.markApplied(ctx, tx, env.TenantID, prof.ID, env.EventID, "update", before, after); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit: %w", err)
	}
	return Result{ProfileID: prof.ID, Version: prof.Version, ChangedFields: changed, Applied: true}, nil
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
