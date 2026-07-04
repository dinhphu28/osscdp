package identity

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/events"
)

// Publisher emits the identity_resolved event.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// Result summarizes a resolution.
type Result struct {
	ClusterID       uuid.UUID
	CanonicalUserID string
	MergeOccurred   bool
	// MergedCanonicalIDs are the loser clusters' canonical_user_ids when a merge
	// occurred — the profile worker reparents their stale profiles into the survivor.
	MergedCanonicalIDs []string
}

// IdentityResolved is the event emitted after resolution. It embeds the original
// envelope so the Phase 6 profile worker has the traits without a second lookup.
type IdentityResolved struct {
	EventType         string          `json:"event_type"`
	TenantID          uuid.UUID       `json:"tenant_id"`
	EventID           string          `json:"event_id"`
	IdentityClusterID uuid.UUID       `json:"identity_cluster_id"`
	CanonicalUserID   string          `json:"canonical_user_id"`
	MergeOccurred     bool            `json:"merge_occurred"`
	// MergedCanonicalUserIDs lists the loser clusters' canonical_user_ids when a
	// merge occurred, so the profile worker can reparent their profiles.
	MergedCanonicalUserIDs []string        `json:"merged_canonical_user_ids,omitempty"`
	ResolvedAt             time.Time       `json:"resolved_at"`
	Event                  events.Envelope `json:"event"`
}

// Service resolves identities and emits identity_resolved.
type Service struct {
	pool  *pgxpool.Pool
	repo  *Repo
	pub   Publisher
	topic string

	// Metric hooks (nil-safe).
	OnResolved func()
	OnMerge    func()
}

// NewService constructs a Service.
func NewService(pool *pgxpool.Pool, pub Publisher, topic string) *Service {
	return &Service{pool: pool, repo: NewRepo(pool), pub: pub, topic: topic}
}

// Resolve connects the event's identifiers into a cluster (creating or merging
// as needed) and emits identity_resolved. Transactional and idempotent.
func (s *Service) Resolve(ctx context.Context, env events.Envelope) error {
	ids := ExtractIdentifiers(env)
	if len(ids) == 0 {
		return nil // nothing to resolve
	}

	res, err := s.resolveTx(ctx, env, ids)
	if err != nil {
		return err
	}

	if s.OnResolved != nil {
		s.OnResolved()
	}
	if res.MergeOccurred && s.OnMerge != nil {
		s.OnMerge()
	}
	return s.emit(ctx, env, res)
}

func (s *Service) resolveTx(ctx context.Context, env events.Envelope, ids []Identifier) (Result, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	nodeIDs := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		nid, err := s.repo.upsertNode(ctx, tx, env.TenantID, id.Namespace, ValueHash(env.TenantID, id.Namespace, id.Value))
		if err != nil {
			return Result{}, err
		}
		nodeIDs = append(nodeIDs, nid)
	}

	clusterIDs, err := s.repo.clustersForNodes(ctx, tx, env.TenantID, nodeIDs)
	if err != nil {
		return Result{}, err
	}
	if len(clusterIDs) > 0 {
		if err := s.repo.lockClusters(ctx, tx, env.TenantID, clusterIDs); err != nil {
			return Result{}, err
		}
		// Re-read under lock: a concurrent merge may have changed the set.
		if clusterIDs, err = s.repo.clustersForNodes(ctx, tx, env.TenantID, nodeIDs); err != nil {
			return Result{}, err
		}
		// Lock again so any cluster the re-read surfaced (a concurrent merge moved a
		// node into a cluster we had not locked) is also held before pickSurvivor/merge.
		if err := s.repo.lockClusters(ctx, tx, env.TenantID, clusterIDs); err != nil {
			return Result{}, err
		}
	}

	var (
		survivor         uuid.UUID
		canonical        string
		merge            bool
		mergedCanonicals []string
	)
	switch len(clusterIDs) {
	case 0:
		survivor, canonical, err = s.repo.createCluster(ctx, tx, env.TenantID)
	case 1:
		survivor = clusterIDs[0]
		canonical, err = s.repo.canonicalFor(ctx, tx, env.TenantID, survivor)
	default:
		survivor, canonical, err = s.repo.pickSurvivor(ctx, tx, env.TenantID, clusterIDs)
		if err == nil {
			losers := without(clusterIDs, survivor)
			// Capture loser canonicals before the merge (canonical_user_id is
			// unchanged by merge, but read it while the clusters are still distinct).
			if mergedCanonicals, err = s.repo.canonicalsFor(ctx, tx, env.TenantID, losers); err == nil {
				err = s.repo.mergeClusters(ctx, tx, env.TenantID, survivor, losers, env.EventID)
				merge = true
			}
		}
	}
	if err != nil {
		return Result{}, err
	}

	if err := s.repo.addMembers(ctx, tx, env.TenantID, survivor, nodeIDs, "event:"+env.Type); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit: %w", err)
	}
	return Result{ClusterID: survivor, CanonicalUserID: canonical, MergeOccurred: merge, MergedCanonicalIDs: mergedCanonicals}, nil
}

func (s *Service) emit(ctx context.Context, env events.Envelope, res Result) error {
	payload := IdentityResolved{
		EventType:         "identity_resolved",
		TenantID:          env.TenantID,
		EventID:           env.EventID,
		IdentityClusterID:      res.ClusterID,
		CanonicalUserID:        res.CanonicalUserID,
		MergeOccurred:          res.MergeOccurred,
		MergedCanonicalUserIDs: res.MergedCanonicalIDs,
		ResolvedAt:             time.Now().UTC(),
		Event:                  env,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal identity_resolved: %w", err)
	}
	key := env.TenantID.String() + "|" + res.CanonicalUserID
	return s.pub.Publish(ctx, s.topic, key, b)
}

func without(ids []uuid.UUID, exclude uuid.UUID) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if id != exclude {
			out = append(out, id)
		}
	}
	return out
}
