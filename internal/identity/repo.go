package identity

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so repo ops compose
// inside a single transaction.
type querier interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Repo holds identity-graph persistence ops.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

// upsertNode inserts or returns the identity node for (tenant, namespace, hash).
func (r *Repo) upsertNode(ctx context.Context, q querier, tenantID uuid.UUID, namespace, valueHash, valueEncrypted string) (uuid.UUID, error) {
	var id uuid.UUID
	// value_encrypted is filled opportunistically: set on first insert, and
	// backfilled on re-ingest for a node that predates encryption (COALESCE keeps
	// the existing ciphertext so the randomized GCM output does not churn).
	err := q.QueryRow(ctx, `
		INSERT INTO identity_node (id, tenant_id, namespace, value_hash, value_encrypted)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, namespace, value_hash) DO UPDATE
		SET updated_at = now(),
		    value_encrypted = COALESCE(identity_node.value_encrypted, EXCLUDED.value_encrypted)
		RETURNING id`,
		uuid.New(), tenantID, namespace, valueHash, nullString(valueEncrypted)).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("upsert identity_node: %w", err)
	}
	return id, nil
}

// clustersForNodes returns the distinct cluster ids the given nodes belong to.
func (r *Repo) clustersForNodes(ctx context.Context, q querier, tenantID uuid.UUID, nodeIDs []uuid.UUID) ([]uuid.UUID, error) {
	rows, err := q.Query(ctx,
		`SELECT DISTINCT cluster_id FROM identity_cluster_member WHERE tenant_id = $1 AND identity_node_id = ANY($2)`,
		tenantID, nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("clusters for nodes: %w", err)
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// lockClusters takes row locks on the candidate clusters in a deadlock-safe
// order so concurrent merges serialize.
func (r *Repo) lockClusters(ctx context.Context, q querier, tenantID uuid.UUID, clusterIDs []uuid.UUID) error {
	rows, err := q.Query(ctx,
		`SELECT id FROM identity_cluster WHERE tenant_id = $1 AND id = ANY($2) ORDER BY id FOR UPDATE`,
		tenantID, clusterIDs)
	if err != nil {
		return fmt.Errorf("lock clusters: %w", err)
	}
	rows.Close()
	return rows.Err()
}

// createCluster makes a new active cluster with a CDP-generated canonical id.
func (r *Repo) createCluster(ctx context.Context, q querier, tenantID uuid.UUID) (uuid.UUID, string, error) {
	v7, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, "", err
	}
	canonical := "customer_" + v7.String()
	id := uuid.New()
	_, err = q.Exec(ctx,
		`INSERT INTO identity_cluster (id, tenant_id, canonical_user_id, status) VALUES ($1, $2, $3, $4)`,
		id, tenantID, canonical, ClusterActive)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("create cluster: %w", err)
	}
	return id, canonical, nil
}

// pickSurvivor chooses the oldest active cluster among the candidates and
// returns its id + canonical_user_id.
func (r *Repo) pickSurvivor(ctx context.Context, q querier, tenantID uuid.UUID, clusterIDs []uuid.UUID) (uuid.UUID, string, error) {
	var id uuid.UUID
	var canonical string
	err := q.QueryRow(ctx, `
		SELECT id, canonical_user_id FROM identity_cluster
		WHERE tenant_id = $1 AND id = ANY($2)
		ORDER BY created_at, id LIMIT 1`,
		tenantID, clusterIDs).Scan(&id, &canonical)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("pick survivor: %w", err)
	}
	return id, canonical, nil
}

// canonicalsFor returns the canonical_user_ids for the given clusters (used to
// tell the profile worker which losers to reparent after a merge).
func (r *Repo) canonicalsFor(ctx context.Context, q querier, tenantID uuid.UUID, clusterIDs []uuid.UUID) ([]string, error) {
	rows, err := q.Query(ctx,
		`SELECT canonical_user_id FROM identity_cluster WHERE tenant_id = $1 AND id = ANY($2)`,
		tenantID, clusterIDs)
	if err != nil {
		return nil, fmt.Errorf("canonicals for clusters: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// canonicalFor returns the canonical_user_id for a cluster.
func (r *Repo) canonicalFor(ctx context.Context, q querier, tenantID, clusterID uuid.UUID) (string, error) {
	var canonical string
	err := q.QueryRow(ctx,
		`SELECT canonical_user_id FROM identity_cluster WHERE tenant_id = $1 AND id = $2`,
		tenantID, clusterID).Scan(&canonical)
	if err != nil {
		return "", fmt.Errorf("canonical for cluster: %w", err)
	}
	return canonical, nil
}

// mergeClusters moves losers' members to the survivor, marks losers merged, and
// records merge history.
func (r *Repo) mergeClusters(ctx context.Context, q querier, tenantID, survivor uuid.UUID, losers []uuid.UUID, eventID string) error {
	if _, err := q.Exec(ctx,
		`UPDATE identity_cluster_member SET cluster_id = $1 WHERE tenant_id = $2 AND cluster_id = ANY($3)`,
		survivor, tenantID, losers); err != nil {
		return fmt.Errorf("move members: %w", err)
	}
	if _, err := q.Exec(ctx,
		`UPDATE identity_cluster SET status = $1, updated_at = now() WHERE tenant_id = $2 AND id = ANY($3)`,
		ClusterMerged, tenantID, losers); err != nil {
		return fmt.Errorf("mark merged: %w", err)
	}
	for _, loser := range losers {
		if _, err := q.Exec(ctx, `
			INSERT INTO identity_merge_history (id, tenant_id, from_cluster_id, to_cluster_id, reason, event_id)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			uuid.New(), tenantID, loser, survivor, "deterministic_match", nullString(eventID)); err != nil {
			return fmt.Errorf("merge history: %w", err)
		}
	}
	return nil
}

// addMembers attaches nodes to a cluster, leaving any already-assigned node in
// place (after a merge it is already on the survivor).
func (r *Repo) addMembers(ctx context.Context, q querier, tenantID, clusterID uuid.UUID, nodeIDs []uuid.UUID, source string) error {
	for _, nid := range nodeIDs {
		if _, err := q.Exec(ctx, `
			INSERT INTO identity_cluster_member (tenant_id, identity_node_id, cluster_id, source)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, identity_node_id) DO NOTHING`,
			tenantID, nid, clusterID, source); err != nil {
			return fmt.Errorf("add member: %w", err)
		}
	}
	return nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
