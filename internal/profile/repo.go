package profile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a profile does not exist.
var ErrNotFound = errors.New("profile not found")

type querier interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Repo persists customer profiles.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

const profileCols = `id, tenant_id, canonical_user_id, identity_cluster_id,
	traits_json, computed_attributes_json, first_seen_at, last_seen_at, version, created_at, updated_at`

func scanProfile(row pgx.Row) (Profile, error) {
	var p Profile
	var traits, computed []byte
	if err := row.Scan(&p.ID, &p.TenantID, &p.CanonicalUserID, &p.IdentityClusterID,
		&traits, &computed, &p.FirstSeenAt, &p.LastSeenAt, &p.Version, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return Profile{}, err
	}
	p.Traits = decodeMap(traits)
	p.ComputedAttributes = decodeMap(computed)
	return p, nil
}

func decodeMap(b []byte) map[string]any {
	m := map[string]any{}
	if len(b) > 0 {
		_ = json.Unmarshal(b, &m)
	}
	return m
}

// getForUpdate loads a profile by canonical id with a row lock.
func (r *Repo) getForUpdate(ctx context.Context, q querier, tenantID uuid.UUID, canonicalUserID string) (Profile, bool, error) {
	row := q.QueryRow(ctx,
		`SELECT `+profileCols+` FROM customer_profile WHERE tenant_id=$1 AND canonical_user_id=$2 FOR UPDATE`,
		tenantID, canonicalUserID)
	p, err := scanProfile(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, false, nil
	}
	if err != nil {
		return Profile{}, false, fmt.Errorf("get profile for update: %w", err)
	}
	return p, true, nil
}

// create inserts a new profile. Returns ErrConflict-free; relies on caller lock.
func (r *Repo) create(ctx context.Context, q querier, p Profile) error {
	traits, _ := json.Marshal(p.Traits)
	computed, _ := json.Marshal(p.ComputedAttributes)
	_, err := q.Exec(ctx, `
		INSERT INTO customer_profile
			(id, tenant_id, canonical_user_id, identity_cluster_id, traits_json,
			 computed_attributes_json, first_seen_at, last_seen_at, version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		p.ID, p.TenantID, p.CanonicalUserID, p.IdentityClusterID, traits, computed,
		p.FirstSeenAt, p.LastSeenAt, p.Version)
	if err != nil {
		return fmt.Errorf("insert profile: %w", err)
	}
	return nil
}

// update writes merged state and bumps version (optimistic check on version).
func (r *Repo) update(ctx context.Context, q querier, p Profile, fromVersion int64) error {
	traits, _ := json.Marshal(p.Traits)
	computed, _ := json.Marshal(p.ComputedAttributes)
	ct, err := q.Exec(ctx, `
		UPDATE customer_profile
		SET traits_json=$1, computed_attributes_json=$2, first_seen_at=$3, last_seen_at=$4,
		    version=$5, updated_at=now()
		WHERE id=$6 AND version=$7`,
		traits, computed, p.FirstSeenAt, p.LastSeenAt, p.Version, p.ID, fromVersion)
	if err != nil {
		return fmt.Errorf("update profile: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("optimistic lock conflict on profile %s", p.ID)
	}
	return nil
}

// resolveSurvivorCluster follows the merge chain from clusterID to the surviving
// active cluster, returning its canonical_user_id + id and whether a redirect is
// needed. redirected is false when clusterID is itself active (or unknown) — the
// caller then behaves as before. Used to stop a late/duplicate loser event from
// resurrecting a profile for a cluster that has already been merged away.
func (r *Repo) resolveSurvivorCluster(ctx context.Context, q querier, tenantID, clusterID uuid.UUID) (canonical string, survivor uuid.UUID, redirected bool, err error) {
	err = q.QueryRow(ctx, `
		WITH RECURSIVE chain AS (
			SELECT c.id, c.canonical_user_id, c.status
			FROM identity_cluster c
			WHERE c.tenant_id=$1 AND c.id=$2
			UNION ALL
			SELECT c.id, c.canonical_user_id, c.status
			FROM identity_merge_history h
			JOIN identity_cluster c ON c.tenant_id=h.tenant_id AND c.id=h.to_cluster_id
			JOIN chain ON chain.id=h.from_cluster_id
			WHERE h.tenant_id=$1
		)
		SELECT id, canonical_user_id FROM chain WHERE status='active' LIMIT 1`,
		tenantID, clusterID).Scan(&survivor, &canonical)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", uuid.Nil, false, nil // unknown/no active survivor — caller creates as before
	}
	if err != nil {
		return "", uuid.Nil, false, fmt.Errorf("resolve survivor cluster: %w", err)
	}
	return canonical, survivor, survivor != clusterID, nil
}

// reparentProfileChildren migrates a loser profile's customer-scoped child rows
// to the survivor, then deletes the loser customer_profile row. Used when a
// cluster merge folds a loser profile into the survivor. Unlike a plain delete,
// this preserves the idempotency ledger, consent opt-outs, segment memberships,
// and activation history/queue — deleting only what the survivor already has.
// Identity nodes/clusters are owned by the identity package and untouched here.
func (r *Repo) reparentProfileChildren(ctx context.Context, q querier, tenantID, loserID, survivorID uuid.UUID) error {
	// customer_profile_history is the idempotency ledger (alreadyApplied checks it
	// by event_id). Re-key rows whose event_id the survivor lacks so the survivor
	// inherits the loser's dedup records; leftovers are dropped below.
	if _, err := q.Exec(ctx, `
		UPDATE customer_profile_history h
		SET customer_profile_id=$3
		WHERE h.tenant_id=$1 AND h.customer_profile_id=$2
		  AND NOT EXISTS (SELECT 1 FROM customer_profile_history s
		                  WHERE s.tenant_id=$1 AND s.customer_profile_id=$3 AND s.event_id=h.event_id)`,
		tenantID, loserID, survivorID); err != nil {
		return fmt.Errorf("reparent history: %w", err)
	}

	// customer_consent: merge with denied-wins so an opt-out is never weakened.
	if _, err := q.Exec(ctx, `
		INSERT INTO customer_consent (id, tenant_id, customer_profile_id, channel, purpose, status, source, updated_at)
		SELECT gen_random_uuid(), tenant_id, $3, channel, purpose, status, source, now()
		FROM customer_consent
		WHERE tenant_id=$1 AND customer_profile_id=$2
		ON CONFLICT (tenant_id, customer_profile_id, channel, purpose) DO UPDATE
		SET status = CASE WHEN customer_consent.status='denied' OR EXCLUDED.status='denied'
		                  THEN 'denied' ELSE EXCLUDED.status END,
		    updated_at = now()`,
		tenantID, loserID, survivorID); err != nil {
		return fmt.Errorf("reparent consent: %w", err)
	}

	// segment_membership: union onto the survivor; keep the survivor's row on conflict.
	if _, err := q.Exec(ctx, `
		UPDATE segment_membership m
		SET customer_profile_id=$3
		WHERE m.tenant_id=$1 AND m.customer_profile_id=$2
		  AND NOT EXISTS (SELECT 1 FROM segment_membership s
		                  WHERE s.tenant_id=$1 AND s.customer_profile_id=$3 AND s.segment_id=m.segment_id)`,
		tenantID, loserID, survivorID); err != nil {
		return fmt.Errorf("reparent memberships: %w", err)
	}

	// activation rows: re-key (idempotency_key is unchanged; no unique on
	// customer_profile_id). Re-keying instead of deleting keeps pending sends and
	// the delivery audit trail, and avoids an FK race with an in-flight sender.
	for _, tbl := range []string{"activation_delivery", "activation_task"} {
		if _, err := q.Exec(ctx,
			`UPDATE `+tbl+` SET customer_profile_id=$3 WHERE tenant_id=$1 AND customer_profile_id=$2`,
			tenantID, loserID, survivorID); err != nil {
			return fmt.Errorf("reparent %s: %w", tbl, err)
		}
	}

	// Drop the loser's leftover child rows that conflicted with the survivor
	// (children before parent for FK-safety), then the loser profile itself.
	for _, sql := range []string{
		`DELETE FROM customer_profile_history WHERE tenant_id=$1 AND customer_profile_id=$2`,
		`DELETE FROM segment_membership WHERE tenant_id=$1 AND customer_profile_id=$2`,
		`DELETE FROM customer_consent WHERE tenant_id=$1 AND customer_profile_id=$2`,
	} {
		if _, err := q.Exec(ctx, sql, tenantID, loserID); err != nil {
			return fmt.Errorf("reparent cleanup: %w", err)
		}
	}
	if _, err := q.Exec(ctx, `DELETE FROM customer_profile WHERE tenant_id=$1 AND id=$2`, tenantID, loserID); err != nil {
		return fmt.Errorf("reparent delete loser: %w", err)
	}
	return nil
}

// markApplied inserts the history/idempotency row. Returns false if the event
// was already applied to this profile.
func (r *Repo) markApplied(ctx context.Context, q querier, tenantID, profileID uuid.UUID, eventID, changeType string, before, after []byte) (bool, error) {
	ct, err := q.Exec(ctx, `
		INSERT INTO customer_profile_history
			(id, tenant_id, customer_profile_id, event_id, change_type, before_json, after_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (tenant_id, customer_profile_id, event_id) DO NOTHING`,
		uuid.New(), tenantID, profileID, eventID, changeType, before, after)
	if err != nil {
		return false, fmt.Errorf("insert profile history: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// alreadyApplied reports whether an event was already applied to a profile.
func (r *Repo) alreadyApplied(ctx context.Context, q querier, tenantID, profileID uuid.UUID, eventID string) (bool, error) {
	var exists bool
	err := q.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM customer_profile_history WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_id=$3)`,
		tenantID, profileID, eventID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check applied: %w", err)
	}
	return exists, nil
}

// GetByCanonical loads a profile by canonical_user_id (read-only).
func (r *Repo) GetByCanonical(ctx context.Context, tenantID uuid.UUID, canonicalUserID string) (Profile, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT `+profileCols+` FROM customer_profile WHERE tenant_id=$1 AND canonical_user_id=$2`,
		tenantID, canonicalUserID)
	p, err := scanProfile(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, fmt.Errorf("get profile: %w", err)
	}
	return p, nil
}

// GetByID loads a profile by its id (read-only), tenant-scoped.
func (r *Repo) GetByID(ctx context.Context, tenantID, id uuid.UUID) (Profile, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT `+profileCols+` FROM customer_profile WHERE tenant_id=$1 AND id=$2`, tenantID, id)
	p, err := scanProfile(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, fmt.Errorf("get profile by id: %w", err)
	}
	return p, nil
}

// ListByTrait returns profiles whose traits_json[key] equals value.
func (r *Repo) ListByTrait(ctx context.Context, tenantID uuid.UUID, key, value string) ([]Profile, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+profileCols+` FROM customer_profile WHERE tenant_id=$1 AND traits_json->>$2 = $3 ORDER BY updated_at DESC LIMIT 100`,
		tenantID, key, value)
	if err != nil {
		return nil, fmt.Errorf("list profiles by trait: %w", err)
	}
	defer rows.Close()
	var out []Profile
	for rows.Next() {
		p, err := scanProfile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
