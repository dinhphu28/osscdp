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

// deleteProfileCascade removes a profile and all its customer-scoped child rows
// in FK-safe order (children before parent). Used when a cluster merge reparents
// a loser profile into the survivor. Identity nodes/clusters are owned by the
// identity package and are not touched here — the merge already moved the nodes
// to the survivor cluster.
func (r *Repo) deleteProfileCascade(ctx context.Context, q querier, tenantID, profileID uuid.UUID) error {
	children := []string{
		`DELETE FROM activation_delivery WHERE tenant_id=$1 AND customer_profile_id=$2`,
		`DELETE FROM activation_task WHERE tenant_id=$1 AND customer_profile_id=$2`,
		`DELETE FROM customer_profile_history WHERE tenant_id=$1 AND customer_profile_id=$2`,
		`DELETE FROM segment_membership WHERE tenant_id=$1 AND customer_profile_id=$2`,
		`DELETE FROM customer_consent WHERE tenant_id=$1 AND customer_profile_id=$2`,
	}
	for _, sql := range children {
		if _, err := q.Exec(ctx, sql, tenantID, profileID); err != nil {
			return fmt.Errorf("reparent delete child: %w", err)
		}
	}
	if _, err := q.Exec(ctx, `DELETE FROM customer_profile WHERE tenant_id=$1 AND id=$2`, tenantID, profileID); err != nil {
		return fmt.Errorf("reparent delete profile: %w", err)
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
