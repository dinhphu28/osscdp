package segment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status values.
const (
	SegmentActive    = "active"
	VersionActive    = "active"
	MembershipActive = "active"
	MembershipExited = "exited"
)

// Errors.
var (
	ErrNotFound      = errors.New("segment not found")
	ErrDuplicateName = errors.New("segment name already exists for tenant")
)

// Segment is a saved audience definition.
type Segment struct {
	ID               uuid.UUID  `json:"id"`
	TenantID         uuid.UUID  `json:"tenant_id"`
	Name             string     `json:"name"`
	Description      string     `json:"description,omitempty"`
	Status           string     `json:"status"`
	CurrentVersionID *uuid.UUID `json:"current_version_id,omitempty"`
	CurrentVersion   int        `json:"current_version"`
	Rule             Rule       `json:"rule"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// ActiveVersion is an active segment's current rule version.
type ActiveVersion struct {
	SegmentID uuid.UUID
	VersionID uuid.UUID
	Version   int
	Rule      Rule
}

// Membership is a customer's membership in a segment.
type Membership struct {
	SegmentID         uuid.UUID  `json:"segment_id"`
	CustomerProfileID uuid.UUID  `json:"customer_profile_id"`
	Status            string     `json:"status"`
	EnteredAt         *time.Time `json:"entered_at,omitempty"`
	ExitedAt          *time.Time `json:"exited_at,omitempty"`
	LastEvaluatedAt   time.Time  `json:"last_evaluated_at"`
	Version           int64      `json:"version"`
}

// Repo persists segments, versions, and membership.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// CreateSegment creates a segment with version 1 and points current at it.
func (r *Repo) CreateSegment(ctx context.Context, tenantID uuid.UUID, name, description string, rule Rule) (Segment, error) {
	ruleJSON, _ := json.Marshal(rule)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Segment{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	segID := uuid.New()
	_, err = tx.Exec(ctx,
		`INSERT INTO segment (id, tenant_id, name, description, status) VALUES ($1,$2,$3,$4,$5)`,
		segID, tenantID, name, nullString(description), SegmentActive)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Segment{}, ErrDuplicateName
		}
		return Segment{}, fmt.Errorf("insert segment: %w", err)
	}
	verID := uuid.New()
	if _, err := tx.Exec(ctx,
		`INSERT INTO segment_version (id, tenant_id, segment_id, version, rule_json, status) VALUES ($1,$2,$3,1,$4,$5)`,
		verID, tenantID, segID, ruleJSON, VersionActive); err != nil {
		return Segment{}, fmt.Errorf("insert version: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE segment SET current_version_id=$1, updated_at=now() WHERE id=$2`, verID, segID); err != nil {
		return Segment{}, fmt.Errorf("set current version: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Segment{}, err
	}
	return r.GetSegment(ctx, tenantID, segID)
}

// UpdateSegment creates a new version and repoints current.
func (r *Repo) UpdateSegment(ctx context.Context, tenantID, segmentID uuid.UUID, description string, rule Rule) (Segment, error) {
	ruleJSON, _ := json.Marshal(rule)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Segment{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var maxVer int
	err = tx.QueryRow(ctx,
		`SELECT coalesce(max(version),0) FROM segment_version WHERE tenant_id=$1 AND segment_id=$2`,
		tenantID, segmentID).Scan(&maxVer)
	if err != nil {
		return Segment{}, fmt.Errorf("max version: %w", err)
	}
	if maxVer == 0 {
		return Segment{}, ErrNotFound
	}
	verID := uuid.New()
	if _, err := tx.Exec(ctx,
		`INSERT INTO segment_version (id, tenant_id, segment_id, version, rule_json, status) VALUES ($1,$2,$3,$4,$5,$6)`,
		verID, tenantID, segmentID, maxVer+1, ruleJSON, VersionActive); err != nil {
		return Segment{}, fmt.Errorf("insert version: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE segment SET current_version_id=$1, description=coalesce($2, description), updated_at=now() WHERE tenant_id=$3 AND id=$4`,
		verID, nullString(description), tenantID, segmentID); err != nil {
		return Segment{}, fmt.Errorf("repoint current: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Segment{}, err
	}
	return r.GetSegment(ctx, tenantID, segmentID)
}

// GetSegment loads a segment with its current rule.
func (r *Repo) GetSegment(ctx context.Context, tenantID, segmentID uuid.UUID) (Segment, error) {
	var s Segment
	var ruleJSON []byte
	err := r.pool.QueryRow(ctx, `
		SELECT s.id, s.tenant_id, s.name, COALESCE(s.description, ''), s.status, s.current_version_id,
		       v.version, v.rule_json, s.created_at, s.updated_at
		FROM segment s
		JOIN segment_version v ON v.id = s.current_version_id
		WHERE s.tenant_id=$1 AND s.id=$2`,
		tenantID, segmentID).
		Scan(&s.ID, &s.TenantID, &s.Name, &s.Description, &s.Status, &s.CurrentVersionID,
			&s.CurrentVersion, &ruleJSON, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Segment{}, ErrNotFound
	}
	if err != nil {
		return Segment{}, fmt.Errorf("get segment: %w", err)
	}
	_ = json.Unmarshal(ruleJSON, &s.Rule)
	return s, nil
}

// ActiveSegmentVersions returns the current rule version of every active segment.
func (r *Repo) ActiveSegmentVersions(ctx context.Context, tenantID uuid.UUID) ([]ActiveVersion, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT s.id, v.id, v.version, v.rule_json
		FROM segment s
		JOIN segment_version v ON v.id = s.current_version_id
		WHERE s.tenant_id=$1 AND s.status=$2`,
		tenantID, SegmentActive)
	if err != nil {
		return nil, fmt.Errorf("active segments: %w", err)
	}
	defer rows.Close()
	var out []ActiveVersion
	for rows.Next() {
		var av ActiveVersion
		var ruleJSON []byte
		if err := rows.Scan(&av.SegmentID, &av.VersionID, &av.Version, &ruleJSON); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(ruleJSON, &av.Rule); err != nil {
			return nil, fmt.Errorf("unmarshal rule: %w", err)
		}
		out = append(out, av)
	}
	return out, rows.Err()
}

// MembershipStatus returns the current status of a membership ("" if none).
func (r *Repo) MembershipStatus(ctx context.Context, tenantID, segmentID, profileID uuid.UUID) (string, error) {
	var status string
	err := r.pool.QueryRow(ctx,
		`SELECT status FROM segment_membership WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tenantID, segmentID, profileID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("membership status: %w", err)
	}
	return status, nil
}

// Enter inserts or reactivates a membership.
func (r *Repo) Enter(ctx context.Context, tenantID, segmentID, profileID uuid.UUID, version int) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO segment_membership
			(tenant_id, segment_id, customer_profile_id, status, entered_at, exited_at, last_evaluated_at, version)
		VALUES ($1,$2,$3,$4, now(), NULL, now(), $5)
		ON CONFLICT (tenant_id, segment_id, customer_profile_id)
		DO UPDATE SET status=$4, entered_at=now(), exited_at=NULL, last_evaluated_at=now(), version=$5`,
		tenantID, segmentID, profileID, MembershipActive, version)
	if err != nil {
		return fmt.Errorf("enter membership: %w", err)
	}
	return nil
}

// Exit marks a membership exited.
func (r *Repo) Exit(ctx context.Context, tenantID, segmentID, profileID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE segment_membership SET status=$4, exited_at=now(), last_evaluated_at=now()
		WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tenantID, segmentID, profileID, MembershipExited)
	if err != nil {
		return fmt.Errorf("exit membership: %w", err)
	}
	return nil
}

// TouchEvaluated bumps last_evaluated_at for an unchanged membership.
func (r *Repo) TouchEvaluated(ctx context.Context, tenantID, segmentID, profileID uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE segment_membership SET last_evaluated_at=now() WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tenantID, segmentID, profileID)
	if err != nil {
		return fmt.Errorf("touch membership: %w", err)
	}
	return nil
}

// ListMembers returns active memberships of a segment.
func (r *Repo) ListMembers(ctx context.Context, tenantID, segmentID uuid.UUID) ([]Membership, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT segment_id, customer_profile_id, status, entered_at, exited_at, last_evaluated_at, version
		FROM segment_membership
		WHERE tenant_id=$1 AND segment_id=$2 AND status=$3
		ORDER BY entered_at DESC LIMIT 500`,
		tenantID, segmentID, MembershipActive)
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	defer rows.Close()
	out := []Membership{}
	for rows.Next() {
		var m Membership
		if err := rows.Scan(&m.SegmentID, &m.CustomerProfileID, &m.Status, &m.EnteredAt, &m.ExitedAt, &m.LastEvaluatedAt, &m.Version); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
