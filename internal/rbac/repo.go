package rbac

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status values.
const (
	StatusActive  = "active"
	StatusRevoked = "revoked"
)

// TokenSummary is the read model for listing admin tokens. It never carries the
// token hash.
type TokenSummary struct {
	ID        uuid.UUID  `json:"id"`
	Name      string     `json:"name"`
	Role      string     `json:"role"`
	TenantID  *uuid.UUID `json:"tenant_id"`
	Status    string     `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
}

// ErrNotFound is returned when a token does not resolve to an active admin token.
var ErrNotFound = errors.New("admin token not found")

// Repo persists admin tokens.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// CreateToken mints an admin token and returns the plaintext once.
func (r *Repo) CreateToken(ctx context.Context, tenantID *uuid.UUID, name, role string) (string, error) {
	plaintext, hash, err := GenerateToken()
	if err != nil {
		return "", err
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO admin_token (id, tenant_id, name, role, token_hash, status) VALUES ($1,$2,$3,$4,$5,$6)`,
		uuid.New(), tenantID, name, role, hash, StatusActive)
	if err != nil {
		return "", fmt.Errorf("insert admin token: %w", err)
	}
	return plaintext, nil
}

// ListTokens returns admin tokens, newest first, capped at 500. When tenantID is
// non-nil, only that tenant's tokens are returned (used to scope TENANT_ADMIN to
// its own tenant); nil returns all tenants (SUPER_ADMIN). The token hash is never
// selected or returned.
func (r *Repo) ListTokens(ctx context.Context, tenantID *uuid.UUID) ([]TokenSummary, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, role, tenant_id, status, created_at
		FROM admin_token
		WHERE ($1::uuid IS NULL OR tenant_id = $1)
		ORDER BY created_at DESC LIMIT 500`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list admin tokens: %w", err)
	}
	defer rows.Close()
	out := []TokenSummary{}
	for rows.Next() {
		var t TokenSummary
		if err := rows.Scan(&t.ID, &t.Name, &t.Role, &t.TenantID, &t.Status, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetTokenTenant returns the tenant_id of an admin token (nil for a cross-tenant
// super-admin token). Used to enforce tenant scope on revoke. Returns ErrNotFound
// if the token does not exist.
func (r *Repo) GetTokenTenant(ctx context.Context, id uuid.UUID) (*uuid.UUID, error) {
	var tenantID *uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT tenant_id FROM admin_token WHERE id=$1`, id).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get admin token tenant: %w", err)
	}
	return tenantID, nil
}

// RevokeToken marks an ACTIVE admin token revoked, immediately invalidating it
// (auth resolves only active tokens). The active-status guard makes revoke
// idempotent: a second revoke matches no row and returns ErrNotFound (so the
// caller 404s and no duplicate audit entry is written). Returns ErrNotFound if
// the token is unknown or already revoked.
func (r *Repo) RevokeToken(ctx context.Context, id uuid.UUID) error {
	ct, err := r.pool.Exec(ctx,
		`UPDATE admin_token SET status=$2, updated_at=now() WHERE id=$1 AND status=$3`,
		id, StatusRevoked, StatusActive)
	if err != nil {
		return fmt.Errorf("revoke admin token: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// FindByTokenHash resolves an active token hash to a principal.
func (r *Repo) FindByTokenHash(ctx context.Context, hash string) (Principal, error) {
	var p Principal
	var tenantID *uuid.UUID
	err := r.pool.QueryRow(ctx,
		`SELECT role, tenant_id FROM admin_token WHERE token_hash=$1 AND status=$2`, hash, StatusActive).
		Scan(&p.Role, &tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrNotFound
	}
	if err != nil {
		return Principal{}, fmt.Errorf("find admin token: %w", err)
	}
	p.TenantID = tenantID
	return p, nil
}
