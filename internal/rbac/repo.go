package rbac

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status values.
const (
	StatusActive = "active"
)

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
