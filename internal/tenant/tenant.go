// Package tenant manages CDP tenants — the top-level isolation boundary.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/audit"
)

// Status values for a tenant.
const (
	StatusActive = "active"
)

// ErrNotFound is returned when a tenant does not exist.
var ErrNotFound = errors.New("tenant not found")

// Tenant is the aggregate root of all business data.
type Tenant struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Repository persists tenants.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository constructs a Repository.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Insert stores a new tenant.
func (r *Repository) Insert(ctx context.Context, t Tenant) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO tenant (id, name, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $4)`,
		t.ID, t.Name, t.Status, t.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert tenant: %w", err)
	}
	return nil
}

// Get loads a tenant by ID.
func (r *Repository) Get(ctx context.Context, id uuid.UUID) (Tenant, error) {
	var t Tenant
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, status, created_at, updated_at FROM tenant WHERE id = $1`, id).
		Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("get tenant: %w", err)
	}
	return t, nil
}

// List returns all tenants, newest first (super-admin browse; capped at 500).
func (r *Repository) List(ctx context.Context) ([]Tenant, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, status, created_at, updated_at FROM tenant ORDER BY created_at DESC LIMIT 500`)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()
	out := []Tenant{}
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Service holds tenant business logic.
type Service struct {
	repo  *Repository
	audit *audit.Recorder
	now   func() time.Time
}

// NewService constructs a Service.
func NewService(repo *Repository, recorder *audit.Recorder) *Service {
	return &Service{repo: repo, audit: recorder, now: time.Now}
}

// Create makes a new active tenant and records an audit entry.
func (s *Service) Create(ctx context.Context, name string) (Tenant, error) {
	if name == "" {
		return Tenant{}, errors.New("name is required")
	}
	t := Tenant{
		ID:        uuid.New(),
		Name:      name,
		Status:    StatusActive,
		CreatedAt: s.now().UTC(),
	}
	t.UpdatedAt = t.CreatedAt
	if err := s.repo.Insert(ctx, t); err != nil {
		return Tenant{}, err
	}
	if err := s.audit.Record(ctx, audit.Entry{
		TenantID:     &t.ID,
		ActorType:    audit.ActorAdmin,
		Action:       "create",
		ResourceType: "tenant",
		ResourceID:   t.ID.String(),
		After:        t,
	}); err != nil {
		return Tenant{}, fmt.Errorf("audit tenant create: %w", err)
	}
	return t, nil
}

// Get loads a tenant by ID.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (Tenant, error) {
	return s.repo.Get(ctx, id)
}

// List returns all tenants for the super-admin browse view.
func (s *Service) List(ctx context.Context) ([]Tenant, error) {
	return s.repo.List(ctx)
}
