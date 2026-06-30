// Package source manages event sources and their API keys. tenant_id and
// source_id are always resolved from the API key, never trusted from a payload.
package source

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/audit"
)

// Status values for a source.
const (
	StatusActive   = "active"
	StatusDisabled = "disabled"
)

// Errors.
var (
	ErrNotFound       = errors.New("source not found")
	ErrDuplicateName  = errors.New("source name already exists for tenant")
	ErrTenantNotFound = errors.New("tenant not found")
)

// Source is an authenticated origin of events for a tenant.
type Source struct {
	ID        uuid.UUID `json:"id"`
	TenantID  uuid.UUID `json:"tenant_id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Repository persists sources.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository constructs a Repository.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Insert stores a source together with its API key hash.
func (r *Repository) Insert(ctx context.Context, s Source, apiKeyHash string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO source (id, tenant_id, name, type, status, api_key_hash, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
		s.ID, s.TenantID, s.Name, s.Type, s.Status, apiKeyHash, s.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "23505": // unique_violation
				return ErrDuplicateName
			case "23503": // foreign_key_violation
				return ErrTenantNotFound
			}
		}
		return fmt.Errorf("insert source: %w", err)
	}
	return nil
}

// FindByAPIKeyHash resolves the source that owns the given key hash. Only active
// sources are returned. The lookup is global-by-hash but each hash maps to
// exactly one (tenant, source) row, so tenant scope is preserved by the result.
func (r *Repository) FindByAPIKeyHash(ctx context.Context, apiKeyHash string) (Source, error) {
	var s Source
	err := r.pool.QueryRow(ctx, `
		SELECT id, tenant_id, name, type, status, created_at, updated_at
		FROM source WHERE api_key_hash = $1 AND status = $2`, apiKeyHash, StatusActive).
		Scan(&s.ID, &s.TenantID, &s.Name, &s.Type, &s.Status, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Source{}, ErrNotFound
	}
	if err != nil {
		return Source{}, fmt.Errorf("find source by api key hash: %w", err)
	}
	return s, nil
}

// CreateResult is returned once on source creation and includes the plaintext key.
type CreateResult struct {
	Source Source `json:"source"`
	APIKey string `json:"api_key"`
}

// Service holds source business logic.
type Service struct {
	repo  *Repository
	audit *audit.Recorder
	now   func() time.Time
}

// NewService constructs a Service.
func NewService(repo *Repository, recorder *audit.Recorder) *Service {
	return &Service{repo: repo, audit: recorder, now: time.Now}
}

// Create makes a new source under a tenant, generates an API key, persists the
// hash, records an audit entry, and returns the plaintext key exactly once.
func (s *Service) Create(ctx context.Context, tenantID uuid.UUID, name, typ string) (CreateResult, error) {
	if name == "" || typ == "" {
		return CreateResult{}, errors.New("name and type are required")
	}
	plaintext, hash, err := GenerateAPIKey()
	if err != nil {
		return CreateResult{}, err
	}
	src := Source{
		ID:        uuid.New(),
		TenantID:  tenantID,
		Name:      name,
		Type:      typ,
		Status:    StatusActive,
		CreatedAt: s.now().UTC(),
	}
	src.UpdatedAt = src.CreatedAt
	if err := s.repo.Insert(ctx, src, hash); err != nil {
		return CreateResult{}, err
	}
	if err := s.audit.Record(ctx, audit.Entry{
		TenantID:     &tenantID,
		ActorType:    audit.ActorAdmin,
		Action:       "create",
		ResourceType: "source",
		ResourceID:   src.ID.String(),
		After:        src, // never includes the API key
	}); err != nil {
		return CreateResult{}, fmt.Errorf("audit source create: %w", err)
	}
	return CreateResult{Source: src, APIKey: plaintext}, nil
}

// Authenticate resolves a source from an API key plaintext.
func (s *Service) Authenticate(ctx context.Context, plaintext string) (Source, error) {
	if !LooksLikeAPIKey(plaintext) {
		return Source{}, ErrNotFound
	}
	return s.repo.FindByAPIKeyHash(ctx, HashAPIKey(plaintext))
}
