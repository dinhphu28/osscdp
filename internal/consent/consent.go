// Package consent stores per-customer consent by channel and purpose, and is
// consulted before activation. See docs/cdp/08-governance-security-observability.md.
package consent

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Channels.
const (
	ChannelEmail   = "email"
	ChannelSMS     = "sms"
	ChannelPush    = "push"
	ChannelAds     = "ads"
	ChannelWebhook = "webhook"
)

// Purposes.
const (
	PurposeMarketing       = "marketing"
	PurposeAnalytics       = "analytics"
	PurposePersonalization = "personalization"
	PurposeTransactional   = "transactional"
)

// Statuses.
const (
	StatusGranted = "granted"
	StatusDenied  = "denied"
	StatusUnknown = "unknown"
)

// Record is a single consent entry.
type Record struct {
	Channel   string    `json:"channel"`
	Purpose   string    `json:"purpose"`
	Status    string    `json:"status"`
	Source    string    `json:"source,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Repo persists consent.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// Set upserts a consent record.
func (r *Repo) Set(ctx context.Context, tenantID, profileID uuid.UUID, channel, purpose, status, source string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO customer_consent (id, tenant_id, customer_profile_id, channel, purpose, status, source, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7, now())
		ON CONFLICT (tenant_id, customer_profile_id, channel, purpose)
		DO UPDATE SET status=$6, source=$7, updated_at=now()`,
		uuid.New(), tenantID, profileID, channel, purpose, status, nullString(source))
	if err != nil {
		return fmt.Errorf("set consent: %w", err)
	}
	return nil
}

// Get returns the consent status for a (channel, purpose), defaulting to unknown.
func (r *Repo) Get(ctx context.Context, tenantID, profileID uuid.UUID, channel, purpose string) (string, error) {
	var status string
	err := r.pool.QueryRow(ctx,
		`SELECT status FROM customer_consent WHERE tenant_id=$1 AND customer_profile_id=$2 AND channel=$3 AND purpose=$4`,
		tenantID, profileID, channel, purpose).Scan(&status)
	if err != nil {
		return StatusUnknown, nil //nolint:nilerr // absence means unknown
	}
	return status, nil
}

// ListForProfile returns all consent records for a profile.
func (r *Repo) ListForProfile(ctx context.Context, tenantID, profileID uuid.UUID) ([]Record, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT channel, purpose, status, COALESCE(source,''), updated_at
		 FROM customer_consent WHERE tenant_id=$1 AND customer_profile_id=$2 ORDER BY channel, purpose`,
		tenantID, profileID)
	if err != nil {
		return nil, fmt.Errorf("list consent: %w", err)
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var rec Record
		if err := rows.Scan(&rec.Channel, &rec.Purpose, &rec.Status, &rec.Source, &rec.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
