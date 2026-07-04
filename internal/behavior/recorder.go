// Package behavior records durable per-profile behavioral events for Level 3
// stateful segmentation. See docs/cdp/16-stateful-segmentation.md.
package behavior

import (
	"bytes"
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dinhphu28/osscdp/internal/events"
)

// Recorder appends behavioral_event rows inside the caller's profile-update tx,
// so the append shares the profile's idempotency ledger (exactly-once counters).
// (The BehaviorEventsAppended metric is wired post-commit in Phase 8, not here —
// firing it inside the tx would over-count on rollback.)
type Recorder struct {
	// PropsGate (nil-safe) decides whether verbatim event props may be persisted. When
	// it denies, the event is still counted but props_json is dropped, so an opted-out
	// or erased subject's PII cannot be (re-)captured via behavioural writes.
	PropsGate PropsGate
}

// PropsGate authorizes persisting behavioral props for a profile.
type PropsGate interface {
	Allowed(ctx context.Context, tx pgx.Tx, tenantID, profileID uuid.UUID) (bool, error)
}

// ConsentPropsGate drops behavioral props when the profile has an explicit 'denied'
// analytics consent (channel-agnostic). Granted/absent consent stores props, matching
// the system's default-allow consent model.
type ConsentPropsGate struct{}

// Allowed reports false only when analytics consent is explicitly denied.
func (ConsentPropsGate) Allowed(ctx context.Context, tx pgx.Tx, tenantID, profileID uuid.UUID) (bool, error) {
	var denied bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM customer_consent WHERE tenant_id=$1 AND customer_profile_id=$2 AND purpose='analytics' AND status='denied')`,
		tenantID, profileID).Scan(&denied)
	return !denied, err
}

// NewRecorder constructs a Recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// Append writes one behavioral_event row inside tx. It is a no-op for anything
// other than a track event with a non-empty event_name (identify/alias carry no
// behavior). occurred_at is clamped to LEAST(Timestamp, ReceivedAt) so a spoofed
// future client timestamp cannot poison windows or defeat retention. The insert
// is idempotent by (profile, event_id): occurred_at collapses to a per-delivery
// ReceivedAt when the client timestamp is in the future, so it is not a reliable
// dedup key — we guard on event_id (like the merge fold), which is stable.
func (r *Recorder) Append(ctx context.Context, tx pgx.Tx, profileID uuid.UUID, env events.Envelope) error {
	if env.Type != events.TypeTrack || env.EventName == "" {
		return nil
	}
	occurredAt := env.Timestamp
	if env.ReceivedAt.Before(occurredAt) {
		occurredAt = env.ReceivedAt
	}
	var props any
	if p := bytes.TrimSpace(env.Properties); len(p) > 0 && !bytes.Equal(p, []byte("null")) {
		props = []byte(env.Properties)
	}
	// Consent gate: drop the verbatim props for an opted-out subject (still count it).
	if props != nil && r.PropsGate != nil {
		allowed, err := r.PropsGate.Allowed(ctx, tx, env.TenantID, profileID)
		if err != nil {
			return fmt.Errorf("props consent gate: %w", err)
		}
		if !allowed {
			props = nil
		}
	}
	ct, err := tx.Exec(ctx, `
		INSERT INTO behavioral_event
			(tenant_id, customer_profile_id, event_id, event_name, occurred_at, props_json)
		SELECT $1,$2,$3,$4,$5,$6
		WHERE NOT EXISTS (
			SELECT 1 FROM behavioral_event
			WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_id=$3)`,
		env.TenantID, profileID, env.EventID, env.EventName, occurredAt, props)
	if err != nil {
		return fmt.Errorf("append behavioral_event: %w", err)
	}
	// Only roll the hourly bucket forward when the log row was actually new — the
	// bucket aggregates and has no event_id, so a redelivery must not re-increment it.
	if ct.RowsAffected() == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO profile_behavior_bucket
			(tenant_id, customer_profile_id, event_name, bucket_start, count, first_at, last_at)
		VALUES ($1,$2,$3, date_trunc('hour', $4::timestamptz), 1, $4, $4)
		ON CONFLICT (tenant_id, customer_profile_id, event_name, bucket_start)
		DO UPDATE SET count    = profile_behavior_bucket.count + 1,
		              first_at = LEAST(profile_behavior_bucket.first_at, $4),
		              last_at  = GREATEST(profile_behavior_bucket.last_at, $4)`,
		env.TenantID, profileID, env.EventName, occurredAt); err != nil {
		return fmt.Errorf("upsert behavior bucket: %w", err)
	}
	return nil
}
