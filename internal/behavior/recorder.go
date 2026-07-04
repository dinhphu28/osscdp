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
type Recorder struct{}

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
	if _, err := tx.Exec(ctx, `
		INSERT INTO behavioral_event
			(tenant_id, customer_profile_id, event_id, event_name, occurred_at, props_json)
		SELECT $1,$2,$3,$4,$5,$6
		WHERE NOT EXISTS (
			SELECT 1 FROM behavioral_event
			WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_id=$3)`,
		env.TenantID, profileID, env.EventID, env.EventName, occurredAt, props); err != nil {
		return fmt.Errorf("append behavioral_event: %w", err)
	}
	return nil
}
