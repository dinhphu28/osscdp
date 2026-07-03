package activation

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

// Errors.
var (
	ErrNotFound      = errors.New("destination not found")
	ErrDuplicateName = errors.New("destination name already exists for tenant")
)

// Repo persists destinations, subscriptions, tasks, and deliveries.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo constructs a Repo.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

const destCols = `id, tenant_id, type, name, status, config_json, secret_ref, created_at, updated_at`

func scanDestination(row pgx.Row) (Destination, error) {
	var d Destination
	err := row.Scan(&d.ID, &d.TenantID, &d.Type, &d.Name, &d.Status, &d.Config, &d.SecretRef, &d.CreatedAt, &d.UpdatedAt)
	return d, err
}

// CreateDestination inserts a destination.
func (r *Repo) CreateDestination(ctx context.Context, tenantID uuid.UUID, typ, name string, config json.RawMessage, secretRef string) (Destination, error) {
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	id := uuid.New()
	_, err := r.pool.Exec(ctx,
		`INSERT INTO destination (id, tenant_id, type, name, status, config_json, secret_ref)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		id, tenantID, typ, name, StatusActive, config, nullString(secretRef))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Destination{}, ErrDuplicateName
		}
		return Destination{}, fmt.Errorf("insert destination: %w", err)
	}
	return r.GetDestination(ctx, tenantID, id)
}

// UpdateDestination updates status and/or config.
func (r *Repo) UpdateDestination(ctx context.Context, tenantID, id uuid.UUID, status string, config json.RawMessage) (Destination, error) {
	ct, err := r.pool.Exec(ctx, `
		UPDATE destination
		SET status = COALESCE($3, status),
		    config_json = COALESCE($4, config_json),
		    updated_at = now()
		WHERE tenant_id=$1 AND id=$2`,
		tenantID, id, nullString(status), nullJSON(config))
	if err != nil {
		return Destination{}, fmt.Errorf("update destination: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return Destination{}, ErrNotFound
	}
	return r.GetDestination(ctx, tenantID, id)
}

// GetDestination loads a destination by id.
func (r *Repo) GetDestination(ctx context.Context, tenantID, id uuid.UUID) (Destination, error) {
	d, err := scanDestination(r.pool.QueryRow(ctx,
		`SELECT `+destCols+` FROM destination WHERE tenant_id=$1 AND id=$2`, tenantID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Destination{}, ErrNotFound
	}
	if err != nil {
		return Destination{}, fmt.Errorf("get destination: %w", err)
	}
	return d, nil
}

// CreateSubscription connects a segment to a destination.
func (r *Repo) CreateSubscription(ctx context.Context, tenantID, destinationID uuid.UUID, triggerType string, segmentID *uuid.UUID) (Subscription, error) {
	s := Subscription{
		ID: uuid.New(), TenantID: tenantID, DestinationID: destinationID,
		TriggerType: triggerType, SegmentID: segmentID, Status: StatusActive,
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO destination_subscription (id, tenant_id, destination_id, trigger_type, segment_id, status)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		s.ID, s.TenantID, s.DestinationID, s.TriggerType, s.SegmentID, s.Status)
	if err != nil {
		return Subscription{}, fmt.Errorf("insert subscription: %w", err)
	}
	return s, nil
}

// DisableSubscription soft-disables a subscription (status='disabled') so the
// sender stops dispatching it, without deleting the row — the activation_task
// foreign key forbids hard deletes. Scoped by destination_id so the nested
// route's {destinationID} must match. Idempotent: disabling an already-disabled
// subscription still matches and returns it. Returns ErrNotFound if no row matches.
func (r *Repo) DisableSubscription(ctx context.Context, tenantID, destinationID, subscriptionID uuid.UUID) (Subscription, error) {
	var s Subscription
	err := r.pool.QueryRow(ctx, `
		UPDATE destination_subscription
		SET status=$4, updated_at=now()
		WHERE tenant_id=$1 AND destination_id=$2 AND id=$3
		RETURNING id, tenant_id, destination_id, trigger_type, segment_id, event_name, status`,
		tenantID, destinationID, subscriptionID, StatusDisabled,
	).Scan(&s.ID, &s.TenantID, &s.DestinationID, &s.TriggerType, &s.SegmentID, &s.EventName, &s.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	if err != nil {
		return Subscription{}, fmt.Errorf("disable subscription: %w", err)
	}
	return s, nil
}

// ActiveSubscriptionsForSegment returns active subscriptions on active
// destinations triggered by membership of the given segment.
func (r *Repo) ActiveSubscriptionsForSegment(ctx context.Context, tenantID, segmentID uuid.UUID) ([]Subscription, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT s.id, s.tenant_id, s.destination_id, s.trigger_type, s.segment_id, s.status
		FROM destination_subscription s
		JOIN destination d ON d.id = s.destination_id
		WHERE s.tenant_id=$1 AND s.segment_id=$2 AND s.trigger_type=$3
		  AND s.status=$4 AND d.status=$4`,
		tenantID, segmentID, TriggerSegmentMembership, StatusActive)
	if err != nil {
		return nil, fmt.Errorf("active subscriptions: %w", err)
	}
	defer rows.Close()
	var out []Subscription
	for rows.Next() {
		var s Subscription
		if err := rows.Scan(&s.ID, &s.TenantID, &s.DestinationID, &s.TriggerType, &s.SegmentID, &s.Status); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// SubscriptionsBySegment lists every destination wired to a segment via a
// subscription, including disabled subscriptions and destinations (unlike
// ActiveSubscriptionsForSegment, which is active-only) so admins can audit the
// full wiring. Ordered by destination name.
func (r *Repo) SubscriptionsBySegment(ctx context.Context, tenantID, segmentID uuid.UUID) ([]SegmentDestination, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT s.id, s.status, d.id, d.name, d.type, d.status
		FROM destination_subscription s
		JOIN destination d ON d.id = s.destination_id
		WHERE s.tenant_id=$1 AND s.segment_id=$2
		ORDER BY d.name`,
		tenantID, segmentID)
	if err != nil {
		return nil, fmt.Errorf("subscriptions by segment: %w", err)
	}
	defer rows.Close()
	var out []SegmentDestination
	for rows.Next() {
		var sd SegmentDestination
		if err := rows.Scan(&sd.SubscriptionID, &sd.SubscriptionStatus, &sd.DestinationID, &sd.Name, &sd.Type, &sd.DestinationStatus); err != nil {
			return nil, err
		}
		out = append(out, sd)
	}
	return out, rows.Err()
}

// CreateTask inserts a task idempotently with the given status. Returns false if
// a task with the same idempotency key already exists. A "skipped" task records a
// consent denial; it is never claimed by the sender.
func (r *Repo) CreateTask(ctx context.Context, t Task, status, lastError string) (bool, error) {
	ct, err := r.pool.Exec(ctx, `
		INSERT INTO activation_task
			(id, tenant_id, destination_id, subscription_id, customer_profile_id,
			 source_event_id, idempotency_key, payload_json, status, attempt_count, next_attempt_at, last_error)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0, now(), $10)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		uuid.New(), t.TenantID, t.DestinationID, t.SubscriptionID, t.CustomerProfileID,
		nullString(t.SourceEventID), t.IdempotencyKey, t.Payload, status, nullString(lastError))
	if err != nil {
		return false, fmt.Errorf("insert task: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// ClaimDueTasks atomically marks up to limit due tasks as sending and returns
// them. Safe for concurrent runners (FOR UPDATE SKIP LOCKED).
func (r *Repo) ClaimDueTasks(ctx context.Context, limit int) ([]Task, error) {
	rows, err := r.pool.Query(ctx, `
		UPDATE activation_task t SET status=$1, updated_at=now()
		WHERE t.id IN (
			SELECT id FROM activation_task
			WHERE status IN ($2, $3) AND (next_attempt_at IS NULL OR next_attempt_at <= now())
			ORDER BY next_attempt_at NULLS FIRST
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		)
		RETURNING t.id, t.tenant_id, t.destination_id, t.subscription_id, t.customer_profile_id,
		          COALESCE(t.source_event_id, ''), t.idempotency_key, t.payload_json, t.attempt_count`,
		TaskSending, TaskPending, TaskFailedRetryable, limit)
	if err != nil {
		return nil, fmt.Errorf("claim due tasks: %w", err)
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.TenantID, &t.DestinationID, &t.SubscriptionID, &t.CustomerProfileID,
			&t.SourceEventID, &t.IdempotencyKey, &t.Payload, &t.AttemptCount); err != nil {
			return nil, err
		}
		t.Status = TaskSending
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkResult updates a task's terminal/retry state after a send attempt.
func (r *Repo) MarkResult(ctx context.Context, taskID uuid.UUID, status string, attemptCount int, nextAttemptAt *time.Time, lastError string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE activation_task
		SET status=$2, attempt_count=$3, next_attempt_at=$4, last_error=$5, updated_at=now()
		WHERE id=$1`,
		taskID, status, attemptCount, nextAttemptAt, nullString(lastError))
	if err != nil {
		return fmt.Errorf("mark task result: %w", err)
	}
	return nil
}

// Delivery is a single send-attempt record.
type Delivery struct {
	TenantID          uuid.UUID
	TaskID            uuid.UUID
	DestinationID     uuid.UUID
	CustomerProfileID uuid.UUID
	SourceEventID     string
	IdempotencyKey    string
	Status            string
	HTTPStatus        *int
	ResponseBodyHash  string
	ErrorMessage      string
	AttemptCount      int
	SentAt            *time.Time
}

// InsertDelivery records a delivery attempt.
func (r *Repo) InsertDelivery(ctx context.Context, d Delivery) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO activation_delivery
			(id, tenant_id, activation_task_id, destination_id, customer_profile_id, source_event_id,
			 idempotency_key, status, http_status, response_body_hash, error_message, attempt_count, sent_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		uuid.New(), d.TenantID, d.TaskID, d.DestinationID, d.CustomerProfileID, nullString(d.SourceEventID),
		d.IdempotencyKey, d.Status, d.HTTPStatus, nullString(d.ResponseBodyHash), nullString(d.ErrorMessage),
		d.AttemptCount, d.SentAt)
	if err != nil {
		return fmt.Errorf("insert delivery: %w", err)
	}
	return nil
}

// ListDeliveries returns delivery records for a destination, newest first.
func (r *Repo) ListDeliveries(ctx context.Context, tenantID, destinationID uuid.UUID) ([]Delivery, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT tenant_id, activation_task_id, destination_id, customer_profile_id,
		       COALESCE(source_event_id,''), idempotency_key, status, http_status,
		       COALESCE(response_body_hash,''), COALESCE(error_message,''), attempt_count, sent_at
		FROM activation_delivery
		WHERE tenant_id=$1 AND destination_id=$2
		ORDER BY created_at DESC LIMIT 200`,
		tenantID, destinationID)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	defer rows.Close()
	out := []Delivery{}
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(&d.TenantID, &d.TaskID, &d.DestinationID, &d.CustomerProfileID, &d.SourceEventID,
			&d.IdempotencyKey, &d.Status, &d.HTTPStatus, &d.ResponseBodyHash, &d.ErrorMessage, &d.AttemptCount, &d.SentAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullJSON(j json.RawMessage) []byte {
	if len(j) == 0 {
		return nil
	}
	return j
}
