package rawevent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrNotFound is returned when a raw event does not exist for the tenant.
var ErrNotFound = errors.New("raw event not found")

// ErrInvalidCursor is returned when a pagination cursor cannot be decoded.
var ErrInvalidCursor = errors.New("invalid cursor")

// Default and maximum page sizes for List.
const (
	DefaultLimit = 50
	MaxLimit     = 500
)

// RawEvent is the read model for a stored event.
type RawEvent struct {
	ID               uuid.UUID       `json:"id"`
	TenantID         uuid.UUID       `json:"tenant_id"`
	SourceID         uuid.UUID       `json:"source_id"`
	EventID          string          `json:"event_id"`
	Type             string          `json:"type"`
	EventName        *string         `json:"event_name,omitempty"`
	IdentifierKey    *string         `json:"identifier_key,omitempty"`
	PayloadJSON      json.RawMessage `json:"payload_json"`
	PayloadHash      string          `json:"payload_hash"`
	Timestamp        time.Time       `json:"timestamp"`
	ReceivedAt       time.Time       `json:"received_at"`
	ProcessingStatus string          `json:"processing_status"`
	ErrorReason      *string         `json:"error_reason,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
}

const selectColumns = `id, tenant_id, source_id, event_id, type, event_name, identifier_key,
	payload_json, payload_hash, timestamp, received_at, processing_status, error_reason, created_at`

func scanRawEvent(row pgx.Row) (RawEvent, error) {
	var e RawEvent
	err := row.Scan(&e.ID, &e.TenantID, &e.SourceID, &e.EventID, &e.Type, &e.EventName,
		&e.IdentifierKey, &e.PayloadJSON, &e.PayloadHash, &e.Timestamp, &e.ReceivedAt,
		&e.ProcessingStatus, &e.ErrorReason, &e.CreatedAt)
	return e, err
}

// GetByEventID loads one event by tenant + event ID.
func (r *Repo) GetByEventID(ctx context.Context, tenantID uuid.UUID, eventID string) (RawEvent, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT `+selectColumns+` FROM raw_event WHERE tenant_id = $1 AND event_id = $2`,
		tenantID, eventID)
	e, err := scanRawEvent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return RawEvent{}, ErrNotFound
	}
	if err != nil {
		return RawEvent{}, fmt.Errorf("get raw_event: %w", err)
	}
	return e, nil
}

// ListQuery parameterizes a List call. Empty filters are omitted.
type ListQuery struct {
	TenantID      uuid.UUID
	IdentifierKey string
	EventName     string
	Limit         int
	Cursor        string
}

// List returns a page of events ordered newest-first, plus a next_cursor (empty
// when there are no more rows). Keyset pagination on (received_at, id).
func (r *Repo) List(ctx context.Context, q ListQuery) ([]RawEvent, string, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	if limit > MaxLimit {
		limit = MaxLimit
	}

	var (
		conds = []string{"tenant_id = $1"}
		args  = []any{q.TenantID}
	)
	if q.IdentifierKey != "" {
		args = append(args, q.IdentifierKey)
		conds = append(conds, fmt.Sprintf("identifier_key = $%d", len(args)))
	}
	if q.EventName != "" {
		args = append(args, q.EventName)
		conds = append(conds, fmt.Sprintf("event_name = $%d", len(args)))
	}
	if q.Cursor != "" {
		ct, cid, err := decodeCursor(q.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, ct, cid)
		conds = append(conds, fmt.Sprintf("(received_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	args = append(args, limit+1)

	sql := `SELECT ` + selectColumns + ` FROM raw_event WHERE ` + strings.Join(conds, " AND ") +
		` ORDER BY received_at DESC, id DESC LIMIT $` + strconv.Itoa(len(args))

	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list raw_event: %w", err)
	}
	defer rows.Close()

	out := make([]RawEvent, 0, limit)
	for rows.Next() {
		e, err := scanRawEvent(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scan raw_event: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate raw_event: %w", err)
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		next = encodeCursor(last.ReceivedAt, last.ID)
		out = out[:limit]
	}
	return out, next, nil
}

// encodeCursor builds an opaque keyset cursor from (received_at, id).
func encodeCursor(t time.Time, id uuid.UUID) string {
	raw := fmt.Sprintf("%d|%s", t.UTC().UnixNano(), id.String())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (time.Time, uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	return time.Unix(0, nanos).UTC(), id, nil
}
