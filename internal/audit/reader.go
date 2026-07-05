package audit

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInvalidCursor is returned when a pagination cursor cannot be decoded.
var ErrInvalidCursor = errors.New("invalid cursor")

// Default and maximum page sizes for the audit read model.
const (
	DefaultLimit = 50
	MaxLimit     = 500
)

// LogEntry is the metadata-only read model for an audit_log row. before_json and
// after_json are DELIBERATELY omitted — they can carry traits, consent, or
// secrets, so the browse view is PII-safe; a future pii:read detail route may
// expose the bodies. actor_id (often empty) and ip_address (not stored) are dropped.
type LogEntry struct {
	ID           uuid.UUID `json:"id"`
	ActorType    string    `json:"actor_type"`
	Action       string    `json:"action"`
	ResourceType string    `json:"resource_type"`
	ResourceID   string    `json:"resource_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// Reader is the audit_log read model, kept separate from the single-purpose
// Recorder (write model) so metadata reads never touch the sensitive bodies.
type Reader struct {
	pool *pgxpool.Pool
}

// NewReader constructs a Reader.
func NewReader(pool *pgxpool.Pool) *Reader { return &Reader{pool: pool} }

// List returns a page of a tenant's audit entries newest-first plus a next_cursor
// (empty when the page is the last). Keyset pagination on (created_at, id), the
// same opaque base64 codec as rawevent. Metadata only — never selects the bodies.
func (r *Reader) List(ctx context.Context, tenantID uuid.UUID, cursor string, limit int) ([]LogEntry, string, error) {
	if limit <= 0 {
		limit = DefaultLimit
	}
	if limit > MaxLimit {
		limit = MaxLimit
	}

	conds := []string{"tenant_id = $1"}
	args := []any{tenantID}
	if cursor != "" {
		ct, cid, err := decodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, ct, cid)
		conds = append(conds, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	args = append(args, limit+1)

	sql := `SELECT id, actor_type, action, resource_type, COALESCE(resource_id, ''), created_at
		FROM audit_log WHERE ` + strings.Join(conds, " AND ") +
		` ORDER BY created_at DESC, id DESC LIMIT $` + strconv.Itoa(len(args))

	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list audit_log: %w", err)
	}
	defer rows.Close()

	out := make([]LogEntry, 0, limit)
	for rows.Next() {
		var e LogEntry
		if err := rows.Scan(&e.ID, &e.ActorType, &e.Action, &e.ResourceType, &e.ResourceID, &e.CreatedAt); err != nil {
			return nil, "", fmt.Errorf("scan audit_log: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate audit_log: %w", err)
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		next = encodeCursor(last.CreatedAt, last.ID)
		out = out[:limit]
	}
	return out, next, nil
}

// encodeCursor builds an opaque keyset cursor from (created_at, id).
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
