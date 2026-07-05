// Package stats aggregates per-tenant resource counts for the admin dashboard.
package stats

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/dlq"
	"github.com/dinhphu28/osscdp/internal/platform/httpx"
	"github.com/dinhphu28/osscdp/internal/segment"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Handler exposes the admin per-tenant stats endpoint.
type Handler struct {
	pool     *pgxpool.Pool
	segments *segment.Repo
}

// NewHandler constructs a Handler. The segment count reuses SegmentsEpoch so the
// dashboard and the worker agree on the active-segment set.
func NewHandler(pool *pgxpool.Pool, segments *segment.Repo) *Handler {
	return &Handler{pool: pool, segments: segments}
}

// statsResponse is a flat counts object the dashboard renders directly.
type statsResponse struct {
	DLQOpen      int64 `json:"dlq_open"`
	Sources      int64 `json:"sources"`
	Segments     int64 `json:"segments"`
	Destinations int64 `json:"destinations"`
	Profiles     int64 `json:"profiles"`
}

// Stats handles GET /admin/v1/tenants/{tenantID}/stats. Best-effort: a failed
// sub-count yields -1 for that field and never fails the whole response, so the
// dashboard always renders.
func (h *Handler) Stats(w http.ResponseWriter, r *http.Request) {
	tenantID, err := uuid.Parse(chi.URLParam(r, "tenantID"))
	if err != nil {
		apierror.BadRequest(w, "invalid tenant id")
		return
	}
	ctx := r.Context()
	out := statsResponse{
		DLQOpen:      h.countArg(ctx, `SELECT count(*) FROM dlq_event WHERE tenant_id=$1 AND status=$2`, tenantID, dlq.StatusOpen),
		Sources:      h.count(ctx, `SELECT count(*) FROM source WHERE tenant_id=$1`, tenantID),
		Destinations: h.count(ctx, `SELECT count(*) FROM destination WHERE tenant_id=$1`, tenantID),
		Profiles:     h.count(ctx, `SELECT count(*) FROM customer_profile WHERE tenant_id=$1`, tenantID),
		Segments:     h.segmentCount(ctx, tenantID),
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func (h *Handler) count(ctx context.Context, sql string, tenantID uuid.UUID) int64 {
	var n int64
	if err := h.pool.QueryRow(ctx, sql, tenantID).Scan(&n); err != nil {
		return -1
	}
	return n
}

func (h *Handler) countArg(ctx context.Context, sql string, tenantID uuid.UUID, arg any) int64 {
	var n int64
	if err := h.pool.QueryRow(ctx, sql, tenantID, arg).Scan(&n); err != nil {
		return -1
	}
	return n
}

func (h *Handler) segmentCount(ctx context.Context, tenantID uuid.UUID) int64 {
	count, _, _, err := h.segments.SegmentsEpoch(ctx, tenantID)
	if err != nil {
		return -1
	}
	return count
}
