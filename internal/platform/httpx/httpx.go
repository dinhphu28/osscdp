// Package httpx provides shared HTTP helpers: JSON encoding, request-scoped
// logging middleware, and health endpoints.
package httpx

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/platform/logging"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// WriteJSON encodes v as a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// NewRouter builds a chi router with request ID, panic recovery, and a JSON
// access log that carries the request_id into the request context.
func NewRouter(logger *slog.Logger) *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(accessLog(logger))
	return r
}

func accessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	log := logging.Component(logger, "http")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqID := middleware.GetReqID(r.Context())
			ctx := logging.WithFields(r.Context())
			logging.AddFields(ctx, slog.String("request_id", reqID))
			r = r.WithContext(ctx)

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)

			logging.FromContext(r.Context(), log).Info("request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", ww.Status()),
				slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			)
		})
	}
}

// Health registers liveness (/healthz) and readiness (/readyz) endpoints.
func Health(r chi.Router, pool *pgxpool.Pool) {
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			apierror.Write(w, http.StatusServiceUnavailable, "not_ready", "database unavailable")
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
}
