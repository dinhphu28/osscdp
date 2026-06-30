// Package auth provides API-key authentication middleware for ingress routes.
package auth

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/dinhphu28/osscdp/internal/platform/logging"
	"github.com/dinhphu28/osscdp/internal/source"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

type ctxKey int

const (
	tenantIDKey ctxKey = iota
	sourceIDKey
)

// Authenticator resolves a source from an API key plaintext.
type Authenticator interface {
	Authenticate(ctx context.Context, plaintext string) (source.Source, error)
}

// APIKey returns middleware that authenticates requests via API key. On success
// it injects tenant_id and source_id into the context and log fields. The key
// plaintext is never logged.
func APIKey(authn Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractKey(r)
			if key == "" {
				apierror.Unauthorized(w, "missing API key")
				return
			}
			src, err := authn.Authenticate(r.Context(), key)
			if err != nil {
				if errors.Is(err, source.ErrNotFound) {
					apierror.Unauthorized(w, "invalid API key")
					return
				}
				apierror.Internal(w)
				return
			}

			ctx := context.WithValue(r.Context(), tenantIDKey, src.TenantID)
			ctx = context.WithValue(ctx, sourceIDKey, src.ID)
			// Mutates the shared request field holder so the outer access log
			// also picks up tenant_id/source_id. Never logs the key itself.
			logging.AddFields(ctx,
				slog.String("tenant_id", src.TenantID.String()),
				slog.String("source_id", src.ID.String()),
			)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func extractKey(r *http.Request) string {
	if h := r.Header.Get("X-CDP-Api-Key"); h != "" {
		return strings.TrimSpace(h)
	}
	if h := r.Header.Get("Authorization"); h != "" {
		if rest, ok := strings.CutPrefix(h, "Bearer "); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

// TenantID returns the authenticated tenant ID from the context.
func TenantID(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(tenantIDKey).(uuid.UUID)
	return id, ok
}

// SourceID returns the authenticated source ID from the context.
func SourceID(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(sourceIDKey).(uuid.UUID)
	return id, ok
}
