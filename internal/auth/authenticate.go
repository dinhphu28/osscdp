package auth

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/dinhphu28/osscdp/internal/rbac"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

type principalCtxKey int

const principalKey principalCtxKey = 0

// AdminTokenResolver resolves a token hash to a principal.
type AdminTokenResolver interface {
	FindByTokenHash(ctx context.Context, hash string) (rbac.Principal, error)
}

// Authenticate guards admin routes. The static token authenticates as
// SUPER_ADMIN (the bootstrap); any other bearer token is resolved against the
// admin_token store. The resolved principal is injected into the context.
func Authenticate(staticToken string, repo AdminTokenResolver) func(http.Handler) http.Handler {
	want := []byte(staticToken)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := bearer(r)
			if tok == "" {
				apierror.Unauthorized(w, "missing admin token")
				return
			}
			var p rbac.Principal
			if subtle.ConstantTimeCompare([]byte(tok), want) == 1 {
				p = rbac.Principal{Role: rbac.RoleSuperAdmin} // TenantID nil = cross-tenant
			} else {
				resolved, err := repo.FindByTokenHash(r.Context(), rbac.HashToken(tok))
				if err != nil {
					apierror.Unauthorized(w, "invalid admin token")
					return
				}
				p = resolved
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey, p)))
		})
	}
}

// PrincipalFromContext returns the authenticated admin principal.
func PrincipalFromContext(ctx context.Context) (rbac.Principal, bool) {
	p, ok := ctx.Value(principalKey).(rbac.Principal)
	return p, ok
}

func bearer(r *http.Request) string {
	return strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
}
