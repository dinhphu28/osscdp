package auth

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Require authorizes a request: the principal must hold the permission, and for
// routes carrying {tenantID}, a non-super principal must match that tenant.
func Require(perm string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, ok := PrincipalFromContext(r.Context())
			if !ok {
				apierror.Unauthorized(w, "not authenticated")
				return
			}
			if !p.Can(perm) {
				apierror.Forbidden(w, "missing permission: "+perm)
				return
			}
			if !p.IsSuperAdmin() {
				if tid := chi.URLParam(r, "tenantID"); tid != "" {
					if p.TenantID == nil || p.TenantID.String() != tid {
						apierror.Forbidden(w, "tenant scope violation")
						return
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RequireSuperAdmin restricts a route to cross-tenant (super-admin) principals.
func RequireSuperAdmin() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, ok := PrincipalFromContext(r.Context())
			if !ok || !p.IsSuperAdmin() {
				apierror.Forbidden(w, "super-admin required")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
