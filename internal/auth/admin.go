package auth

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// AdminToken returns middleware guarding admin routes with a static bearer
// token. Full RBAC arrives in Phase 9; this is the minimal Phase 1 guard.
func AdminToken(token string) func(http.Handler) http.Handler {
	want := []byte(token)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			if got == "" || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
				apierror.Unauthorized(w, "invalid admin token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
