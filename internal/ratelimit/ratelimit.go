// Package ratelimit provides per-source ingress rate limiting via in-memory
// token buckets. Per-instance; Redis-backed distributed limiting is the
// multi-instance upgrade.
package ratelimit

import (
	"net/http"
	"strconv"
	"sync"

	"golang.org/x/time/rate"

	"github.com/dinhphu28/osscdp/internal/auth"
	"github.com/dinhphu28/osscdp/pkg/apierror"
)

// Limiter holds a token bucket per source.
type Limiter struct {
	rps   rate.Limit
	burst int

	mu      sync.Mutex
	buckets map[string]*rate.Limiter

	// OnLimited is a metric hook (nil-safe), called when a request is throttled.
	OnLimited func()
}

// New constructs a Limiter with the given steady-state rate and burst.
func New(rps float64, burst int) *Limiter {
	return &Limiter{rps: rate.Limit(rps), burst: burst, buckets: make(map[string]*rate.Limiter)}
}

// allow reports whether a request from the source key may proceed.
func (l *Limiter) allow(key string) bool {
	l.mu.Lock()
	b, ok := l.buckets[key]
	if !ok {
		b = rate.NewLimiter(l.rps, l.burst)
		l.buckets[key] = b
	}
	l.mu.Unlock()
	return b.Allow()
}

// Middleware rate-limits ingress requests keyed by the authenticated source.
// Must run after auth.APIKey (which populates the source in context).
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceID, ok := auth.SourceID(r.Context())
		if !ok {
			next.ServeHTTP(w, r) // unauthenticated requests are handled upstream
			return
		}
		if !l.allow(sourceID.String()) {
			if l.OnLimited != nil {
				l.OnLimited()
			}
			w.Header().Set("Retry-After", strconv.Itoa(1))
			apierror.TooManyRequests(w, "rate limit exceeded for source")
			return
		}
		next.ServeHTTP(w, r)
	})
}
