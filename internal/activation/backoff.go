package activation

import "time"

// Backoff parameters (doc-07).
const (
	backoffBase = 10 * time.Second
	backoffMax  = 15 * time.Minute
	maxRetries  = 5
)

// Backoff returns the delay before the given attempt (1-based), exponential and
// capped at backoffMax. Jitter is applied by the caller via jitterFn to keep this
// pure and testable.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := backoffBase
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= backoffMax {
			return backoffMax
		}
	}
	if d > backoffMax {
		return backoffMax
	}
	return d
}
