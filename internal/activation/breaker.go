package activation

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// Breaker is a per-destination circuit breaker: after `threshold` failures within
// `window` it opens for `cooldown`, then allows a single half-open trial.
type Breaker struct {
	threshold int
	window    time.Duration
	cooldown  time.Duration
	now       func() time.Time

	mu     sync.Mutex
	states map[uuid.UUID]*bkState
}

type bkState struct {
	failures []time.Time
	openedAt time.Time
	open     bool
	halfOpen bool
}

// NewBreaker constructs a Breaker.
func NewBreaker(threshold int, window, cooldown time.Duration) *Breaker {
	return &Breaker{
		threshold: threshold, window: window, cooldown: cooldown,
		now: time.Now, states: make(map[uuid.UUID]*bkState),
	}
}

// Cooldown returns the open duration (used by the runner to defer tasks).
func (b *Breaker) Cooldown() time.Duration { return b.cooldown }

func (b *Breaker) state(id uuid.UUID) *bkState {
	s, ok := b.states[id]
	if !ok {
		s = &bkState{}
		b.states[id] = s
	}
	return s
}

// Allow reports whether a send to the destination may proceed.
func (b *Breaker) Allow(id uuid.UUID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.state(id)
	if !s.open {
		return true
	}
	if b.now().Sub(s.openedAt) >= b.cooldown {
		if !s.halfOpen {
			s.halfOpen = true // first caller after cooldown gets the trial
			return true
		}
		return false
	}
	return false
}

// Record reports the outcome of a send, updating the breaker.
func (b *Breaker) Record(id uuid.UUID, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.state(id)
	now := b.now()
	if success {
		s.open, s.halfOpen, s.failures = false, false, nil
		return
	}
	if s.halfOpen {
		s.openedAt, s.halfOpen, s.failures = now, false, nil // trial failed → reopen
		return
	}
	cutoff := now.Add(-b.window)
	kept := s.failures[:0]
	for _, t := range s.failures {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	s.failures = append(kept, now)
	if len(s.failures) >= b.threshold {
		s.open, s.openedAt, s.failures = true, now, nil
	}
}
