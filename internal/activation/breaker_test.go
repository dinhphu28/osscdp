package activation

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestBreaker_OpensAfterThreshold(t *testing.T) {
	now := time.Now()
	b := NewBreaker(3, time.Minute, 30*time.Second)
	b.now = func() time.Time { return now }
	d := uuid.New()

	for i := 0; i < 2; i++ {
		if !b.Allow(d) {
			t.Fatalf("attempt %d should be allowed before threshold", i)
		}
		b.Record(d, false)
	}
	// 3rd failure trips it.
	if !b.Allow(d) {
		t.Fatal("3rd attempt allowed (still closed)")
	}
	b.Record(d, false)
	if b.Allow(d) {
		t.Fatal("breaker should be open after threshold failures")
	}
}

func TestBreaker_HalfOpenThenClose(t *testing.T) {
	now := time.Now()
	b := NewBreaker(1, time.Minute, 30*time.Second)
	b.now = func() time.Time { return now }
	d := uuid.New()

	b.Record(d, false) // threshold=1 → open
	if b.Allow(d) {
		t.Fatal("should be open")
	}
	// Advance past cooldown → half-open trial allowed once.
	now = now.Add(31 * time.Second)
	if !b.Allow(d) {
		t.Fatal("half-open trial should be allowed")
	}
	if b.Allow(d) {
		t.Fatal("only one half-open trial allowed")
	}
	// Trial succeeds → closed.
	b.Record(d, true)
	if !b.Allow(d) {
		t.Fatal("should be closed after a successful trial")
	}
}

func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	now := time.Now()
	b := NewBreaker(1, time.Minute, 10*time.Second)
	b.now = func() time.Time { return now }
	d := uuid.New()

	b.Record(d, false) // open
	now = now.Add(11 * time.Second)
	b.Allow(d)         // half-open trial
	b.Record(d, false) // trial fails → reopen
	if b.Allow(d) {
		t.Fatal("breaker should reopen after a failed half-open trial")
	}
}
