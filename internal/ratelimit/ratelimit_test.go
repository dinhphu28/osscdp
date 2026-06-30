package ratelimit

import "testing"

func TestLimiter_BurstThenBlock(t *testing.T) {
	// 0 rps refill, burst 3 → first 3 allowed, rest blocked.
	l := New(0, 3)
	allowed := 0
	for i := 0; i < 10; i++ {
		if l.allow("src-1") {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d, want 3 (burst)", allowed)
	}
}

func TestLimiter_PerSourceIndependent(t *testing.T) {
	l := New(0, 2)
	// Exhaust src-1.
	l.allow("src-1")
	l.allow("src-1")
	if l.allow("src-1") {
		t.Fatal("src-1 should be exhausted")
	}
	// src-2 has its own bucket.
	if !l.allow("src-2") {
		t.Fatal("src-2 should have its own budget")
	}
}
