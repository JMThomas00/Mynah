package ratelimit

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLimiterAllowsUpToBurst(t *testing.T) {
	l := New(3, 60)
	user := uuid.New()

	for i := 0; i < 3; i++ {
		if !l.Allow(user) {
			t.Fatalf("expected request %d to be allowed within burst", i+1)
		}
	}
	if l.Allow(user) {
		t.Fatal("expected the 4th request to be rejected once burst is exhausted")
	}
}

func TestLimiterTracksUsersIndependently(t *testing.T) {
	l := New(1, 60)
	userA := uuid.New()
	userB := uuid.New()

	if !l.Allow(userA) {
		t.Fatal("expected userA's first request to be allowed")
	}
	if l.Allow(userA) {
		t.Fatal("expected userA's second request to be rejected")
	}
	if !l.Allow(userB) {
		t.Fatal("expected userB to have their own independent bucket")
	}
}

func TestLimiterRefillsOverTime(t *testing.T) {
	l := New(1, 3600) // 1 token/hour refill == 1 token/3600s, easy to fast-forward deterministically
	user := uuid.New()

	if !l.Allow(user) {
		t.Fatal("expected the first request to be allowed")
	}
	if l.Allow(user) {
		t.Fatal("expected the second request to be rejected immediately after")
	}

	// Simulate the passage of time by rewinding the bucket's own lastFill,
	// rather than sleeping — deterministic and fast.
	v, ok := l.buckets.Load(user)
	if !ok {
		t.Fatal("expected a bucket to exist for user after their first request")
	}
	b := v.(*bucket)
	b.mu.Lock()
	b.lastFill = b.lastFill.Add(-3600 * time.Second)
	b.mu.Unlock()

	if !l.Allow(user) {
		t.Fatal("expected a request to be allowed again after a full refill interval")
	}
}
