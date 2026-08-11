// Package ratelimit is a per-user token bucket, mirroring the hand-rolled
// pattern Concord's own Grapevine join endpoint already uses
// (d:\Concord\internal\hub\handlers.go:47-85) — same struct shape and
// refill math, just keyed by Concord user ID instead of IP, and with a
// configurable burst/refill rate instead of a hardcoded 5-per-minute.
package ratelimit

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type bucket struct {
	mu       sync.Mutex
	tokens   float64
	lastFill time.Time
}

// allow consumes one token if available, refilling based on elapsed time
// since the last check. ratePerHour and maxTokens are the limiter's
// configured burst/refill (see Limiter).
func (b *bucket) allow(ratePerHour, maxTokens float64) bool {
	ratePerSecond := ratePerHour / 3600.0

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastFill).Seconds()
	b.tokens = min(maxTokens, b.tokens+elapsed*ratePerSecond)
	b.lastFill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Limiter is a per-user rate limiter with a fixed burst/refill
// configuration — one Limiter per rate-limit "scope" (e.g. one for
// dedicated-channel mode using that channel's create_field values, one for
// mention mode using the server-wide config fields), since each scope can
// be configured independently.
type Limiter struct {
	burst       float64
	refillPerHr float64
	buckets     sync.Map // uuid.UUID (user ID) -> *bucket
}

// New constructs a Limiter. burst is the maximum tokens a user can bank
// (how many messages they can send in a rapid burst); refillPerHour is how
// many tokens accumulate per hour after that.
func New(burst, refillPerHour int) *Limiter {
	return &Limiter{burst: float64(burst), refillPerHr: float64(refillPerHour)}
}

// Allow reports whether userID may act now, consuming a token if so.
func (l *Limiter) Allow(userID uuid.UUID) bool {
	v, _ := l.buckets.LoadOrStore(userID, &bucket{tokens: l.burst, lastFill: time.Now()})
	return v.(*bucket).allow(l.refillPerHr, l.burst)
}
