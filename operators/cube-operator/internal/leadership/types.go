// Package leadership defines the storage contract used to fence router leaders.
package leadership

import (
	"context"
	"errors"
	"time"
)

// ErrStaleLease identifies a fencing mismatch. Storage implementations should
// use this predicate as their compare-and-swap precondition; this package does
// not implement an external storage CAS operation.
var ErrStaleLease = errors.New("stale lease")

// LeaseRecord is a fenced lease snapshot. Epoch and Token must change whenever
// leadership changes so callers can reject stale writes from former leaders.
type LeaseRecord struct {
	ClusterID string
	HolderID  string
	Epoch     int64
	Token     string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// ValidateLeaseFence accepts only the exact current holder, epoch, and token.
// A newer epoch or token therefore fences every record issued to a former
// holder before that holder can perform a protected write.
func ValidateLeaseFence(current, presented LeaseRecord) error {
	if current.ClusterID == "" || current.HolderID == "" || current.Token == "" {
		return ErrStaleLease
	}
	if presented.ClusterID != current.ClusterID ||
		presented.HolderID != current.HolderID ||
		presented.Epoch != current.Epoch ||
		presented.Token != current.Token {
		return ErrStaleLease
	}
	return nil
}

// LeaseStore is the durable lease backend contract. A false acquisition or
// renewal result represents a contention or fencing loss, not an error.
type LeaseStore interface {
	Acquire(context.Context, string, string, time.Duration) (LeaseRecord, bool, error)
	Renew(context.Context, LeaseRecord, time.Duration) (LeaseRecord, bool, error)
	Release(context.Context, LeaseRecord) error
	Get(context.Context, string) (LeaseRecord, error)
}
