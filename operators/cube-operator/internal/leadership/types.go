// Package leadership defines the storage contract used to fence router leaders.
package leadership

import (
	"context"
	"time"
)

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

// LeaseStore is the durable lease backend contract. A false acquisition or
// renewal result represents a contention or fencing loss, not an error.
type LeaseStore interface {
	Acquire(context.Context, string, string, time.Duration) (LeaseRecord, bool, error)
	Renew(context.Context, LeaseRecord, time.Duration) (LeaseRecord, bool, error)
	Release(context.Context, LeaseRecord) error
	Get(context.Context, string) (LeaseRecord, error)
}
