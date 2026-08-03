// Package agent mirrors an authoritative router lease into a local, expiring
// leadership file. The file is a fail-closed data-plane input, not a source of
// leadership truth.
package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cube-js/cube-operator/internal/leadership"
)

const (
	DefaultLeadershipFile = "/var/run/cubestore-ha/leadership.json"
	leadershipFileMode    = 0o640
)

var ErrInvalidLeaseRecord = errors.New("invalid authoritative lease record")

// LeaseStore is the read-only portion of leadership.LeaseStore required by a
// per-pod agent. A leadership.LeaseStore satisfies this interface directly.
type LeaseStore interface {
	Get(context.Context, string) (leadership.LeaseRecord, error)
}

// Config configures a lease agent for one Router pod.
type Config struct {
	Store         LeaseStore
	ClusterID     string
	HolderID      string
	Path          string
	RetryPeriod   time.Duration
	RenewDeadline time.Duration
	Now           func() time.Time
}

// LeadershipFile is the strict, local data-plane representation consumed by
// the Router. The lease token is deliberately hashed before it reaches disk.
type LeadershipFile struct {
	HolderID  string    `json:"holderId"`
	Epoch     int64     `json:"epoch"`
	TokenHash string    `json:"tokenHash"`
	IssuedAt  time.Time `json:"issuedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// Agent polls the authoritative lease store and maintains the local file.
type Agent struct {
	store         LeaseStore
	clusterID     string
	holderID      string
	path          string
	retryPeriod   time.Duration
	renewDeadline time.Duration
	now           func() time.Time

	lastSuccess time.Time
	maxEpoch    int64
	lastToken   string
}

// New validates configuration and constructs an Agent.
func New(config Config) (*Agent, error) {
	if config.Store == nil {
		return nil, errors.New("lease store is required")
	}
	if config.ClusterID == "" {
		return nil, errors.New("cluster ID is required")
	}
	if config.HolderID == "" {
		return nil, errors.New("holder ID is required")
	}
	if config.Path == "" {
		config.Path = DefaultLeadershipFile
	}
	if config.RetryPeriod <= 0 {
		return nil, errors.New("retry period must be positive")
	}
	if config.RenewDeadline <= 0 {
		return nil, errors.New("renew deadline must be positive")
	}
	if config.Now == nil {
		config.Now = time.Now
	}

	return &Agent{
		store:         config.Store,
		clusterID:     config.ClusterID,
		holderID:      config.HolderID,
		path:          config.Path,
		retryPeriod:   config.RetryPeriod,
		renewDeadline: config.RenewDeadline,
		now:           config.Now,
	}, nil
}

// Run synchronizes immediately, then at RetryPeriod until the context ends.
// It always leaves an expired follower file when the agent stops.
func (a *Agent) Run(ctx context.Context) error {
	defer a.writeExpiredFollower()

	if err := a.Sync(ctx); err != nil && ctx.Err() != nil {
		return nil
	}

	ticker := time.NewTicker(a.retryPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_ = a.Sync(ctx)
		}
	}
}

// Sync reads the authoritative lease and writes it atomically. A malformed
// response and an epoch regression fail closed immediately. Transport errors
// preserve the last file only for RenewDeadline, then expire it.
func (a *Agent) Sync(ctx context.Context) error {
	record, err := a.store.Get(ctx, a.clusterID)
	if err != nil {
		if a.lastSuccess.IsZero() || !a.now().Before(a.lastSuccess.Add(a.renewDeadline)) {
			if writeErr := a.writeExpiredFollower(); writeErr != nil {
				return fmt.Errorf("read lease: %w; expire leadership file: %v", err, writeErr)
			}
		}
		return fmt.Errorf("read lease: %w", err)
	}
	if err := validate(record, a.maxEpoch, a.lastToken); err != nil {
		if writeErr := a.writeExpiredFollower(); writeErr != nil {
			return fmt.Errorf("%w: %v; expire leadership file: %v", ErrInvalidLeaseRecord, err, writeErr)
		}
		return fmt.Errorf("%w: %v", ErrInvalidLeaseRecord, err)
	}

	file := LeadershipFile{
		HolderID:  record.HolderID,
		Epoch:     record.Epoch,
		TokenHash: tokenHash(record.Token),
		IssuedAt:  record.IssuedAt.UTC(),
		ExpiresAt: record.ExpiresAt.UTC(),
	}
	if err := writeAtomic(a.path, file); err != nil {
		return fmt.Errorf("write leadership file: %w", err)
	}
	a.lastSuccess = a.now()
	if record.Epoch > a.maxEpoch {
		a.maxEpoch = record.Epoch
	}
	a.lastToken = record.Token
	return nil
}

func validate(record leadership.LeaseRecord, maxEpoch int64, lastToken string) error {
	if record.HolderID == "" {
		return errors.New("holder ID is empty")
	}
	if record.Epoch <= 0 {
		return errors.New("epoch must be positive")
	}
	if record.Epoch < maxEpoch {
		return fmt.Errorf("epoch %d is less than previously observed epoch %d", record.Epoch, maxEpoch)
	}
	if record.Epoch == maxEpoch && lastToken != "" && record.Token != lastToken {
		return errors.New("token changed without an epoch change")
	}
	if record.Token == "" {
		return errors.New("token is empty")
	}
	if record.IssuedAt.IsZero() || record.ExpiresAt.IsZero() {
		return errors.New("lease timestamps are required")
	}
	if !record.ExpiresAt.After(record.IssuedAt) {
		return errors.New("lease expiry must be after issuance")
	}
	return nil
}

func (a *Agent) writeExpiredFollower() error {
	now := a.now().UTC()
	return writeAtomic(a.path, LeadershipFile{
		Epoch:     a.maxEpoch,
		IssuedAt:  now,
		ExpiresAt: now.Add(-time.Second),
	})
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func writeAtomic(path string, file LeadershipFile) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}

	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+"-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err := temporary.Chmod(leadershipFileMode); err != nil {
		temporary.Close()
		return err
	}
	encoded, err := json.Marshal(file)
	if err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(append(encoded, '\n')); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}

	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
