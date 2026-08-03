package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/internal/leadership"
)

type fakeStore struct {
	get func() (leadership.LeaseRecord, error)
}

func (s fakeStore) Get(context.Context, string) (leadership.LeaseRecord, error) {
	return s.get()
}

func TestSyncWritesLeadershipFileAtomically(t *testing.T) {
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	record := validRecord("router-a", 42, "secret", now)
	agent := newTestAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) { return record, nil }})

	if err := agent.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	file := readLeadershipFile(t, agent.path)
	if file.HolderID != "router-a" || file.Epoch != 42 || file.TokenHash != tokenHash("secret") {
		t.Fatalf("unexpected leadership file: %+v", file)
	}
	contents, err := os.ReadFile(agent.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "secret") {
		t.Fatal("raw lease token was written to leadership file")
	}
	info, err := os.Stat(agent.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != leadershipFileMode {
		t.Fatalf("mode = %o, want %o", info.Mode().Perm(), leadershipFileMode)
	}
	entries, err := os.ReadDir(filepath.Dir(agent.path))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".leadership.json-") {
			t.Fatalf("temporary file %q was not removed", entry.Name())
		}
	}
}

func TestSyncExpiresOnMalformedBackendData(t *testing.T) {
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	agent := newTestAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) {
		return leadership.LeaseRecord{Epoch: 1}, nil
	}})

	if err := agent.Sync(context.Background()); !errors.Is(err, ErrInvalidLeaseRecord) {
		t.Fatalf("Sync error = %v, want ErrInvalidLeaseRecord", err)
	}
	file := readLeadershipFile(t, agent.path)
	if file.HolderID != "" || !file.ExpiresAt.Before(now) {
		t.Fatalf("malformed lease did not fail closed: %+v", file)
	}
}

func TestSyncExpiresImmediatelyOnBackendOutage(t *testing.T) {
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	available := true
	agent := newTestAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) {
		if !available {
			return leadership.LeaseRecord{}, errors.New("backend unavailable")
		}
		return validRecord("router-a", 3, "secret", now), nil
	}})
	if err := agent.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	available = false
	now = now.Add(time.Second)
	if err := agent.Sync(context.Background()); err == nil {
		t.Fatal("expected backend error")
	}
	if file := readLeadershipFile(t, agent.path); file.HolderID != "" || !file.ExpiresAt.Before(now) {
		t.Fatalf("file did not expire on backend outage: %+v", file)
	}
}

func TestSyncExpiresForAnotherHolder(t *testing.T) {
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	agent := newTestAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) {
		return validRecord("router-b", 4, "secret", now), nil
	}})

	if err := agent.Sync(context.Background()); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("Sync error = %v, want ErrNotHolder", err)
	}
	file := readLeadershipFile(t, agent.path)
	if file.HolderID != "" || file.Epoch != 4 || !file.ExpiresAt.Before(now) {
		t.Fatalf("non-owner lease did not fail closed: %+v", file)
	}
}

func TestSyncExpiresForAlreadyExpiredLease(t *testing.T) {
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	agent := newTestAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) {
		return leadership.LeaseRecord{
			ClusterID: "cube-router-demo",
			HolderID:  "router-a",
			Epoch:     4,
			Token:     "secret",
			IssuedAt:  now.Add(-time.Minute),
			ExpiresAt: now.Add(-time.Second),
		}, nil
	}})

	if err := agent.Sync(context.Background()); !errors.Is(err, ErrInvalidLeaseRecord) {
		t.Fatalf("Sync error = %v, want ErrInvalidLeaseRecord", err)
	}
	file := readLeadershipFile(t, agent.path)
	if file.HolderID != "" || !file.ExpiresAt.Before(now) {
		t.Fatalf("expired lease did not fail closed: %+v", file)
	}
}

func TestSyncReflectsHolderChange(t *testing.T) {
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	record := validRecord("router-a", 7, "a", now)
	agent := newTestAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) { return record, nil }})
	if err := agent.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	record = validRecord("router-b", 8, "b", now.Add(time.Second))
	if err := agent.Sync(context.Background()); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("holder change error = %v, want ErrNotHolder", err)
	}
	file := readLeadershipFile(t, agent.path)
	if file.HolderID != "" || !file.ExpiresAt.Before(now.Add(time.Second)) {
		t.Fatalf("holder change did not fail closed: %+v", file)
	}
}

func TestSyncRejectsDecreasingEpoch(t *testing.T) {
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	record := validRecord("router-a", 9, "a", now)
	agent := newTestAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) { return record, nil }})
	if err := agent.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	record = validRecord("router-a", 8, "b", now.Add(time.Second))
	if err := agent.Sync(context.Background()); !errors.Is(err, ErrInvalidLeaseRecord) {
		t.Fatalf("Sync error = %v, want ErrInvalidLeaseRecord", err)
	}
	file := readLeadershipFile(t, agent.path)
	if file.Epoch != 9 || file.HolderID != "" || !file.ExpiresAt.Before(now) {
		t.Fatalf("epoch regression did not fail closed: %+v", file)
	}
}

func TestSyncRejectsOldTokenForSameEpoch(t *testing.T) {
	now := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	record := validRecord("router-a", 11, "current-token", now)
	agent := newTestAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) { return record, nil }})
	if err := agent.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	record = validRecord("router-a", 11, "old-token", now.Add(time.Second))
	if err := agent.Sync(context.Background()); !errors.Is(err, ErrInvalidLeaseRecord) {
		t.Fatalf("Sync error = %v, want ErrInvalidLeaseRecord", err)
	}
	file := readLeadershipFile(t, agent.path)
	if file.HolderID != "" || file.Epoch != 11 || !file.ExpiresAt.Before(now) {
		t.Fatalf("old token did not fail closed: %+v", file)
	}
}

func newTestAgent(t *testing.T, now *time.Time, store LeaseStore) *Agent {
	t.Helper()
	agent, err := New(Config{
		Store:         store,
		ClusterID:     "cube-router-demo",
		HolderID:      "router-a",
		Path:          filepath.Join(t.TempDir(), "leadership.json"),
		RetryPeriod:   time.Second,
		Now:           func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func validRecord(holder string, epoch int64, token string, issuedAt time.Time) leadership.LeaseRecord {
	return leadership.LeaseRecord{
		ClusterID: "cube-router-demo",
		HolderID:  holder,
		Epoch:     epoch,
		Token:     token,
		IssuedAt:  issuedAt,
		ExpiresAt: issuedAt.Add(15 * time.Second),
	}
}

func readLeadershipFile(t *testing.T, path string) LeadershipFile {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var file LeadershipFile
	if err := json.Unmarshal(contents, &file); err != nil {
		t.Fatal(err)
	}
	return file
}
