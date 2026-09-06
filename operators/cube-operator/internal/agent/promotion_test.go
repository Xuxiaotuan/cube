package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/internal/leadership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type promotionReaderFunc func(context.Context) ([]byte, error)

func (f promotionReaderFunc) Read(ctx context.Context) ([]byte, error) { return f(ctx) }

type contextLeaseStore struct {
	LeaseStore
	read func(context.Context) (leadership.LeaseRecord, error)
}

func (s contextLeaseStore) Get(ctx context.Context, _ string) (leadership.LeaseRecord, error) {
	return s.read(ctx)
}

func promotionJSON(t *testing.T, record leadership.LeaseRecord) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"activeLeader": record.HolderID, "leaderEpoch": record.Epoch,
		"leaseClusterID": record.ClusterID, "leaseEpoch": record.Epoch,
		"leaseToken": record.Token, "extraRuntimeField": "preserved",
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func promotionAgent(t *testing.T, now *time.Time, store LeaseStore, source PromotionSource) *Agent {
	t.Helper()
	dir := t.TempDir()
	a, err := New(Config{Store: store, ClusterID: "cube-router-demo", HolderID: "router-a",
		Path: filepath.Join(dir, "leadership.json"), PromotionPath: filepath.Join(dir, "promotion.json"),
		PromotionSource: source, RetryPeriod: time.Second, SyncTimeout: 30 * time.Millisecond,
		Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func assertPromotionFenced(t *testing.T, a *Agent, now time.Time) {
	t.Helper()
	lease := readLeadershipFile(t, a.path)
	if lease.ExpiresAt.After(now) {
		t.Fatalf("leadership remains valid: %+v", lease)
	}
	raw, err := os.ReadFile(a.promotionPath)
	if err != nil {
		t.Fatal(err)
	}
	var marker promotionDocument
	if err := json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	if marker.ActiveLeader != "" || marker.LeaderEpoch != 0 || marker.LeaseToken != "" {
		t.Fatalf("promotion not fenced: holder=%q epoch=%d", marker.ActiveLeader, marker.LeaderEpoch)
	}
}

func TestPromotionDirectConfigMapPreservesContract(t *testing.T) {
	now := time.Now().UTC()
	record := validRecord("router-a", 9, "current-token", now)
	raw := promotionJSON(t, record)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "router-role-state", Namespace: "test"}, Data: map[string]string{promotionDataKey: string(raw)}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	calls := 0
	a := promotionAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) {
		calls++
		return record, nil
	}}, &ConfigMapPromotionSource{Reader: c, Namespace: "test", Name: cm.Name})
	if err := a.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("want lease read before and after marker, got %d", calls)
	}
	written, err := os.ReadFile(a.promotionPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(written, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["extraRuntimeField"] != "preserved" || doc["leaseToken"] != record.Token {
		t.Fatal("promotion JSON contract lost")
	}
	info, err := os.Stat(a.promotionPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0640 {
		t.Fatalf("unexpected permissions: %v", info.Mode())
	}
	// Updating the API object is sufficient: no projected ConfigMap volume exists.
	record = validRecord("router-a", 10, "new-token", now)
	cm.Data[promotionDataKey] = string(promotionJSON(t, record))
	if err := c.Update(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	if err := a.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := readLeadershipFile(t, a.path); got.Epoch != 10 {
		t.Fatalf("epoch=%d", got.Epoch)
	}
}

func TestPromotionRejectsStaleAndMalformedMarkers(t *testing.T) {
	for _, field := range []string{"activeLeader", "leaderEpoch", "leaseClusterID", "leaseEpoch", "leaseToken", "missing", "malformed", "oversized"} {
		t.Run(field, func(t *testing.T) {
			now := time.Now().UTC()
			record := validRecord("router-a", 9, "current-token", now)
			raw := promotionJSON(t, record)
			a := promotionAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) { return record, nil }}, promotionReaderFunc(func(context.Context) ([]byte, error) { return raw, nil }))
			if err := a.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "missing":
				raw = nil
			case "malformed":
				raw = []byte("{")
			case "oversized":
				raw = make([]byte, maxPromotionBytes+1)
			default:
				if field == "leaderEpoch" || field == "leaseEpoch" {
					doc[field] = 8
				} else {
					doc[field] = "stale"
				}
				var err error
				raw, err = json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := a.Sync(context.Background()); err == nil {
				t.Fatal("stale marker accepted")
			}
			assertPromotionFenced(t, a, now)
		})
	}
}

func TestPromotionDisconnectAndTimeoutFenceBothFiles(t *testing.T) {
	for _, failure := range []string{"lease", "configmap", "timeout", "cancelled", "not-holder", "expired"} {
		t.Run(failure, func(t *testing.T) {
			now := time.Now().UTC()
			record := validRecord("router-a", 9, "current-token", now)
			failing := false
			a := promotionAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) {
				if failing && failure == "lease" {
					return leadership.LeaseRecord{}, errors.New("API unavailable")
				}
				return record, nil
			}}, promotionReaderFunc(func(ctx context.Context) ([]byte, error) {
				if failing && failure == "configmap" {
					return nil, errors.New("ConfigMap unavailable")
				}
				if failing && failure == "timeout" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return promotionJSON(t, record), nil
			}))
			if err := a.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			failing = true
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "cancelled" {
				cancel()
			}
			if failure == "not-holder" {
				record.HolderID = "router-b"
			}
			if failure == "expired" {
				now = now.Add(time.Minute)
			}
			start := time.Now()
			if err := a.Sync(ctx); err == nil {
				t.Fatal("failed synchronization accepted")
			}
			if time.Since(start) > time.Second {
				t.Fatal("sync exceeded bounded timeout")
			}
			assertPromotionFenced(t, a, now)
		})
	}
}

func TestPromotionLeaseRaceFailsClosed(t *testing.T) {
	for _, change := range []string{"holder", "epoch", "token", "read-error"} {
		t.Run(change, func(t *testing.T) {
			now := time.Now().UTC()
			record := validRecord("router-a", 9, "current-token", now)
			calls := 0
			a := promotionAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) {
				calls++
				current := record
				if calls == 2 {
					switch change {
					case "holder":
						current.HolderID = "router-b"
					case "epoch":
						current.Epoch++
					case "token":
						current.Token = "replaced-token"
					case "read-error":
						return leadership.LeaseRecord{}, errors.New("API disconnected")
					}
				}
				return current, nil
			}}, promotionReaderFunc(func(context.Context) ([]byte, error) { return promotionJSON(t, record), nil }))
			if err := a.Sync(context.Background()); err == nil {
				t.Fatal("lease changed during marker fetch but promotion accepted")
			}
			assertPromotionFenced(t, a, now)
		})
	}
}

func TestConfigMapPromotionMissingDataFails(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "router-role-state", Namespace: "test"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	source := &ConfigMapPromotionSource{Reader: c, Namespace: "test", Name: cm.Name}
	if _, err := source.Read(context.Background()); err == nil {
		t.Fatal("missing data accepted")
	}
	if err := c.Delete(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Read(context.Background()); err == nil {
		t.Fatal("missing ConfigMap accepted")
	}
}

func TestPromotionLeaseReadTimeoutFencesPreviousMarker(t *testing.T) {
	now := time.Now().UTC()
	record := validRecord("router-a", 9, "current-token", now)
	blocked := false
	store := contextLeaseStore{read: func(ctx context.Context) (leadership.LeaseRecord, error) {
		if blocked {
			<-ctx.Done()
			return leadership.LeaseRecord{}, ctx.Err()
		}
		return record, nil
	}}
	a := promotionAgent(t, &now, store, promotionReaderFunc(func(context.Context) ([]byte, error) { return promotionJSON(t, record), nil }))
	if err := a.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	blocked = true
	start := time.Now()
	if err := a.Sync(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want bounded deadline error, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("lease API timeout was not bounded")
	}
	assertPromotionFenced(t, a, now)
}

func TestPromotionShutdownFencesBothFiles(t *testing.T) {
	now := time.Now().UTC()
	record := validRecord("router-a", 9, "current-token", now)
	a := promotionAgent(t, &now, fakeStore{get: func() (leadership.LeaseRecord, error) { return record, nil }}, promotionReaderFunc(func(context.Context) ([]byte, error) { return promotionJSON(t, record), nil }))
	if err := a.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = a.Run(ctx)
	assertPromotionFenced(t, a, now)
}
