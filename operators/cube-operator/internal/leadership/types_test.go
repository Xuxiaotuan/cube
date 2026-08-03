package leadership

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	v1alpha1 "github.com/cube-js/cube-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

type testLeaseStore struct{}

func (testLeaseStore) Acquire(context.Context, string, string, time.Duration) (LeaseRecord, bool, error) {
	return LeaseRecord{}, false, nil
}

func (testLeaseStore) Renew(context.Context, LeaseRecord, time.Duration) (LeaseRecord, bool, error) {
	return LeaseRecord{}, false, nil
}

func (testLeaseStore) Release(context.Context, LeaseRecord) error { return nil }

func (testLeaseStore) Get(context.Context, string) (LeaseRecord, error) { return LeaseRecord{}, nil }

var _ LeaseStore = testLeaseStore{}

func TestRouterSpecApplyDefaults(t *testing.T) {
	spec := validRouterSpec()
	spec.ElectionStrategy = ""
	spec.LeaseDurationSeconds = 0
	spec.RenewDeadlineSeconds = 0
	spec.RetryPeriodSeconds = 0

	spec.ApplyDefaults()

	if spec.ElectionStrategy != v1alpha1.ElectionStrategyLease ||
		spec.LeaseDurationSeconds != v1alpha1.DefaultLeaseDurationSeconds ||
		spec.RenewDeadlineSeconds != v1alpha1.DefaultRenewDeadlineSeconds ||
		spec.RetryPeriodSeconds != v1alpha1.DefaultRetryPeriodSeconds {
		t.Fatalf("defaults were not applied: %#v", spec)
	}
}

func TestRouterSpecValidateAppliesDefaults(t *testing.T) {
	spec := validRouterSpec()
	spec.ElectionStrategy = ""
	spec.LeaseDurationSeconds = 0
	spec.RenewDeadlineSeconds = 0
	spec.RetryPeriodSeconds = 0

	if err := spec.Validate(); err != nil {
		t.Fatalf("Validate should accept omitted defaulted values: %v", err)
	}
	if spec.ElectionStrategy != v1alpha1.ElectionStrategyLease ||
		spec.LeaseDurationSeconds != v1alpha1.DefaultLeaseDurationSeconds ||
		spec.RenewDeadlineSeconds != v1alpha1.DefaultRenewDeadlineSeconds ||
		spec.RetryPeriodSeconds != v1alpha1.DefaultRetryPeriodSeconds {
		t.Fatalf("Validate did not apply defaults: %#v", spec)
	}
}

func TestRouterSpecRejectsInvalidSelector(t *testing.T) {
	spec := validRouterSpec()
	spec.Selector = map[string]string{}

	if err := spec.Validate(); err == nil {
		t.Fatal("expected empty selector to be rejected")
	}
}

func TestRouterSpecRejectsEmptyNamespace(t *testing.T) {
	spec := validRouterSpec()
	spec.Namespace = ""

	if err := spec.Validate(); err == nil {
		t.Fatal("expected empty namespace to be rejected")
	}
}

func TestRouterSpecRejectsInvalidTiming(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1alpha1.CubestoreRouterSpec)
	}{
		{name: "non-positive duration", mutate: func(spec *v1alpha1.CubestoreRouterSpec) { spec.LeaseDurationSeconds = -1 }},
		{name: "non-positive deadline", mutate: func(spec *v1alpha1.CubestoreRouterSpec) { spec.RenewDeadlineSeconds = -1 }},
		{name: "deadline equals duration", mutate: func(spec *v1alpha1.CubestoreRouterSpec) { spec.RenewDeadlineSeconds = spec.LeaseDurationSeconds }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := validRouterSpec()
			tt.mutate(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatal("expected invalid timing to be rejected")
			}
		})
	}
}

func TestRouterSpecValidatesSecretReferences(t *testing.T) {
	t.Run("requires state store secret name", func(t *testing.T) {
		spec := validRouterSpec()
		spec.StateStore.SecretRef.Name = ""
		if err := spec.Validate(); err == nil {
			t.Fatal("expected missing state store secret name to be rejected")
		}
	})

	t.Run("requires object store secret namespace", func(t *testing.T) {
		spec := validRouterSpec()
		spec.Storage.ObjectStoreSecretRef = &corev1.SecretReference{Name: "object-store"}
		if err := spec.Validate(); err == nil {
			t.Fatal("expected missing object store secret namespace to be rejected")
		}
	})
}

func TestRouterSpecAcceptsLegacyDSNForMigration(t *testing.T) {
	spec := validRouterSpec()
	spec.StateStore = nil
	spec.LeaderStateStore = &v1alpha1.LeaderStateStore{
		Type: "redis",
		DSN:  "redis://legacy.example.invalid:6379/0",
	}

	if err := spec.Validate(); err != nil {
		t.Fatalf("legacy DSN should remain valid for migration: %v", err)
	}
}

func TestRouterSpecRejectsUnknownLegacyStoreType(t *testing.T) {
	spec := validRouterSpec()
	spec.StateStore = nil
	spec.LeaderStateStore = &v1alpha1.LeaderStateStore{
		Type: "etcd",
		DSN:  "etcd://legacy.example.invalid:2379",
	}

	if err := spec.Validate(); err == nil {
		t.Fatal("expected unknown legacy store type to be rejected")
	}
}

func TestLeaseStoreFencesStaleEpoch(t *testing.T) {
	store := &fencingLeaseStore{}
	ctx := context.Background()

	first, acquired, err := store.Acquire(ctx, "cluster-a", "holder-a", time.Hour)
	if err != nil || !acquired {
		t.Fatalf("first holder should acquire: record=%#v acquired=%v err=%v", first, acquired, err)
	}
	if err := store.Release(ctx, first); err != nil {
		t.Fatalf("release should succeed: %v", err)
	}

	second, acquired, err := store.Acquire(ctx, "cluster-a", "holder-b", time.Hour)
	if err != nil || !acquired {
		t.Fatalf("second holder should acquire: record=%#v acquired=%v err=%v", second, acquired, err)
	}
	if second.Epoch != first.Epoch+1 || second.Token == first.Token {
		t.Fatalf("new holder must receive a new fencing epoch and token: first=%#v second=%#v", first, second)
	}
	if err := ValidateLeaseFence(second, first); err == nil {
		t.Fatal("production fence predicate must reject the old epoch and token")
	}
	if err := ValidateLeaseFence(second, second); err != nil {
		t.Fatalf("production fence predicate must accept the current epoch and token: %v", err)
	}

	if renewed, ok, err := store.Renew(ctx, first, time.Hour); err != nil || ok || renewed.Epoch != second.Epoch {
		t.Fatalf("stale holder must be fenced: record=%#v renewed=%v err=%v", renewed, ok, err)
	}
	if renewed, ok, err := store.Renew(ctx, second, time.Hour); err != nil || !ok || renewed.Token != second.Token {
		t.Fatalf("current holder should renew: record=%#v renewed=%v err=%v", renewed, ok, err)
	}
}

type fencingLeaseStore struct {
	mu        sync.Mutex
	record    LeaseRecord
	nextEpoch int64
}

func (s *fencingLeaseStore) Acquire(_ context.Context, clusterID, holderID string, duration time.Duration) (LeaseRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.record.Token != "" && s.record.ExpiresAt.After(time.Now()) {
		return s.record, false, nil
	}
	s.nextEpoch++
	now := time.Now()
	s.record = LeaseRecord{
		ClusterID: clusterID,
		HolderID:  holderID,
		Epoch:     s.nextEpoch,
		Token:     fmt.Sprintf("token-%d", s.nextEpoch),
		IssuedAt:  now,
		ExpiresAt: now.Add(duration),
	}
	return s.record, true, nil
}

func (s *fencingLeaseStore) Renew(_ context.Context, lease LeaseRecord, duration time.Duration) (LeaseRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateLeaseFence(s.record, lease); err != nil || !s.record.ExpiresAt.After(time.Now()) {
		return s.record, false, nil
	}
	s.record.ExpiresAt = time.Now().Add(duration)
	return s.record, true, nil
}

func (s *fencingLeaseStore) Release(_ context.Context, lease LeaseRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateLeaseFence(s.record, lease); err != nil {
		return err
	}
	s.record = LeaseRecord{}
	return nil
}

func (s *fencingLeaseStore) Get(_ context.Context, _ string) (LeaseRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.record, nil
}

func validRouterSpec() v1alpha1.CubestoreRouterSpec {
	return v1alpha1.CubestoreRouterSpec{
		Selector:             map[string]string{"app": "cubestore-router"},
		Namespace:            "router-system",
		ElectionStrategy:     v1alpha1.ElectionStrategyLease,
		LeaseDurationSeconds: 30,
		RenewDeadlineSeconds: 20,
		RetryPeriodSeconds:   5,
		StateStore: &v1alpha1.StateStore{
			Type:      "redis",
			SecretRef: corev1.SecretReference{Name: "router-state", Namespace: "router-system"},
		},
		MetaStore: v1alpha1.MetaStore{Address: "http://metastore.router-system.svc:9090"},
		Storage:   v1alpha1.Storage{DataPVC: "router-data"},
	}
}
