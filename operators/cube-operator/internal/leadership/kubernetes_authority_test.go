package leadership

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Reads deliberately see an independent stale cache; writes reach authority.
type authoritySplitClient struct {
	client.Client
	cache        client.Reader
	beforeUpdate func()
	updates      int
}

func (c *authoritySplitClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.cache.Get(ctx, key, obj, opts...)
}

func (c *authoritySplitClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.updates++
	if c.beforeUpdate != nil {
		f := c.beforeUpdate
		c.beforeUpdate = nil
		f()
		return apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName(), errors.New("concurrent holder"))
	}
	return c.Client.Update(ctx, obj, opts...)
}

type unavailableLeaseReader struct{ client.Reader }

func (unavailableLeaseReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("API unavailable")
}

func authorityFixture(t *testing.T) (*KubernetesStore, *authoritySplitClient, client.Client, LeaseRecord) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	old := LeaseRecord{ClusterID: "cluster", HolderID: "router-a", Epoch: 7, Generation: "1", Token: "old", IssuedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Minute)}
	obj := buildLeaseObject("ns", "leader", old)
	authority := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()
	cache := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj.DeepCopy()).Build()
	split := &authoritySplitClient{Client: authority, cache: cache}
	return NewKubernetesStoreWithReader(split, authority, "ns", "leader"), split, authority, old
}

func advanceAuthority(t *testing.T, authority client.Client) {
	t.Helper()
	var obj coordinationv1.Lease
	if err := authority.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "leader"}, &obj); err != nil {
		t.Fatal(err)
	}
	obj.Spec.LeaseTransitions = int32Ptr(8)
	obj.Annotations[leaseTokenAnnotation] = "new"
	if err := authority.Update(context.Background(), &obj); err != nil {
		t.Fatal(err)
	}
}

func TestKubernetesAuthorityDivergentCache(t *testing.T) {
	s, split, authority, old := authorityFixture(t)
	advanceAuthority(t, authority)
	got, err := s.Get(context.Background(), old.ClusterID)
	if err != nil || got.Epoch != 8 {
		t.Fatalf("authoritative Get = %+v, %v", got, err)
	}
	if _, ok, err := s.Renew(context.Background(), old, time.Minute); err != nil || ok {
		t.Fatalf("stale renewal: %v %v", ok, err)
	}
	if split.updates != 0 {
		t.Fatal("stale lease reached writer")
	}
}

func TestKubernetesAuthorityUnavailableAndMissingReader(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "missing"}[missing], func(t *testing.T) {
			s, split, _, old := authorityFixture(t)
			if missing {
				s.reader = nil
			} else {
				s.reader = unavailableLeaseReader{}
			}
			if _, err := s.Get(context.Background(), old.ClusterID); err == nil {
				t.Fatal("Get fell back to cache")
			}
			if _, ok, err := s.Renew(context.Background(), old, time.Minute); err == nil || ok {
				t.Fatal("Renew fell back to cache")
			}
			if _, ok, err := s.Acquire(context.Background(), old.ClusterID, "router-b", time.Minute); err == nil || ok {
				t.Fatal("Acquire fell back to cache")
			}
			if split.updates != 0 {
				t.Fatal("unavailable authority allowed write")
			}
		})
	}
}

func TestKubernetesAuthorityConflictRereadsIdentity(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(map[bool]string{false: "renew", true: "release"}[release], func(t *testing.T) {
			s, split, authority, old := authorityFixture(t)
			split.beforeUpdate = func() { advanceAuthority(t, authority) }
			if release {
				if err := s.Release(context.Background(), old); !errors.Is(err, ErrStaleLease) {
					t.Fatalf("release = %v", err)
				}
			} else if current, ok, err := s.Renew(context.Background(), old, time.Minute); err != nil || ok || current.Epoch != 8 {
				t.Fatalf("renew = %+v %v %v", current, ok, err)
			}
			if split.updates != 1 {
				t.Fatalf("stale identity retried write %d times", split.updates)
			}
			if current, err := s.Get(context.Background(), old.ClusterID); err != nil || current.Epoch != 8 {
				t.Fatalf("new holder damaged: %+v %v", current, err)
			}
		})
	}
}

func TestKubernetesReleasePreservesEpoch(t *testing.T) {
	s, _, authority, old := authorityFixture(t)
	if err := s.Release(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	var persisted coordinationv1.Lease
	if err := authority.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "leader"}, &persisted); err != nil {
		t.Fatal(err)
	}
	if *persisted.Spec.LeaseTransitions != 7 {
		t.Fatal("release lost epoch")
	}
	if _, ok, err := s.Renew(context.Background(), old, time.Minute); err != nil || ok {
		t.Fatal("released holder renewed")
	}
	next, ok, err := s.Acquire(context.Background(), old.ClusterID, "router-b", time.Minute)
	if err != nil || !ok || next.Epoch != 8 || next.Token == old.Token {
		t.Fatalf("next = %+v %v %v", next, ok, err)
	}
}

func TestKubernetesManualRecoveryModelAndEpochExhaustion(t *testing.T) {
	for _, epoch := range []int64{9, math.MaxInt32} {
		s, _, authority, old := authorityFixture(t)
		// Models only the reviewed tombstone after external isolation and a
		// proven historical upper bound. It does not prove those prerequisites.
		var obj coordinationv1.Lease
		if err := authority.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "leader"}, &obj); err != nil {
			t.Fatal(err)
		}
		obj.Spec.LeaseTransitions = int32Ptr(int32(epoch))
		obj.Annotations[leaseTokenAnnotation] = "unique-recovery-token"
		obj.Spec.RenewTime = obj.Spec.AcquireTime.DeepCopy()
		obj.Spec.RenewTime.Time = time.Unix(0, 0).UTC()
		if err := authority.Update(context.Background(), &obj); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := s.Renew(context.Background(), old, time.Minute); err != nil || ok {
			t.Fatal("old identity renewed recovered state")
		}
		next, ok, err := s.Acquire(context.Background(), old.ClusterID, "router-b", time.Minute)
		if epoch == math.MaxInt32 {
			if err == nil || ok {
				t.Fatal("epoch overflow accepted")
			}
		} else if err != nil || !ok || next.Epoch != epoch+1 {
			t.Fatalf("recovery = %+v %v %v", next, ok, err)
		}
	}
}

func TestKubernetesCancelledControllerCannotRenew(t *testing.T) {
	s, split, _, old := authorityFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, err := s.Renew(ctx, old, time.Minute); !errors.Is(err, context.Canceled) || ok {
		t.Fatalf("renew = %v %v", ok, err)
	}
	if split.updates != 0 {
		t.Fatal("cancelled controller wrote")
	}
}
