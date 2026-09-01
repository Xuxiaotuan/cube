package leadership

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestKubernetesStoreAcquireRenewReleaseFlow(t *testing.T) {
	clock := &testClock{t: time.Now()}
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	store := NewKubernetesStore(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		"default",
		"router-leader",
		clock,
	)
	ctx := context.Background()

	first, acquired, err := store.Acquire(ctx, "cluster-a", "router-0", 2*time.Second)
	if err != nil || !acquired {
		t.Fatalf("first acquire = (%#v, %t, %v)", first, acquired, err)
	}

	other, acquired, err := store.Acquire(ctx, "cluster-a", "router-1", 2*time.Second)
	if err != nil || acquired || other.Epoch != first.Epoch {
		t.Fatalf("contended acquire = (%#v, %t, %v)", other, acquired, err)
	}

	clock.Advance(2100 * time.Millisecond)
	gone, err := store.Get(ctx, "cluster-a")
	if !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("expired lease must be not found: lease=%#v err=%v", gone, err)
	}

	second, acquired, err := store.Acquire(ctx, "cluster-a", "router-1", 2*time.Second)
	if err != nil || !acquired || second.Epoch != first.Epoch+1 || second.Token == first.Token {
		t.Fatalf("stale holder acquire = (%#v, %t, %v)", second, acquired, err)
	}

	renewed, ok, err := store.Renew(ctx, second, 2*time.Second)
	if err != nil || !ok || renewed.Token != second.Token || renewed.Epoch != second.Epoch {
		t.Fatalf("renew = (%#v, %t, %v)", renewed, ok, err)
	}

	stale := second
	stale.Token = "not-a-current-token"
	if _, ok, err = store.Renew(ctx, stale, 2*time.Second); err != nil || ok {
		t.Fatalf("stale-token renew = (%t, %v)", ok, err)
	}

	if err := store.Release(ctx, stale); err == nil {
		t.Fatalf("stale release should fail: %v", err)
	}
	if err := store.Release(ctx, renewed); err != nil {
		t.Fatalf("valid release = %v", err)
	}

	if _, err := store.Get(ctx, "cluster-a"); err == nil {
		t.Fatal("expected no lease after release")
	}
}

func TestKubernetesStoreRejectsMalformedLease(t *testing.T) {
	clock := &testClock{t: time.Now()}
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	ttl := int32(30)
	holder := "router-0"
	epoch := int32(1)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "router-leader",
			Namespace: "default",
			Annotations: map[string]string{
				leaseClusterAnnotation:    "cluster-a",
				leaseGenerationAnnotation: "1",
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       stringPtr(holder),
			LeaseTransitions:     int32Ptr(epoch),
			AcquireTime:          &metav1.MicroTime{Time: clock.Now()},
			RenewTime:            &metav1.MicroTime{Time: clock.Now()},
			LeaseDurationSeconds: &ttl,
		},
	}
	store := NewKubernetesStore(
		fake.NewClientBuilder().WithScheme(scheme).WithObjects(lease).Build(),
		"default",
		"router-leader",
		clock,
	)

	if _, err := store.Get(context.Background(), "cluster-a"); !errors.Is(err, ErrLeaseUnknown) {
		t.Fatalf("malformed lease = %v, want %v", err, ErrLeaseUnknown)
	}
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}
