package leadership

import (
	"context"
	"errors"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// No production resource names or environment-provided namespace overrides.
const liveCASNamespace = "cube-ha-remediation"
const liveCASOwnerLabel = "cubejs.io/cas-test-id"

type liveCASWriter struct {
	client.Client
	writes    atomic.Int32
	conflicts atomic.Int32
}

func (c *liveCASWriter) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.writes.Add(1)
	err := c.Client.Update(ctx, obj, opts...)
	if apierrors.IsConflict(err) {
		c.conflicts.Add(1)
	}
	return err
}

// Every read still reaches the direct API client. Only each contender's first
// response is held, so all CAS attempts present the same server resourceVersion.
type liveCASBarrierReader struct {
	client.Reader
	once    sync.Once
	ready   chan<- struct{}
	release <-chan struct{}
}

func (r *liveCASBarrierReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := r.Reader.Get(ctx, key, obj, opts...)
	r.once.Do(func() {
		r.ready <- struct{}{}
		select {
		case <-r.release:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}

func TestKubernetesLiveLeaseCAS(t *testing.T) {
	if os.Getenv("CUBE_HA_LIVE_LEASE_TEST") != "1" {
		t.Skip("opt in with CUBE_HA_LIVE_LEASE_TEST=1; current context must be orbstack")
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	raw, err := loader.RawConfig()
	if err != nil {
		t.Fatal("cannot load kubeconfig")
	}
	if raw.CurrentContext != "orbstack" || raw.Contexts["orbstack"] == nil {
		t.Fatal("refusing live test: current kubecontext must be exactly orbstack")
	}
	config, err := loader.ClientConfig()
	if err != nil {
		t.Fatal("cannot resolve orbstack client config")
	}
	server, err := url.Parse(config.Host)
	if err != nil || server.Scheme != "https" || (server.Hostname() != "127.0.0.1" && server.Hostname() != "localhost" && server.Hostname() != "::1") {
		t.Fatal("refusing live test: orbstack API must use a local HTTPS endpoint")
	}
	config.Timeout = 10 * time.Second
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	direct, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal("cannot construct direct Kubernetes client")
	}
	reader, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal("cannot construct independent direct API reader")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	id := uuid.NewString()
	key := client.ObjectKey{Namespace: liveCASNamespace, Name: "cube-ha-cas-test-" + id}
	clusterID := key.Namespace + "/" + key.Name
	t.Logf("AUDIT context=orbstack server=%s://%s namespace=%s lease=%s contenders=8", server.Scheme, server.Host, key.Namespace, key.Name)
	var createdUID types.UID
	// Registered before Create so an ambiguous response can still be inspected.
	// Never delete a same-name replacement or an object without our unique label.
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		var current coordinationv1.Lease
		if err := reader.Get(cleanupCtx, key, &current); apierrors.IsNotFound(err) {
			t.Logf("AUDIT cleanup=absent lease=%s", key.Name)
			return
		} else if err != nil {
			t.Error("cleanup authority read failed; exact test Lease requires inspection")
			return
		}
		if current.Labels[liveCASOwnerLabel] != id || current.UID == "" || (createdUID != "" && current.UID != createdUID) {
			t.Error("cleanup refused: test ownership or UID changed")
			return
		}
		uid := current.UID
		if err := direct.Delete(cleanupCtx, &current, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
			t.Error("UID-preconditioned cleanup failed")
			return
		}
		if err := reader.Get(cleanupCtx, key, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
			t.Error("cleanup deletion not confirmed by direct API read")
			return
		}
		t.Logf("AUDIT cleanup=deleted uidPrecondition=%s lease=%s", uid, key.Name)
	})
	clock := &testClock{t: time.Now().UTC().Truncate(time.Second)}
	token, err := newLeaseToken()
	if err != nil {
		t.Fatal("cannot create fixture identity")
	}
	seed := buildLeaseObject(key.Namespace, key.Name, LeaseRecord{
		ClusterID: clusterID, HolderID: "fixture", Epoch: 1, Generation: "1", Token: token,
		IssuedAt: clock.Now().Add(-2 * time.Second), ExpiresAt: clock.Now().Add(-time.Second),
	})
	seed.Labels = map[string]string{liveCASOwnerLabel: id}
	if err := direct.Create(ctx, seed); err != nil {
		t.Fatal("test-owned Lease creation failed; cleanup will inspect the exact name")
	}
	createdUID = seed.UID
	if createdUID == "" {
		t.Fatal("API returned no UID for created Lease")
	}
	t.Logf("AUDIT created uid=%s epoch=1 lease=%s", createdUID, key.Name)
	writer := &liveCASWriter{Client: direct}
	store := NewKubernetesStoreWithReader(writer, reader, key.Namespace, key.Name)
	store.clock = clock
	first, ok, err := store.Acquire(ctx, clusterID, "router-initial", time.Second)
	if err != nil || !ok || first.Epoch != 2 {
		t.Fatal("initial acquisition did not increment persisted epoch to 2")
	}
	clock.Advance(2 * time.Second)
	writer.writes.Store(0)
	writer.conflicts.Store(0)
	const contenders = 8
	ready := make(chan struct{}, contenders)
	release := make(chan struct{})
	type result struct {
		record LeaseRecord
		won    bool
		err    error
	}
	results := make(chan result, contenders)
	for i := 0; i < contenders; i++ {
		contender := NewKubernetesStoreWithReader(writer, &liveCASBarrierReader{Reader: reader, ready: ready, release: release}, key.Namespace, key.Name)
		contender.clock = clock
		go func(s *KubernetesStore) {
			record, won, err := s.Acquire(ctx, clusterID, "router-"+uuid.NewString(), 30*time.Second)
			results <- result{record: record, won: won, err: err}
		}(contender)
	}
	for i := 0; i < contenders; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("timed out waiting for authoritative contender reads")
		}
	}
	close(release)
	winners := 0
	var winner LeaseRecord
	for i := 0; i < contenders; i++ {
		select {
		case got := <-results:
			if got.err != nil || got.record.Epoch != first.Epoch+1 {
				t.Fatal("concurrent acquisition failed or returned a non-monotonic epoch")
			}
			if got.won {
				winners++
				winner = got.record
			}
		case <-ctx.Done():
			t.Fatal("concurrent acquisition did not finish within deadline")
		}
	}
	if winners != 1 || writer.conflicts.Load() < contenders-1 {
		t.Fatalf("CAS winners=%d conflicts=%d; want one winner and at least seven actual API conflicts", winners, writer.conflicts.Load())
	}
	t.Logf("AUDIT concurrent winners=%d epoch=%d writes=%d apiConflicts=%d", winners, winner.Epoch, writer.writes.Load(), writer.conflicts.Load())
	if _, ok, err := store.Renew(ctx, first, 30*time.Second); err != nil || ok {
		t.Fatal("stale identity was not rejected by Renew")
	}
	if err := store.Release(ctx, first); !errors.Is(err, ErrStaleLease) {
		t.Fatal("stale identity was not rejected by Release")
	}
	if err := store.Release(ctx, winner); err != nil {
		t.Fatal("current holder release failed")
	}
	var persisted coordinationv1.Lease
	if err := reader.Get(ctx, key, &persisted); err != nil || persisted.UID != createdUID || persisted.Spec.LeaseTransitions == nil || int64(*persisted.Spec.LeaseTransitions) != winner.Epoch {
		t.Fatal("release deleted or replaced the durable epoch")
	}
	if _, err := store.Get(ctx, clusterID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatal("released record remains live")
	}
	if _, ok, err := store.Renew(ctx, winner, 30*time.Second); err != nil || ok {
		t.Fatal("released identity could renew")
	}
	next, ok, err := store.Acquire(ctx, clusterID, "router-after-release", 30*time.Second)
	if err != nil || !ok || next.Epoch != winner.Epoch+1 || next.Token == winner.Token {
		t.Fatal("acquisition after release did not advance epoch and token")
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	writesBefore := writer.writes.Load()
	if _, ok, err := store.Renew(cancelled, next, 30*time.Second); ok || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled renewal did not refuse")
	}
	if _, ok, err := store.Acquire(cancelled, clusterID, "cancelled", 30*time.Second); ok || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled acquisition did not refuse")
	}
	if err := store.Release(cancelled, next); err == nil {
		t.Fatal("cancelled release did not refuse")
	}
	if writer.writes.Load() != writesBefore {
		t.Fatal("cancelled operation attempted an API mutation")
	}
	current, err := store.Get(ctx, clusterID)
	if err != nil || ValidateLeaseFence(current, next) != nil {
		t.Fatal("cancelled operations damaged the current identity")
	}
	t.Logf("AUDIT staleRenew=rejected staleRelease=rejected release=epoch-preserved nextEpoch=%d cancelledWrites=0", next.Epoch)
}
