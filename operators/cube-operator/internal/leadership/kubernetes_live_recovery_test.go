package leadership

import (
	"context"
	"errors"
	"net/url"
	"os"
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

// Response loss is injected outside the real API client, not inside the store.
// The committed variant really creates a Lease in Kubernetes before losing the
// response. This is a controlled fault model, not an API-server network fault.
type recoveryCreateClient struct {
	client.Client
	owner string
	loss  string
}

var errRecoveryResponseLost = errors.New("modeled Create response loss")

func (c *recoveryCreateClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	obj.SetLabels(map[string]string{liveCASOwnerLabel: c.owner})
	if c.loss == "uncommitted" {
		return errRecoveryResponseLost
	}
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if c.loss == "committed" {
		return errRecoveryResponseLost
	}
	return nil
}

func TestKubernetesLiveControlledRecovery(t *testing.T) {
	if os.Getenv("CUBE_HA_LIVE_LEASE_TEST") != "1" {
		t.Skip("requires explicit live opt-in and current context orbstack")
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	raw, err := loader.RawConfig()
	if err != nil || raw.CurrentContext != "orbstack" || raw.Contexts["orbstack"] == nil {
		t.Fatal("refusing live recovery test: current context must be exactly orbstack")
	}
	cfg, err := loader.ClientConfig()
	if err != nil {
		t.Fatal("cannot resolve orbstack configuration")
	}
	server, err := url.Parse(cfg.Host)
	if err != nil || server.Scheme != "https" || (server.Hostname() != "127.0.0.1" && server.Hostname() != "localhost" && server.Hostname() != "::1") {
		t.Fatal("refusing non-local or non-HTTPS API endpoint")
	}
	cfg.Timeout = 10 * time.Second
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	direct, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal("cannot construct direct API client")
	}
	reader, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal("cannot construct independent direct API reader")
	}
	for _, scenario := range []string{"reservation-interrupted", "create-uncommitted", "create-committed", "running-lease-lost"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			id := uuid.NewString()
			key := client.ObjectKey{Namespace: liveCASNamespace, Name: "cube-ha-cas-test-" + id}
			clusterID := key.Namespace + "/" + key.Name
			var ownedUID types.UID
			t.Logf("AUDIT scenario=%s context=orbstack server=%s://%s namespace=%s lease=%s", scenario, server.Scheme, server.Host, key.Namespace, key.Name)
			t.Cleanup(func() {
				cleanupCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
				defer stop()
				var obj coordinationv1.Lease
				if err := reader.Get(cleanupCtx, key, &obj); apierrors.IsNotFound(err) {
					t.Log("AUDIT cleanup=absent")
					return
				} else if err != nil {
					t.Error("cleanup direct read failed")
					return
				}
				if obj.Labels[liveCASOwnerLabel] != id || obj.UID == "" || (ownedUID != "" && ownedUID != obj.UID) {
					t.Error("cleanup refused: unexpected ownership or UID")
					return
				}
				uid := obj.UID
				if err := direct.Delete(cleanupCtx, &obj, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
					t.Error("UID-preconditioned cleanup failed")
					return
				}
				if err := reader.Get(cleanupCtx, key, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
					t.Error("cleanup absence not confirmed")
					return
				}
				t.Logf("AUDIT cleanup=deleted uidPrecondition=%s", uid)
			})
			writer := &recoveryCreateClient{Client: direct, owner: id}
			if scenario == "create-uncommitted" {
				writer.loss = "uncommitted"
			}
			if scenario == "create-committed" {
				writer.loss = "committed"
			}
			store := NewKubernetesStoreWithReader(writer, reader, key.Namespace, key.Name)
			clock := &testClock{t: time.Now().UTC().Truncate(time.Second)}
			store.clock = clock
			// This models the durable CR reservation, without creating or editing
			// any CR. The separate controller tests exercise the real CR guard.
			reserved := scenario == "reservation-interrupted"
			guardErr := errors.New("reserved bootstrap cannot reset a missing Lease")
			store.BeforeCreate = func(context.Context) error {
				if reserved {
					return guardErr
				}
				reserved = true
				return nil
			}
			old, acquired, acquireErr := store.Acquire(ctx, clusterID, "old-test-holder", 30*time.Second)
			if scenario == "running-lease-lost" {
				if acquireErr != nil || !acquired || old.Epoch != 1 {
					t.Fatal("initial acquisition failed")
				}
			} else if scenario == "reservation-interrupted" {
				if !errors.Is(acquireErr, guardErr) || acquired {
					t.Fatal("reservation was bypassed")
				}
			} else if !errors.Is(acquireErr, errRecoveryResponseLost) || acquired {
				t.Fatal("Create response loss not injected")
			}
			var original coordinationv1.Lease
			var highWater int64
			if scenario == "running-lease-lost" || scenario == "create-committed" {
				if err := reader.Get(ctx, key, &original); err != nil {
					t.Fatal("created Lease not visible to direct reader")
				}
				ownedUID = original.UID
				if original.UID == "" || original.Labels[liveCASOwnerLabel] != id || original.Spec.LeaseTransitions == nil || *original.Spec.LeaseTransitions != 1 {
					t.Fatal("unexpected fixture identity")
				}
				highWater = 1 // Complete ledger: this test is the sole writer.
				if scenario == "create-committed" {
					current, ok, err := store.Acquire(ctx, clusterID, "other-test-holder", 30*time.Second)
					if err != nil || ok || current.Epoch != 1 {
						t.Fatal("unknown committed Create was not rediscovered")
					}
					if !reserved {
						t.Fatal("reservation disappeared")
					}
					t.Log("AUDIT unknownCreate=rediscovered epoch=1 recovery=not-needed modeledGuard=retained")
					return
				}
				uid := original.UID
				if err := direct.Delete(ctx, &original, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
					t.Fatal("test-only Lease loss injection failed")
				}
				ownedUID = ""
			}
			if err := reader.Get(ctx, key, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
				t.Fatal("missing Lease not confirmed")
			}
			if _, ok, err := store.Acquire(ctx, clusterID, "unsafe-reset", 30*time.Second); !errors.Is(err, guardErr) || ok {
				t.Fatal("missing Lease reset the epoch")
			}
			// All writers are synchronous and owned by this test. H is known
			// from the complete ledger, never inferred from a lagging mirror.
			token, err := newLeaseToken()
			if err != nil {
				t.Fatal("cannot generate recovery identity")
			}
			recovery := buildLeaseObject(key.Namespace, key.Name, LeaseRecord{ClusterID: clusterID, HolderID: "recovery-" + id, Epoch: highWater + 1, Generation: "1", Token: token, IssuedAt: time.Unix(0, 0).UTC(), ExpiresAt: time.Unix(1, 0).UTC()})
			recovery.Labels = map[string]string{liveCASOwnerLabel: id}
			if err := direct.Create(ctx, recovery); err != nil {
				t.Fatal("create-only recovery failed; do not retry or remove guard")
			}
			ownedUID = recovery.UID
			if ownedUID == "" {
				t.Fatal("recovery returned no UID")
			}
			if original.UID != "" {
				staleUID := original.UID
				if err := direct.Delete(ctx, &original, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &staleUID}}); !apierrors.IsConflict(err) {
					t.Fatal("old UID deletion was not rejected by API")
				}
			}
			if _, err := store.Get(ctx, clusterID); !errors.Is(err, ErrLeaseNotFound) {
				t.Fatal("recovery tombstone granted leadership")
			}
			next, ok, err := store.Acquire(ctx, clusterID, "new-test-holder", 30*time.Second)
			if err != nil || !ok || next.Epoch != highWater+2 || next.Token == token {
				t.Fatal("recovery failed monotonic acquisition")
			}
			if old.Epoch > 0 {
				if _, ok, err := store.Renew(ctx, old, 30*time.Second); err != nil || ok {
					t.Fatal("old identity renewed after recovery")
				}
				if err := store.Release(ctx, old); !errors.Is(err, ErrStaleLease) {
					t.Fatal("old identity released recovered Lease")
				}
			}
			current, err := store.Get(ctx, clusterID)
			if err != nil || ValidateLeaseFence(current, next) != nil || !reserved {
				t.Fatal("current identity or modeled reservation changed")
			}
			t.Logf("AUDIT completeTestLedgerH=%d restoredEpoch=%d acquiredEpoch=%d modeledGuard=retained oldIdentityIssued=%t oldIdentityRejected=%t", highWater, highWater+1, next.Epoch, old.Epoch > 0, old.Epoch > 0)
		})
	}
}
