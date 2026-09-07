package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/internal/leadership"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func leaseRecoveryFixture(t *testing.T) (*CubestoreRouterReconciler, *v1alpha1.CubestoreRouter) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1alpha1.AddToScheme, corev1.AddToScheme, coordinationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cr := externalLeaseTestRouterWithType("kubernetes")
	cr.UID = "router-uid"
	cr.Spec.Selector = map[string]string{"app": "router"}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	return &CubestoreRouterReconciler{Client: kube, APIReader: kube, Scheme: scheme}, cr
}

func TestLeaseBootstrapInterruptedReservationAndCompetition(t *testing.T) {
	r, cr := leaseRecoveryFixture(t)
	if err := r.reserveLeaseBootstrap(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	// A second initializer (or restart after reservation) cannot clear the guard.
	for i := 0; i < 2; i++ {
		other := &CubestoreRouterReconciler{Client: r.Client, APIReader: r.APIReader}
		if err := other.reserveLeaseBootstrap(context.Background(), cr); !errors.Is(err, errLeaseStateLost) {
			t.Fatalf("reservation retry = %v", err)
		}
	}
}

type unknownCreateClient struct {
	client.Client
	committed bool
}

func (c *unknownCreateClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*coordinationv1.Lease); ok {
		if c.committed {
			if err := c.Client.Create(ctx, obj, opts...); err != nil {
				return err
			}
		}
		return errors.New("create response lost")
	}
	return c.Client.Create(ctx, obj, opts...)
}

func TestLeaseBootstrapUnknownCreateAndLeaseDeletion(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(strconv.FormatBool(committed), func(t *testing.T) {
			r, cr := leaseRecoveryFixture(t)
			r.Client = &unknownCreateClient{Client: r.Client, committed: committed}
			store, closeStore, err := r.routerLeaseStore(context.Background(), cr)
			if err != nil {
				t.Fatal(err)
			}
			defer closeStore()
			cluster := resolveLeaderStateRecordID(cr.Namespace, cr.Name)
			if _, _, err := store.Acquire(context.Background(), cluster, "router-a", time.Minute); err == nil {
				t.Fatal("expected ambiguous creation result")
			}
			got, acquired, err := store.Acquire(context.Background(), cluster, "router-b", time.Minute)
			if !committed {
				if !errors.Is(err, errLeaseStateLost) || acquired {
					t.Fatalf("unknown uncommitted create reset epoch: %v", err)
				}
				return
			}
			if err != nil || acquired || got.Epoch != 1 || got.HolderID != "router-a" {
				t.Fatalf("committed create not rediscovered: %+v %v %v", got, acquired, err)
			}
			var lease coordinationv1.Lease
			if err := r.APIReader.Get(context.Background(), client.ObjectKey{Namespace: cr.Namespace, Name: kubernetesLeaseName(cr)}, &lease); err != nil {
				t.Fatal(err)
			}
			if err := r.Delete(context.Background(), &lease); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := store.Acquire(context.Background(), cluster, "router-b", time.Minute); !errors.Is(err, errLeaseStateLost) || ok {
				t.Fatalf("Lease deletion reset epoch: %v", err)
			}
		})
	}
}

func TestLeaseBootstrapRejectsHistoricalFencesAndMissingReader(t *testing.T) {
	for _, source := range []string{"CR", "ConfigMap", "Pod", "reader", "uid"} {
		t.Run(source, func(t *testing.T) {
			r, cr := leaseRecoveryFixture(t)
			switch source {
			case "CR":
				fresh := &v1alpha1.CubestoreRouter{}
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(cr), fresh); err != nil {
					t.Fatal(err)
				}
				fresh.Annotations = map[string]string{leaseEpochAnnotation: "10"}
				if err := r.Update(context.Background(), fresh); err != nil {
					t.Fatal(err)
				}
			case "ConfigMap":
				if err := r.Create(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: resolveRoleConfigMapName(cr), Namespace: cr.Namespace}, Data: map[string]string{defaultRoleDataKey: "historical"}}); err != nil {
					t.Fatal(err)
				}
			case "Pod":
				if err := r.Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "router-a", Namespace: cr.Namespace, Labels: cr.Spec.Selector, Annotations: map[string]string{leaseEpochAnnotation: "10"}}}); err != nil {
					t.Fatal(err)
				}
			case "reader":
				r.APIReader = nil
			case "uid":
				cr.UID = "recreated-router"
			}
			if err := r.reserveLeaseBootstrap(context.Background(), cr); err == nil {
				t.Fatal("unsafe bootstrap accepted")
			}
			if source == "reader" {
				if _, _, err := r.routerLeaseStore(context.Background(), cr); err == nil {
					t.Fatal("production Lease path accepted missing APIReader")
				}
			}
		})
	}
}

type adoptionLeaseStore struct {
	controllerLeaseStore
	renewals int
}

func (s *adoptionLeaseStore) Renew(ctx context.Context, lease leadership.LeaseRecord, _ time.Duration) (leadership.LeaseRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return leadership.LeaseRecord{}, false, err
	}
	s.renewals++
	if err := leadership.ValidateLeaseFence(s.current, lease); err != nil {
		return s.current, false, nil
	}
	return s.current, true, nil
}

func TestLeaseManagerAdoptionRequiresAcknowledgement(t *testing.T) {
	for _, mode := range []string{"healthy", "no-election", "wrong-token", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			r, cr := leaseRecoveryFixture(t)
			lease := roleWriteLease(7, "adoption-token")
			store := &adoptionLeaseStore{controllerLeaseStore: controllerLeaseStore{current: lease}}
			r.leaseStore = store
			r.ManagerLeaderElection = mode != "no-election"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				token := lease.Token
				if mode == "wrong-token" {
					token = "stale"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"isLeader": true, "metaStoreReady": true, "leaderEpoch": lease.Epoch, "leaseEpoch": lease.Epoch, "leaseTokenHash": hashLeaseToken(token)})
			}))
			defer server.Close()
			host, p, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(p)
			if err != nil {
				t.Fatal(err)
			}
			cr.Spec.RouterPort = int32(port)
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: lease.HolderID, Namespace: cr.Namespace, Labels: cr.Spec.Selector}, Status: corev1.PodStatus{PodIP: host, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
			if err := r.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			pods, err := r.listRouters(context.Background(), cr.Namespace, cr.Spec.Selector)
			if err != nil {
				t.Fatal(err)
			}
			candidates := r.probeCandidates(context.Background(), pods, *cr)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			_, got, err := r.resolveRouterLease(ctx, cr, candidates)
			if mode == "cancelled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel = %v", err)
				}
			} else if err != nil || got.Epoch != lease.Epoch {
				t.Fatalf("adoption changed epoch: %+v %v", got, err)
			}
			want := 0
			if mode == "healthy" {
				want = 1
			}
			if store.renewals != want {
				t.Fatalf("renewals = %d, want %d", store.renewals, want)
			}
		})
	}
}

func TestLeaseCancelledReconcileStopsBeforeClientAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &CubestoreRouterReconciler{}
	if _, err := r.Reconcile(ctx, ctrl.Request{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcile = %v", err)
	}
}
