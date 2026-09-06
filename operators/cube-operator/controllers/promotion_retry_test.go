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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/cube-js/cube-operator/internal/leadership"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPromotionFixedRetryWhileMarkerAndEndpointsConverge(t *testing.T) {
	ctx := context.Background()
	lease := leadership.LeaseRecord{ClusterID: "default/router", HolderID: "router-a", Epoch: 7, Token: "test-fence", ExpiresAt: time.Now().Add(time.Minute)}
	var acknowledged atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"isLeader": acknowledged.Load(), "metaStoreReady": true, "leaderEpoch": lease.Epoch, "leaseEpoch": lease.Epoch, "leaseTokenHash": hashLeaseToken(lease.Token)})
	}))
	defer server.Close()
	host, portText, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	annotations := leaseFenceValues(lease)
	annotations[promotionPhaseAnnotation] = promotionPhaseFenced
	annotations[promotionCandidateAnnotation] = lease.HolderID
	annotations[promotionEpochAnnotation] = strconv.FormatInt(lease.Epoch, 10)
	cr := &v1alpha1.CubestoreRouter{ObjectMeta: metav1.ObjectMeta{Name: "router", Namespace: "default", UID: "router-uid", Annotations: annotations}, Spec: v1alpha1.CubestoreRouterSpec{Namespace: "default", Selector: map[string]string{"app": "router"}, RouterPort: int32(port), StateStore: &v1alpha1.StateStore{Type: "kubernetes"}}, Status: v1alpha1.CubestoreRouterStatus{LeaderEpoch: lease.Epoch}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: lease.HolderID, Namespace: "default", Labels: map[string]string{"app": "router", labelNamespace: labelFollower}}, Status: corev1.PodStatus{PodIP: host, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "router-leader", Namespace: "default"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "router", labelNamespace: labelLeader}}}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1alpha1.AddToScheme, corev1.AddToScheme, discoveryv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.CubestoreRouter{}).WithObjects(cr, pod, service).Build()
	r := &CubestoreRouterReconciler{Client: kube, APIReader: kube, Scheme: scheme, leaseStore: &controllerLeaseStore{current: lease}}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cr)}
	reconcile := func() {
		t.Helper()
		result, err := r.Reconcile(ctx, request)
		if err != nil {
			t.Fatalf("expected propagation wait entered error backoff: %v (result=%+v)", err, result)
		}
		if result.RequeueAfter != 5*time.Second || result.Requeue {
			t.Fatalf("expected fixed 5s retry, got %+v", result)
		}
	}
	// Eighteen polls represent a 90s projection wait. Neither missing role
	// acknowledgement nor unchanged candidate status must stop scheduled probes.
	for i := 0; i < 18; i++ {
		reconcile()
	}
	var marker corev1.ConfigMap
	if err := kube.Get(ctx, client.ObjectKey{Namespace: "default", Name: resolveRoleConfigMapName(cr)}, &marker); err != nil {
		t.Fatal(err)
	}
	var state routerLeaderState
	if err := json.Unmarshal([]byte(marker.Data[defaultRoleDataKey]), &state); err != nil {
		t.Fatal(err)
	}
	if state.ActiveLeader != lease.HolderID || state.LeaseEpoch != lease.Epoch || state.LeaseToken != lease.Token {
		t.Fatal("waiting stopped publishing the fenced promotion marker")
	}
	var current v1alpha1.CubestoreRouter
	if err := kube.Get(ctx, request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Leader != "" {
		t.Fatal("traffic promoted before runtime acknowledgement")
	}
	acknowledged.Store(true)
	reconcile() // fenced -> ready
	reconcile() // ready -> promoted
	// EndpointSlice propagation is expected waiting, not a failure. Repeated
	// pending outcomes must not accumulate the workqueue's exponential backoff.
	for i := 0; i < 18; i++ {
		reconcile()
	}
	if err := kube.Get(ctx, request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Leader != "" {
		t.Fatal("traffic reported serving before EndpointSlice convergence")
	}
	ready := true
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "router-leader-one", Namespace: "default", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}}, AddressType: discoveryv1.AddressTypeIPv4, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{host}, Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: pod.Name}}}}
	if err := kube.Create(ctx, slice); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if err := kube.Get(ctx, request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Leader != pod.Name || current.Status.LeaderEpoch != lease.Epoch || current.Status.Recovery.Promotion.State != v1alpha1.RecoveryStateReady {
		t.Fatalf("converged promotion not recognized: %#v", current.Status)
	}
	// An actual backend failure must remain an error, not be softened to an
	// expected wait by the retry fix.
	r.leaseStore = &controllerLeaseStore{err: errors.New("lease backend unavailable")}
	if _, err := r.Reconcile(ctx, request); err == nil {
		t.Fatal("genuine lease backend failure was swallowed")
	}
}
