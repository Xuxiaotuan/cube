package controllers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type readinessReply struct {
	code int
	body string
}
type readinessReplies map[string]readinessReply

func (replies readinessReplies) RoundTrip(req *http.Request) (*http.Response, error) {
	reply, ok := replies[req.URL.Hostname()]
	if !ok {
		reply = replies["default"]
	}
	return &http.Response{StatusCode: reply.code, Body: io.NopCloser(strings.NewReader(reply.body)), Header: http.Header{}}, nil
}
func readinessPayload(value any) string {
	raw, _ := json.Marshal(map[string]any{"health": "HEALTH", "recoveryCapabilities": map[string]any{"fileImportBuildScheduling": value}})
	return string(raw)
}

func TestReadinessCapabilityDoesNotChangeHTTPReadiness(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		name, body string
		code       int
		ready      bool
		capability *bool
	}{
		{"supported", readinessPayload(true), 200, true, &yes},
		{"unsupported", readinessPayload(false), 200, true, &no},
		{"legacy", "{}", 200, true, nil},
		{"null", readinessPayload(nil), 200, true, nil},
		{"invalid boolean", readinessPayload("true"), 200, true, nil},
		{"malformed", "not-json", 200, true, nil},
		{"multiple documents", readinessPayload(true) + "{}", 200, true, nil},
		{"oversized", readinessPayload(true) + strings.Repeat(" ", 64<<10), 200, true, nil},
		{"unready", readinessPayload(true), 503, false, nil},
		{"redirect", readinessPayload(true), 302, false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := businessCluster()
			r := clusterReconciler(t, c)
			addReadyPods(t, r, c, cubeComponentAPI, 1)
			r.HTTPClient = &http.Client{Transport: readinessReplies{"default": {code: tc.code, body: tc.body}}}
			ready, capability, err := r.applicationObservation(context.Background(), c, cubeComponentAPI, 1)
			if err != nil || ready != tc.ready {
				t.Fatalf("readiness=%v error=%v", ready, err)
			}
			if (capability == nil) != (tc.capability == nil) {
				t.Fatalf("capability=%v want %v", capability, tc.capability)
			}
			if capability != nil && *capability != *tc.capability {
				t.Fatalf("capability=%v want %v", *capability, *tc.capability)
			}
		})
	}
}

func TestReadinessCapabilityRequiresReplicaConsensus(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name       string
		second     readinessReply
		ready      bool
		capability *bool
	}{
		{"all supported", readinessReply{200, readinessPayload(true)}, true, &yes},
		{"one legacy", readinessReply{200, "{}"}, true, nil},
		{"one unsupported", readinessReply{200, readinessPayload(false)}, true, &no},
		{"one unavailable", readinessReply{503, readinessPayload(true)}, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := businessCluster()
			r := clusterReconciler(t, c)
			for i := 1; i <= 2; i++ {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-" + strconv.Itoa(i), Namespace: c.Namespace, Labels: clusterLabels(c, cubeComponentAPI)}, Status: corev1.PodStatus{PodIP: "127.0.0." + strconv.Itoa(i), Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
				if err := r.Create(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			}
			r.HTTPClient = &http.Client{Transport: readinessReplies{"127.0.0.1": {200, readinessPayload(true)}, "127.0.0.2": tc.second}}
			ready, capability, err := r.applicationObservation(context.Background(), c, cubeComponentAPI, 2)
			if err != nil || ready != tc.ready || (capability == nil) != (tc.capability == nil) {
				t.Fatalf("ready=%v capability=%v err=%v", ready, capability, err)
			}
			if capability != nil && *capability != *tc.capability {
				t.Fatalf("capability=%v want %v", *capability, *tc.capability)
			}
		})
	}
}

func TestFileImportRecoveryNeedsCurrentWorkloadsAndPromotedRuntime(t *testing.T) {
	type evidence struct {
		cluster      *v1alpha1.CubeCluster
		components   map[string]v1alpha1.CubeComponentStatus
		recovery     v1alpha1.RouterRecoveryStatus
		capabilities map[string]*bool
	}
	for _, tc := range []struct {
		name   string
		mutate func(*evidence)
		state  string
	}{
		{"supported", func(*evidence) {}, "Ready"},
		{"not managed", func(e *evidence) { e.cluster.Spec.Refresher = nil }, "NeedsContext"},
		{"API old generation", func(e *evidence) {
			s := e.components[cubeComponentAPI]
			s.ObservedGeneration--
			e.components[cubeComponentAPI] = s
		}, "NeedsContext"},
		{"refresher not ready", func(e *evidence) {
			s := e.components[cubeComponentRefresher]
			s.Ready = false
			e.components[cubeComponentRefresher] = s
		}, "NeedsContext"},
		{"API missing capability", func(e *evidence) { delete(e.capabilities, cubeComponentAPI) }, "Ready"},
		{"refresher missing capability", func(e *evidence) { delete(e.capabilities, cubeComponentRefresher) }, "NeedsContext"},
		{"API unsupported", func(e *evidence) { no := false; e.capabilities[cubeComponentAPI] = &no }, "Ready"},
		{"refresher unsupported", func(e *evidence) { no := false; e.capabilities[cubeComponentRefresher] = &no }, "Blocked"},
		{"not promoted", func(e *evidence) { e.recovery.Promotion.State = "NeedsContext" }, "NeedsContext"},
		{"jobs unverified", func(e *evidence) { e.recovery.JobRecovery.State = "Blocked" }, "NeedsContext"},
		{"uploads unverified", func(e *evidence) { e.recovery.MutationReconcile.State = "NeedsContext" }, "NeedsContext"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			yes := true
			c := businessCluster()
			c.Spec.Refresher = &v1alpha1.CubeComponentSpec{}
			component := v1alpha1.CubeComponentStatus{Ready: true, WorkloadGeneration: 3, ObservedGeneration: 3}
			gate := v1alpha1.RecoveryGateStatus{State: "Ready"}
			e := evidence{c, map[string]v1alpha1.CubeComponentStatus{cubeComponentAPI: component, cubeComponentRefresher: component}, v1alpha1.RouterRecoveryStatus{Promotion: gate, JobRecovery: gate, MutationReconcile: gate}, map[string]*bool{cubeComponentAPI: &yes, cubeComponentRefresher: &yes}}
			tc.mutate(&e)
			result := refresherBuildRecovery(e.cluster, e.components, e.recovery, e.capabilities)
			if result.State != tc.state {
				t.Fatalf("state=%s want=%s: %s", result.State, tc.state, result.Message)
			}
			if result.State == "Ready" && (!strings.Contains(result.Message, "file-import") || !strings.Contains(result.Message, "not arbitrary mutation exactly-once")) {
				t.Fatal("supported gate lost its scoped semantics")
			}
		})
	}
}

func TestClusterConsumesAPIAndRefresherBuildRecoveryEvidence(t *testing.T) {
	c := businessCluster()
	c.Spec.Refresher = &v1alpha1.CubeComponentSpec{}
	r := clusterReconciler(t, c)
	ctx := context.Background()
	r.HTTPClient = &http.Client{Transport: readinessReplies{"default": {200, readinessPayload(true)}}}
	reconcileClusterTest(t, r, c)
	markCurrent(t, r, c, cubeComponentAPI)
	if err := r.reconcileRefresher(ctx, c); err != nil {
		t.Fatal(err)
	}
	for component, spec := range componentSpecs(c) {
		addReadyPods(t, r, c, component, spec.Replicas)
		markCurrent(t, r, c, component)
	}
	var router v1alpha1.CubestoreRouter
	getComponent(t, r, c, cubeComponentRouter, &router)
	gate := v1alpha1.RecoveryGateStatus{State: "Ready", Reason: "RuntimeSupported", Message: "Current promoted runtime"}
	router.Status.Recovery = v1alpha1.RouterRecoveryStatus{Promotion: gate, JobRecovery: gate, MutationReconcile: gate}
	for _, typ := range []string{v1alpha1.CubestoreRouterConditionPromotionReady, v1alpha1.CubestoreRouterConditionJobRecovery, v1alpha1.CubestoreRouterConditionMutationReconcile} {
		apiMeta.SetStatusCondition(&router.Status.Conditions, metav1.Condition{Type: typ, Status: metav1.ConditionTrue, ObservedGeneration: router.Generation, Reason: "RuntimeSupported", Message: "Current promoted runtime"})
	}
	if err := r.Status().Update(ctx, &router); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, body string
		condition  metav1.ConditionStatus
		state      string
	}{
		{"supported", readinessPayload(true), metav1.ConditionTrue, "Ready"},
		{"legacy", "{}", metav1.ConditionUnknown, "NeedsContext"},
		{"unsupported", readinessPayload(false), metav1.ConditionFalse, "Blocked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.HTTPClient = &http.Client{Transport: readinessReplies{"default": {200, tc.body}}}
			var current v1alpha1.CubeCluster
			if err := r.Get(ctx, client.ObjectKeyFromObject(c), &current); err != nil {
				t.Fatal(err)
			}
			if err := r.reconcileStatus(ctx, &current); err != nil {
				t.Fatal(err)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(c), &current); err != nil {
				t.Fatal(err)
			}
			condition := apiMeta.FindStatusCondition(current.Status.Conditions, "RefresherRecovery")
			if condition == nil || condition.Status != tc.condition || current.Status.Recovery.Refresher.State != tc.state {
				t.Fatalf("recovery=%#v condition=%#v", current.Status.Recovery.Refresher, condition)
			}
			if !apiMeta.IsStatusConditionTrue(current.Status.Conditions, "RefresherReady") {
				t.Fatal("driver capability incorrectly changed process readiness")
			}
			if apiMeta.IsStatusConditionTrue(current.Status.Conditions, "ProductionReady") {
				t.Fatal("scoped driver recovery improperly certified production")
			}
		})
	}
}

func TestExternalRefreshAPIIsNotRequiredToScheduleBuilds(t *testing.T) {
	yes, no := true, false
	c := businessCluster()
	c.Spec.Refresher = &v1alpha1.CubeComponentSpec{}
	// externalRefresh configures the API as a reader of completed builds, so
	// its /readyz correctly reports scheduling=false. It must still be ready.
	current := v1alpha1.CubeComponentStatus{Ready: true, WorkloadGeneration: 4, ObservedGeneration: 4}
	components := map[string]v1alpha1.CubeComponentStatus{cubeComponentAPI: current, cubeComponentRefresher: current}
	gate := v1alpha1.RecoveryGateStatus{State: v1alpha1.RecoveryStateReady}
	recovery := v1alpha1.RouterRecoveryStatus{Promotion: gate, JobRecovery: gate, MutationReconcile: gate}
	for _, tc := range []struct {
		name      string
		refresher *bool
		want      string
	}{
		{"API externalRefresh false scheduling and refresher supports", &yes, v1alpha1.RecoveryStateReady},
		{"refresher explicitly unsupported", &no, v1alpha1.RecoveryStateBlocked},
		{"refresher missing capability", nil, v1alpha1.RecoveryStateNeedsContext},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capabilities := map[string]*bool{cubeComponentAPI: &no, cubeComponentRefresher: tc.refresher}
			got := refresherBuildRecovery(c, components, recovery, capabilities)
			if got.State != tc.want {
				t.Fatalf("recovery=%#v, want state %s", got, tc.want)
			}
		})
	}
	// Removing the API scheduling requirement must not weaken readiness or
	// generation checks for this read-only role.
	for _, tc := range []struct {
		name  string
		state v1alpha1.CubeComponentStatus
	}{
		{"API unavailable", v1alpha1.CubeComponentStatus{Ready: false, WorkloadGeneration: 4, ObservedGeneration: 4}},
		{"API stale generation", v1alpha1.CubeComponentStatus{Ready: true, WorkloadGeneration: 4, ObservedGeneration: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			states := map[string]v1alpha1.CubeComponentStatus{cubeComponentAPI: tc.state, cubeComponentRefresher: current}
			got := refresherBuildRecovery(c, states, recovery, map[string]*bool{cubeComponentAPI: &no, cubeComponentRefresher: &yes})
			if got.State != v1alpha1.RecoveryStateNeedsContext {
				t.Fatalf("API readiness/generation bypassed: %#v", got)
			}
		})
	}
}
