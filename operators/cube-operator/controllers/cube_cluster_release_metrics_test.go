package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReleaseDigestValidationDedicated(t *testing.T) {
	image := "fixture/cube@sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name   string
		change func(*v1alpha1.CubeClusterImages)
		valid  bool
	}{
		{"all_local_tags_remain_valid", func(i *v1alpha1.CubeClusterImages) {
			*i = v1alpha1.CubeClusterImages{API: "api:dev", Router: "rust:dev", MetaStore: "rust:dev", Worker: "rust:dev", LeaseAgent: "agent:dev"}
		}, true},
		{"all_pinned", func(*v1alpha1.CubeClusterImages) {}, true},
		{"same_rust_digest_other_repository", func(i *v1alpha1.CubeClusterImages) { i.Worker = "mirror/rust@sha256:" + strings.Repeat("a", 64) }, true},
		{"mixed_tag_and_digest", func(i *v1alpha1.CubeClusterImages) { i.API = "api:dev" }, false},
		{"short_digest", func(i *v1alpha1.CubeClusterImages) { i.LeaseAgent = "agent@sha256:abcd" }, false},
		{"nonhex_digest", func(i *v1alpha1.CubeClusterImages) { i.API = "api@sha256:" + strings.Repeat("z", 64) }, false},
		{"missing_repository", func(i *v1alpha1.CubeClusterImages) { i.API = "@sha256:" + strings.Repeat("a", 64) }, false},
		{"different_metastore_digest", func(i *v1alpha1.CubeClusterImages) { i.MetaStore = "rust@sha256:" + strings.Repeat("b", 64) }, false},
		{"different_worker_digest", func(i *v1alpha1.CubeClusterImages) { i.Worker = "rust@sha256:" + strings.Repeat("b", 64) }, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := &v1alpha1.CubeCluster{Spec: v1alpha1.CubeClusterSpec{Images: v1alpha1.CubeClusterImages{API: image, Router: image, MetaStore: image, Worker: image, LeaseAgent: image}}}
			test.change(&c.Spec.Images)
			if err := validateClusterOptions(c); (err == nil) != test.valid {
				t.Fatalf("valid=%v, error=%v", test.valid, err)
			}
		})
	}
}

// Avoid parallel tests: these exercise the production registry's shared vectors.
func releaseMetricCluster(t *testing.T) *v1alpha1.CubeCluster {
	t.Helper()
	c := &v1alpha1.CubeCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "release-metric-fixture", Name: t.Name(), Generation: 2}}
	t.Cleanup(func() {
		clusterConditionMetric.DeletePartialMatch(map[string]string{"namespace": c.Namespace, "cluster": c.Name})
		clusterObservationMetric.DeleteLabelValues(c.Namespace, c.Name)
	})
	return c
}

func releaseMetricValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	var metric dto.Metric
	if err := gauge.Write(&metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetGauge().GetValue()
}

func TestReleaseMetricGenerationDedicated(t *testing.T) {
	tests := []struct {
		name       string
		status     metav1.ConditionStatus
		generation int64
		want       float64
	}{
		{"current_true", metav1.ConditionTrue, 2, 1},
		{"current_false", metav1.ConditionFalse, 2, 0},
		{"current_unknown", metav1.ConditionUnknown, 2, -1},
		{"stale_true", metav1.ConditionTrue, 1, -1},
		{"stale_false", metav1.ConditionFalse, 1, -1},
		{"future_generation", metav1.ConditionTrue, 3, -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := releaseMetricCluster(t)
			c.Status.Conditions = []metav1.Condition{{Type: "RouterServingReady", Status: test.status, ObservedGeneration: test.generation}}
			observeClusterConditions(c, false)
			got := releaseMetricValue(t, clusterConditionMetric.WithLabelValues(c.Namespace, c.Name, "RouterServingReady"))
			if got != test.want {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
}

func TestReleaseMetricMissingConditionClearsPreviousTrue(t *testing.T) {
	c := releaseMetricCluster(t)
	c.Status.Conditions = []metav1.Condition{{Type: "JobRecovery", Status: metav1.ConditionTrue, ObservedGeneration: c.Generation}}
	observeClusterConditions(c, false)
	gauge := clusterConditionMetric.WithLabelValues(c.Namespace, c.Name, "JobRecovery")
	if got := releaseMetricValue(t, gauge); got != 1 {
		t.Fatalf("initial condition = %v, want 1", got)
	}
	c.Status.Conditions = nil
	observeClusterConditions(c, false)
	if got := releaseMetricValue(t, gauge); got != -1 {
		t.Fatalf("missing condition = %v, want -1", got)
	}
}

func TestReleaseMetricObservationFreshnessDedicated(t *testing.T) {
	c := releaseMetricCluster(t)
	gauge := clusterObservationMetric.WithLabelValues(c.Namespace, c.Name)
	gauge.Set(123)
	observeClusterConditions(c, false)
	if got := releaseMetricValue(t, gauge); got != 123 {
		t.Fatalf("partial observation refreshed timestamp: %v", got)
	}
	before := float64(time.Now().UnixNano()) / 1e9
	observeClusterConditions(c, true)
	after := float64(time.Now().UnixNano()) / 1e9
	if got := releaseMetricValue(t, gauge); got < before || got > after {
		t.Fatalf("full observation timestamp %v outside [%v, %v]", got, before, after)
	}
}

func releaseMetricReconciler(t *testing.T, objects ...client.Object) *CubeClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	gv := schema.GroupVersion{Group: "cubestore.io", Version: "v1alpha1"}
	scheme.AddKnownTypes(gv, &v1alpha1.CubeCluster{}, &v1alpha1.CubeClusterList{})
	metav1.AddToGroupVersion(scheme, gv)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.CubeCluster{}).WithObjects(objects...).Build()
	return &CubeClusterReconciler{Client: c, Scheme: scheme}
}

func TestReleaseMetricInvalidReconcileDedicated(t *testing.T) {
	c := releaseMetricCluster(t) // Missing required images makes this an invalid spec.
	c.Status.Conditions = []metav1.Condition{{Type: "RouterServingReady", Status: metav1.ConditionTrue, ObservedGeneration: c.Generation}}
	observeClusterConditions(c, true)
	stamp := clusterObservationMetric.WithLabelValues(c.Namespace, c.Name)
	stamp.Set(123)
	r := releaseMetricReconciler(t, c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}); err != nil {
		t.Fatal(err)
	}
	for condition, want := range map[string]float64{"ResourcesReady": 0, "ProductionReady": 0, "RouterServingReady": -1, "JobRecovery": -1} {
		if got := releaseMetricValue(t, clusterConditionMetric.WithLabelValues(c.Namespace, c.Name, condition)); got != want {
			t.Errorf("%s = %v, want %v", condition, got, want)
		}
	}
	if got := releaseMetricValue(t, stamp); got != 123 {
		t.Errorf("invalid reconciliation refreshed timestamp: %v", got)
	}
	var persisted v1alpha1.CubeCluster
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(c), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.Phase != "Invalid" {
		t.Fatalf("persisted phase = %q", persisted.Status.Phase)
	}
}

func releaseMetricSeries(t *testing.T, collector prometheus.Collector, c *v1alpha1.CubeCluster) int {
	t.Helper()
	// Only these test fixtures contribute; asynchronous collection avoids a
	// capacity assumption when other controller tests have also emitted metrics.
	ch := make(chan prometheus.Metric)
	go func() { collector.Collect(ch); close(ch) }()
	count := 0
	for value := range ch {
		var metric dto.Metric
		if err := value.Write(&metric); err != nil {
			t.Error(err)
			continue
		}
		labels := map[string]string{}
		for _, pair := range metric.Label {
			labels[pair.GetName()] = pair.GetValue()
		}
		if labels["namespace"] == c.Namespace && labels["cluster"] == c.Name {
			count++
		}
	}
	return count
}

func TestReleaseMetricDeletedReconcileDedicated(t *testing.T) {
	c := releaseMetricCluster(t)
	other := releaseMetricCluster(t)
	other.Name += "-retained"
	observeClusterConditions(c, true)
	observeClusterConditions(other, true)
	if n := releaseMetricSeries(t, clusterConditionMetric, c); n != 7 {
		t.Fatalf("before deletion: condition series = %d, want 7", n)
	}
	r := releaseMetricReconciler(t) // NotFound is the observed CR deletion path.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: c.Namespace, Name: c.Name}}); err != nil {
		t.Fatal(err)
	}
	for _, collector := range []prometheus.Collector{clusterConditionMetric, clusterObservationMetric} {
		if n := releaseMetricSeries(t, collector, c); n != 0 {
			t.Errorf("deleted cluster retained %d metric series", n)
		}
		if n := releaseMetricSeries(t, collector, other); n == 0 {
			t.Error("deleting one cluster removed another cluster's metrics")
		}
	}
}
