package controllers

import (
	"testing"

	"github.com/cube-js/cube-operator/api/v1alpha1"
)

func TestValidateCubeClusterRejectsMultiWriterMetaStore(t *testing.T) {
	cluster := &v1alpha1.CubeCluster{}
	cluster.Namespace = "cube-operator-demo"
	cluster.Spec.Images = v1alpha1.CubeClusterImages{API: "api", Router: "router", MetaStore: "meta", Worker: "worker", LeaseAgent: "operator"}
	cluster.Spec.Storage.Endpoint = "http://object-store:9000"
	cluster.Spec.Storage.Bucket = "cube"
	cluster.Spec.MetaStore.Replicas = 2
	if err := validateCubeCluster(cluster); err == nil {
		t.Fatal("expected multi-writer MetaStore to be rejected")
	}
}

func TestWorkerAddressesUsesStableStatefulSetDNS(t *testing.T) {
	cluster := &v1alpha1.CubeCluster{}
	cluster.Name = "analytics"
	cluster.Namespace = "cube-operator-demo"
	cluster.Spec.Workers.Replicas = 2
	want := "analytics-worker-0.analytics-worker.cube-operator-demo.svc:10001,analytics-worker-1.analytics-worker.cube-operator-demo.svc:10001"
	if got := workerAddresses(cluster); got != want {
		t.Fatalf("worker addresses = %q, want %q", got, want)
	}
}
