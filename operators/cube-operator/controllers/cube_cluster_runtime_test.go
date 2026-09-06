package controllers

import (
	"context"
	"encoding/json"
	"github.com/cube-js/cube-operator/api/v1alpha1"
	"io"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structural "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"
	"strconv"
	"strings"
	"testing"
)

func businessCluster() *v1alpha1.CubeCluster {
	return &v1alpha1.CubeCluster{ObjectMeta: metav1.ObjectMeta{Name: "analytics", Namespace: "test", UID: "cluster-uid", Generation: 2}, Spec: v1alpha1.CubeClusterSpec{Images: v1alpha1.CubeClusterImages{API: "api", Router: "router", MetaStore: "meta", Worker: "worker", LeaseAgent: "agent"}, Router: v1alpha1.CubeRouterClusterSpec{HighAvailability: true}, Storage: v1alpha1.CubeStorageSpec{Endpoint: "http://object-store:9000", Bucket: "cube", ObjectStoreSecretRef: &corev1.SecretReference{Name: "object-store"}}}}
}
func clusterReconciler(t *testing.T, c *v1alpha1.CubeCluster, extra ...client.Object) *CubeClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1alpha1.AddToScheme, corev1.AddToScheme, apps.AddToScheme, policyv1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	objects := []client.Object{c, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "object-store", Namespace: c.Namespace}, Data: map[string][]byte{"accessKey": []byte("access"), "secretKey": []byte("secret")}}}
	objects = append(objects, extra...)
	return &CubeClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.CubeCluster{}, &v1alpha1.CubestoreRouter{}, &apps.Deployment{}, &apps.StatefulSet{}).WithObjects(objects...).Build(), Scheme: scheme}
}
func reconcileClusterTest(t *testing.T, r *CubeClusterReconciler, c *v1alpha1.CubeCluster) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}); err != nil {
		t.Fatal(err)
	}
}
func getComponent(t *testing.T, r client.Reader, c *v1alpha1.CubeCluster, component string, obj client.Object) {
	t.Helper()
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: clusterName(c, component)}, obj); err != nil {
		t.Fatal(err)
	}
}
func environment(env []corev1.EnvVar) map[string]corev1.EnvVar {
	out := map[string]corev1.EnvVar{}
	for _, e := range env {
		out[e.Name] = e
	}
	return out
}

type responseTransport int

func (s responseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: int(s), Body: io.NopCloser(strings.NewReader("{}")), Header: http.Header{}}, nil
}
func addReadyPods(t *testing.T, r *CubeClusterReconciler, c *v1alpha1.CubeCluster, component string, count int32) {
	t.Helper()
	for i := int32(0); i < count; i++ {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: component + strconv.Itoa(int(i)), Namespace: c.Namespace, Labels: clusterLabels(c, component)}, Status: corev1.PodStatus{PodIP: "127.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
		if err := r.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
	}
}
func TestClusterWiresAllRustRolesAndProbes(t *testing.T) {
	c := businessCluster()
	c.Spec.Router.DrainCommand = []string{"/cube/cubestored", "--drain"}
	r := clusterReconciler(t, c)
	reconcileClusterTest(t, r, c)
	var router apps.Deployment
	var meta, worker apps.StatefulSet
	getComponent(t, r, c, cubeComponentRouter, &router)
	getComponent(t, r, c, cubeComponentMeta, &meta)
	getComponent(t, r, c, cubeComponentWorker, &worker)
	for name, container := range map[string]corev1.Container{"router": router.Spec.Template.Spec.Containers[1], "meta": meta.Spec.Template.Spec.Containers[0], "worker": worker.Spec.Template.Spec.Containers[0]} {
		env := environment(container.Env)
		for key, want := range map[string]string{"CUBESTORE_WORKERS": workerAddresses(c), "CUBESTORE_MINIO_BUCKET": "cube", "CUBESTORE_MINIO_SERVER_ENDPOINT": c.Spec.Storage.Endpoint, "CUBESTORE_HTTP_BIND_ADDR": "0.0.0.0:3030"} {
			if env[key].Value != want {
				t.Fatalf("%s %s=%q want %q", name, key, env[key].Value, want)
			}
		}
		ref := env["CUBESTORE_MINIO_SECRET_ACCESS_KEY"].ValueFrom
		if ref == nil || ref.SecretKeyRef == nil || ref.SecretKeyRef.Name != "object-store" || ref.SecretKeyRef.Key != "secretKey" {
			t.Fatalf("%s credentials not connected", name)
		}
		if container.ReadinessProbe == nil || container.ReadinessProbe.HTTPGet.Path != "/readyz" || container.LivenessProbe.HTTPGet.Path != "/livez" {
			t.Fatalf("%s lacks application probes", name)
		}
	}
	if !reflect.DeepEqual(router.Spec.Template.Spec.Containers[1].Lifecycle.PreStop.Exec.Command, c.Spec.Router.DrainCommand) {
		t.Fatal("drain command changed")
	}
	var service corev1.Service
	getComponent(t, r, c, cubeComponentWorker, &service)
	if !service.Spec.PublishNotReadyAddresses {
		t.Fatal("startup DNS can deadlock")
	}
}
func TestSecretPrintableStableAndExternalRotation(t *testing.T) {
	c := businessCluster()
	r := clusterReconciler(t, c)
	ctx := context.Background()
	if err := r.reconcileAPISecret(ctx, c); err != nil {
		t.Fatal(err)
	}
	var first corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: apiSecretName(c)}, &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Data[cubeAPISecretKey]) != 64 || !printableSecret(first.Data[cubeAPISecretKey]) {
		t.Fatal("secret is not printable 256-bit hex")
	}
	if err := r.reconcileAPISecret(ctx, c); err != nil {
		t.Fatal(err)
	}
	var second corev1.Secret
	if err := r.Get(ctx, client.ObjectKeyFromObject(&first), &second); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Data, second.Data) {
		t.Fatal("secret rotated on normal reconcile")
	}
	c.Spec.APISecretRef = &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "user-auth"}, Key: "token"}
	external := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "user-auth", Namespace: c.Namespace}, Data: map[string][]byte{"token": []byte("first-token")}}
	if err := r.Create(ctx, external); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileAPISecret(ctx, c); err != nil {
		t.Fatal(err)
	}
	before, err := r.configurationDigest(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	external.Data["token"] = []byte("second-token")
	if err := r.Update(ctx, external); err != nil {
		t.Fatal(err)
	}
	after, err := r.configurationDigest(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("rotation did not change rollout hash")
	}
	if len(external.OwnerReferences) > 0 {
		t.Fatal("external secret adopted")
	}
	var saved v1alpha1.CubeCluster
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), &saved); err != nil {
		t.Fatal(err)
	}
	saved.Spec.APISecretRef = c.Spec.APISecretRef
	if err := r.Update(ctx, &saved); err != nil {
		t.Fatal(err)
	}
	if len(r.configurationChanged(ctx, external)) != 1 {
		t.Fatal("external Secret watch not mapped")
	}
}
func TestBusinessConfigRefresherAndDurableCache(t *testing.T) {
	c := businessCluster()
	c.Spec.Refresher = &v1alpha1.CubeComponentSpec{}
	c.Spec.API.Pod = v1alpha1.CubePodSpec{Env: []corev1.EnvVar{{Name: "CUBEJS_DB_TYPE", Value: "postgres"}}, EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "datasource"}}}}, Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "model"}}}}}, VolumeMounts: []corev1.VolumeMount{{Name: "model", MountPath: "/cube/conf/model", ReadOnly: true}}}
	r := clusterReconciler(t, c, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "datasource", Namespace: c.Namespace}, Data: map[string][]byte{"CUBEJS_DB_HOST": []byte("db")}}, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: c.Namespace}, Data: map[string]string{"Orders.js": "cube('Orders', {})"}})
	reconcileClusterTest(t, r, c)
	var api, refresher apps.Deployment
	getComponent(t, r, c, cubeComponentAPI, &api)
	getComponent(t, r, c, cubeComponentRefresher, &refresher)
	if *refresher.Spec.Replicas != 0 {
		t.Fatal("refresher started before API scheduling-disabled rollout")
	}
	if refresher.Spec.Strategy.Type != apps.RecreateDeploymentStrategyType {
		t.Fatal("refresher permits rolling overlap")
	}
	for name, d := range map[string]apps.Deployment{"api": api, "refresher": refresher} {
		container := d.Spec.Template.Spec.Containers[0]
		env := environment(container.Env)
		if env["CUBEJS_CACHE_AND_QUEUE_DRIVER"].Value != "cubestore" || env["CUBEJS_DB_TYPE"].Value != "postgres" {
			t.Fatalf("%s datasource/cache incorrect", name)
		}
		if len(container.EnvFrom) != 1 || len(container.VolumeMounts) != 1 || len(d.Spec.Template.Spec.Volumes) != 1 || d.Spec.Template.Annotations[configurationAnnotation] == "" {
			t.Fatalf("%s pod inheritance lost", name)
		}
	}
	if environment(api.Spec.Template.Spec.Containers[0].Env)["CUBEJS_REFRESH_WORKER"].Value != "false" || environment(refresher.Spec.Template.Spec.Containers[0].Env)["CUBEJS_REFRESH_WORKER"].Value != "true" {
		t.Fatal("scheduled roles not isolated")
	}
	api.Status = apps.DeploymentStatus{ObservedGeneration: api.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if err := r.Status().Update(context.Background(), &api); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileRefresher(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	getComponent(t, r, c, cubeComponentRefresher, &refresher)
	if *refresher.Spec.Replicas != 1 {
		t.Fatal("refresher never starts after handover")
	}
	c.Spec.Refresher = nil
	if err := r.reconcileAPI(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	getComponent(t, r, c, cubeComponentAPI, &api)
	if environment(api.Spec.Template.Spec.Containers[0].Env)["CUBEJS_REFRESH_WORKER"].Value != "false" {
		t.Fatal("API resumed before refresher stopped")
	}
	if err := r.reconcileRefresher(context.Background(), c); err != nil {
		t.Fatal(err)
	}
}
func TestValidationAndStorageTransitions(t *testing.T) {
	cases := map[string]func(*v1alpha1.CubeCluster){"negative replicas": func(c *v1alpha1.CubeCluster) { c.Spec.API.Replicas = -1 }, "quantity": func(c *v1alpha1.CubeCluster) { c.Spec.Storage.DataSize = "banana" }, "multi refresher": func(c *v1alpha1.CubeCluster) { c.Spec.Refresher = &v1alpha1.CubeComponentSpec{Replicas: 2} }, "memory cache": func(c *v1alpha1.CubeCluster) {
		c.Spec.API.Pod.Env = []corev1.EnvVar{{Name: "CUBEJS_CACHE_AND_QUEUE_DRIVER", Value: "memory"}}
	}}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := businessCluster()
			mutate(c)
			if validateCubeCluster(c) == nil {
				t.Fatal("invalid spec accepted")
			}
		})
	}
	c := businessCluster()
	r := clusterReconciler(t, c)
	reconcileClusterTest(t, r, c)
	c.Spec.Storage.DataSize = "4Gi"
	if err := r.validateStorageTransition(context.Background(), c); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("resize not meaningfully rejected: %v", err)
	}
	c.Spec.Storage.DataSize = "2Gi"
	class := "different"
	c.Spec.Storage.StorageClassName = &class
	if err := r.validateStorageTransition(context.Background(), c); err == nil {
		t.Fatal("class transition accepted")
	}
	var set apps.StatefulSet
	getComponent(t, r, c, cubeComponentMeta, &set)
	q := set.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	if q.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Fatal("validation modified template")
	}
}
func TestComponentGenerationAndLiveFailureCannotBeMasked(t *testing.T) {
	c := businessCluster()
	r := clusterReconciler(t, c)
	reconcileClusterTest(t, r, c)
	ctx := context.Background()
	var api apps.Deployment
	getComponent(t, r, c, cubeComponentAPI, &api)
	api.Status = apps.DeploymentStatus{ObservedGeneration: api.Generation, Replicas: 10, UpdatedReplicas: 10, ReadyReplicas: 10, AvailableReplicas: 10}
	if err := r.Status().Update(ctx, &api); err != nil {
		t.Fatal(err)
	}
	var latest v1alpha1.CubeCluster
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), &latest); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileStatus(ctx, &latest); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), &latest); err != nil {
		t.Fatal(err)
	}
	if apiMeta.IsStatusConditionTrue(latest.Status.Conditions, cubeClusterConditionResources) || latest.Status.Phase == "Running" {
		t.Fatal("aggregate count masked missing components")
	}
	if len(latest.Status.Components) != 4 || latest.Status.ObservedGeneration != c.Generation {
		t.Fatal("component observations absent")
	}
	addReadyPods(t, r, c, cubeComponentAPI, 1)
	r.HTTPClient = &http.Client{Transport: responseTransport(503)}
	ready, err := r.applicationReady(ctx, c, cubeComponentAPI, 1)
	if err != nil || ready {
		t.Fatalf("HTTP failure accepted: %v %v", ready, err)
	}
	api.Generation = 3
	api.Status.ObservedGeneration = 2
	if deploymentCurrent(&api, 1) {
		t.Fatal("stale generation accepted")
	}
}
func TestPDBSingletonAndDeepCopy(t *testing.T) {
	c := businessCluster()
	r := clusterReconciler(t, c)
	reconcileClusterTest(t, r, c)
	var pdb policyv1.PodDisruptionBudget
	getComponent(t, r, c, cubeComponentMeta, &pdb)
	if !metav1.IsControlledBy(&pdb, c) || pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntVal != 1 {
		t.Fatal("singleton not protected")
	}
	c.Spec.MetaStore.AllowSingleReplicaDisruption = true
	if err := r.reconcilePDBs(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	getComponent(t, r, c, cubeComponentMeta, &pdb)
	if pdb.Spec.MinAvailable != nil || pdb.Spec.MaxUnavailable == nil {
		t.Fatal("explicit interruption ignored")
	}
	c.Spec.API.Pod.Env = []corev1.EnvVar{{Name: "DB", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "source"}, Key: "key"}}}}
	c.Spec.Refresher = &v1alpha1.CubeComponentSpec{Pod: v1alpha1.CubePodSpec{NodeSelector: map[string]string{"zone": "a"}}}
	cp := c.DeepCopy()
	cp.Spec.API.Pod.Env[0].ValueFrom.SecretKeyRef.Name = "changed"
	cp.Spec.Refresher.Pod.NodeSelector["zone"] = "b"
	if c.Spec.API.Pod.Env[0].ValueFrom.SecretKeyRef.Name != "source" || c.Spec.Refresher.Pod.NodeSelector["zone"] != "a" {
		t.Fatal("deepcopy aliases config")
	}
}
func TestCRDSchemaAndExample(t *testing.T) {
	raw, err := os.ReadFile("../config/crd/bases/cubestore.io_cubeclusters.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatal(err)
	}
	schema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	var internal apiextensions.JSONSchemaProps
	if err := apiextv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(schema, &internal, nil); err != nil {
		t.Fatal(err)
	}
	s, err := structural.NewStructural(&internal)
	if err != nil {
		t.Fatal(err)
	}
	if errs := structural.ValidateStructural(field.NewPath("schema"), s); len(errs) > 0 {
		t.Fatal(errs)
	}
	for _, name := range []string{"api", "router", "metaStore", "workers", "refresher"} {
		for _, key := range []string{"env", "envFrom", "volumes", "volumeMounts", "resources", "nodeSelector", "affinity", "startupProbe", "readinessProbe", "livenessProbe"} {
			if _, ok := schema.Properties["spec"].Properties[name].Properties["pod"].Properties[key]; !ok {
				t.Fatalf("schema prunes %s.pod.%s", name, key)
			}
		}
	}
	raw, err = os.ReadFile("../examples/cubecluster.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var c v1alpha1.CubeCluster
	if err := yaml.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	if err := validateCubeCluster(&c); err != nil {
		t.Fatal(err)
	}
	invalid := v1alpha1.CubestoreRouterSpec{Selector: map[string]string{"app": "router"}, Namespace: "default", StateStore: &v1alpha1.StateStore{Type: "kubernetes"}}
	if invalid.Validate() == nil {
		t.Fatal("Kubernetes bypasses business validation")
	}
}
func TestRouterLiveProbeAndScopedCapabilities(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"isLeader": false, "metaStoreReady": req.URL.Path != "/unhealthy", "recoveryCapabilities": map[string]bool{"uploadReceipts": true, "preAggregationStatus": true}})
	}))
	defer server.Close()
	host, ps, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(ps)
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "router", Namespace: "default"}, Status: corev1.PodStatus{PodIP: host, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}}
	r := &CubestoreRouterReconciler{}
	cr := v1alpha1.CubestoreRouter{Spec: v1alpha1.CubestoreRouterSpec{RouterPort: int32(port)}}
	candidates := r.probeCandidates(context.Background(), pods, cr)
	if !candidates[0].Ready || candidates[0].IsLeader {
		t.Fatal("healthy follower cannot become candidate")
	}
	if candidates[0].RecoveryCapabilities == nil {
		t.Fatal("capabilities ignored")
	}
	cr.Spec.HealthPath = "/unhealthy"
	if r.probeCandidates(context.Background(), pods, cr)[0].Ready {
		t.Fatal("failed MetaStore probe accepted")
	}
	yes, no := true, false
	cand := &candidate{RecoveryCapabilities: &runtimeRecoveryCapabilities{UploadReceipts: &yes, PreAggregationStatus: &yes, JobAttemptFencing: &yes}}
	status := withRecoveryStatus(v1alpha1.CubestoreRouterStatus{}, true, cand)
	if status.Recovery.JobRecovery.State != "Ready" || status.Recovery.MutationReconcile.State != "Ready" {
		t.Fatal("supported capabilities not surfaced")
	}
	status = withRecoveryStatus(status, false, cand)
	if status.Recovery.JobRecovery.State != "NeedsContext" {
		t.Fatal("unpromoted capability upgraded recovery")
	}
	cand.RecoveryCapabilities.JobAttemptFencing = &no
	status = withRecoveryStatus(status, true, cand)
	if status.Recovery.JobRecovery.State != "Blocked" {
		t.Fatal("unsupported flag accepted")
	}
	cand.RecoveryCapabilities.JobAttemptFencing = nil
	status = withRecoveryStatus(status, true, cand)
	if status.Recovery.JobRecovery.State != "NeedsContext" {
		t.Fatal("missing legacy field not unknown")
	}
}

type generationClient struct{ client.Client }

func (g generationClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	switch next := obj.(type) {
	case *apps.StatefulSet:
		var old apps.StatefulSet
		if err := g.Get(ctx, client.ObjectKeyFromObject(next), &old); err == nil && !reflect.DeepEqual(old.Spec, next.Spec) {
			next.Generation = old.Generation + 1
		}
	case *apps.Deployment:
		var old apps.Deployment
		if err := g.Get(ctx, client.ObjectKeyFromObject(next), &old); err == nil && !reflect.DeepEqual(old.Spec, next.Spec) {
			next.Generation = old.Generation + 1
		}
	}
	return g.Client.Update(ctx, obj, opts...)
}
func markCurrent(t *testing.T, r *CubeClusterReconciler, c *v1alpha1.CubeCluster, component string) {
	t.Helper()
	ctx := context.Background()
	count := componentSpecs(c)[component].Replicas
	if component == cubeComponentMeta || component == cubeComponentWorker {
		var set apps.StatefulSet
		getComponent(t, r, c, component, &set)
		set.Status = apps.StatefulSetStatus{ObservedGeneration: set.Generation, Replicas: count, ReadyReplicas: count, UpdatedReplicas: count, CurrentRevision: "revision", UpdateRevision: "revision"}
		if err := r.Status().Update(ctx, &set); err != nil {
			t.Fatal(err)
		}
	} else {
		var d apps.Deployment
		getComponent(t, r, c, component, &d)
		d.Status = apps.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: count, UpdatedReplicas: count, ReadyReplicas: count, AvailableReplicas: count}
		if err := r.Status().Update(ctx, &d); err != nil {
			t.Fatal(err)
		}
	}
}
func TestOrderedUpgradeNewRPCClientsWaitForMetaStore(t *testing.T) {
	c := businessCluster()
	r := clusterReconciler(t, c)
	r.Client = generationClient{r.Client}
	r.HTTPClient = &http.Client{Transport: responseTransport(200)}
	reconcileClusterTest(t, r, c)
	for name, spec := range componentSpecs(c) {
		addReadyPods(t, r, c, name, spec.Replicas)
		markCurrent(t, r, c, name)
	}
	var desired v1alpha1.CubeCluster
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(c), &desired); err != nil {
		t.Fatal(err)
	}
	desired.Generation++
	desired.Spec.Images.MetaStore = "meta-v2"
	desired.Spec.Images.Worker = "worker-v2"
	desired.Spec.Images.Router = "router-v2"
	desired.Spec.Images.API = "api-v2"
	if err := r.Update(context.Background(), &desired); err != nil {
		t.Fatal(err)
	}
	reconcileClusterTest(t, r, &desired)
	var meta, worker apps.StatefulSet
	var router, api apps.Deployment
	getComponent(t, r, c, cubeComponentMeta, &meta)
	getComponent(t, r, c, cubeComponentWorker, &worker)
	getComponent(t, r, c, cubeComponentRouter, &router)
	if meta.Spec.Template.Spec.Containers[0].Image != "meta-v2" || worker.Spec.Template.Spec.Containers[0].Image != "worker" || router.Spec.Template.Spec.Containers[1].Image != "router" {
		t.Fatal("new RPC clients rolled before MetaStore")
	}
	markCurrent(t, r, c, cubeComponentMeta)
	reconcileClusterTest(t, r, &desired)
	getComponent(t, r, c, cubeComponentWorker, &worker)
	getComponent(t, r, c, cubeComponentRouter, &router)
	if worker.Spec.Template.Spec.Containers[0].Image != "worker-v2" || router.Spec.Template.Spec.Containers[1].Image != "router" {
		t.Fatal("router rolled before Worker")
	}
	markCurrent(t, r, c, cubeComponentWorker)
	reconcileClusterTest(t, r, &desired)
	getComponent(t, r, c, cubeComponentRouter, &router)
	getComponent(t, r, c, cubeComponentAPI, &api)
	if router.Spec.Template.Spec.Containers[1].Image != "router-v2" || api.Spec.Template.Spec.Containers[0].Image != "api" {
		t.Fatal("API rolled before Router")
	}
	markCurrent(t, r, c, cubeComponentRouter)
	reconcileClusterTest(t, r, &desired)
	getComponent(t, r, c, cubeComponentAPI, &api)
	if api.Spec.Template.Spec.Containers[0].Image != "api-v2" {
		t.Fatal("API did not advance")
	}
}

func TestLegacyRecoveryConditionsAreUnknownNotUnsupported(t *testing.T) {
	r := &CubestoreRouterReconciler{}
	status := withRecoveryStatus(v1alpha1.CubestoreRouterStatus{}, false)
	conditions := r.withRecoveryConditions(nil, status.Recovery, 1)
	for _, name := range []string{v1alpha1.CubestoreRouterConditionJobRecovery, v1alpha1.CubestoreRouterConditionMutationReconcile, v1alpha1.CubestoreRouterConditionRefresherReady} {
		condition := apiMeta.FindStatusCondition(conditions, name)
		if condition == nil || condition.Status != metav1.ConditionUnknown {
			t.Fatalf("legacy %s condition=%#v, expected Unknown", name, condition)
		}
	}
}

func TestMetadataOnlyConfigUpdateDoesNotRollPods(t *testing.T) {
	c := businessCluster()
	c.Spec.API.Pod.EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "business-config"}}}}
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "business-config", Namespace: c.Namespace}, Data: map[string]string{"CUBEJS_DB_TYPE": "postgres"}}
	r := clusterReconciler(t, c, config)
	ctx := context.Background()
	if err := r.reconcileAPISecret(ctx, c); err != nil {
		t.Fatal(err)
	}
	before, err := r.configurationDigest(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(config), config); err != nil {
		t.Fatal(err)
	}
	config.Labels = map[string]string{"note": "changed"}
	if err := r.Update(ctx, config); err != nil {
		t.Fatal(err)
	}
	after, err := r.configurationDigest(ctx, c)
	if err != nil || before != after {
		t.Fatalf("metadata churn triggered roll: %v", err)
	}
	config.Data["CUBEJS_DB_TYPE"] = "mysql"
	if err := r.Update(ctx, config); err != nil {
		t.Fatal(err)
	}
	after, err = r.configurationDigest(ctx, c)
	if err != nil || before == after {
		t.Fatalf("business config data change failed to trigger roll: %v", err)
	}
}
