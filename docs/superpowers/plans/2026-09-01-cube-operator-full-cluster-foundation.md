# Cube Operator Full Cluster Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one namespace-scoped `CubeCluster` CR deploy Cube API, Refresher, Router, Workers, MetaStore, Services, persistent storage references, RBAC, and observable status.

**Architecture:** Add a new `cubejs.io/v1alpha1` API package while retaining the legacy `cubestore.io/v1alpha1` Router API. A focused aggregate reconciler uses deterministic resource builders and owner references to create the complete workload.

**Tech Stack:** Go 1.25, controller-runtime 0.18.4, Kubernetes apps/core/policy APIs, envtest or controller-runtime fake client, shell-based local Kubernetes E2E.

**Spec:** `docs/superpowers/specs/2026-09-01-cube-operator-kubernetes-native-cluster-design.md`

## Global Constraints

- `CubeCluster` is the user-facing aggregate CRD.
- The first API version is namespace-scoped and creates workloads only in the CR namespace.
- Referenced Secrets, schema ConfigMaps, PVCs, and buckets are not owned or deleted.
- Persistent storage defaults to `Retain`.
- This milestone deploys the Lease objects but does not yet replace the legacy Router election path; M2 owns that behavior.
- Commit steps require explicit user authorization before execution.

---

### Task 1: Define and register the CubeCluster API

**Files:**
- Create: `operators/cube-operator/api/cubejs/v1alpha1/groupversion_info.go`
- Create: `operators/cube-operator/api/cubejs/v1alpha1/cubecluster_types.go`
- Create: `operators/cube-operator/api/cubejs/v1alpha1/cubecluster_types_test.go`
- Create: `operators/cube-operator/config/crd/bases/cubejs.io_cubeclusters.yaml`
- Modify: `operators/cube-operator/main.go`

**Interfaces:**
- Produces: `CubeCluster`, `CubeClusterSpec`, `CubeClusterStatus`, `ApplyDefaults()`, and `Validate() error`.
- Produces: `SchemeGroupVersion = schema.GroupVersion{Group: "cubejs.io", Version: "v1alpha1"}`.
- Consumed by: all M1-M4 controllers and tests.

- [ ] **Step 1: Write API validation tests**

```go
func TestCubeClusterSpecValidate(t *testing.T) {
    spec := validCubeClusterSpec()
    if err := spec.Validate(); err != nil { t.Fatalf("valid spec: %v", err) }
    spec.CubeStore.Router.HA.RenewDeadlineSeconds = spec.CubeStore.Router.HA.LeaseDurationSeconds
    if err := spec.Validate(); err == nil { t.Fatal("expected invalid lease timing") }
}

func TestCubeClusterDefaults(t *testing.T) {
    spec := minimalCubeClusterSpec()
    spec.ApplyDefaults()
    if spec.Cube.Replicas != 1 || spec.CubeStore.Router.Replicas != 2 || spec.CubeStore.Workers.Replicas != 1 {
        t.Fatalf("unexpected defaults: %#v", spec)
    }
}
```

- [ ] **Step 2: Run the focused test and confirm it fails**

Run: `cd operators/cube-operator && go test ./api/cubejs/v1alpha1`

Expected: FAIL because the package and API types do not exist.

- [ ] **Step 3: Implement API types and validation**

Define these stable top-level types:

```go
type CubeClusterSpec struct {
    Cube         CubeSpec         `json:"cube"`
    Refresher    RefresherSpec    `json:"refresher,omitempty"`
    CubeStore    CubeStoreSpec    `json:"cubestore"`
    Availability AvailabilitySpec `json:"availability,omitempty"`
}

type HAConfig struct {
    Mode                  string `json:"mode"`
    LeaseDurationSeconds  int32  `json:"leaseDurationSeconds"`
    RenewDeadlineSeconds  int32  `json:"renewDeadlineSeconds"`
    RetryPeriodSeconds    int32  `json:"retryPeriodSeconds"`
}

type CubeClusterStatus struct {
    ObservedGeneration int64              `json:"observedGeneration,omitempty"`
    Phase              string             `json:"phase,omitempty"`
    CubeAPI            WorkloadStatus     `json:"cubeAPI,omitempty"`
    Refresher          LeaderStatus       `json:"refresher,omitempty"`
    Router             RouterStatus       `json:"router,omitempty"`
    Workers            WorkloadStatus     `json:"workers,omitempty"`
    MetaStore          ComponentStatus    `json:"metaStore,omitempty"`
    Recovery           RecoveryStatus     `json:"recovery,omitempty"`
    Conditions         []metav1.Condition `json:"conditions,omitempty"`
}
```

Implement validation for replica floors, lease timing, required images, schema reference exclusivity, object-store Secret reference, and MetaStore persistence.

- [ ] **Step 4: Register both API groups and install the CRD schema**

Add `cubejsv1alpha1.AddToScheme(scheme)` in `main.go`. Generate or hand-maintain the CRD schema so it contains status subresource, list-map conditions, field defaults, and CEL timing validations matching `Validate()`.

- [ ] **Step 5: Run API tests and schema validation**

Run:

```bash
cd operators/cube-operator
go test ./api/cubejs/v1alpha1
kubectl apply --dry-run=client -f config/crd/bases/cubejs.io_cubeclusters.yaml
```

Expected: tests PASS and kubectl accepts the CRD manifest.

- [ ] **Step 6: Commit after authorization**

```bash
git add operators/cube-operator/api/cubejs/v1alpha1 operators/cube-operator/config/crd/bases/cubejs.io_cubeclusters.yaml operators/cube-operator/main.go
git commit -m "feat(operator): add CubeCluster API"
```

### Task 2: Add deterministic resource identity and builders

**Files:**
- Create: `operators/cube-operator/controllers/resources/names.go`
- Create: `operators/cube-operator/controllers/resources/common.go`
- Create: `operators/cube-operator/controllers/resources/cube_api.go`
- Create: `operators/cube-operator/controllers/resources/router.go`
- Create: `operators/cube-operator/controllers/resources/worker.go`
- Create: `operators/cube-operator/controllers/resources/metastore.go`
- Create: `operators/cube-operator/controllers/resources/refresher.go`
- Create: `operators/cube-operator/controllers/resources/resources_test.go`

**Interfaces:**
- Consumes: `*cubejsv1alpha1.CubeCluster`.
- Produces: `NamesFor(*CubeCluster) Names` and pure builder functions returning Kubernetes objects.

- [ ] **Step 1: Write table-driven builder tests**

```go
func TestBuildRouterDeployment(t *testing.T) {
    cluster := validCluster()
    got := BuildRouterDeployment(cluster)
    if *got.Spec.Replicas != 2 { t.Fatalf("replicas=%d", *got.Spec.Replicas) }
    if got.Spec.Template.Spec.ServiceAccountName != NamesFor(cluster).RouterServiceAccount {
        t.Fatalf("serviceAccount=%q", got.Spec.Template.Spec.ServiceAccountName)
    }
    assertControllerOwner(t, got, cluster)
}
```

Cover Cube API, Router, Worker, MetaStore, Refresher, Services, PDBs, ConfigMaps, ServiceAccounts, and Leases. Assert stable names, labels, selectors, owner references, ports, Secret references, and resources.

- [ ] **Step 2: Run tests and confirm missing builders**

Run: `cd operators/cube-operator && go test ./controllers/resources`

Expected: FAIL because the resource package does not exist.

- [ ] **Step 3: Implement the pure builder API**

```go
type Names struct {
    CubeAPIService, RouterService, RouterLeaderService string
    MetaStoreService, RouterLease, RefresherLease       string
    CubeAPIDeployment, RouterDeployment                 string
    WorkerDeployment, RefresherDeployment               string
    MetaStoreStatefulSet                                string
    RouterServiceAccount, RefresherServiceAccount       string
}

func BuildCubeAPIDeployment(cluster *v1alpha1.CubeCluster) *appsv1.Deployment
func BuildRouterDeployment(cluster *v1alpha1.CubeCluster) *appsv1.Deployment
func BuildWorkerDeployment(cluster *v1alpha1.CubeCluster) *appsv1.Deployment
func BuildMetaStoreStatefulSet(cluster *v1alpha1.CubeCluster) *appsv1.StatefulSet
func BuildRefresherDeployment(cluster *v1alpha1.CubeCluster) *appsv1.Deployment
func BuildServices(cluster *v1alpha1.CubeCluster) []*corev1.Service
func BuildLeases(cluster *v1alpha1.CubeCluster) []*coordinationv1.Lease
func BuildPDBs(cluster *v1alpha1.CubeCluster) []*policyv1.PodDisruptionBudget
```

Builders must be deterministic, side-effect free, and use controller owner references only for generated resources.

- [ ] **Step 4: Run builder tests**

Run: `cd operators/cube-operator && go test ./controllers/resources`

Expected: PASS.

- [ ] **Step 5: Commit after authorization**

```bash
git add operators/cube-operator/controllers/resources
git commit -m "feat(operator): build full Cube workload resources"
```

### Task 3: Reconcile the complete owned resource graph

**Files:**
- Create: `operators/cube-operator/controllers/cubecluster_controller.go`
- Create: `operators/cube-operator/controllers/cubecluster_controller_test.go`
- Modify: `operators/cube-operator/main.go`
- Modify: `operators/cube-operator/config/rbac/role.yaml`

**Interfaces:**
- Consumes: M1 Task 1 API and M1 Task 2 builders.
- Produces: `CubeClusterReconciler.Reconcile(context.Context, ctrl.Request)` and `SetupWithManager(ctrl.Manager)`.

- [ ] **Step 1: Write reconcile tests**

```go
func TestCubeClusterReconcileCreatesOwnedResources(t *testing.T) {
    reconciler, kubeClient, key := newCubeClusterReconciler(t, validCluster())
    if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
        t.Fatal(err)
    }
    assertObjectExists[appsv1.Deployment](t, kubeClient, key.Namespace, "production-router")
    assertObjectExists[appsv1.StatefulSet](t, kubeClient, key.Namespace, "production-metastore")
    assertObjectExists[corev1.Service](t, kubeClient, key.Namespace, "production-router-leader")
}
```

Add tests for update convergence, unchanged-resource no-op, user-owned Secret preservation, and deletion retention policy.

- [ ] **Step 2: Run tests and confirm failure**

Run: `cd operators/cube-operator && go test ./controllers -run CubeCluster`

Expected: FAIL because `CubeClusterReconciler` is undefined.

- [ ] **Step 3: Implement server-side-apply reconciliation**

Use one helper with a stable field manager:

```go
func (r *CubeClusterReconciler) applyOwned(ctx context.Context, cluster *v1alpha1.CubeCluster, obj client.Object) error {
    if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil { return err }
    return r.Patch(ctx, obj, client.Apply, client.FieldOwner("cube-operator"), client.ForceOwnership)
}
```

Reconcile builders in dependency order: ServiceAccounts/RBAC, configuration, Services, Leases, MetaStore, Workers, Routers, Refresher, Cube API, PDBs.

- [ ] **Step 4: Register watches and RBAC**

Watch `CubeCluster`, owned Deployments, StatefulSets, Services, Leases, ConfigMaps, and PDBs. Add least-privilege verbs for those resources and status updates.

- [ ] **Step 5: Run controller tests**

Run: `cd operators/cube-operator && go test ./controllers ./controllers/resources`

Expected: PASS.

- [ ] **Step 6: Commit after authorization**

```bash
git add operators/cube-operator/controllers/cubecluster_controller.go operators/cube-operator/controllers/cubecluster_controller_test.go operators/cube-operator/config/rbac/role.yaml operators/cube-operator/main.go
git commit -m "feat(operator): reconcile complete CubeCluster"
```

### Task 4: Compute status and readiness conditions

**Files:**
- Create: `operators/cube-operator/controllers/cubecluster_status.go`
- Create: `operators/cube-operator/controllers/cubecluster_status_test.go`
- Modify: `operators/cube-operator/controllers/cubecluster_controller.go`

**Interfaces:**
- Produces: `BuildCubeClusterStatus(context.Context, *CubeCluster) (CubeClusterStatus, error)`.
- Produces conditions listed in design section 7.4.

- [ ] **Step 1: Write status transition tests**

```go
func TestStatusReadyRequiresEveryRequiredComponent(t *testing.T) {
    status := buildStatusFixture(allComponentsReady())
    if status.Phase != "Ready" || condition(status.Conditions, "Ready").Status != metav1.ConditionTrue {
        t.Fatalf("status=%#v", status)
    }
    status = buildStatusFixture(metaStoreUnavailable())
    if status.Phase != "Degraded" || condition(status.Conditions, "MetaStoreReady").Status != metav1.ConditionFalse {
        t.Fatalf("status=%#v", status)
    }
}
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd operators/cube-operator && go test ./controllers -run CubeClusterStatus`

Expected: FAIL because the status builder is undefined.

- [ ] **Step 3: Implement status aggregation**

Read current Deployments, StatefulSet, Pods, Leases, and EndpointSlices through `APIReader`. Preserve `LastTransitionTime` when condition semantic content does not change. Set `ObservedGeneration` only after all desired resources have been applied.

- [ ] **Step 4: Run status tests and full Go tests**

Run: `cd operators/cube-operator && go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit after authorization**

```bash
git add operators/cube-operator/controllers/cubecluster_status.go operators/cube-operator/controllers/cubecluster_status_test.go operators/cube-operator/controllers/cubecluster_controller.go
git commit -m "feat(operator): report CubeCluster readiness"
```

### Task 5: Add a one-CR local Kubernetes demonstration

**Files:**
- Create: `operators/cube-operator/demo/k8s/cubecluster.yaml`
- Create: `operators/cube-operator/demo/k8s/cubecluster-e2e.sh`
- Modify: `operators/cube-operator/demo/k8s/run.sh`
- Modify: `operators/cube-operator/demo/k8s/README.md`

**Interfaces:**
- Consumes: the `CubeCluster` CRD and aggregate reconciler.
- Produces: one command that proves full deployment and one real Cube API query.

- [ ] **Step 1: Add the CR fixture and failing E2E assertions**

The script must assert exact resources and fail on absence:

```bash
kubectl -n cube-operator-demo wait --for=condition=Ready cubecluster/demo --timeout=300s
kubectl -n cube-operator-demo get deploy demo-cube-api demo-router demo-worker demo-refresher
kubectl -n cube-operator-demo get statefulset demo-metastore
kubectl -n cube-operator-demo get service demo-cube-api demo-router-leader
```

- [ ] **Step 2: Run the script before wiring it into run.sh**

Run: `bash operators/cube-operator/demo/k8s/cubecluster-e2e.sh`

Expected: FAIL because the CRD/controller is not installed in the current demo.

- [ ] **Step 3: Update run.sh to install only the required M1 resources**

Install the new CRD, RBAC, Operator, object-store fixture, and `CubeCluster` CR. Do not install demo Redis for the new path.

- [ ] **Step 4: Run the M1 E2E**

Run:

```bash
bash operators/cube-operator/demo/k8s/run.sh
bash operators/cube-operator/demo/k8s/cubecluster-e2e.sh
```

Expected: `CubeCluster Ready`, all desired workloads ready, and real Cube API query PASS.

- [ ] **Step 5: Commit after authorization**

```bash
git add operators/cube-operator/demo/k8s
git commit -m "test(operator): demonstrate one-CR Cube deployment"
```
