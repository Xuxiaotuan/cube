# Cube Operator Production Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the security, lifecycle, observability, backup, upgrade, rollback, and failure evidence required to approve the Kubernetes-native Cube Operator for production.

**Architecture:** Production readiness is implemented as enforced defaults and explicit conditions, then proven through upgrade, backup/restore, partition, and repeated failover tests. Documentation records measured evidence and does not promote lower-level test results to production proof.

**Tech Stack:** Go controller-runtime, Kubernetes RBAC/PDB/NetworkPolicy/Lease/Events, Prometheus metrics, Rust/Node workload metrics, shell E2E, object storage and MetaStore backups.

**Spec:** `docs/superpowers/specs/2026-09-01-cube-operator-kubernetes-native-cluster-design.md`

## Global Constraints

- Production defaults use non-root containers, Secret references, PDB, and topology spread.
- Persistent data defaults to `Retain` on CR deletion.
- Upgrade does not reuse old Lease generation, epoch, or token.
- Production approval requires executed machine-readable evidence.
- MetaStore Raft is not required for this milestone; tested backup/restore and declared RPO/RTO are required.
- Commit steps require explicit user authorization before execution.

---

### Task 1: Harden RBAC, pod security, disruption, and network policy

**Files:**
- Modify: `operators/cube-operator/config/rbac/role.yaml`
- Modify: `operators/cube-operator/config/manager/manager.yaml`
- Create: `operators/cube-operator/controllers/resources/network_policy.go`
- Modify: `operators/cube-operator/controllers/resources/common.go`
- Test: `operators/cube-operator/controllers/resources/resources_test.go`

**Interfaces:**
- Produces least-privilege Operator and sidecar permissions and production Pod security defaults.

- [ ] **Step 1: Write resource security tests**

```go
func TestProductionPodsAreNonRootAndDropCapabilities(t *testing.T) {
    for _, podSpec := range allGeneratedPodSpecs(validCluster()) {
        if podSpec.SecurityContext == nil || podSpec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
            t.Fatal("missing RuntimeDefault seccomp")
        }
        for _, container := range podSpec.Containers {
            if container.SecurityContext == nil || container.SecurityContext.RunAsNonRoot == nil || !*container.SecurityContext.RunAsNonRoot {
                t.Fatalf("container %s is not non-root", container.Name)
            }
        }
    }
}
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd operators/cube-operator && go test ./controllers/resources -run 'Security|NetworkPolicy|PDB'`

Expected: new security assertions FAIL.

- [ ] **Step 3: Implement exact policies**

Generate namespace-scoped NetworkPolicies allowing Cube API to Router, Router to MetaStore/Workers/object store, Workers to MetaStore/object store, sidecars to Kubernetes API, and Refresher to Cube API/Router. Apply non-root, dropped capabilities, read-only root filesystem where supported, RuntimeDefault seccomp, PDB, and topology spread.

- [ ] **Step 4: Run resource and RBAC tests**

Run: `cd operators/cube-operator && go test ./controllers/resources ./controllers`

Expected: PASS.

- [ ] **Step 5: Commit after authorization**

```bash
git add operators/cube-operator/config operators/cube-operator/controllers/resources
git commit -m "feat(operator): harden production Kubernetes resources"
```

### Task 2: Implement finalizer, retention, upgrade, and rollback

**Files:**
- Create: `operators/cube-operator/controllers/cubecluster_lifecycle.go`
- Create: `operators/cube-operator/controllers/cubecluster_lifecycle_test.go`
- Modify: `operators/cube-operator/controllers/cubecluster_controller.go`
- Modify: `operators/cube-operator/api/cubejs/v1alpha1/cubecluster_types.go`

**Interfaces:**
- Produces: `cubejs.io/finalizer`, retention policy, ordered rollout, and recorded current/target versions.

- [ ] **Step 1: Write deletion and rollout tests**

```go
func TestDeletionRetainsPersistentDataByDefault(t *testing.T) {
    fixture := deletingClusterFixture("Retain")
    fixture.reconcile()
    fixture.assertDeleted("production-router")
    fixture.assertExists("production-metastore-data")
    fixture.assertUserSecretExists("cube-object-store")
}
```

Add tests for Worker-before-Router rollout, follower readiness before leader replacement, blocked incompatible MetaStore downgrade, and Lease generation preservation rules.

- [ ] **Step 2: Run and confirm failure**

Run: `cd operators/cube-operator && go test ./controllers -run 'Lifecycle|Upgrade|Rollback|Retention'`

Expected: FAIL because lifecycle reconciliation is absent.

- [ ] **Step 3: Implement lifecycle state machine**

```text
PreparingMetaStore
-> RollingWorkers
-> RollingRouterFollowers
-> TransferringRouterLeader
-> RollingCubeAPI
-> RollingRefresher
-> Ready
```

Persist current and target image versions in status. On deletion, fence traffic and leadership first, delete stateless owned resources, then honor `Retain` or `Delete` for Operator-created PVCs. Never delete referenced user objects.

- [ ] **Step 4: Run lifecycle and full Go tests**

Run: `cd operators/cube-operator && go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit after authorization**

```bash
git add operators/cube-operator/controllers/cubecluster_lifecycle.go operators/cube-operator/controllers/cubecluster_lifecycle_test.go operators/cube-operator/controllers/cubecluster_controller.go operators/cube-operator/api/cubejs/v1alpha1/cubecluster_types.go
git commit -m "feat(operator): manage CubeCluster lifecycle"
```

### Task 3: Add metrics, events, and actionable conditions

**Files:**
- Create: `operators/cube-operator/internal/observability/metrics.go`
- Create: `operators/cube-operator/internal/observability/metrics_test.go`
- Modify: `operators/cube-operator/controllers/cubecluster_controller.go`
- Modify: `operators/cube-operator/controllers/cubestore_router_controller.go`
- Modify: `operators/cube-operator/controllers/cubecluster_status.go`

**Interfaces:**
- Produces named Prometheus metrics and Kubernetes Events from design section 15.

- [ ] **Step 1: Write metric transition tests**

```go
func TestPromotionMetricsRecordNoLeaderAndOverlap(t *testing.T) {
    recorder := newTestRecorder()
    recorder.ObserveLeaderEndpoints("ns", "cluster", 0)
    recorder.ObserveLeaderEndpoints("ns", "cluster", 2)
    assertMetric(t, "cube_router_no_leader", 1)
    assertMetric(t, "cube_router_endpoint_overlap_total", 1)
}
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd operators/cube-operator && go test ./internal/observability ./controllers -run 'Metric|Event|Condition'`

Expected: FAIL because the observability package is missing.

- [ ] **Step 3: Implement bounded-cardinality metrics and events**

Do not label metrics by Pod UID, mutation ID, table, or request ID. Use namespace, cluster, component, phase, and reason. Emit warning Events for overlap, stale writes, UNKNOWN age, dependency outage, blocked upgrade, and rollback.

- [ ] **Step 4: Run tests**

Run: `cd operators/cube-operator && go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit after authorization**

```bash
git add operators/cube-operator/internal/observability operators/cube-operator/controllers
git commit -m "feat(operator): expose CubeCluster HA observability"
```

### Task 4: Prove MetaStore backup and restore

**Files:**
- Create: `operators/cube-operator/demo/k8s/metastore-backup-restore.sh`
- Create: `operators/cube-operator/demo/k8s/metastore-backup-result.json`
- Modify: `operators/cube-operator/controllers/resources/metastore.go`
- Modify: `operators/cube-operator/HA-PRODUCTION-CHECKLIST.md`

**Interfaces:**
- Produces a versioned backup artifact, checksum, measured RPO/RTO, and restore evidence.

- [ ] **Step 1: Add backup manifest generation and pre-restore assertions**

The script creates real tables, partitions, chunks, a committed pre-aggregation, and mutation records, then records row and metadata hashes.

- [ ] **Step 2: Execute backup and destructive fixture recovery**

Run: `bash operators/cube-operator/demo/k8s/metastore-backup-restore.sh`

Expected: the script deletes only its dedicated demo MetaStore PVC after producing a verified backup, restores a new PVC, and returns matching data and metadata hashes.

- [ ] **Step 3: Record machine-readable results**

Required result shape:

```json
{"backupChecksum":"sha256:<value>","dataHashBefore":"<value>","dataHashAfter":"<value>","metadataHashBefore":"<value>","metadataHashAfter":"<value>","rpoSeconds":0,"rtoSeconds":0,"passed":true}
```

`rtoSeconds` contains the measured value; zero is not hard-coded.

- [ ] **Step 4: Commit after authorization**

```bash
git add operators/cube-operator/demo/k8s/metastore-backup-restore.sh operators/cube-operator/demo/k8s/metastore-backup-result.json operators/cube-operator/controllers/resources/metastore.go operators/cube-operator/HA-PRODUCTION-CHECKLIST.md
git commit -m "test(operator): verify metastore backup and restore"
```

### Task 5: Execute the production failure and upgrade matrix

**Files:**
- Create: `operators/cube-operator/demo/k8s/production-gate.sh`
- Create: `operators/cube-operator/demo/k8s/production-gate-result.json`
- Modify: `operators/cube-operator/HA-FAILURE-MATRIX.md`
- Modify: `operators/cube-operator/HA-ROUTER-K8S-DEMO.md`
- Modify: `operators/cube-operator/HA-PRODUCTION-CHECKLIST.md`

**Interfaces:**
- Consumes: M1-M3 E2E scripts and M4 Tasks 1-4.
- Produces: final evidence and GO/NO-GO decision.

- [ ] **Step 1: Encode all hard gates**

The runner must execute and fail independently on:

```text
full cluster install
operator leader loss
router pod loss
router API partition
100 router failovers
worker loss during each Job phase
metastore restart
object-store outage
pre-aggregation failure matrix
refresher leader loss
rolling upgrade
rollback
backup and restore
```

- [ ] **Step 2: Run the complete gate**

Run: `bash operators/cube-operator/demo/k8s/production-gate.sh`

Required final fields:

```json
{"install":true,"routerFailovers":100,"holdersOverlap":0,"duplicateCommitted":0,"duplicateChunks":0,"unknownUnresolved":0,"rpo":0,"upgrade":true,"rollback":true,"backupRestore":true,"passed":true}
```

- [ ] **Step 3: Run repository checks**

Run:

```bash
cd operators/cube-operator && go test ./...
yarn workspace @cubejs-backend/cubestore-driver lint
yarn workspace @cubejs-backend/cubestore-driver test
yarn workspace @cubejs-backend/query-orchestrator test
yarn workspace @cubejs-backend/server-core test RefreshScheduler.test.ts
cd rust/cubestore && cargo test -p cubestore --lib
```

Expected: every command PASS. Existing unrelated failures must be resolved or explicitly block production approval; they cannot be silently ignored.

- [ ] **Step 4: Write the GO/NO-GO conclusion from actual output**

Set GO only when `production-gate-result.json.passed=true` and every required repository check passes. Otherwise set NO-GO with the exact failed gate and preserve `evidence_incomplete` or `external_blocked` as applicable.

- [ ] **Step 5: Commit after authorization**

```bash
git add operators/cube-operator/demo/k8s/production-gate.sh operators/cube-operator/demo/k8s/production-gate-result.json operators/cube-operator/HA-FAILURE-MATRIX.md operators/cube-operator/HA-ROUTER-K8S-DEMO.md operators/cube-operator/HA-PRODUCTION-CHECKLIST.md
git commit -m "test(operator): enforce production release gates"
```
