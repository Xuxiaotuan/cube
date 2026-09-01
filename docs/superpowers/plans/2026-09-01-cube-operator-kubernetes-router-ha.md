# Cube Operator Kubernetes-Native Router HA Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Router HA Redis/PostgreSQL leadership with Kubernetes Lease while preserving fail-closed Router fencing and staged Service promotion.

**Architecture:** The Operator conditionally writes one per-cluster Lease. A per-Pod sidecar watches that Lease and writes local expiring identity files; Router status acknowledges the exact generation, epoch, holder UID, and token hash before the Operator labels it for the leader Service.

**Tech Stack:** Go client-go/controller-runtime, `coordination.k8s.io/v1`, Rust Router HTTP service, TypeScript CubeStoreDriver, EndpointSlice, Kubernetes E2E.

**Spec:** `docs/superpowers/specs/2026-09-01-cube-operator-kubernetes-native-cluster-design.md`

## Global Constraints

- No Redis or PostgreSQL is required for Router election, renewal, fencing, or sidecar operation.
- `resourceVersion` is an update precondition, not an application epoch.
- Holder identity uses Pod UID.
- Lease recreation changes `lease-generation`.
- The data plane fails closed when the sidecar cannot prove an unexpired exact Lease.
- A no-leader window is valid; two ready leader endpoints are never valid.
- Commit steps require explicit user authorization before execution.

---

### Task 1: Implement Kubernetes LeaseStore

**Files:**
- Create: `operators/cube-operator/internal/leadership/kubernetes_store.go`
- Create: `operators/cube-operator/internal/leadership/kubernetes_store_test.go`
- Modify: `operators/cube-operator/internal/leadership/types.go`

**Interfaces:**
- Consumes: `client.Client`, namespace, Lease name, and a clock.
- Produces: existing `leadership.LeaseStore` methods and extended `LeaseRecord.Generation`, `LeaseRecord.HolderUID`.

- [ ] **Step 1: Write conflict, renewal, expiry, and recreation tests**

```go
func TestKubernetesStoreFencesStaleHolder(t *testing.T) {
    store := newFakeKubernetesStore(t)
    first, acquired, err := store.Acquire(ctx, "cluster", "pod-a-uid", 30*time.Second)
    if err != nil || !acquired { t.Fatalf("acquire=%v err=%v", acquired, err) }
    expireLease(t, store, first)
    second, acquired, err := store.Acquire(ctx, "cluster", "pod-b-uid", 30*time.Second)
    if err != nil || !acquired { t.Fatalf("second acquire=%v err=%v", acquired, err) }
    if second.Epoch != first.Epoch+1 || second.Generation != first.Generation {
        t.Fatalf("first=%#v second=%#v", first, second)
    }
    if _, ok, _ := store.Renew(ctx, first, 30*time.Second); ok { t.Fatal("stale renew succeeded") }
}
```

- [ ] **Step 2: Run tests and confirm failure**

Run: `cd operators/cube-operator && go test ./internal/leadership -run Kubernetes`

Expected: FAIL because `KubernetesStore` and extended fields do not exist.

- [ ] **Step 3: Implement exact conditional Lease updates**

```go
type LeaseRecord struct {
    ClusterID, HolderID, HolderUID, Generation, Token string
    Epoch int64
    IssuedAt, ExpiresAt time.Time
}

func NewKubernetesStore(c client.Client, namespace, leaseName string, clock Clock) *KubernetesStore
```

Acquire performs `Get`, creates a Lease when absent, or updates an expired Lease with the read `resourceVersion`. The same update changes holder UID, increments `cubejs.io/fencing-epoch`, generates a new token, and sets `renewTime`. Renew verifies exact generation, holder UID, epoch, and token before updating `renewTime`.

- [ ] **Step 4: Run LeaseStore tests**

Run: `cd operators/cube-operator && go test ./internal/leadership`

Expected: PASS, including legacy store tests until cleanup.

- [ ] **Step 5: Commit after authorization**

```bash
git add operators/cube-operator/internal/leadership
git commit -m "feat(operator): add Kubernetes router LeaseStore"
```

### Task 2: Replace the Redis-only sidecar with a Lease watcher

**Files:**
- Create: `operators/cube-operator/internal/agent/kubernetes_reader.go`
- Create: `operators/cube-operator/internal/agent/kubernetes_reader_test.go`
- Modify: `operators/cube-operator/internal/agent/agent.go`
- Modify: `operators/cube-operator/internal/agent/agent_test.go`
- Modify: `operators/cube-operator/cmd/lease-agent/main.go`
- Modify: `operators/cube-operator/cmd/lease-agent/main_test.go`

**Interfaces:**
- Consumes: namespaced Lease watch and Pod UID from Downward API.
- Produces: `leadership.json` containing holder UID, generation, epoch, token hash, observed time, and local expiry.

- [ ] **Step 1: Write sidecar fail-closed tests**

```go
func TestAgentExpiresOnWatchLoss(t *testing.T) {
    clock := newFakeClock()
    reader := newFakeLeaseReader(validLeaseEvent(clock.Now()))
    a := newAgentForTest(t, reader, clock)
    require.NoError(t, a.Sync(context.Background()))
    reader.Fail(errors.New("apiserver unavailable"))
    clock.Advance(31 * time.Second)
    require.Error(t, a.Sync(context.Background()))
    assertExpiredFollowerFile(t, a.path)
}
```

Cover holder UID mismatch, generation change, epoch regression, token change without epoch, malformed annotations, and watcher restart.

- [ ] **Step 2: Run tests and confirm failure**

Run: `cd operators/cube-operator && go test ./internal/agent ./cmd/lease-agent`

Expected: FAIL because the Kubernetes reader and new identity fields are absent.

- [ ] **Step 3: Implement the Lease reader and local observation deadline**

```go
type LeaseReader interface {
    Get(context.Context) (leadership.LeaseRecord, error)
    Watch(context.Context, string) (watch.Interface, error)
}

type LeadershipFile struct {
    HolderID, HolderUID, Generation, TokenHash string
    Epoch int64
    ObservedAt, ExpiresAt time.Time
}
```

The agent computes local expiry from the observation time and remaining Lease duration. It writes atomically to `emptyDir` and writes an expired follower identity on every unprovable state.

- [ ] **Step 4: Replace CLI flags and Redis client initialization**

Required flags are `--namespace`, `--lease-name`, `--holder-uid`, `--path`, and `--retry-period`. Initialize in-cluster config and `CoordinationV1().Leases(namespace)`. Remove Redis URL/password/prefix handling from the command.

- [ ] **Step 5: Run tests**

Run: `cd operators/cube-operator && go test ./internal/agent ./cmd/lease-agent`

Expected: PASS.

- [ ] **Step 6: Commit after authorization**

```bash
git add operators/cube-operator/internal/agent operators/cube-operator/cmd/lease-agent
git commit -m "feat(operator): watch Kubernetes Lease in router sidecar"
```

### Task 3: Extend Router and Driver fencing identity

**Files:**
- Modify: `rust/cubestore/cubestore/src/http/mod.rs`
- Modify: `rust/cubestore/cubestore/src/config/mod.rs`
- Modify: `packages/cubejs-cubestore-driver/src/CubeStoreDriver.ts`
- Modify: `packages/cubejs-backend-shared/src/env.ts`
- Test: `packages/cubejs-cubestore-driver/test/CubeStoreDriver.test.ts`

**Interfaces:**
- Router status produces: `leaseGeneration`, `leaderEpoch`, `holderUID`, `leaseTokenHash`, `expiresAt`, `is_leader`, and `metaStoreReady`.
- Driver caches `(leaseGeneration, leaderEpoch)` and closes connections on generation or leader change.

- [ ] **Step 1: Write Rust stale-generation and expiry tests**

```rust
#[test]
fn promotion_rejects_different_lease_generation() {
    let lease = lease_file("generation-a", 7, "pod-uid", "token");
    let marker = promotion_marker("generation-b", 7, "pod-uid", "token");
    assert!(!HttpServer::promotion_matches(&lease, &marker, "router-a"));
}
```

- [ ] **Step 2: Write Driver generation-reset test**

```ts
it('closes cached connections when lease generation changes', async () => {
  statusProbe.replyOnce({ is_leader: true, leaseGeneration: 'a', leaderEpoch: 9 });
  await driver.testConnection();
  statusProbe.replyOnce({ is_leader: true, leaseGeneration: 'b', leaderEpoch: 1 });
  await driver.testConnection();
  expect(closeAllConnections).toHaveBeenCalled();
});
```

- [ ] **Step 3: Run focused tests and confirm failure**

Run:

```bash
cd rust/cubestore && cargo test -p cubestore http::tests --lib
yarn workspace @cubejs-backend/cubestore-driver test CubeStoreDriver.test.ts
```

Expected: new tests FAIL because generation and holder UID are not part of the contract.

- [ ] **Step 4: Implement exact identity parsing and checks**

Extend Rust `LocalLeaseFile`, `PromotionMarker`, `promotion_matches`, `ensure_write_fence`, and status payload. Extend Driver payload parsing and cache identity to reject stale generation/epoch responses and reset connections on a generation change.

- [ ] **Step 5: Run focused Rust and Driver tests**

Run the commands from Step 3.

Expected: PASS.

- [ ] **Step 6: Commit after authorization**

```bash
git add rust/cubestore/cubestore/src/http/mod.rs rust/cubestore/cubestore/src/config/mod.rs packages/cubejs-cubestore-driver packages/cubejs-backend-shared/src/env.ts
git commit -m "feat(cubestore): fence routers by Kubernetes lease generation"
```

### Task 4: Adapt promotion and Pod event reconciliation

**Files:**
- Modify: `operators/cube-operator/controllers/cubestore_router_controller.go`
- Modify: `operators/cube-operator/controllers/election_controller.go`
- Modify: `operators/cube-operator/controllers/routing_controller.go`
- Modify: `operators/cube-operator/controllers/cubecluster_controller.go`
- Test: `operators/cube-operator/controllers/cubestore_router_controller_test.go`
- Test: `operators/cube-operator/controllers/cubecluster_controller_test.go`

**Interfaces:**
- Consumes: Kubernetes `LeaseRecord` from M2 Task 1 and Router status from M2 Task 3.
- Produces: `Candidate -> Fenced -> LeaseObserved -> RouterAcknowledged -> LabeledLeader -> EndpointServing`.

- [ ] **Step 1: Write no-overlap and Pod-event tests**

```go
func TestPromotionWaitsForZeroOldEndpoints(t *testing.T) {
    fixture := promotionFixtureWithOldReadyEndpoint()
    _, phase, err := fixture.reconcile()
    if !errors.Is(err, errPromotionPending) || phase != promotionPhaseFenced {
        t.Fatalf("phase=%s err=%v", phase, err)
    }
    fixture.assertNoPodHasLeaderLabel()
}
```

Add a test proving a Router Pod deletion enqueues its owning `CubeCluster` without waiting for periodic requeue.

- [ ] **Step 2: Run tests and confirm failure**

Run: `cd operators/cube-operator && go test ./controllers -run 'Promotion|PodEvent|Endpoint'`

Expected: new phase and map-watch tests FAIL.

- [ ] **Step 3: Replace external store selection with Kubernetes LeaseStore**

Construct the store from the current cluster namespace and generated Router Lease name. Re-read the Lease through `APIReader` before every Pod label, promotion state, and CR status write.

- [ ] **Step 4: Implement explicit Pod watch mapping**

Use `Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(...))` and map labels `cubejs.io/cluster=<name>` to the matching `CubeCluster` request. Keep periodic requeue only as a safety net.

- [ ] **Step 5: Run all Operator tests**

Run: `cd operators/cube-operator && go test ./...`

Expected: PASS.

- [ ] **Step 6: Commit after authorization**

```bash
git add operators/cube-operator/controllers
git commit -m "feat(operator): promote routers from Kubernetes Lease"
```

### Task 5: Remove Router HA Redis/PG manifests and prove failover

**Files:**
- Modify: `operators/cube-operator/demo/k8s/mock-routers.yaml`
- Modify: `operators/cube-operator/demo/k8s/operator-rbac.yaml`
- Modify: `operators/cube-operator/demo/k8s/run.sh`
- Modify: `operators/cube-operator/demo/k8s/cube-api-failover-check.sh`
- Modify: `operators/cube-operator/demo/k8s/README.md`
- Delete after migration evidence: `operators/cube-operator/demo/k8s/redis.yaml`
- Delete after migration evidence: `operators/cube-operator/internal/leadership/redis_store.go`
- Delete after migration evidence: `operators/cube-operator/internal/leadership/postgres_store.go`

**Interfaces:**
- Produces: a Kubernetes-only Router HA demo and machine-readable failover result.

- [ ] **Step 1: Add no-external-lease assertions**

```bash
if kubectl -n cube-operator-demo get deploy cube-router-lease-redis >/dev/null 2>&1; then
  echo "FAIL: Router HA deployed Redis" >&2
  exit 1
fi
kubectl -n cube-operator-demo get lease demo-router-leader >/dev/null
```

- [ ] **Step 2: Update sidecar RBAC and Deployment**

Grant Router ServiceAccount `get`, `list`, and `watch` on Leases in the namespace. Inject `POD_UID` through Downward API. Remove all Redis URL/password environment variables and Secrets.

- [ ] **Step 3: Run focused build and unit tests**

Run:

```bash
cd operators/cube-operator
go test ./...
go build ./cmd/lease-agent
```

Expected: PASS with no Redis/PG leadership imports.

- [ ] **Step 4: Run real Kubernetes failover**

Run:

```bash
bash operators/cube-operator/demo/k8s/run.sh
bash operators/cube-operator/demo/k8s/cube-api-failover-check.sh
```

Expected: before/after real-data hash equal, Lease epoch increases, Lease generation remains stable during holder transition, and Endpoint overlap is zero.

- [ ] **Step 5: Run API partition test**

Block the leader sidecar's API Server access with a test-only NetworkPolicy, leave Router network reachable, and assert `/router/status` becomes follower/unready after the local deadline before a new leader is exposed.

- [ ] **Step 6: Remove legacy code only after migration evidence passes**

Remove Redis/PG Router leadership stores, dependencies, examples, and manifests. Preserve unrelated Cube application database support.

- [ ] **Step 7: Commit after authorization**

```bash
git add operators/cube-operator rust/cubestore/cubestore/src/http/mod.rs packages/cubejs-cubestore-driver
git commit -m "test(operator): prove Kubernetes-native router failover"
```
