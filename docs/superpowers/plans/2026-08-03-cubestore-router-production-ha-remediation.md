# CubeStore Router Production HA Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the current Router failover prototype into a production-safe, single-writer CubeStore Router HA system with durable metadata, hard fencing, safe retries, and verified failover of real data and refresh jobs.

**Architecture:** The first production milestone uses two Router candidates but only one write-capable Router, a dedicated authoritative MetaStore service on an RWO persistent volume, an external lease with monotonically increasing fencing epochs, and object storage for CubeStore data files. A local lease-agent sidecar gives each Router a short-lived leadership file so a partitioned Router fails closed. True hot MetaStore replication with Raft is a separate follow-up milestone and is not required to ship the first safe single-writer version.

**Tech Stack:** Go/controller-runtime, Kubernetes CRD/Lease/EndpointSlice, Rust/Tokio/CubeStore, TypeScript/Node.js, PostgreSQL or Redis for leadership leases, Redis for mutation idempotency, RocksDB on CSI RWO PVC, S3/MinIO for data files.

## Global Constraints

- Never allow two Router processes to commit MetaStore mutations under the same or different epochs.
- Redis is allowed for leases and idempotency but is not the authoritative CubeStore MetaStore.
- PostgreSQL is preferred for durable lease records; Redis requires AOF, replication, and fail-closed behavior.
- A Router must reject traffic when leadership state is missing, malformed, expired, or older than the highest epoch observed by that process.
- HTTP, WebSocket, MySQL, uploads, internal RPC, Scheduler, Job reconcile, Snapshot upload, and cleanup loops must obey the same leadership guard.
- CubeStore business files must use S3/MinIO or another supported durable object store.
- RocksDB must never be opened concurrently by two processes on a shared path.
- Production rollout must use immutable image tags or digests.
- Every P0 task requires automated tests before merge.

---

## Delivery Gates

| Gate | Outcome | Required tasks |
|---|---|---|
| G0 | Current prototype is testable without false-positive `SELECT 1` checks | 1, 2 |
| G1 | Split brain is prevented and every entry point is fenced | 3, 4, 5, 6 |
| G2 | Router candidates use one authoritative durable MetaStore | 7, 8 |
| G3 | Driver writes and refresh scheduling are idempotent | 9, 10, 11 |
| G4 | Kubernetes deployment survives process/node maintenance | 12 |
| G5 | Production release candidate passes destructive acceptance tests | 13, 14 |
| G6 | Optional zero-RTO MetaStore hot standby | 15 |

## Parallel Execution Waves

| Wave | Tasks that may run in parallel | Start condition |
|---|---|---|
| 1 | 1, 2, 3, 4 | Immediately |
| 2 | 5, 7, 9, 11 | Task 3 interfaces agreed |
| 3 | 6, 8, 10, 12 | Corresponding Wave 2 task merged |
| 4 | 13 | Tasks 1 through 12 merged |
| 5 | 14 | Task 13 passes |
| Future | 15 | Production single-writer release is stable |

---

### Task 1: Replace False-Positive HA Tests with Real Data Fixtures

**Files:**

- Modify: `operators/cube-operator/demo/k8s/data-consistency-check.sh`
- Modify: `operators/cube-operator/demo/k8s/cube-api-failover-check.sh`
- Modify: `operators/cube-operator/demo/cube-api/schema/cubes/RouterHaProbe.js`
- Create: `operators/cube-operator/demo/k8s/fixtures/router-ha-data.sql`
- Create: `operators/cube-operator/demo/k8s/real-data-failover-check.sh`

**Interfaces:**

- Consumes: Existing `CubestoreRouter.status.leader` and `leaderEpoch`.
- Produces: A destructive E2E test that creates a real table, imports rows, verifies metadata and row hashes, deletes the Leader, and verifies the same identifiers and rows after promotion.

- [ ] Write a fixture that creates a uniquely named schema/table, imports at least 10,000 deterministic rows, and records table, partition, chunk, and row-count checksums.
- [ ] Make service-query failures fatal by default and remove `|| true` from required assertions.
- [ ] Make a non-increasing `leaderEpoch` a hard failure.
- [ ] Remove direct follower MySQL success as an expected consistency condition; the follower query must fail with `STALE_LEADER`.
- [ ] Add assertions for `system.tables`, `system.partitions`, `system.chunks`, and the business-row hash.
- [ ] Run `bash operators/cube-operator/demo/k8s/real-data-failover-check.sh` and require a failure against the current two-local-RocksDB deployment.
- [ ] Commit as `test: add real data router failover coverage`.

**Acceptance:** The old deployment fails for a meaningful data reason, while a correct shared-MetaStore deployment preserves the same table ID, chunk set, row count, and business hash.

---

### Task 2: Restore Rust Network Protocol Compatibility

**Files:**

- Modify: `rust/cubestore/cubestore/src/cluster/message.rs`
- Modify: `rust/cubestore/cubestore/src/cluster/mod.rs`
- Create: `rust/cubestore/cubestore/tests/network_message_compat.rs`

**Interfaces:**

- Consumes: `NetworkMessage` flexbuffers protocol version 1.
- Produces: Stable serialization for every pre-existing enum variant during rolling upgrades.

- [ ] Add golden-byte tests for `NotifyJobListeners`, `NotifyJobListenersSuccess`, `MetaStoreCall`, and `MetaStoreCallResult` using the master-branch encoding.
- [ ] Remove unused `GetRouterInfo`, `RouterInfo`, and `available_nodes` changes unless a caller is introduced in the same task.
- [ ] If Router info RPC is required, append new variants after every existing variant and bump or negotiate `NETWORK_MESSAGE_VERSION` with explicit backward-compatibility tests.
- [ ] Run the focused Rust compatibility test against old and new fixtures.
- [ ] Commit as `fix: preserve cubestore network protocol compatibility`.

**Acceptance:** New Router and Worker binaries decode all old messages correctly; mixed-version tests either pass or fail fast with an explicit protocol mismatch before processing a wrong message.

---

### Task 3: Define the Leadership and Fencing Contract

**Files:**

- Modify: `operators/cube-operator/api/v1alpha1/cubestore_router_types.go`
- Modify: `operators/cube-operator/config/crd/bases/cubestore.io_cubestorerouters.yaml`
- Create: `operators/cube-operator/internal/leadership/types.go`
- Create: `operators/cube-operator/internal/leadership/types_test.go`

**Interfaces:**

- Produces:

```go
type LeaseRecord struct {
    ClusterID string
    HolderID  string
    Epoch     int64
    Token     string
    IssuedAt  time.Time
    ExpiresAt time.Time
}

type LeaseStore interface {
    Acquire(context.Context, string, string, time.Duration) (LeaseRecord, bool, error)
    Renew(context.Context, LeaseRecord, time.Duration) (LeaseRecord, bool, error)
    Release(context.Context, LeaseRecord) error
    Get(context.Context, string) (LeaseRecord, error)
}
```

- [ ] Add CRD fields for `leaseDurationSeconds`, `renewDeadlineSeconds`, `retryPeriodSeconds`, `stateStore.secretRef`, `metaStore.address`, `storage.dataPVC`, and `storage.objectStoreSecretRef`.
- [ ] Remove plaintext DSN fields from the production API and support migration from the alpha field only through a documented compatibility path.
- [ ] Add validation requiring a non-empty selector, namespace equality, strict election mode, positive timing values, and `renewDeadline < leaseDuration`.
- [ ] Define condition types `LeaseAcquired`, `LeaderReady`, `MetaStoreReady`, `DataPlaneReady`, and `Degraded`.
- [ ] Write Go tests for defaulting, invalid selectors, invalid timing, and secret references.
- [ ] Commit as `feat: define router leadership fencing contract`.

**Acceptance:** Invalid or unsafe HA CRs are rejected by the API server before reconciliation.

---

### Task 4: Implement an Atomic External Lease Store

**Files:**

- Create: `operators/cube-operator/internal/leadership/redis_store.go`
- Create: `operators/cube-operator/internal/leadership/postgres_store.go`
- Create: `operators/cube-operator/internal/leadership/redis_store_test.go`
- Create: `operators/cube-operator/internal/leadership/postgres_store_test.go`
- Modify: `operators/cube-operator/controllers/cubestore_router_controller.go`

**Interfaces:**

- Consumes: `LeaseStore` from Task 3.
- Produces: Atomic acquire/renew/release with a monotonically increasing epoch and an unforgeable owner token.

- [ ] Implement Redis acquire and renew as Lua scripts that compare holder and token, set an expiry, and increment a durable epoch key only on ownership transfer.
- [ ] Implement PostgreSQL acquire and renew in a transaction using `SELECT ... FOR UPDATE`, server-side `now()`, and a unique cluster row.
- [ ] Make backend read/write errors fail closed; never fall back to CR status or ConfigMap for election.
- [ ] Keep Kubernetes manager leader election separate from Router leadership.
- [ ] Add contention tests with 20 concurrent contenders and assert exactly one successful holder per epoch.
- [ ] Add expiry, stale-token renewal, backend outage, and clock-skew tests.
- [ ] Commit as `feat: add atomic router leadership lease`.

**Acceptance:** Under concurrency and failure injection, two holders can never receive valid leases for the same epoch, and stale holders cannot renew or release a newer lease.

---

### Task 5: Add a Per-Pod Lease Agent and Expiring Leadership File

**Files:**

- Create: `operators/cube-operator/cmd/lease-agent/main.go`
- Create: `operators/cube-operator/internal/agent/agent.go`
- Create: `operators/cube-operator/internal/agent/agent_test.go`
- Modify: `operators/cube-operator/Dockerfile`
- Modify: `operators/cube-operator/config/manager/manager.yaml`
- Modify: `operators/cube-operator/demo/k8s/mock-routers.yaml`

**Interfaces:**

- Consumes: External `LeaseRecord` from Task 4.
- Produces: `/var/run/cubestore-ha/leadership.json`, written atomically with mode `0640`.

```json
{
  "holderId": "router-pod-name",
  "epoch": 42,
  "tokenHash": "sha256:...",
  "issuedAt": "2026-08-03T10:00:00Z",
  "expiresAt": "2026-08-03T10:00:15Z"
}
```

- [ ] Poll the authoritative lease store at `retryPeriodSeconds` and write through a temporary file plus atomic rename.
- [ ] Write an expired follower state immediately when the backend is unavailable beyond `renewDeadlineSeconds`.
- [ ] Mount the file through `emptyDir` shared only between the lease-agent and Router containers.
- [ ] Stop using ConfigMap projection as the runtime fencing source; retain ConfigMap only for human-readable status if desired.
- [ ] Add tests for atomic writes, malformed backend data, outage expiry, holder changes, and monotonic epoch handling.
- [ ] Commit as `feat: add router lease agent sidecar`.

**Acceptance:** Disconnecting a Pod from Kubernetes and the lease backend causes its leadership file to expire and the Router to reject traffic within the configured lease duration.

---

### Task 6: Centralize Router Leadership Guard and Fence Every Execution Path

**Files:**

- Create: `rust/cubestore/cubestore/src/leadership/mod.rs`
- Modify: `rust/cubestore/cubestore/src/lib.rs`
- Modify: `rust/cubestore/cubestore/src/http/mod.rs`
- Modify: `rust/cubestore/cubestore/src/mysql/mod.rs`
- Modify: `rust/cubestore/cubestore/src/sql/mod.rs`
- Modify: `rust/cubestore/cubestore/src/config/mod.rs`
- Modify: `rust/cubestore/cubestore/src/scheduler/mod.rs`
- Create: `rust/cubestore/cubestore/tests/leadership_fencing.rs`

**Interfaces:**

- Consumes: Expiring leadership JSON from Task 5.
- Produces:

```rust
pub struct LeadershipPermit {
    pub epoch: u64,
    pub expires_at: SystemTime,
}

pub trait LeadershipGuard: Send + Sync {
    fn require_leader(&self) -> Result<LeadershipPermit, CubeError>;
    fn validate(&self, permit: &LeadershipPermit) -> Result<(), CubeError>;
    fn current_epoch(&self) -> Option<u64>;
}
```

- [ ] Parse the file strictly and fail closed for missing, malformed, expired, wrong-holder, or decreasing-epoch state.
- [ ] Inject one shared `LeadershipGuard` into HTTP, MySQL, SqlService, Scheduler, upload, Job reconcile, Snapshot upload, and cleanup paths.
- [ ] Require a permit before work starts and validate it again immediately before every durable metadata commit.
- [ ] Stop Scheduler, Job assignment, Snapshot upload, and cleanup loops while follower; resume only after acquiring a valid newer permit.
- [ ] Return a typed `STALE_LEADER` error containing the observed epoch but never the secret token.
- [ ] Add tests for HTTP, WebSocket, MySQL, upload, long-running writes, Scheduler, and demotion during commit.
- [ ] Commit as `feat: enforce router fencing across all execution paths`.

**Acceptance:** Direct Pod IP, MySQL, stale WebSocket, background loops, and long-running writes all stop committing after lease expiry or epoch change.

---

### Task 7: Introduce One Authoritative Durable MetaStore Service

**Files:**

- Modify: `rust/cubestore/cubestore/src/config/mod.rs`
- Modify: `rust/cubestore/cubestore/src/config/injection.rs`
- Create: `operators/cube-operator/config/metastore/statefulset.yaml`
- Create: `operators/cube-operator/config/metastore/service.yaml`
- Create: `operators/cube-operator/config/metastore/pdb.yaml`
- Create: `operators/cube-operator/demo/k8s/metastore.yaml`

**Interfaces:**

- Produces: `cubestore-metastore.<namespace>.svc:9999` as the single authoritative MetaStore RPC endpoint.
- Consumes: CSI `ReadWriteOnce` PVC and CubeStore MetaStore RPC already used by Worker nodes.

- [ ] Add focused tests proving a Router can use `CUBESTORE_META_ADDR` without requiring a local RocksMetaStore.
- [ ] Remove the Router validation error only after those tests pass.
- [ ] Run MetaStore as a dedicated single-writer StatefulSet with a stable Service and RWO PVC.
- [ ] Configure both Router candidates and all Workers to use the same MetaStore RPC address.
- [ ] Ensure Router followers never create `/cube/.cubestore/data/metastore`.
- [ ] Configure storage reclaim, snapshots, backup, restore, and CSI node-detach behavior.
- [ ] Add a MetaStore restart test proving the same table IDs, jobs, and chunk metadata survive Pod and node replacement.
- [ ] Commit as `feat: add authoritative cubestore metastore service`.

**Acceptance:** Both Routers observe identical table, partition, chunk, and Job records because neither owns an independent local MetaStore. Only one process opens the RocksDB PVC at any time.

---

### Task 8: Implement Two-Phase Router Promotion

**Files:**

- Split: `operators/cube-operator/controllers/cubestore_router_controller.go`
- Create: `operators/cube-operator/controllers/election_controller.go`
- Create: `operators/cube-operator/controllers/routing_controller.go`
- Create: `operators/cube-operator/controllers/election_controller_test.go`
- Create: `operators/cube-operator/controllers/routing_controller_test.go`

**Interfaces:**

- Consumes: Lease from Task 4, Router status with epoch from Task 6, MetaStore readiness from Task 7.
- Produces: Promotion state machine `FencingOld -> LeaseGranted -> WaitingForAck -> Routing -> Active`.

- [ ] Fence or wait for expiry of the old holder before granting a new epoch.
- [ ] Wait until the candidate returns `is_leader=true`, the exact new epoch, unexpired lease, and MetaStore readiness.
- [ ] Patch the new Leader label only after acknowledgement.
- [ ] Use EndpointSlice observation to confirm the Service has exactly one ready endpoint.
- [ ] Roll back to a no-leader state if acknowledgement times out; never route to an unacknowledged candidate.
- [ ] Preserve `lastTransitionTime` unless a condition actually changes.
- [ ] Add state-machine tests for Pod deletion, network partition, ConfigMap delay, Router startup delay, stale epoch, two reported leaders, and operator restart.
- [ ] Commit as `feat: add two phase router promotion`.

**Acceptance:** Every Service endpoint has already acknowledged the same valid epoch, and there is no interval where the Service routes to a Router that still considers itself follower.

---

### Task 9: Replace Driver Redis Idempotency with an Owned Renewable State Machine

**Files:**

- Create: `packages/cubejs-cubestore-driver/src/IdempotencyStore.ts`
- Create: `packages/cubejs-cubestore-driver/test/IdempotencyStore.test.ts`
- Modify: `packages/cubejs-cubestore-driver/src/CubeStoreDriver.ts`
- Modify: `packages/cubejs-cubestore-driver/src/WebSocketConnection.ts`

**Interfaces:**

- Produces:

```ts
type MutationLease = {
  key: string;
  ownerToken: string;
  fingerprint: string;
  expiresAt: number;
};

interface IdempotencyStore {
  acquire(mutationId: string, fingerprint: string): Promise<MutationLease | ExistingResult>;
  renew(lease: MutationLease): Promise<MutationLease>;
  complete(lease: MutationLease, resultRef: string): Promise<void>;
  fail(lease: MutationLease, error: SerializedError): Promise<void>;
}
```

- [ ] Implement acquire, renew, complete, and fail with Lua owner-token CAS.
- [ ] Renew before one-third of the pending TTL and abort the mutation if ownership is lost before commit.
- [ ] Fail closed when Redis is required but unavailable.
- [ ] Store a compact result reference, not an unbounded result set, for large mutations.
- [ ] Treat connection loss after send as `UNKNOWN`, reconcile against authoritative metadata, and never blindly replay.
- [ ] Add tests for operations longer than TTL, Redis restart, two Cube API replicas, stale completion, conflicting fingerprints, and connection loss after commit.
- [ ] Commit as `feat: make cubestore mutation idempotency renewable`.

**Acceptance:** A mutation executes at most once across concurrent API replicas, lease expiry, Router failover, and Redis reconnect; uncertain completion is reconciled rather than replayed.

---

### Task 10: Make Job, Upload, and Pre-Aggregation Commits Recoverable

**Files:**

- Modify: `rust/cubestore/cubestore/src/metastore/job.rs`
- Modify: `rust/cubestore/cubestore/src/cluster/ingestion/job_runner.rs`
- Modify: `rust/cubestore/cubestore/src/scheduler/mod.rs`
- Modify: `rust/cubestore/cubestore/src/http/mod.rs`
- Create: `rust/cubestore/cubestore/tests/job_failover.rs`
- Create: `rust/cubestore/cubestore/tests/upload_failover.rs`

**Interfaces:**

- Consumes: Leadership epoch from Task 6 and authoritative MetaStore from Task 7.
- Produces: Durable Job ownership fields `owner_epoch`, `attempt_id`, `heartbeat_at`, and atomic upload commit markers.

- [ ] Persist Job ownership and heartbeat in MetaStore.
- [ ] Permit takeover only when the old owner epoch is fenced and heartbeat is expired.
- [ ] Give each execution attempt a unique ID and make completion conditional on that attempt ID.
- [ ] Upload to a unique temporary object key and atomically publish metadata only after checksum validation.
- [ ] Make orphan cleanup aware of active attempt IDs and fencing epochs.
- [ ] Add failover tests at job assignment, mid-upload, post-upload/pre-commit, post-commit/pre-response, and pre-aggregation version switch.
- [ ] Commit as `feat: recover jobs and uploads across router failover`.

**Acceptance:** Failover produces either one committed result or one recoverable pending attempt, never two committed chunks or a metadata reference to a missing object.

---

### Task 11: Add Refresher Leader Election and Refresh Idempotency

**Files:**

- Create: `operators/cube-operator/api/v1alpha1/cube_refresher_types.go`
- Create: `operators/cube-operator/controllers/cube_refresher_controller.go`
- Create: `operators/cube-operator/config/crd/bases/cubestore.io_cuberefreshers.yaml`
- Create: `operators/cube-operator/config/samples/cube-refresher.yaml`
- Modify: Cube API refresh scheduling integration at the exact refresh-worker entry point selected during implementation.
- Create: `operators/cube-operator/demo/k8s/refresher-failover-check.sh`

**Interfaces:**

- Consumes: `LeaseStore` from Task 4 and mutation idempotency from Task 9.
- Produces: One active Refresher scheduler with refresh key `cube/preAggregation/timeRange/contentVersion`.

- [ ] Run two Refresher Pods but permit scheduling only for the valid lease holder.
- [ ] Persist refresh ownership, attempt, and completion state outside process memory.
- [ ] Make refresh task submission idempotent using a deterministic mutation ID.
- [ ] On takeover, reconcile existing MetaStore Jobs before submitting anything new.
- [ ] Add tests for Refresher death before submit, after submit, during build, and after build before acknowledgement.
- [ ] Commit as `feat: add refresher active standby scheduling`.

**Acceptance:** Killing the active Refresher promotes one standby and does not create a duplicate pre-aggregation build for the same refresh key.

---

### Task 12: Harden Kubernetes Production Deployment

**Files:**

- Modify: `operators/cube-operator/config/manager/manager.yaml`
- Modify: `operators/cube-operator/config/rbac/role.yaml`
- Create: `operators/cube-operator/config/production/router.yaml`
- Create: `operators/cube-operator/config/production/cube-api.yaml`
- Create: `operators/cube-operator/config/production/refresher.yaml`
- Create: `operators/cube-operator/config/production/network-policy.yaml`
- Create: `operators/cube-operator/config/production/pdb.yaml`

**Interfaces:**

- Consumes: Readiness and condition endpoints from Tasks 6 and 8.
- Produces: Deployable production manifests with no broad follower traffic path.

- [ ] Run two Operator replicas with controller-runtime leader election and add `coordination.k8s.io/leases` RBAC.
- [ ] Run at least two Cube API replicas and two Refresher replicas.
- [ ] Remove the broad `cube-router` Service or restrict it to status-only traffic.
- [ ] Expose MySQL and HTTP only through the acknowledged Leader Service.
- [ ] Add PDB, anti-affinity, topology spread, resource requests, termination grace, and preStop fencing.
- [ ] Use Secret references for Redis, PostgreSQL, object storage, and API credentials.
- [ ] Separate liveness, candidate-readiness, and leader-traffic-readiness probes.
- [ ] Pin all images by digest and define rollback-compatible versions.
- [ ] Commit as `feat: add production router ha manifests`.

**Acceptance:** A node drain leaves one API replica, one Operator replica, one Refresher candidate, and either a valid Router Leader or an explicit no-leader state without split brain.

---

### Task 13: Run the Production HA Failure Matrix

**Files:**

- Create: `operators/cube-operator/tests/e2e/ha-matrix.sh`
- Create: `operators/cube-operator/tests/e2e/assertions.sh`
- Create: `operators/cube-operator/tests/e2e/workloads/continuous-read.js`
- Create: `operators/cube-operator/tests/e2e/workloads/idempotent-write.js`
- Create: `operators/cube-operator/tests/e2e/workloads/refresh.js`

**Interfaces:**

- Consumes: Production manifests from Task 12.
- Produces: Machine-readable JSON report containing RTO, RPO, duplicate count, error windows, epochs, and data hashes.

- [ ] Test graceful Leader switch.
- [ ] Test `kill -9` and Pod deletion.
- [ ] Test Router-to-control-plane partition.
- [ ] Test Router-to-lease-store partition.
- [ ] Test Operator restart and loss of one Operator replica.
- [ ] Test MetaStore Pod restart and CSI volume reattachment.
- [ ] Test Redis restart during a mutation longer than the pending TTL.
- [ ] Test Worker death during import and compaction.
- [ ] Test Refresher death at every refresh state transition.
- [ ] Test rolling upgrade with mixed old/new binaries.
- [ ] Run continuous real-data reads and writes during every fault.
- [ ] Fail if epochs decrease, two valid holders overlap, committed rows duplicate, table/chunk metadata changes unexpectedly, or RPO is non-zero.
- [ ] Commit as `test: add production router ha failure matrix`.

**Acceptance:** At least 100 repeated failover cycles pass with RPO 0 for committed metadata, duplicate committed mutations 0, split-brain overlap 0, and an agreed measured RTO target.

---

### Task 14: Release Gate, Runbooks, and Production Sign-Off

**Files:**

- Modify: `operators/cube-operator/HA-PRODUCTION-CHECKLIST.md`
- Modify: `operators/cube-operator/HA-ROUTER-ARCHITECTURE.md`
- Modify: `operators/cube-operator/HA-ROUTER-K8S-DEMO.md`
- Create: `operators/cube-operator/HA-INCIDENT-RUNBOOK.md`
- Create: `operators/cube-operator/HA-BACKUP-RESTORE-RUNBOOK.md`
- Create: `operators/cube-operator/HA-RELEASE-SIGNOFF.md`

**Interfaces:**

- Consumes: JSON evidence from Task 13.
- Produces: A release decision with exact supported failure modes, RTO/RPO, rollback procedure, and residual risks.

- [ ] Replace all `SELECT 1` production claims with real-data evidence.
- [ ] Document Leader, MetaStore, Redis, Worker, Refresher, and object-storage failure procedures.
- [ ] Document emergency manual fencing before any forced promotion.
- [ ] Document backup restoration and prove it in a clean namespace.
- [ ] Record image digests, CRD version, schema migration version, and compatibility matrix.
- [ ] Require Storage, Platform, Cube API, and application owners to sign the acceptance matrix.
- [ ] Commit as `docs: add router ha production release gate`.

**Acceptance:** The sign-off document contains no unverified “production ready” claim and links every claim to a repeatable test result.

---

### Task 15: Optional True Hot MetaStore Standby with Raft

**Files:**

- Create: `rust/cubestore/cubestore/src/metastore/raft/`
- Modify: `rust/cubestore/cubestore/src/metastore/mod.rs`
- Modify: `rust/cubestore/cubestore/src/config/mod.rs`
- Create: `rust/cubestore/cubestore/tests/metastore_raft_failover.rs`

**Interfaces:**

- Consumes: Existing MetaStore mutation model.
- Produces: A three-node quorum where committed MetaStore writes survive loss of one node without CSI detach/attach delay.

- [ ] Define a deterministic replicated command format for every MetaStore mutation.
- [ ] Replicate commands through a maintained Raft implementation and apply them to local RocksDB only after quorum commit.
- [ ] Store membership and snapshots durably and support safe node replacement.
- [ ] Propagate the Raft term/index as the authoritative Router fencing epoch.
- [ ] Run Jepsen-style partition tests for stale reads, lost writes, duplicate apply, and dual leadership.
- [ ] Keep the single-writer RWO deployment available as a rollback mode.
- [ ] Commit in independently reviewable slices; do not combine the full Raft migration into one change.

**Acceptance:** Quorum tests demonstrate linearizable committed metadata under process death and network partitions, and Router failover no longer waits for a single RWO volume to reattach.

---

## Production Definition of Done

- [ ] Exactly one unexpired Router lease holder exists for every epoch.
- [ ] A stale Router cannot commit through HTTP, WebSocket, MySQL, upload, internal RPC, Scheduler, or background loops.
- [ ] Both Router candidates read the same authoritative MetaStore.
- [ ] Committed table, partition, chunk, Job, upload, and pre-aggregation metadata has RPO 0.
- [ ] Mutation and refresh retries do not produce duplicate committed results.
- [ ] Cube API, Operator, Refresher, Redis, MetaStore storage, Workers, and object storage have documented HA behavior.
- [ ] Rolling upgrade compatibility is proven with mixed-version tests.
- [ ] The complete failure matrix passes repeatedly and emits archived evidence.

