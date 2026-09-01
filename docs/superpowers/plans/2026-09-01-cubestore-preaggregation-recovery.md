# CubeStore Pre-Aggregation Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make upload, pre-aggregation mutation, Worker Job, and Refresher takeover recoverable across Router failover without Redis-backed idempotency or duplicate committed data.

**Architecture:** MetaStore owns a durable mutation ledger and fenced Job attempts. Uploads use deterministic object-store staging keys; Router and Driver reconcile UNKNOWN outcomes against MetaStore; Refresher HA uses Kubernetes Lease but workload recovery remains in MetaStore.

**Tech Stack:** Rust CubeStore MetaStore/RocksStore, RemoteFs object storage, Router HTTP/SQL, TypeScript CubeStoreDriver and Query Orchestrator, Kubernetes Lease, integration fault injection.

**Spec:** `docs/superpowers/specs/2026-09-01-cube-operator-kubernetes-native-cluster-design.md`

## Global Constraints

- Do not create Kubernetes CRs for individual mutations, upload parts, Jobs, or pre-aggregation builds.
- Do not retry UNKNOWN mutations until MetaStore reconciliation returns an authoritative outcome.
- A committed mutation is immutable and replay returns its recorded result.
- Job completion requires exact attempt, owner epoch, and lease generation.
- Existing committed pre-aggregation versions remain visible until replacement commit succeeds.
- Durable upload bytes live in object storage; MetaStore stores checksums and references.
- Commit steps require explicit user authorization before execution.

---

### Task 1: Add the MetaStore mutation ledger

**Files:**
- Create: `rust/cubestore/cubestore/src/metastore/mutation.rs`
- Modify: `rust/cubestore/cubestore/src/metastore/mod.rs`
- Modify: `rust/cubestore/cubestore/src/metastore/rocks_store.rs`
- Test: `rust/cubestore/cubestore/src/metastore/mutation.rs`

**Interfaces:**
- Produces: `Mutation`, `MutationState`, `MutationKind`, `MutationAcquireResult` and MetaStore mutation methods.

- [ ] **Step 1: Write state-machine tests**

```rust
#[tokio::test]
async fn mutation_replay_returns_committed_result() {
    let store = test_metastore().await;
    let lease = store.acquire_mutation("m1", "fp1", MutationKind::PreAggregation, "table").await.unwrap();
    store.complete_mutation(&lease, "table:version").await.unwrap();
    let replay = store.acquire_mutation("m1", "fp1", MutationKind::PreAggregation, "table").await.unwrap();
    assert!(matches!(replay, MutationAcquireResult::Committed { result_ref } if result_ref == "table:version"));
}
```

Add tests for fingerprint conflict, stale owner, UNKNOWN, heartbeat expiry, terminal failure, and idempotent completion.

- [ ] **Step 2: Run focused test and confirm failure**

Run: `cd rust/cubestore && cargo test -p cubestore metastore::mutation --lib`

Expected: FAIL because the mutation module and trait methods do not exist.

- [ ] **Step 3: Implement the durable model and trait**

```rust
pub enum MutationState { Pending, Running, Committed, Failed, Unknown }
pub enum MutationKind { Upload, CreateTable, PreAggregation, Compaction }

#[async_trait]
pub trait MutationStore: Send + Sync {
    async fn acquire_mutation(&self, mutation_id: &str, fingerprint: &str, kind: MutationKind, target: &str) -> Result<MutationAcquireResult, CubeError>;
    async fn heartbeat_mutation(&self, lease: &MutationLease) -> Result<(), CubeError>;
    async fn complete_mutation(&self, lease: &MutationLease, result_ref: &str) -> Result<(), CubeError>;
    async fn fail_mutation(&self, lease: &MutationLease, error: MutationError) -> Result<(), CubeError>;
    async fn reconcile_mutation(&self, mutation_id: &str) -> Result<MutationOutcome, CubeError>;
}
```

Persist mutation records through the same MetaStore atomic write path used by table, partition, chunk, and Job metadata.

- [ ] **Step 4: Run MetaStore tests**

Run: `cd rust/cubestore && cargo test -p cubestore metastore::mutation --lib`

Expected: PASS.

- [ ] **Step 5: Commit after authorization**

```bash
git add rust/cubestore/cubestore/src/metastore
git commit -m "feat(cubestore): persist mutation outcomes in metastore"
```

### Task 2: Expose mutation acquisition and reconciliation to Router clients

**Files:**
- Modify: `rust/cubestore/cubestore/src/http/mod.rs`
- Modify: `rust/cubestore/cubestore/src/sql/mod.rs`
- Modify: `rust/cubestore/cubestore/src/cluster/mod.rs`
- Modify: `packages/cubejs-cubestore-driver/src/CubeStoreDriver.ts`
- Delete after replacement: `packages/cubejs-cubestore-driver/src/IdempotencyStore.ts`
- Test: `packages/cubejs-cubestore-driver/test/IdempotencyStore.test.ts`

**Interfaces:**
- Consumes: `mutationId`, fingerprint, Router fencing identity, and MetaStore `MutationStore`.
- Produces: begin, heartbeat, complete, fail, and reconcile protocol operations.

- [ ] **Step 1: Write Driver protocol tests**

```ts
it('reconciles UNKNOWN before executing again', async () => {
  transport.beginMutation.mockResolvedValueOnce({ state: 'UNKNOWN' });
  transport.reconcileMutation.mockResolvedValueOnce({ state: 'COMMITTED', resultRef: 'inline:[]' });
  await expect(driver.query('CREATE TABLE x (id int)', [], { mutationId: 'm1' })).resolves.toEqual([]);
  expect(transport.executeQuery).not.toHaveBeenCalled();
});
```

- [ ] **Step 2: Write Router stale-fence tests**

Assert begin and completion fail with `STALE_LEADER` when generation, epoch, holder UID, or token hash differs from the active local Lease.

- [ ] **Step 3: Run tests and confirm failure**

Run:

```bash
cd rust/cubestore && cargo test -p cubestore http::tests --lib
yarn workspace @cubejs-backend/cubestore-driver test IdempotencyStore.test.ts
```

Expected: new MetaStore protocol tests FAIL.

- [ ] **Step 4: Implement protocol and replace Redis idempotency**

Driver computes the existing canonical SHA-256 fingerprint, begins the mutation through Router, executes only when ownership is granted, heartbeats while running, and records terminal outcome through MetaStore. `UNKNOWN` invokes reconcile and never directly executes.

- [ ] **Step 5: Run Rust and Driver tests**

Run the Step 3 commands plus `yarn workspace @cubejs-backend/cubestore-driver lint`.

Expected: PASS with no Redis idempotency client.

- [ ] **Step 6: Commit after authorization**

```bash
git add rust/cubestore/cubestore/src packages/cubejs-cubestore-driver
git commit -m "feat(cubestore): reconcile mutations through metastore"
```

### Task 3: Make uploads resumable through object-store staging

**Files:**
- Create: `rust/cubestore/cubestore/src/import/staging_upload.rs`
- Modify: `rust/cubestore/cubestore/src/import/mod.rs`
- Modify: `rust/cubestore/cubestore/src/http/mod.rs`
- Modify: `rust/cubestore/cubestore/src/remotefs/mod.rs`
- Modify: `packages/cubejs-cubestore-driver/src/CubeStoreDriver.ts`

**Interfaces:**
- Produces deterministic key `staging/<clusterUID>/<mutationId>/<partNumber>`.
- Produces upload methods `put_part`, `list_parts`, `commit_parts`, and `abort_stale_upload`.

- [ ] **Step 1: Write upload idempotency tests**

```rust
#[tokio::test]
async fn identical_part_retry_is_idempotent_and_mismatch_is_rejected() {
    let upload = test_upload("mutation-1").await;
    upload.put_part(0, b"abc").await.unwrap();
    upload.put_part(0, b"abc").await.unwrap();
    assert!(upload.put_part(0, b"different").await.is_err());
}
```

Add tests for missing part, checksum mismatch, Router restart, committed reference protection, and orphan cleanup.

- [ ] **Step 2: Run and confirm failure**

Run: `cd rust/cubestore && cargo test -p cubestore staging_upload --lib`

Expected: FAIL because staging upload does not exist.

- [ ] **Step 3: Implement staged object writes and metadata checksums**

Use RemoteFs for bytes and MetaStore mutation records for part number, size, SHA-256 checksum, and commit state. Never expose `temp://` local paths as durable recovery references.

- [ ] **Step 4: Update Driver streaming upload**

Generate one mutation ID per table upload, deterministic part numbers, and checksum headers. On reconnect, list accepted parts and send only missing parts before CREATE/import reconciliation.

- [ ] **Step 5: Run focused and Driver tests**

Run:

```bash
cd rust/cubestore && cargo test -p cubestore staging_upload --lib
yarn workspace @cubejs-backend/cubestore-driver test
```

Expected: PASS.

- [ ] **Step 6: Commit after authorization**

```bash
git add rust/cubestore/cubestore/src/import rust/cubestore/cubestore/src/http/mod.rs rust/cubestore/cubestore/src/remotefs packages/cubejs-cubestore-driver
git commit -m "feat(cubestore): resume uploads from object storage"
```

### Task 4: Fence Worker Job assignment, heartbeat, and completion

**Files:**
- Modify: `rust/cubestore/cubestore/src/metastore/job.rs`
- Modify: `rust/cubestore/cubestore/src/cluster/ingestion/job_processor.rs`
- Modify: `rust/cubestore/cubestore/src/cluster/ingestion/job_runner.rs`
- Modify: `rust/cubestore/cubestore/src/scheduler/mod.rs`
- Test: `rust/cubestore/cubestore/src/metastore/job.rs`

**Interfaces:**
- Produces: `JobAttempt { job_id, attempt, owner_id, owner_epoch, lease_generation }`.
- Completion requires the exact accepted `JobAttempt`.

- [ ] **Step 1: Write stale completion and reassignment tests**

```rust
#[tokio::test]
async fn stale_job_attempt_cannot_commit() {
    let store = test_metastore().await;
    let first = store.assign_job(job_id, "worker-a", 7, "generation-a").await.unwrap();
    let second = store.reassign_job(job_id, "worker-b", 8, "generation-a").await.unwrap();
    assert!(store.complete_job(&first, result()).await.is_err());
    store.complete_job(&second, result()).await.unwrap();
}
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd rust/cubestore && cargo test -p cubestore metastore::job --lib`

Expected: new fencing tests FAIL.

- [ ] **Step 3: Implement fenced Job attempts**

Persist attempt and ownership with Job state. Validate exact attempt on heartbeat and completion. Reassignment increments attempt. An accepted duplicate completion returns the prior result without writing duplicate chunks.

- [ ] **Step 4: Wire Scheduler and Worker services**

Carry `JobAttempt` through assignment, Worker execution, heartbeat, and completion messages. Convert stale completion to an explicit non-retryable stale-owner error.

- [ ] **Step 5: Run ingestion and MetaStore tests**

Run:

```bash
cd rust/cubestore
cargo test -p cubestore metastore::job --lib
cargo test -p cubestore cluster::ingestion --lib
```

Expected: PASS.

- [ ] **Step 6: Commit after authorization**

```bash
git add rust/cubestore/cubestore/src/metastore/job.rs rust/cubestore/cubestore/src/cluster/ingestion rust/cubestore/cubestore/src/scheduler
git commit -m "feat(cubestore): fence worker job attempts"
```

### Task 5: Add Refresher Lease ownership and mutation-first takeover

**Files:**
- Modify: `packages/cubejs-server-core/src/core/RefreshScheduler.ts`
- Modify: `packages/cubejs-server-core/src/core/OptsHandler.ts`
- Modify: `packages/cubejs-server-core/src/core/types.ts`
- Modify: `packages/cubejs-query-orchestrator/src/orchestrator/PreAggregationLoader.ts`
- Modify: `packages/cubejs-query-orchestrator/src/orchestrator/PreAggregations.ts`
- Modify: `operators/cube-operator/controllers/cubecluster_controller.go`
- Test: `packages/cubejs-server-core/test/unit/RefreshScheduler.test.ts`

**Interfaces:**
- Consumes: `CUBEJS_REFRESHER_LEASE_NAME`, namespace, Pod UID, and MetaStore mutation reconciliation.
- Produces: one active scheduled-refresh owner and deterministic build mutation IDs.

- [ ] **Step 1: Write active/standby Scheduler tests**

```ts
it('does not schedule until leadership and mutation reconciliation complete', async () => {
  lease.isHolder.mockReturnValue(true);
  mutationClient.reconcileActive.mockResolvedValue(undefined);
  await scheduler.tick();
  expect(mutationClient.reconcileActive).toHaveBeenCalledBefore(refreshRunner.runScheduledRefresh);
});
```

Add tests for standby no-op, lease loss cancellation, deterministic build ID, and previous committed version retention.

- [ ] **Step 2: Run and confirm failure**

Run: `yarn workspace @cubejs-backend/server-core test RefreshScheduler.test.ts`

Expected: new Lease ownership tests FAIL.

- [ ] **Step 3: Implement Refresher leadership gate**

Add a small Kubernetes Lease client to the dedicated Refresher process. The active holder reconciles MetaStore mutations before scheduling. Lease loss stops new scheduling and cancels only tasks that have not entered authoritative execution.

- [ ] **Step 4: Derive deterministic pre-aggregation build IDs**

Hash pre-aggregation ID, partition, structure version, content version, refresh key values, and build range. Pass the result as `mutationId` through `uploadTableWithIndexes` and CREATE/import operations.

- [ ] **Step 5: Run Server Core and Query Orchestrator tests**

Run:

```bash
yarn workspace @cubejs-backend/server-core test RefreshScheduler.test.ts
yarn workspace @cubejs-backend/query-orchestrator test
```

Expected: PASS.

- [ ] **Step 6: Commit after authorization**

```bash
git add packages/cubejs-server-core packages/cubejs-query-orchestrator operators/cube-operator/controllers/cubecluster_controller.go
git commit -m "feat(cube): add refresher lease takeover"
```

### Task 6: Run pre-aggregation failure injection E2E

**Files:**
- Create: `operators/cube-operator/demo/k8s/preaggregation-failover-check.sh`
- Create: `operators/cube-operator/demo/k8s/fixtures/preaggregation-source.sql`
- Create: `operators/cube-operator/demo/k8s/preaggregation-failure-result.json`
- Modify: `operators/cube-operator/HA-FAILURE-MATRIX.md`

**Interfaces:**
- Produces machine-readable evidence with `duplicateCommitted`, `duplicateChunks`, `unknownUnresolved`, `holdersOverlap`, RPO, and RTO.

- [ ] **Step 1: Implement phase-controlled fault hooks in the E2E runner**

The script executes these exact phases independently:

```text
mid-upload
post-upload-pre-create
post-create-pre-response
job-assigned
job-completion
metadata-commit
preaggregation-version-switch
refresher-leader-loss
```

- [ ] **Step 2: Run once and record expected pre-fix failures**

Run: `bash operators/cube-operator/demo/k8s/preaggregation-failover-check.sh --iterations=1`

Expected before M3 integration: at least one phase reports unresolved or duplicate risk and exits non-zero.

- [ ] **Step 3: Run the integrated failure matrix**

Run: `bash operators/cube-operator/demo/k8s/preaggregation-failover-check.sh --iterations=100`

Required result:

```json
{"iterations":100,"duplicateCommitted":0,"duplicateChunks":0,"unknownUnresolved":0,"holdersOverlap":0,"rpo":0,"passed":true}
```

- [ ] **Step 4: Commit after authorization**

```bash
git add operators/cube-operator/demo/k8s/preaggregation-failover-check.sh operators/cube-operator/demo/k8s/fixtures/preaggregation-source.sql operators/cube-operator/demo/k8s/preaggregation-failure-result.json operators/cube-operator/HA-FAILURE-MATRIX.md
git commit -m "test(cubestore): prove pre-aggregation failover recovery"
```
