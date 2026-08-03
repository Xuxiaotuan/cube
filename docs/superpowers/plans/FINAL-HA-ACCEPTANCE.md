# CubeStore Router HA Final Acceptance

Date: 2026-08-03
Branch reviewed: `codex/ha-router-experiment`
Scope: read-only acceptance summary for the production HA remediation plan, Tasks 1-15.

## Executive conclusion

**Production release: FAIL / NO-GO.**

The branch contains substantial prototype and hardening commits, but the available evidence does not establish the production hard gates. In particular, real Redis/PostgreSQL CAS behavior, destructive Cube API cutover E2E, durable storage recovery, complete controller fencing, Driver reconciliation, and production Kubernetes storage/Secret behavior remain unproven or explicitly failing. Task 15 now has an explicit design report and ADR, but no Raft implementation or failover evidence; hot-standby remains undelivered.

`PASS` below means the implementation direction or task artifact is present in the reviewed branch. It does not mean that the production acceptance criteria are proven unless stated explicitly. `NEEDS_CONTEXT` means the task has no complete acceptance report in the reviewed material, or its evidence is only local/static and cannot be promoted to a production claim.

## Task 1-15 status

| Task | Status | Current evidence and commits |
|---|---|---|
| 1. Real-data HA fixtures | **NEEDS_CONTEXT** | `5731b587f0` adds real-data router failover coverage and `47161031f4` documents the failure matrix. The available material does not prove a successful production-shaped shared-MetaStore cutover with preserved table IDs, chunks, row count, and business hash. |
| 2. Rust protocol compatibility | **PASS** | `14e1bbfe8e` and `76d...` preserve CubeStore network protocol compatibility. No contrary report was found, but mixed-version production evidence is not included in the acceptance record. |
| 3. Leadership/fencing contract | **PASS** | `0fac5454b4`, `a8bc67dda6`, and `bf9f2849b4` define and harden the router leadership contract. API-level acceptance and all invalid-CR tests are not independently reported here. |
| 4. External Redis/PostgreSQL lease store | **FAIL** | The local reports `task-4-review-final3.md` and `task-4-review-final4.md` are untracked. No committed, live Redis and PostgreSQL CAS/concurrency evidence is available. Therefore atomic ownership transfer, stale-token rejection, backend outage behavior, and epoch uniqueness are not accepted. |
| 5. Lease-agent sidecar | **NEEDS_CONTEXT** | `c9a0b065c8` adds the lease-agent sidecar. No committed acceptance report proves outage expiry, atomic file replacement, wrong-holder rejection, or fail-closed behavior within the configured duration. |
| 6. Router execution-path fencing | **FAIL** | `ab21764e90`, `c204e94913`, and related hardening commits fence selected controller/router paths, but the required all-entry-point and demotion-during-commit evidence is absent. The controller fencing report is recorded as needing context in `32c5a60105`. |
| 7. Authoritative durable MetaStore | **FAIL** | `0293390f5d`, `03a909c0dd`, `6b41ff5aaf`, and `652736cd29` add/wire the MetaStore and worker state. `task-7-review-final3.md` explicitly identifies worker identity, lease Secret, image rendering, CSI/reclaim, restart, node replacement, and backup/restore gaps. Static wiring is not production durability evidence. |
| 8. Two-phase router promotion | **NEEDS_CONTEXT** | The branch records an explicit blocker in `1d1f5c97e8`; no complete committed report proves `FencingOld -> LeaseGranted -> WaitingForAck -> Routing -> Active`, exactly one ready endpoint, rollback on timeout, and no routing to an unacknowledged candidate. |
| 9. Driver idempotency/reconcile | **FAIL** | `7e5a51290c` and `9e99b7e541` implement/harden the state machine, but both untracked reports `task-9-review.md` and `task-9-review-final.md` identify failures: Redis/Lua integration is skipped, lease loss can leave a replayable mutation, `UNKNOWN` reconciliation has no enforceable authority boundary, and large result references are not replayable. |
| 10. Job/upload/pre-aggregation recovery | **NEEDS_CONTEXT** | `2199d41d7e` records Task 10 as needing context. No complete evidence proves takeover fencing, unique attempt completion, atomic object publication, and all required failover points. |
| 11. Refresher election/idempotency | **NEEDS_CONTEXT** | No Task 11 implementation/acceptance report or corresponding commit is present in the reviewed branch summary. Refresher leader election and refresh recovery therefore remain unaccepted. |
| 12. Kubernetes production hardening | **FAIL** | The reviewed Task 7/manifest report finds missing lease-store Secret provisioning, Router lease-agent image overwrite, non-interpolated Worker identity, independently mutable/non-immutable images, and no live CSI detach/reattach or backup/restore proof. RBAC shape is only static/demo-level evidence. |
| 13. Production HA failure matrix | **FAIL** | `47161031f4` documents the failure matrix, but the required machine-readable evidence, repeated destructive failovers, RPO 0, zero duplicate commits, zero split-brain overlap, and measured RTO are not present. Task 14's report requires this evidence before release sign-off. |
| 14. Release gate/runbooks/sign-off | **FAIL** | `f8577c2ddf` adds the release-gate report. That report explicitly states that local `SELECT 1`, a single probe query, dry-run, compilation, and file presence do not satisfy the production gate. No production sign-off is justified. |
| 15. Optional Raft hot MetaStore | **FAIL** | `docs/superpowers/plans/task-15-report.md` and `docs/superpowers/plans/ADR-015-raft-hot-standby-metastore.md` now define the design and execution gates, but `metastore/raft/`, quorum replication, Raft snapshots, term/index fencing, migration, rollback, and partition/failover evidence are absent. It remains optional for the first safe single-writer release, but blocks any hot-standby or zero-CSI-delay claim. |

## Production hard-gate check

| Hard gate | Result | Acceptance conclusion |
|---|---|---|
| Real Redis/PG CAS | **FAIL** | No live integration evidence is in a tracked branch artifact. Unit/mock behavior cannot prove Lua atomicity, SQL row locking, expiry races, stale-token rejection, or backend outage fail-closed behavior. |
| Cube API cutover E2E | **FAIL** | The real-data/failure-matrix work exists, but the required production-shaped destructive cutover and post-promotion metadata/business-row equivalence are not proven. |
| Router/Worker/MetaStore persistence | **FAIL** | A single MetaStore endpoint and PVC declarations are present statically. Worker identity is broken in the reviewed report, and CSI restart, node replacement, reclaim, snapshot/restore, and table/Job/chunk/data-hash survival are unproven. |
| Controller fencing integration | **FAIL** | Some fencing paths are committed, but the branch also records a fencing blocker and lacks evidence that every controller/router path uses the same authoritative epoch guard. |
| Driver idempotency/reconcile | **FAIL** | The independent final review identifies replay risk after lease/Redis loss, non-authoritative public reconciliation, incomplete large-result replay, and skipped real Redis integration. |
| K8s RBAC/Secret/CSI | **FAIL** | Static Worker RBAC is narrow, but the required lease Secret is not self-contained, normal rendering can overwrite the lease-agent image, Worker identity is unreliable, and CSI durability/recovery is not demonstrated. |
| Refresher/Job recovery | **NEEDS_CONTEXT** | Task 10 is explicitly blocked on context and Task 11 lacks an acceptance report. No release claim can be made. |
| Raft feasibility | **FAIL** | Feasibility and an executable ADR are now recorded, but the implementation, maintained dependency choice, three-node quorum evidence, Router fencing integration, migration rehearsal, rollback rehearsal, and partition tests are not delivered. Design documentation is not production acceptance. |

## Branch and report provenance

- The worktree is on `codex/ha-router-experiment`, ahead of `origin/codex/ha-router-experiment` by 25 commits.
- The following reports exist in the repository worktree but are **untracked** and therefore are not part of the branch commit history: `task-4-review-final3.md`, `task-4-review-final4.md`, `task-7-review-final3.md`, `task-9-review-final.md`, and `task-9-review.md`.
- The main remediation plan `docs/superpowers/plans/2026-08-03-cubestore-router-production-ha-remediation.md` is also untracked.
- No report was found in the reviewed repository listing for Tasks 1, 2, 3, 5, 6, 8, 10, 11, 12, or 13 beyond the commits and the referenced Task 14/review material. Those tasks are not promoted to production PASS solely from commit titles.
- Task 15 now has a tracked report and ADR, but both are design/evidence-boundary documents; they do not claim that the Raft implementation exists.
- Because the report files above are untracked, their findings are useful for this read-only audit but are not yet reproducible from a clean checkout of the branch. No additional repository-external report was incorporated into this document.

## Required disposition

Do not approve production rollout. First close all `FAIL` gates, add the missing reports to the intended branch, and rerun the acceptance matrix with real Redis/PG, real Kubernetes CSI/Secret dependencies, destructive Cube API failover, and machine-readable repeated-failover evidence. Task 15 may remain outside the first safe single-writer release scope, but no Raft/hot-standby or zero-CSI-delay claim is permitted until its report gates pass. No long-running tests were run for this acceptance.
