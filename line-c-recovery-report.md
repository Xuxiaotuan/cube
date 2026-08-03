# Line C Recovery Report: NEEDS_CONTEXT

Date: 2026-08-03

## Scope

This change is limited to the real `CubestoreRouterReconciler.Reconcile` path and the existing Router status/role-state marker paths. It does not claim recovery for runtime entry points that are absent from this repository snapshot.

## Implemented safely

| Area | Result | Evidence |
| --- | --- | --- |
| Promotion marker CAS | PARTIAL | Existing ConfigMap/Redis/Postgres marker CAS is now called from the real `Reconcile` loop. Invalid or missing lease state fails closed. |
| Old epoch/token rejection | PASS for controller writes | Existing lease-store validation and resource-version/lease annotation tests remain the write fence; promotion acknowledgement additionally requires exact epoch and token hash. |
| Router promotion | BLOCKED | Router `/router/status` currently exposes role/activeLeader only. It does not expose the exact `isLeader`, `leaderEpoch`, `leaseEpoch`, `leaseTokenHash`, and `metaStoreReady` acknowledgement required before routing. The controller therefore keeps `status.leader` empty and reports `PromotionReady=False, Reason=NeedsContext`. |
| Orphan Job scan | BLOCKED | Rust has a scheduler `remove_orphaned_jobs` entry, but no shared Router leadership guard or durable owner epoch/attempt contract protects Job assignment/completion during failover. |
| Upload/pre-aggregation mutation reconcile | NEEDS_CONTEXT | The CubeStore driver has a Redis mutation state machine, but no authoritative Job/upload/pre-aggregation metadata reconciliation entry is wired for `UNKNOWN`; this status is not reported as recovered. |
| Refresher active/standby lease | NEEDS_CONTEXT | The refresh timer exists in Cube API, but no Refresher CRD/controller or durable external refresh ownership entry exists. |

## Contract added

`CubestoreRouter.status.recovery` now reports `promotion`, `jobRecovery`, `mutationReconcile`, and `refresher` gates. Missing runtime contracts are explicitly `Blocked` or `NeedsContext`; no success state is synthesized from CR status or ConfigMap state.

## Tests added

- Exact promotion acknowledgement accepts only matching epoch, token hash, and MetaStore readiness.
- Old epoch, old token, missing contract, and missing MetaStore acknowledgement are rejected.
- Missing Job, mutation reconciliation, and Refresher runtime entries remain `Blocked/NeedsContext`.

Tests were not run in this scoped change.
