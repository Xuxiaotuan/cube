# Task 6 Report: NEEDS_CONTEXT

## Status

`NEEDS_CONTEXT`. No Rust Router fencing code or tests were changed.

## Context mismatch

The implementation plan is:

`docs/superpowers/plans/2026-08-03-cubestore-router-production-ha-remediation.md`

The only `task-6-brief.md` found in the surrounding workspace is:

`../RoboLink/.superpowers/sdd/task-6-brief.md`

That brief describes a JavaScript map-editing workflow in `auto-ui`; it does not describe CubeStore Router leadership, lease epochs, tokens, or promotion markers. It cannot be used as the contract for this task.

## Rust Router findings

- `rust/cubestore/cubestore/src/leadership/` and `LeadershipGuard` do not exist.
- The existing `HttpServer::is_router_leader()` check reads `CUBESTORE_ROUTER_ROLE_FILE` as a role/status document and only interprets `activeLeader` or `role`. It does not validate the Task 5 file fields `holderId`, `epoch`, `tokenHash`, `issuedAt`, or `expiresAt`.
- The existing HTTP upload and command checks are boolean role checks. They do not acquire a permit or revalidate epoch/token immediately before a durable metadata commit.
- MySQL dispatches directly to `SqlService`; scheduler, remote upload queue, cleanup, and pre-aggregation/job orchestration have no shared Router leadership guard.
- The Go lease agent writes a hashed token to `leadership.json`, but no Rust-side contract identifies how that hash is bound to the authoritative promotion marker. The current role-state path contains a raw lease token in operator state, but it is not a safe Rust execution-path guard.

## Why implementation is blocked

Adding a parser-only helper would not satisfy Task 6: it would leave HTTP/WebSocket, MySQL, upload, queue, scheduler, job reconcile, snapshot upload, cleanup, and durable metadata commits without one shared permit and demotion revalidation. The missing brief and missing Rust binding for the promotion marker make it unsafe to choose those semantics unilaterally.

## Required context

Please provide the CubeStore-specific Task 6 brief and confirm:

1. The promotion marker's source and schema, and how it binds `holderId`, epoch, and the token hash.
2. Whether Rust should validate the token by continuity of `tokenHash` from the local file or by an authoritative RPC/backend read.
3. The Router identity source that must match `holderId`.
4. Whether read requests are allowed on a follower, or must return `STALE_LEADER` like direct follower MySQL traffic.

After that contract is available, Task 6 can safely add the shared guard, wire every listed path, and add old-token/old-epoch plus demotion-during-commit tests.
