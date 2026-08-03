# Task 10 Report: Job/upload/preaggregation recovery

## Status

`NEEDS_CONTEXT`

No production recovery code or recovery tests were added.

## Context check

The implementation plan was read from:

`docs/superpowers/plans/2026-08-03-cubestore-router-production-ha-remediation.md`

The requested `task-10-brief.md` could not be found under `/Users/xujiawei/magic` or in this repository. The missing brief is material because Task 10 depends on the leadership-epoch and authoritative MetaStore contracts from Tasks 6 and 7.

## Existing entrypoints reviewed

- `rust/cubestore/cubestore/src/metastore/job.rs` stores only `row_reference`, `job_type`, `last_heart_beat`, and `status`. It has no `owner_epoch`, `attempt_id`, or durable upload/pre-aggregation commit marker.
- `rust/cubestore/cubestore/src/cluster/ingestion/job_runner.rs` claims work through `start_processing_job(server_name, pool)`, heartbeats by job ID, and completes by unconditional `update_status(job_id, ...)` followed by deletion. There is no attempt token or epoch-conditional mutation; the file also contains a TODO to cancel work after ownership is lost.
- `rust/cubestore/cubestore/src/scheduler/mod.rs` removes timed-out jobs through `get_orphaned_jobs`, then marks them orphaned and deletes them. It does not distinguish an expired heartbeat from a fenced owner epoch or preserve an active attempt for takeover.
- `rust/cubestore/cubestore/src/http/mod.rs` receives a body into a local temporary file and calls `SqlService::upload_temp_file`. The existing `RemoteFs` path has temporary upload, size checking, and upload operations, but no verified, epoch/attempt-aware atomic publication and metadata commit primitive at this entrypoint.
- The MetaStore trait and implementation in `rust/cubestore/cubestore/src/metastore/mod.rs` expose the current unconditional job mutations. Adding the required durable fields and conditional transitions would require a concrete schema/mutation contract there, which is not specified by the available Task 10 context or the plan's listed file scope.

The repository's `leaderEpoch` state is currently exposed by the Operator/router status path; no Rust Job/MetaStore ownership API consuming that epoch was found in the reviewed CubeStore sources. No concrete pre-aggregation version-switch recovery entrypoint was found among the Task 10 files either.

## Why implementation was stopped

Implementing the requested behavior in only the four named source files would require inventing at least one of the following APIs or semantics:

1. A Rust-side leadership epoch/fencing source and validation contract.
2. Atomic MetaStore compare-and-set mutations for job assignment, heartbeat, takeover, and completion.
3. A durable upload commit-marker schema and an atomic remote-object publication primitive.
4. The authoritative pre-aggregation version-switch entrypoint and its recovery state machine.

Without those contracts, a change could allow a stale owner to complete after failover, delete a recoverable orphan, publish metadata for a missing object, or execute the same pre-aggregation twice. That would violate the Task 10 acceptance criteria, so no API was invented.

## Required context to continue

- The missing `task-10-brief.md` or an equivalent authoritative Task 10 contract.
- The exact Task 6 Rust API/file that supplies and fences the current leadership epoch.
- The Task 7 MetaStore mutation/schema contract, including migration/versioning requirements.
- The supported object-store checksum and atomic publish/rename semantics.
- The exact pre-aggregation version-switch entrypoint and its durable metadata model.
- The intended scope exception, if `rust/cubestore/cubestore/src/metastore/mod.rs` and any remote-object/pre-aggregation state modules must be changed.

## Verification

Tests were not run because the task is blocked before implementation and the required contracts are missing.
