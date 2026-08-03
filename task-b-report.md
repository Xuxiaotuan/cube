# Task B: Router fencing - NEEDS_CONTEXT

## Status

`NEEDS_CONTEXT`. No incomplete Rust fencing helper or partial entry-point guard
was added.

## Real CubeStore Router entry points found

- `rust/cubestore/cubestore/src/http/mod.rs::HttpServer::process_command`
  handles WebSocket SQL, including writes, DDL, queue, and cache commands.
- `rust/cubestore/cubestore/src/http/mod.rs::HttpServer::handle_upload`
  handles HTTP temporary-file uploads.
- `rust/cubestore/cubestore/src/sql/mod.rs::SqlServiceImpl::exec_query_with_context`
  contains the durable SQL mutations: table/index/schema changes, INSERT,
  queue/cache commands, and pre-aggregation table creation.
- Remote upload/delete work is executed asynchronously by
  `rust/cubestore/cubestore/src/remotefs/queue.rs`.
- Background job and pre-aggregation execution is started through the
  Scheduler and ingestion JobRunner paths, not through the HTTP handler.

## Blocking contract gaps

The lease-agent data-plane file is deliberately limited to:

`holderId`, `epoch`, `tokenHash`, `issuedAt`, and `expiresAt`.

It contains no cluster identity, raw token, or promotion marker. The current
operator role-state record contains the raw lease token, but it is not mounted
as the Router's authoritative data-plane input and has no Rust-side binding to
the lease-agent file. The operator's promotion acknowledgement additionally
requires `isLeader`, the exact `leaderEpoch` and `leaseEpoch`,
`leaseTokenHash`, and `metaStoreReady`; the Rust status endpoint does not yet
provide that complete contract.

Consequently Rust cannot safely prove all of the requested conditions:

1. Redis's current epoch/token is still the record represented by the local
   file, rather than a stale file or an unrelated promotion marker.
2. A promotion marker authorizes this exact Router holder and token hash.
3. A permit remains valid immediately before durable metadata commits inside
   `SqlServiceImpl`, queue workers, Scheduler, JobRunner, and pre-aggregation
   publication.

Adding a parser or checking `is_router_leader()` at HTTP entry would leave
those durable paths unfenced and would not satisfy fail-closed behavior for a
stale token or an unavailable Redis backend.

## Required context to implement safely

Please provide or confirm:

1. The authoritative promotion-marker source, path/API, schema, and the exact
   binding between holder ID, epoch, token hash, and MetaStore readiness.
2. Whether the Router should read that marker directly, or whether
   lease-agent must publish a combined atomically replaced file.
3. The required Router identity and cluster identity fields for Rust to match.
4. The permit/revalidation boundary for SQL metadata commit, remote upload
   publication, queue execution, Scheduler/JobRunner assignment and
   completion, and pre-aggregation version switch.

The user-provided read policy is recorded: read requests may continue on a
follower, while all mutation paths must fail closed.

## Focused Rust tests

Not run: the requested fencing behavior has no safe implementation contract in
the current Rust sources, so running existing HTTP tests would not validate
Task B and could falsely suggest coverage.
