# Task 4 Report

## Status

Completed.

## Contract

Task 3 contract commit `0fac5454b4` is present. The temporary `LeaseRecord` and `LeaseStore` declarations were removed from `redis_store.go`; both stores use `internal/leadership/types.go` directly.

## Changes

Added atomic Redis and PostgreSQL lease stores. Redis uses Lua scripts with durable per-cluster epoch keys; PostgreSQL uses `SELECT ... FOR UPDATE`, `now()`, and a primary-keyed cluster row. The router controller now elects only through an external lease backend and fails closed on unavailable backends, stale tokens, and unknown valid holders.

The review fixes preserve PostgreSQL fencing epochs across release, require cluster/holder/epoch/token matches for renew and release, reject Redis hashes with missing or non-positive TTL as `ErrLeaseUnknown`, and keep Redis epoch counters durable across lease-key deletion. Tests exercise production Redis response/CAS parsing and fencing validation without fakes, plus integration coverage for stale release, stale renew, missing TTL, epoch monotonicity, and contention when the real backends are configured.

The final controller fencing fix does not rely on a pre-check alone. Pod role updates and Router status updates first persist the current cluster/epoch/token as object annotations through a resourceVersion JSON Patch CAS, then perform the downstream write with JSON Patch `test` predicates for resourceVersion and all fencing annotations. A marker with a higher epoch or conflicting same-epoch token cannot be downgraded. Controller behavior tests prove an old lease cannot update a Pod after the promotion marker is persisted while the current lease can. The external lease `Get` plus `ValidateLeaseFence` remains fail-closed validation before marker/write operations.

PostgreSQL expiry checks and writes use server `clock_timestamp()` rather than transaction-stable `now()`, and renewal of a missing row returns `ErrLeaseNotFound` without treating it as a successful renewal.

## Tests

Passed: `go test ./internal/leadership ./controllers`.

The production-path unit tests and controller behavior tests ran. Redis and PostgreSQL integration tests were skipped because `CUBESTORE_TEST_REDIS_URL` and `CUBESTORE_TEST_POSTGRES_DSN` are not configured; no real Redis/PostgreSQL CAS execution was performed.

## Required Follow-up

Run the same focused command with `CUBESTORE_TEST_REDIS_URL` and `CUBESTORE_TEST_POSTGRES_DSN` configured to execute the Redis and PostgreSQL integration contention, expiry, stale-token, outage, and clock-skew cases.
