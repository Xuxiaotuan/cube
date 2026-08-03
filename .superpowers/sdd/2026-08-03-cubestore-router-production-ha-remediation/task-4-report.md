# Task 4 Report

## Status

Completed.

## Contract

Task 3 contract commit `0fac5454b4` is present. The temporary `LeaseRecord` and `LeaseStore` declarations were removed from `redis_store.go`; both stores use `internal/leadership/types.go` directly.

## Changes

Added atomic Redis and PostgreSQL lease stores. Redis uses Lua scripts with durable per-cluster epoch keys; PostgreSQL uses `SELECT ... FOR UPDATE`, `now()`, and a primary-keyed cluster row. The router controller now elects only through an external lease backend and fails closed on unavailable backends, stale tokens, and unknown valid holders.

The review fixes preserve PostgreSQL fencing epochs across release, require cluster/holder/epoch/token matches for renew and release, reject Redis hashes with missing or non-positive TTL as `ErrLeaseUnknown`, and keep Redis epoch counters durable across lease-key deletion. Tests exercise production Redis response/CAS parsing and fencing validation without fakes, plus integration coverage for stale release, stale renew, missing TTL, epoch monotonicity, and contention when the real backends are configured.

The final controller fencing fix binds all downstream role/state writes to a fresh backend `Get` followed by `ValidateLeaseFence`: Pod role label updates, Router status updates, ConfigMap state writes, and remote state writes fail closed for missing, expired, stale-token, or stale-epoch leases. Controller behavior tests prove an old lease cannot update a Pod while the current lease can.

## Tests

Passed: `go test ./internal/leadership ./controllers`.

The production-path unit tests and controller behavior tests ran. Redis and PostgreSQL integration tests were skipped because `CUBESTORE_TEST_REDIS_URL` and `CUBESTORE_TEST_POSTGRES_DSN` are not configured; no real Redis/PostgreSQL CAS execution was performed.

## Required Follow-up

Run the same focused command with `CUBESTORE_TEST_REDIS_URL` and `CUBESTORE_TEST_POSTGRES_DSN` configured to execute the Redis and PostgreSQL integration contention, expiry, stale-token, outage, and clock-skew cases.
