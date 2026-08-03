# Task 1 Report

## Scope

Implemented only the Task 1 test and demo files:

- `operators/cube-operator/demo/k8s/data-consistency-check.sh`
- `operators/cube-operator/demo/k8s/cube-api-failover-check.sh`
- `operators/cube-operator/demo/cube-api/schema/cubes/RouterHaProbe.js`
- `operators/cube-operator/demo/k8s/fixtures/router-ha-data.sql`
- `operators/cube-operator/demo/k8s/real-data-failover-check.sh`

The existing untracked plan file was not modified.

## Implementation

- Replaced the historical `SELECT 1` consistency entry point with the real-data failover check.
- Added a unique schema/table fixture with 10,000 deterministic rows.
- Added checks for `system.tables`, `system.partitions`, `system.chunks`, table ID, row count, and a deterministic business-row SHA-256 hash.
- Made missing/zero `status.leaderEpoch` and non-increasing epochs hard failures.
- Required follower reads to fail and include `STALE_LEADER`.
- Changed the Cube API probe to query the fixture table's real row count and amount sum.
- Bound the Cube API probe to the reproducible fixed fixture `router_ha_probe.router_ha_data`.
- Added a MySQL readiness gate with retries, unique temporary client pod names, small configurable import batches, and a reproducible demo startup command in fatal output.
- Added explicit Cube API assertions for 10,000 rows and total amount 850085000 before and after leader deletion; the response hash remains auxiliary.

## Test

Command:

```bash
bash operators/cube-operator/demo/k8s/real-data-failover-check.sh
```

Static checks:

```bash
bash -n operators/cube-operator/demo/k8s/data-consistency-check.sh operators/cube-operator/demo/k8s/cube-api-failover-check.sh operators/cube-operator/demo/k8s/real-data-failover-check.sh
node --check operators/cube-operator/demo/cube-api/schema/cubes/RouterHaProbe.js
```

Result: both passed.

E2E command:

```bash
bash operators/cube-operator/demo/k8s/real-data-failover-check.sh
```

The first run failed before assertions because fixture stdin reached the MySQL endpoint as EOF. The fixture path was changed to health-checked, `-e` based small-batch import. A subsequent run passed the health check and created the fixed fixture table, but was stopped after more than 90 seconds without reaching the data snapshot or failover assertions. The current environment creates one temporary Kubernetes MySQL client pod per batch and did not complete the 10,000-row import within the available run window.

Observed failure after the fixture table was created and real data import began:

```
ERROR 2013 (HY000) at line 1: Lost connection to MySQL server during query
pod cube-operator-demo/cubemysql terminated (Error)
```

The old `SELECT 1` false-positive path is removed. The remaining E2E result is an environment/runtime block: the health check succeeds, but the temporary-client import is too slow to reach the failover assertions. The script now emits a fatal, reproducible startup command if MySQL health cannot be established.

## Risk

The current deployment did not reach leader deletion in this run, so post-promotion table-ID/chunk/row-count/business-hash and Cube API row/aggregate preservation remain unobserved until the import completes.

## P0-C Remediation

- Reworked `cube-api-failover-check.sh` to create one long-lived MySQL client Pod and reuse it for schema/table setup, fixture import, readiness assertions, and all pre-/post-failover MySQL queries. Individual SQL calls now use `kubectl exec`; they no longer create or remove a Pod.
- Added configurable `MYSQL_CLIENT_STARTUP_TIMEOUT_SECONDS` for Pod readiness and `MYSQL_QUERY_TIMEOUT_SECONDS` for both the Kubernetes exec request and MySQL client connection. Cube API and rollout timeouts remain configurable through their existing variables.
- Kept `set -euo pipefail`, required assertion failures, and the original exit code through the cleanup trap. Pod cleanup is best-effort only after the test result is captured.
- Used separate MySQL option arguments (`--protocol TCP`, `--connect-timeout <seconds>`) for compatibility with MySQL client option parsers.

Static check:

```bash
bash -n operators/cube-operator/demo/k8s/cube-api-failover-check.sh
```

The Kubernetes E2E was not rerun in this remediation; the report does not claim a failover pass without runtime evidence.
