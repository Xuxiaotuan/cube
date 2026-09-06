# 2026-09-07 Router HA verification evidence

Actual local OrbStack evidence, not simulated pass reports. Event timestamps are
UTC; the report date uses Asia/Shanghai (UTC+8).

## Accepted cases

| File in `passed/` | Scenario |
|---|---|
| `r14bc37b4bfa7e982-upload-failover.json` | Rows export, partial upload and Router failover |
| `rf07dd05657f38faa-receipt-loss.json` | Durable remote upload, lost response, no leader change |
| `r881fce58f7d219a2-receipt-failover.json` | Durable upload, lost response and Router failover |
| `ra552a74642ac663f-drain-failover.json` | Matching old-connection rejection frame, successful drain and failover |
| `r2eaadaa775662174-upload-failover.json` | File-stream export and Router failover, frozen-script rerun |
| `rf8f47788f4d404f8-refresher-failover.json` | Independent scheduled builder, failover, separate API consumption |

Each JSON contains controller exit status and original events. `checks_passed`
contains exact build identity, manifest, ready/tableId, origin receipt, data hashes
and runtime SQL provenance. The scheduled case also proves the Refresher Pod and
actual build queue. External SELECT may legitimately use skip-queue; this is
explicitly recorded, not described as acquiring a shared query queue lock.

[Summary](acceptance-summary.json) is generated from these six cases.
[Independent SQL reconciliation](runtime/data-reconciliation.json) compares the
last source and rollup totals. All test data is synthetic.

## Runtime and diagnostics

- `runtime/pods.json`: reduced role/image/readiness/restart snapshot, without environment credentials.
- `runtime/cubecluster.json`: actual generations, readiness and remaining ProductionReady=False gate.
- `runtime/cubestorerouter.json` and `router-endpointslices.json`: actual epoch, promotion and routed endpoint.
- `runtime/*.log`: captured API, Refresher, Operator and controller run logs.
- `native-tests/`: preserved targeted Rust logs; repeated filters are not counted twice.
- `diagnostics/`: failed attempts, excluded from accepted cases.
- `diagnostics/mixed-script-stream-not-accepted.log`: business checks passed, but concurrent editing of the running shell caused a parsing error. NOT clean acceptance; replaced by the frozen-script rerun.
- `diagnostics/refresher-preflight-not-ready.log`: preflight found a candidate count other than one; no fault run began. No historical candidate snapshot exists to identify which role/count caused it.

The first multi-mode controller log contains three successes and an earlier drain
failure. Drain was repaired and separately rerun; the original combined invocation
must not be relabeled as wholly successful.

Some Refresher logs show errors from a failed demo context after its fault proxy
closed, and idle scheduler messages when no demo context is registered. Logs are
retained rather than filtered into a false all-green report. Identify the accepted
run by exact run ID. Registry expiry does not cancel every already-enqueued job.

## Scope

Six scenarios passed in batches on one ARM64 Kubernetes node. This establishes
Router-pod cutover and scoped file-import pre-aggregation recovery, not node/disk/AZ
disaster recovery, all-tenant scheduler ownership, arbitrary mutation exactly-once,
automatic terminal-ledger GC or long-duration load SLOs. API images evolved between
batches; provenance and test counts are in
[the remediation report](../../../../HA-REMEDIATION-2026-09-07.md).
