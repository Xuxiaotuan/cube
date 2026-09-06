# Real Cube API pre-aggregation faults

This is a destructive **test-namespace-only** harness, not a production recovery
tool. The old `cube-api-failover-check.sh` only proves pre-seeded query continuity;
it does not replace this test.

## Prerequisites and parent-owned deployment

- Use the isolated namespace `cube-ha-remediation`. The fault runner refuses any
  other namespace and never reuses existing business schemas.
- Parent rebuilds the changed workspace Node packages and the real router image.
  `build-image.sh` packages workspace `dist` output, not stale `lib` directories
  or installed registry copies. It checks required build inputs before Docker.
- Reuse the valid Linux native module through `NATIVE_INDEX_NODE`. Do not pass a
  macOS Mach-O native module. No image build/deployment is performed by this task.
- CubeCluster is `analytics`; API workload is `analytics-api`, router child is
  `analytics-router`, and leader Service is `analytics-router-leader`.
- The API must use this image's `cube.js`, `CUBEJS_HA_DEMO=true`,
  `CUBEJS_CACHE_AND_QUEUE_DRIVER=cubestore`, and its operator-injected
  `CUBEJS_API_SECRET` Secret reference. No secret value is supplied on the command
  line or written into evidence. Keep one API replica for the loopback harness.
- The runtime must implement real `/router/build-status`,
  `/upload-temp-file-status`, immutable compressed-byte upload receipts, and
  `/cube/cubestored --drain`. Recovery must use shared CACHE build manifests.
- Local tools: Bash, kubectl, jq, od, tr, date, sleep. Pod tools: Node and the
  dependencies already installed in the API image. No proxy Pod, sidecar, new
  package installation, special Pod RBAC, or production fault hook is needed.

Parent commands, only after code/build readiness has been established:

```bash
cd /Users/xujiawei/magic/workbench/cube
API_IMAGE=cube-studio-api:ha-local \
NATIVE_INDEX_NODE="$PWD/packages/cubejs-backend-native/target/release/libcubejs_native.so" \
bash operators/cube-operator/demo/cube-api/build-image.sh

# Applies cluster-wide CRDs as well as the isolated demo manifests.
# This is a deployment command, not part of the fault runner.
NAMESPACE=cube-ha-remediation \
bash operators/cube-operator/demo/k8s/run-cubecluster.sh
```

`run-cubecluster.sh` renders namespace, Service DNS, Secret namespace and manager
WATCH_NAMESPACE substitutions without editing source manifests. It does not
touch the old demo namespace. Image tags remain the CR's values; the parent must
ensure the isolated pods actually run the freshly built images.

## Run

```bash
cd /Users/xujiawei/magic/workbench/cube
NAMESPACE=cube-ha-remediation \
EVIDENCE_DIR=/tmp/cube-ha-preagg-evidence \
bash operators/cube-operator/demo/k8s/preagg-fault-check.sh

# A single fault, or the optional stream transfer variant:
FAULT_MODES=receipt-failover HA_TRANSFER=stream \
EVIDENCE_DIR=/tmp/cube-ha-preagg-stream \
bash operators/cube-operator/demo/k8s/preagg-fault-check.sh
```

Modes run sequentially with independent random run IDs:

| Mode | Exact fault barrier | Controller action |
| --- | --- | --- |
| `upload-failover` | Real API rollup manifest is `uploading`; HTTP upload bytes have been forwarded, final byte withheld | Delete pinned leader, observe higher epoch and sole ready new endpoint, close old tunnels and release failed upload |
| `receipt-loss` | Real upload returned 2xx and independent authoritative receipt matches compressed SHA256 and size; API has not received response | Drop response without changing leader |
| `receipt-failover` | Same durable receipt barrier | Delete pinned leader before dropping response; reconcile receipt through new leader |
| `drain-failover` | Same in-flight upload barrier | Execute drain, attempt INSERT on a raw WebSocket opened to the old leader before drain, require explicit server fence rejection, then delete leader and recover |

Force deletion targets only the captured test leader pod in the authorized
namespace. It can interrupt all work on that isolated cluster. There is no
rollback of the deleted pod; the workload controller replaces it. The driver
upload timeout is 60 seconds, so failover observation defaults to 45 seconds and
cannot exceed 50. Failure to meet that window is a failure, not a skipped pass.
Polling intervals wait for observable conditions; they never select a fault
moment by sleeping and hoping the build is active.

Each mode seeds only its own deterministic CubeStore source table, then a signed
Cube API request compiles a fresh external, read-only rollup. The default source
adapter returns real query rows to exercise `rows -> stream -> gzip -> upload ->
CREATE`; `HA_TRANSFER=stream` changes the adapter to a row stream. The source
driver uses the normal leader Service; only that run's external driver uses the
loopback data proxy on 13330 (`CUBEJS_HA_PROXY_PORT`). Control is loopback 13331
(`HA_CONTROL_PORT`). The proxy is started by the stdin runner, not by the API.

## Required evidence

- An actual pending API query and `uploading` manifest, never a prebuilt rollup.
- Exact compressed-byte hash/name/size matching the driver's durable upload list.
- Higher leader epoch and the sole ready EndpointSlice target for failover modes.
- A returned `usedPreAggregations` entry containing the exact durable build ID.
- Independent expected bucket counts, amounts and ID checksums, plus canonical
  SHA256 equality. The physical rollup must contain all expected per-ID rows.
- The same immutable `PRE_AGG_BUILD_V1:<target>` identity and
  `PRE_AGG_MANIFEST_V1:<target>` upload/CREATE fingerprint before and after
  recovery. Append-only `PRE_AGG_PHASE_V1:<target>:ready` must exist, contain the
  full ready record and match the real `tableId` and immutable
  `PRE_AGG_TABLE_ID_V1:<target>`. Retirement takes precedence over ready, so a
  retired build never passes. The immutable BUILD identity's original phase is
  not a completion signal.
- A post-fault driver receipt read. Receipt-loss modes require **exactly one**
  POST of the confirmed file; partial-upload modes require a resumed POST.
- Repeated API query preserves the result hash and ready manifest. Unique run
  schemas prevent reuse of a previous test; the unsupported API `renewQuery`
  property is not used.
- `drain-failover` additionally requires old-connection write rejection with a
  server `QueryError`. Connection loss/timeouts alone cannot pass that assertion.

Per-run `.events.jsonl`, `.stderr.log`, and final `.json` files are written under
`EVIDENCE_DIR`. They include exact targets, manifest snapshots, transport events,
hashes and failure reasons. `PASS` is emitted only after all applicable runtime
checks; absent fault observation, unavailable endpoints, unknown results and
deadlines produce `FAIL` / `evidence_incomplete`.

Settings: `HA_ROWS=4096` (32..100000), `HA_TIMEOUT_SECONDS=600`,
`FAILOVER_TIMEOUT_SECONDS=45`, `HA_TRANSFER=rows`, `HA_CLEANUP=0`,
`API_DEPLOYMENT=analytics-api`, `CR_NAME=analytics-router`,
`LEADER_SERVICE_NAME=analytics-router-leader`,
`ROUTER_CONTAINER=cube-studio-router`; `API_CONTAINER` defaults to the first API
container. Do not run two harnesses concurrently in the same API pod.

By default all isolated artifacts are retained for evidence. `HA_CLEANUP=1`
drops only the exact run-created source and verified rollup tables after success.
It retains schemas, CACHE manifests and content-addressed objects. Failure never
triggers database cleanup. No blanket DROP, namespace/PVC deletion, cache flush,
or object-store deletion is performed. The parent may later retire the entire
isolated namespace with separate explicit authorization.

This harness proves upload/build recovery within a running CubeCluster. It does
not prove metastore disaster recovery, cross-node storage durability, refresher
process restart recovery, arbitrary INSERT replay safety, or network-partition
fencing beyond the explicit old-connection drain check.

## Pure harness checks, not cluster evidence

```bash
node --test operators/cube-operator/demo/cube-api/ha-fault-proxy.test.js
node --test operators/cube-operator/demo/cube-api/ha-scheduled-contexts.test.js
bash -n operators/cube-operator/demo/k8s/preagg-fault-check.sh
node --check operators/cube-operator/demo/k8s/preagg-fault-check.js
```

The proxy tests use an explicitly mocked upstream on ephemeral loopback ports.
They test transport barriers, receipt response loss and websocket closure only;
their passing output is never evidence that CubeStore HA or real rollups work.

## Opt-in independent refresher mode (r2 images)

The original four modes and their default shell entrypoint are unchanged. This
additional test proves **real scheduled-refresher warmup followed by API
consumption**, not API-to-refresher task delegation and not API-local on-demand
refresh. It does not introduce a production endpoint or manufacture API data.

The runtime overlay is `cube-cluster-refresher.yaml`, for `cube-ha-remediation`
only. It retains the existing MinIO bucket/PVC and source-storage subpath, enables
`refresher.replicas=1`, and inherits API pod model/config/resources. The operator
injects the shared CubeStore cache and generated Secret, and distinguishes the
two REFRESH_WORKER roles. API readiness is expected HTTP 200 with build capability
false; refresher readiness must be HTTP 200 with build capability true. Capability
is separate configuration evidence, never the completed business-workflow proof.

Parent deployment commands, only after all three r2 images are available:

```bash
kubectl --context orbstack -n cube-ha-remediation set image deployment/cube-operator \
  manager=cube-operator:ha-remediation-20260907-r2
kubectl --context orbstack -n cube-ha-remediation rollout status deployment/cube-operator --timeout=180s
kubectl --context orbstack -n cube-ha-remediation apply --dry-run=server --validate=strict \
  -f operators/cube-operator/demo/k8s/cube-cluster-refresher.yaml
kubectl --context orbstack -n cube-ha-remediation apply \
  -f operators/cube-operator/demo/k8s/cube-cluster-refresher.yaml
kubectl --context orbstack -n cube-ha-remediation rollout status deployment/analytics-api --timeout=300s
kubectl --context orbstack -n cube-ha-remediation rollout status deployment/analytics-refresher --timeout=300s

NAMESPACE=cube-ha-remediation EVIDENCE_DIR=/tmp/cube-ha-refresher-r2 \
bash operators/cube-operator/demo/k8s/refresher-fault-check.sh
```

The controller pins different API and refresher pod UIDs. It starts the stdin
runner and loopback proxy **in the refresher pod**, seeds only its unique source,
then registers `HA_DEMO_CONTEXT_V1:<haRun>` in the real shared CacheStore with a
bounded TTL. The real Cube scheduler polls registered contexts every two seconds;
unregistered, expired, malformed or concurrent contexts do not start this test.
Only the signed `haScheduled=true` model is scheduled. The legacy fixed fixture
is excluded from this run's compiler, so the scheduler never needs business data.

At the upload barrier, the runner requires a real scheduler log and a `Performing
query` event written by the actual server process after queue-lock acquisition.
The event must include queue ID, processing ID, query key/prefix, server process
UID, refresher pod UID and a version entry deriving the exact durable build ID.
The API load has not started at that point. Only then can the local controller
delete the captured router leader and release the partial upload after promotion.

After the real scheduler reaches the ready manifest, the runner unregisters the
context and calls the real API pod. That API has `externalRefresh=true` for the
scheduled context and connects directly to the leader, with no API fault proxy.
The original count/sum/checksum/hash, physical-row, exact usedPreAggregations,
immutable-manifest, receipt and stable table-ID checks all still apply. Independent
API-process logs must contain actual query execution but no pre-aggregation build
or upload. Both actual `/readyz` bodies are recorded before/after the fault, and
the pod UIDs must remain unchanged.

Additional settings: `API_DEPLOYMENT=analytics-api`,
`REFRESHER_DEPLOYMENT=analytics-refresher`, optional `API_CONTAINER` and
`REFRESHER_CONTAINER`; transfer/rows/deadlines/cleanup settings match the base
harness. API and refresher do not need extra Kubernetes RBAC. Only the local
controller uses kubectl. No workload is created or restarted by this harness.

Registry entries are removed on completion, or removal is attempted with a
bounded deadline after failure; TTL expiry is the backstop. Source/build artifacts
and runtime logs remain for inspection by default. Per-run runtime events live
under `/tmp/cube-ha-runtime-<run>.jsonl` in each pod and are included/referenced in
the evidence bundle. This is not durable refresher ownership, refresher failover,
or partition fencing: it is a singleton refresher surviving **router** failover.
