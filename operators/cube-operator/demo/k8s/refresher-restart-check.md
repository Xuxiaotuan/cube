# Refresher restart candidate: task 5 / task 10

This is a candidate single-node, single-configured-context acceptance harness.
Helper tests, including mock Kubernetes authorization and local WebSocket peers,
are NOT real scheduler, CubeStore, process-crash, or Kubernetes acceptance.
Do not inherit a PASS from the historical six scenarios.

## Parent preparation (not performed by this change)

1. Include the modified `demo/cube-api/cube.js`, existing
   `ha-scheduled-contexts.js`, and existing `schema/cubes/RouterHaRollup.js` in the
   parent's normal Linux API image. Include the parent's frozen TS recovery
   artifacts and matching Linux native module. Do not use a macOS native binary.
2. The image must contain `ws`, `flatbuffers`, and the compiled
   `@cubejs-backend/cubestore-driver/dist/codegen` exports. The source API used is
   `CubeStoreDriver.getPreAggregationBuildStatus(table)`. No production fault
   hook or new tenant discovery API is required.
3. Deploy that SAME image to the existing API and Refresher roles, using the
   parent's existing build/deployment flow. Keep API scheduling disabled and
   Refresher scheduling enabled. Keep `CUBEJS_HA_DEMO=true`,
   `CUBEJS_HA_SCHEDULED_DEMO=true`, and downward-API pod name/UID configuration.
   No historical-six role overlay changes are needed.
4. Set `CUBEJS_HA_RESTART_PROXY_HOST` on both roles to the unaffected API Pod IP.
   This is a Pod IP, not loopback; `haRestart=true` selects this demo-only route.
   Ensure the API pod does not change while configuring this value. If setting
   the shared environment recreates the API pod, use the resulting stable
   deployment/config mechanism to inject its final IP before running. The
   current candidate explicitly compares this setting with that pinned Pod IP;
   it does not accept Service DNS as an alternative.
5. Allow Refresher -> API Pod TCP 13332 and API -> Router HTTP/WebSocket access.
   The control listener is API-loopback TCP 13333. These test-only ports have no
   production authentication boundary. Use only the isolated test namespace.
6. Obtain the full `status.containerStatuses[].imageID` shared by API/Refresher;
   the `93e4...` abbreviation is not accepted. Freeze all component artifacts
   separately for task 10; this harness records pod images but is not a full
   release-manifest verifier.

The controller streams this JS file into the API pod using `kubectl exec -i`;
the new harness need not be baked into the image. It creates 32 real source rows,
starts the independent proxy, writes one expiring durable demo context, and
waits for the actual configured Refresher scheduler. It never calls API load,
manual build, or the scheduler directly to create/recover the rollup.

## Local checks

From `operators/cube-operator`:

```sh
node --test demo/k8s/refresher-restart-check.test.js
node --check demo/k8s/refresher-restart-check.js
node --check demo/cube-api/cube.js
bash -n demo/k8s/refresher-restart-check.sh
```

## Parent-authorized live invocation ONLY

The following command really requests immediate deletion of the pinned
Refresher Pod with a UID precondition. It must not be run as a helper test.
The controller is hard-pinned to context `orbstack`, namespace
`cube-ha-remediation`. Run one mode at a time, only after parent approval.

```sh
HA_ALLOW_REFRESHER_DELETE=1 \
HA_MODE=uploaded \
HA_EXPECTED_IMAGE_ID='docker-pullable://YOUR_IMAGE@sha256:FULL_64_HEX_DIGEST' \
bash demo/k8s/refresher-restart-check.sh
```

Required environment: `HA_ALLOW_REFRESHER_DELETE=1`, `HA_MODE`, and the exact
`HA_EXPECTED_IMAGE_ID`. Modes: `uploaded`, `create-sent`,
`physical-ready-ledger-unacked`.

Optional controller environment:

| Parameter | Default / constraint |
| --- | --- |
| `HA_TIMEOUT_SECONDS` | 180; positive integer, maximum 600 |
| `KUBECTL` | kubectl executable |
| `API_DEPLOYMENT` | analytics-api; one ready replica |
| `REFRESHER_DEPLOYMENT` | analytics-refresher; one ready replica |
| `API_CONTAINER`, `REFRESHER_CONTAINER` | first container of each pinned pod |
| `HA_UPSTREAM` | http://analytics-router-leader:3030 |
| `EVIDENCE_DIR` | existing parent directory, default /tmp; creates unique child |

`HA_RUN`, `HA_OLD_UID`, `HA_API_IP`, and `HA_SCRIPT_HASH` are controller-generated
runtime inputs. Do not run the internal `runtime` entrypoint manually.

## Exact barriers and current evidence limits

| Mode | Wire boundary and independent checks | What it does not establish |
| --- | --- | --- |
| uploaded | Withhold `CACHE SET NX PRE_AGG_PHASE...:create`; require uploaded ledger, all remote SHA/size receipts, physical target absent | Partial-upload/local-file-loss recovery |
| create-sent | Forward the exact traced CREATE once; suppress replies and subsequent SQL; require physical building or ready | Cannot force CREATE to remain building at death; may already be ready |
| physical-ready-ledger-unacked | Withhold the ready phase write before forwarding; independently require physical ready and no ready ledger marker | Not a committed-ready-ledger response-loss test |

All three have implemented transport interception points, not validated live
E2E results. Runtime layout mismatch fails or times out rather than generating a
synthetic PASS. The proxy requires formatted SQL (zero wire parameters) and the
current generated FlatBuffers envelope. Local peers do not prove compatibility
with the parent's Linux runtime, authentication, version negotiation or model
column aliases.

## Candidate limitations / parent go-no-go checks

- The controller waits for a replacement Ready Pod BEFORE releasing the gate.
  If `/readyz` depends on recovery through that gate, this ordering cannot work
  and will time out. Confirm the frozen runtime readiness contract before live
  execution; do not claim the barrier is runnable if this is circular.
- Direct API Pod-IP configuration needs a stable setup. A shared env update
  that repeatedly recreates the API pod can invalidate the target. No workload
  patch/Service automation is provided by this harness.
- This is an explicit, still-unexpired durable demo context, not reconstruction
  of arbitrary tenants/data sources. Global recovery requires other build
  records to be terminal. Existing scheduler/queue ownership timeouts can still
  prevent bounded recovery; they are not overridden or repaired here.
- Pod UID disappearance plus replacement is Kubernetes-level evidence, not
  proof of old-process fencing during node isolation. Immediate Pod deletion
  cannot establish node-loss safety or multi-node DR.
- Result checking reads the real physical rollup and compares every row plus a
  repeat read; it does not prove API consumption/provenance. Only one CREATE
  dispatch is accepted and build/manifest/table identity must remain stable.
- Old scheduler provenance and replacement scheduler wire connection are
  required. The test proxy survives Refresher disconnection in the API pod;
  unrelated pod/image/restart changes cause failure.
- Unrelated Pods must remain present under the same UID, without deletion in
  progress, and retain their container image/restart identities. Only a Pod
  with `status.phase=Succeeded` and a `Job` ownerReference is exempt from Ready.
  Failed Pods, running not-Ready Jobs, and Succeeded non-Job/ownerless Pods are
  not exempt. A completed init Job therefore does not cause a false failure,
  while missing services and replaced service Pods still fail the check.
- Failed-run evidence, source tables, manifests and objects are retained.
  Registration expires by the bounded run deadline; success removes only its
  own registration. A cancelled exec may leave the remote runner until its
  deadline. Do not overlap runs or automatically drop retained build data.
- The independent HTTP receipt probe currently sends no Router authorization
  header. Auth-required layouts are unsupported by this candidate.

Evidence files include `before.json`, `after.json`, `events.jsonl`, old/new
runtime events, stderr, and `summary.json`. A helper-test PASS or shell exit code
does not satisfy task 5/10; the parent must inspect the actual barrier, durable
identity, table identity, scheduler origin, result hashes and topology evidence.
