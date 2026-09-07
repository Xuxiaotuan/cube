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
4. Parent must provision a test-only ClusterIP Service named
   `analytics-refresher-restart-proxy` in `cube-ha-remediation`, selecting ONLY
   the API deployment's Pods, with TCP port 13332 and numeric targetPort 13332.
   Set `CUBEJS_HA_RESTART_PROXY_HOST` on both roles to
   `analytics-refresher-restart-proxy.cube-ha-remediation.svc.cluster.local`.
   `haRestart=true` selects this demo-only route. This stable DNS configuration
   no longer needs a new env value after API Pod recreation. The controller
   validates the actual Service and EndpointSlice mapping before registration,
   at barrier release, and before verification; stale/multiple endpoints fail.
5. Allow Refresher -> proxy Service/API Pod TCP 13332 and API -> Router HTTP/WebSocket access.
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
manual build, or the scheduler directly to create/recover the rollup. Only after
durable recovery and replacement Ready does it issue two real, authenticated,
renewed API loads to consume the exact recovered rollup.

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
HA_PROXY_SERVICE=analytics-refresher-restart-proxy \
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
| `HA_PROXY_SERVICE` | analytics-refresher-restart-proxy; existing verified ClusterIP Service |
| `EVIDENCE_DIR` | existing parent directory, default /tmp; creates unique child |

`HA_RUN`, `HA_OLD_UID`, `HA_API_IP`, `HA_API_UID`, `HA_PROXY_HOST`,
`HA_REF_CONTAINER`, and `HA_SCRIPT_HASH` are controller-generated runtime
inputs. `HA_API_IP` is used only to bind the proxy in the pinned API Pod, not as
the Refresher routing configuration. Do not run the internal `runtime`
entrypoint manually. The API container must have its normal `CUBEJS_API_SECRET`
and pod-UID configuration; the signing secret is not copied into local evidence.

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

- Barrier ordering: independently confirm the selected fault phase; observe old
  UID disappearance and one replacement `Running` Pod whose target container
  has a runtime containerID, running startedAt, and the frozen imageID; then
  discard held traffic and release the gate. Only afterwards require durable
  ready and final Pod Ready, preserving the same replacement UID. Readiness is
  deliberately NOT a release prerequisite. Local sequencing tests do not prove
  the parent's actual scheduler/startup/runtime behavior.
- Service DNS removes the env/Pod-IP recreation coupling, but the independent
  fault process still lives in one pinned API Pod. API recreation during a run
  fails the test. Parent must provision the Service and configure the image;
  this harness neither deploys nor patches workloads or network resources.
- This is an explicit, still-unexpired durable demo context, not reconstruction
  of arbitrary tenants/data sources. Global recovery requires other build
  records to be terminal. Existing scheduler/queue ownership timeouts can still
  prevent bounded recovery; they are not overridden or repaired here.
- Pod UID disappearance plus replacement is Kubernetes-level evidence, not
  proof of old-process fencing during node isolation. Immediate Pod deletion
  cannot establish node-loss safety or multi-node DR.
- Result checking reads the physical rollup and compares every row plus a
  repeat read. After recovery it also makes two authenticated Cube API loads,
  comparing every dimension/metric and requiring exact rollup provenance via
  existing API debug metadata or independently logged external SQL execution.
  API runtime events must not show a build. Build/manifest/table identity is
  checked again after each consumption; only one CREATE dispatch is accepted.
  Unsupported response/member/provenance layouts fail closed, not just on HTTP
  status. Control `/verify` acknowledges only dispatch; the final result event
  must arrive within the original deadline to produce a PASS.
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

Evidence files include `before.json`, `after.json`, `events.jsonl`,
`replacement-at-release.json`, `network-before.json`, `network-at-release.json`,
`network-after.json`, old/new
runtime events, stderr, and `summary.json`. A helper-test PASS or shell exit code
does not satisfy task 5/10; the parent must inspect the actual barrier, durable
identity, table identity, scheduler origin, result hashes and topology evidence.

## Evidence confidentiality

Structured controller snapshots and runtime events omit `env`, `envFrom`,
annotations, managedFields, and credential-named fields recursively, including
Pod objects nested in release/verification proofs. Saved JSON files use mode
0600. Assertions still use the original in-memory Kubernetes objects. This is
not a general free-text log scrubber: do not log tokens, Secrets, environment
values, or authentication headers; arbitrary upstream error text is not certified
secret-free by this filter. Tokens remain inside the API runtime.
