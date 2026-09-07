# Frozen release acceptance (tasks 8 and 10)

The smallest offline suite entrypoint is:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 demo/k8s/release-check.test.py
```

Run from `operators/cube-operator`. It uses only temporary fixture files and has no Kubernetes/network/shell mutation path. Fixture PASS is not live acceptance. The checker does not execute existing fault scripts. The parent owns those executions and their raw evidence.

## Freeze, then observe

`demo/k8s/release-check.py freeze --root REPOSITORY --cluster INTENDED-CR.json --operator-image REPOSITORY@sha256:DIGEST --artifact crd=RELATIVE-PATH --artifact config=RELATIVE-PATH --artifact test=RELATIVE-PATH --artifact source=RELATIVE-PATH --out NEW-LOCK.json`

Repeat `--artifact` for every dependency: both CRDs, Manager/SA/Role/RoleBinding, rendered configuration, model/config source, test scripts including checker, controller/agent/Rust/TS sources and image provenance. The checker enforces nonempty categories, NOT completeness of the dependency inventory. The release owner must review that inventory; freeze after all agents finish. The intended CR must be actual `cubestore.io/v1alpha1` JSON. Do not include secret contents; separately archive protected secret versions/checksums in restricted evidence. Refresher uses `spec.images.api`; no separate Refresher image field is invented.

Freeze prints the release content ID and the exact lock file SHA256. Archive/approve that hash outside the mutable workspace. Tags, mixed pinned/unpinned images and unequal Router/MetaStore/Worker digests are rejected. Matching Rust digests is only a conservative necessary constraint, never proof of compatibility with API, agent or persisted state. Legacy all-tag CRs remain supported by the controller for development. The controller checks the desired artifact combination, not every transient mixed Pod set.

`demo/k8s/release-check.py check --root REPOSITORY --lock LOCK.json --lock-sha256 APPROVED-SHA256 --mode preflight`

Preflight returns `ARTIFACTS_MATCH_NOT_COMPATIBILITY_PROOF`. It is not permission to apply manifests. This offline gate is a release-process control, not an admission webhook; direct CR edits can bypass the release process. Do not expose production write credentials to unreviewed deployment paths.

## Evidence contract

For `--mode acceptance --evidence evidence-index.json`, supply:

```json
{
  "release_id": "content ID printed at freeze",
  "installation": "fresh",
  "checks": {
    "historical-1": {"path": "reports/historical-1.json", "sha256": "actual file sha256"}
  }
}
```

All eleven keys are required: `historical-1` through `historical-6`, `control-plane`, `worker-stale-attempt`, `refresher-crash`, `network-partition`, `repeated-load`. Map the six historical IDs to their actual original named business cases in each report's `case`; this document does not invent their historical results. Include control-plane regression output and Operator restart observations. Each referenced report is JSON:

```json
{
  "release_id": "exact frozen ID",
  "scenario": "historical-1",
  "case": "actual executed case and fault boundary",
  "status": "PASS",
  "started_at": "actual UTC timestamp",
  "finished_at": "actual UTC timestamp",
  "observed_images": {"operator": "actual repo@sha256:digest", "leaseAgent": "actual repo@sha256:digest", "router": "actual repo@sha256:digest", "metaStore": "actual repo@sha256:digest", "worker": "actual repo@sha256:digest", "api": "actual repo@sha256:digest", "refresher": "actual repo@sha256:digest"},
  "assertions": [{"name": "actual query result", "expected": [1, 2], "actual": [1, 2]}],
  "raw_evidence": [{"path": "raw/query-response.json", "sha256": "actual file sha256"}]
}
```

Paths in reports and the evidence index are relative to the index directory. Include real buildId/tableId/manifest, attempts and publication counts in assertions/raw evidence, plus independently measured old-writer rejection, leader serving, query recovery and build completion windows. An exit code alone is insufficient. The checker verifies references, exact release/image identity and asserted data equality; it cannot independently attest that a human-supplied report is truthful or that selected assertions cover the entire business contract. Review raw evidence and scenario coverage before approval. Production observability of build backlog/capacity is still a separate application integration requirement.

## Upgrade and rollback

For an existing installation add `--from-lock PREVIOUS-LOCK.json --transition upgrade` (or `rollback`). The target is always `--lock`, even on rollback. Besides the base suite require report keys `upgrade`/`rollback`, `interruption`, `partial-failure`, `old-worker-drained`, `persistent-state-readable`. Each must carry exact `from_release_id` and `to_release_id`; the main transition must have `mode: mixed-version` or `fenced-maintenance`. Never infer a reverse edge from a successful forward upgrade. Writing a new persistent protocol means rollback is blocked until reverse-state readability is tested, or a separately approved consistent backup restore is performed.

Operator ordering is MetaStore -> Workers -> Router -> API -> Refresher. Old Workers remain alive while MetaStore changes. Therefore a transition without proven old/new RPC coexistence requires an explicitly approved maintenance window: stop business ingress/scheduling, reconcile outstanding uploads/Jobs, drain and fence old executors, back up all state, then coordinate restart outside ordinary ordered rollout. No automatic drain/scale/fencing command is supplied here. Report actual interruption and partial-component failure behavior; do not relabel readiness as compatibility. Incomplete transitions stop deployment, not silently downgrade acceptance.

## Production extension

`--mode production` additionally requires reports `node-fencing`, `volume-reattach`, `backup-restore`, `object-reconciliation`, `unfinished-build-restore`, `manager-rbac`, `drain-probes`, `admin-isolation` and an index `production` object with actual distinct `router_nodes`, `metastore_writers: 1`, matching `manager_namespace`/`lease_namespace`, explicitly agreed `rpo_seconds`/`rto_seconds` and measured `observed_data_loss_seconds`/`observed_restore_seconds`. Loss and duration must meet those bounds. Reports still require raw evidence and assertions.

Single-node OrbStack cannot meet this topology requirement. No production topology, RPO/RTO, storage class, drain behavior or backup consistency is asserted by these tools. Missing production inputs are `external_blocked`; missing same-release scenarios are `evidence_incomplete`. `ProductionReady=False` is intentionally not overridden.
