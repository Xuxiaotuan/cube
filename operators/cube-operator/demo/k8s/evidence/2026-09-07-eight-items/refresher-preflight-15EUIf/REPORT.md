# Refresher / real API / production preflight

Observed 2026-09-07, context orbstack, namespace cube-ha-remediation.
Scope: existing deployment only. New Rust authority was not built or deployed
by this task and is NOT tested by these results.

## Evidence

- Baseline harness: 27/27 pass, harness-tests.log.
- Production collector unit tests: 7/7 pass, preflight-tests.log.
- Read-only production collector: exit 1, BLOCKED, productionGo=false;
  production-preflight.json contains specific deployment blockers.
- Authenticated existing-run API meta and load: HTTP 200, real data retained in
  api-readonly-baseline.json. Run r3203dbb2fbc324ea. This is NOT restart recovery,
  not two renewed loads, and not an independent source reconciliation/provenance
  acceptance. Signing secret and JWT remained in remote process memory.
- Required analytics-refresher-restart-proxy Service absent in live Service list;
  no 13332 Service exists. No Service was created or patched.
- Ledger includes nonterminal records, including physical-ready tableId 6 and
  12 without ready/failed/retired markers. This violates existing harness global
  quiescence prerequisite. No UNKNOWN record, table or object was removed.

## Classification

Code issue fixed in allowed harness only: complete Kubernetes Pod/Deployment
snapshots and nested runtime proofs previously serialized env and annotations.
Added recursive structured evidence omission, 0600 JSON output, and a regression
test. No protocol, fault sequencing, endpoint or ledger acceptance was relaxed.
This does not certify arbitrary free-text runtime stderr as secret-free.

Environment/acceptance blockers: missing proxy Service, nonterminal retained
ledger, one physical node, local-path volumes, image references not digest-pinned,
no namespace NetworkPolicy, no approved RPO/RTO targets. Historical nonterminal
ledger proves a runtime state inconsistency but does not by itself identify its
root cause or demonstrate a defect in the unshipped Rust authority.

## Parent authorization needed before fault execution

Do not execute the controller yet. Parent must first decide isolation/recovery
of retained unfinished ledger WITHOUT deleting UNKNOWN records to force PASS.
Provisioning would create ONLY Service
cube-ha-remediation/analytics-refresher-restart-proxy, ClusterIP, TCP 13332 ->
numeric 13332, selecting only the pinned analytics API. It adds an unauthenticated
test-only listener route; restrict to the isolated namespace. Rollback is removal
of that newly created Service after confirming no active harness. Existing role
proxy-host configuration must be checked in memory; rollout changes require a
separate precise plan, not an implicit permission here.

A subsequent fault proposal must re-pin current identities. Observed target:
Pod analytics-refresher-7d4bfb9b78-mqhfg,
UID 03712823-ffc3-4ca1-8803-1d68a52ef2cf. API pinned in this observation:
analytics-api-8d47d47-52mxd, UID 40ff853a-59c0-413b-a760-66e47ef35817.
Only after parent approval and barrier proof would the controller delete that
Refresher Pod with its UID precondition and gracePeriodSeconds=0. Expected impact:
interrupt the single scheduler and discard its process-local state; Deployment
creates a replacement with the same frozen image, then the independent API proxy
releases held traffic. The exact old process cannot be restored; retained ledger,
source and objects permit diagnosis. If replacement fails, stop and request parent
recovery rather than modifying additional objects. Safer alternative: the current
read-only baseline, which does not prove crash recovery.

## Remaining boundary

B1 = external_blocked / evidence_incomplete, not PASS. ProductionReady remains
false. No Pod/Service mutation, Git, image build, deployment, shared HA document
edit, registration write or cleanup was performed. Initial local evidence-directory
creation failed before tests; initial combined baseline stopped at absent Service.
Independent API baseline subsequently succeeded. No failure was promoted to PASS.
