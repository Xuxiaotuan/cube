# Closure evidence: incomplete release

Working tree after baseline `87020e400f`; not a certified or pushed release.

| File | Actual evidence |
|---|---|
| go-test.log | Initial complete Go run with healthy-fixture timeout failure |
| go-final.log | Final complete Go run after fixture correction; PASS |
| kubernetes-cas.log | Three real API CAS runs, unique test objects, exact UID cleanup |
| operator-build.log | Linux ARM64 Operator image build, actual image content ID |
| baseline-api.jsonl | Ten existing-rollup API reads, before Operator update; not fault or fresh-build proof |
| post-operator-api.json | Explicit independent aggregate checks after Operator update |
| router-before-operator-update.json | Actual Router status before manager image change |
| router-after-operator-update.json | Actual Router status after manager image change; same leader/epoch |
| runtime-images.json | Post-update Pod UIDs, nodes and actual container image IDs, without environment secrets |

The new query observer has a known missing-data error path; its five helper tests
do not certify the entire observer. The baseline log contains ten actual validated
data responses, but is not used to certify fault-window continuity. The separate
post-update command explicitly rejects missing data and mismatched totals.

Rust RPC fixture compilation and Refresher restart script syntax are currently
failing. Their scenarios have NO PASS evidence here. Earlier 2026-09-07 evidence
belongs to earlier image sets and is not relabeled as this release. See
`../../../HA-CLOSURE-2026-09-07.md` for all twelve task states and remaining gates.
