# Task 3 Report: Leadership and Fencing Contract

## Delivered

- Defined the `LeaseRecord` and `LeaseStore` fencing contract in `internal/leadership`.
- Added lease timing defaults and safe timing validation to the router API and CRD.
- Added `stateStore.secretRef`, `metaStore.address`, `storage.dataPVC`, and `storage.objectStoreSecretRef`.
- Kept `leaderStateStore.dsn` as a deprecated, explicit v1alpha1 compatibility path so existing objects remain readable. New objects use `stateStore.secretRef`; the CRD requires exactly one legacy migration credential form (`dsn` or `secretRef`).
- Kept `leaderStateStore.type` explicitly constrained to the supported `redis` or `postgres` enum when present, while not making the legacy field newly required for older objects.
- Added router condition types: `LeaseAcquired`, `LeaderReady`, `MetaStoreReady`, `DataPlaneReady`, and `Degraded`.
- Kept `status.conditions[*].type` as an unrestricted string for status-object compatibility.
- Added tests for defaults, empty selectors and namespaces, unsafe timing, incomplete Secret references, legacy DSN migration, and fencing/epoch semantics.

## Namespace handling

`spec.namespace` is required and has `minLength: 1`; `ApplyDefaults` and `Validate` both preserve that non-empty requirement. Equality between `spec.namespace`, Secret reference namespaces, and top-level `metadata.namespace` is not expressed by this CRD because the `spec` validation scope cannot reference top-level metadata. This report does not claim that admission blocks those cross-namespace mismatches; that limitation remains explicit for follow-up enforcement.

`Validate` applies API defaults internally before checking timing and election fields, so callers do not need a separate defaulting step.

The production leadership package exposes `ValidateLeaseFence`, which rejects a presented lease unless cluster, holder, epoch, and token exactly match the current record. The behavior test proves an old epoch/token is rejected and a newly acquired epoch/token is accepted. The test store is in-memory only; this is a reusable fencing predicate, not an implementation or claim of external storage CAS.

## Validation

- `go test ./internal/leadership` passed.
- `kubectl apply --dry-run=client --validate=true -f config/crd/bases/cubestore.io_cubestorerouters.yaml` passed.

## Concurrent follow-up

Task 3 changes were paused while the concurrent Task 4/7 work stabilized. After the branch HEAD remained stable, `go test ./internal/leadership` passed from cache. No `ErrStaleLease` duplicate declaration was present, so no Task 3 implementation file was changed and no Task 4 file was modified in this follow-up.

The follow-up did not add a production call site for `ValidateLeaseFence`; it remains the reusable fencing predicate documented above.

## Runtime reconciliation boundary

The current controller runtime reads `stateStore.secretRef` (or the legacy field's `secretRef`) and fetches the `dsn` key from that Kubernetes Secret. It does not read `leaderStateStore.dsn` directly. Therefore the API/leadership-only scope cannot make an old plaintext-DSN object semantically equivalent to the SecretRef form: doing so requires changing the controller's `externalLeaseConfig` path or introducing an explicit migration that materializes the DSN into a Secret. No schema-only claim of runtime equivalence is made here, and no controller file was changed.

The lease configuration names currently align for the shared runtime path: `electionStrategy=lease`, `leaseDurationSeconds`, `renewDeadlineSeconds`, and `retryPeriodSeconds`, with defaults defined in the API and CRD. The controller consumes the lease strategy and lease duration; renew/retry timing is consumed by the leadership agent. There is no second API concept introduced in this task.

The CRD requires a non-empty `spec.namespace`, but its `spec` CEL scope cannot compare that value with top-level `metadata.namespace`; admission therefore cannot enforce namespace equality. Secret-reference namespace equality has the same limitation. This is an explicit admission limitation, not a claim that the API server blocks those mismatches.
