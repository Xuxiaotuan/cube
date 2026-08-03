# Task 3 Report: Leadership and Fencing Contract

## Delivered

- Defined the `LeaseRecord` and `LeaseStore` fencing contract in `internal/leadership`.
- Added lease timing defaults and safe timing validation to the router API and CRD.
- Added `stateStore.secretRef`, `metaStore.address`, `storage.dataPVC`, and `storage.objectStoreSecretRef`.
- Kept `leaderStateStore.dsn` as a deprecated, explicit v1alpha1 compatibility path so existing objects remain readable. New objects use `stateStore.secretRef`; the CRD requires exactly one legacy migration credential form (`dsn` or `secretRef`).
- Added router condition types: `LeaseAcquired`, `LeaderReady`, `MetaStoreReady`, `DataPlaneReady`, and `Degraded`.
- Kept `status.conditions[*].type` as an unrestricted string for status-object compatibility.
- Added tests for defaults, empty selectors and namespaces, unsafe timing, incomplete Secret references, legacy DSN migration, and fencing/epoch semantics.

## Namespace handling

`spec.namespace` is required and has `minLength: 1`; `ApplyDefaults` and `Validate` both preserve that non-empty requirement. Equality between `spec.namespace`, Secret reference namespaces, and top-level `metadata.namespace` is not expressed by this CRD because the `spec` validation scope cannot reference top-level metadata. This report does not claim that admission blocks those cross-namespace mismatches; that limitation remains explicit for follow-up enforcement.

`Validate` applies API defaults internally before checking timing and election fields, so callers do not need a separate defaulting step.

## Validation

- `go test ./internal/leadership` passed.
- `kubectl apply --dry-run=client --validate=true -f config/crd/bases/cubestore.io_cubestorerouters.yaml` passed.
