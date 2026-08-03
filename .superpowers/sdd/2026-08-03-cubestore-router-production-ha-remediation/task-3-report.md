# Task 3 Report: Leadership and Fencing Contract

## Delivered

- Defined the `LeaseRecord` and `LeaseStore` fencing contract in `internal/leadership`.
- Added lease timing defaults and safe timing validation to the router API and CRD.
- Added `stateStore.secretRef`, `metaStore.address`, `storage.dataPVC`, and `storage.objectStoreSecretRef`.
- Removed plaintext DSN support. `leaderStateStore` remains only as a documented, secret-backed v1alpha1 migration field.
- Added router condition types: `LeaseAcquired`, `LeaderReady`, `MetaStoreReady`, `DataPlaneReady`, and `Degraded`.
- Added tests for defaults, empty selectors, unsafe timing, and incomplete Secret references.

## Namespace handling

Namespace equality rules were intentionally removed from CRD CEL validation because the `spec` validation scope cannot reference top-level `metadata.namespace`. Namespace consistency is deferred to controller enforcement as directed.

## Validation

- `go test ./internal/leadership` passed.
- `kubectl apply --dry-run=client --validate=true -f config/crd/bases/cubestore.io_cubestorerouters.yaml` passed.
