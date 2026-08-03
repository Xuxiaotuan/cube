# Task 5 report: per-pod lease agent

Status: complete

## Implementation

- Added the `lease-agent` sidecar entrypoint and an agent backed by the Task 4 `LeaseStore` read contract.
- The agent polls the authoritative store, hashes the lease token as `sha256:<hex>`, preserves the lease epoch, and writes `/var/run/cubestore-ha/leadership.json` with mode `0640`.
- Leadership files are written to a same-directory temporary file, synced, atomically renamed, and followed by a directory sync.
- Backend errors retain the last state only through `renewDeadline`; after that deadline, and on malformed records, epoch regression, or same-epoch token changes, the agent writes an expired follower state.
- The mock Router uses a private in-memory `emptyDir` shared only with the sidecar. The ConfigMap projection is no longer the runtime fencing source.

## Tests

Command:

```sh
go test ./internal/agent
```

Result: passed.

Coverage includes atomic file replacement and permissions, malformed backend data, outage expiry, holder changes, epoch monotonicity, epoch/token propagation, and rejection of an old token without an epoch change.

## Risks

- The sidecar executable expects an authoritative Task 4 HTTP adapter at `GET /leases/{clusterID}` through `CUBESTORE_LEASE_STORE_URL`; the deployment that provides this endpoint must be configured separately.
- Router-side strict parsing and validation are implemented by the later fencing task; this task fails closed by publishing an expired follower state but cannot itself prevent a Router that ignores the file.
- The manager manifest declares `CUBESTORE_LEASE_AGENT_IMAGE` for controller-side injection; the current controller does not yet consume that setting.
