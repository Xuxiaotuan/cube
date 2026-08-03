# Task 7 Report: Authoritative Durable MetaStore Service

## Implementation

- Removed the Router validation rejection for `CUBESTORE_META_ADDR`.
- Registered remote MetaStore RPC before cache configuration so Router candidates use RPC-backed MetaStore and do not register local `RocksMetaStore` or `LazyRocksCacheStore` services.
- Made Router processing loops tolerate the absence of local Rocks services in remote MetaStore mode.
- Added a focused async unit test covering Router validation, remote MetaStore injection, and absence of local Rocks services.
- Added a single-replica StatefulSet with a stable ClusterIP Service on TCP 9999, an RWO PVC, retained PVCs, graceful termination, readiness/liveness probes, PDB, ServiceAccount, and least-privilege empty RBAC rules.
- Added a demo ConfigMap containing the authoritative client address `cubestore-metastore.cube-operator-demo.svc:9999` and a namespace-scoped MetaStore deployment bundle.
- The authoritative process intentionally does not set `CUBESTORE_META_ADDR`; it is the only process configured with `CUBESTORE_META_BIND_ADDR` and the PVC mount at `/cube/.cubestore/data`.

## Verification

- `cargo test -p cubestore config::tests::router_remote_metastore_skips_local_rocks_store --no-fail-fast`: PASS, 1 test passed.
- `rustfmt --edition 2021 --check rust/cubestore/cubestore/src/config/mod.rs`: PASS.
- `kubectl create --dry-run=client` for all four Task 7 Kubernetes manifests: PASS.
- `cargo fmt --check -p cubestore`: NOT CLEAN because pre-existing formatting differences remain in unrelated `http/mod.rs` and `tests/network_message_compat.rs`; those files were not modified by Task 7.

## Risks and follow-up

- A live CSI node replacement, VolumeSnapshot backup/restore, and MetaStore Pod restart durability test were not runnable in this workspace because no Kubernetes/CSI cluster was provided. PVC retention and the Velero volume annotation are declarative safeguards; the CSI driver, VolumeSnapshotClass, backup controller, and restore runbook remain cluster prerequisites.
- The existing `demo/k8s/mock-routers.yaml` is outside the Task 7 allowed file list, so it was not edited. The demo ConfigMap exposes the exact `CUBESTORE_META_ADDR`, but Router and Worker Deployments must consume that ConfigMap in their owning task/manifests before the demo is a complete end-to-end shared-MetaStore deployment.
- The StatefulSet image uses the repository's existing demo default `cube-studio-router:ha-local`; production must replace it with an immutable CubeStore image tag or digest.

Commit message: `feat: add authoritative cubestore metastore service`
