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

## Final fix

- Updated the demo startup chain to apply `demo/k8s/metastore.yaml` and wait for the single MetaStore StatefulSet before creating/updating Router Pods.
- Added an explicit `CUBESTORE_META_ADDR` ConfigMap reference to the Router container; both replicas now resolve the same `cubestore-metastore.cube-operator-demo.svc:9999` endpoint.
- Changed the singleton PDB from `maxUnavailable: 1` to `minAvailable: 1`. This blocks voluntary eviction of the only writer and does not claim that the PDB provides HA.
- Re-ran manifest client-side dry-run checks after the changes.

## Worker blocking fix

- The actual Rust Worker interface is `CUBESTORE_WORKER_PORT`, `CUBESTORE_WORKERS`, and `CUBESTORE_META_ADDR`; `Config::default` parses the latter and `MetaStoreTransport` connects it to the RPC service. The Operator demo previously had no Worker Deployment, so the prior ConfigMap was not consumed by any Worker.
- Added `demo/k8s/mock-workers.yaml` as a stable two-replica StatefulSet. Each Worker gets a stable StatefulSet DNS server name, the same `CUBESTORE_WORKERS` list, and the same `CUBESTORE_META_ADDR` from `cubestore-metastore-client`.
- Added `CUBESTORE_WORKERS` consumption to both Router replicas.
- Added `run.sh --dry-run`. It renders the same image substitutions as the normal path, runs client-side validation for namespace, RBAC, MetaStore, Workers, and Routers, and fails unless the deterministic order is MetaStore -> Workers -> Routers. It also asserts that Workers and Routers consume `CUBESTORE_META_ADDR` and MetaStore remains single-replica.

## Continued hardening

- Replaced both demo Worker `emptyDir` volumes with per-Pod RWO PVC templates and `Retain` retention policy. The demo now makes persistence explicit without claiming that it is the production object-store design.
- Added a Worker ServiceAccount, empty Role, and RoleBinding. The current CubeStore MetaStore RPC has no authentication or credential input, so no Secret reference was invented; image pull credentials remain a cluster-specific ServiceAccount/image-pull configuration.
- Added `WORKER_IMAGE`, defaulting to the existing Router image for the demo while allowing the run script to render a distinct Worker image deterministically.
- Extended `run.sh --dry-run` to require Worker PVC templates, Worker identity/RBAC, and consistent `CUBESTORE_META_ADDR`/`CUBESTORE_WORKERS` consumers in both Workers and Routers.
