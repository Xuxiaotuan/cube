# Cube Operator Kubernetes-Native Program Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver one `CubeCluster` CR that deploys and operates a complete Cube installation with Kubernetes-native Router and Refresher HA and recoverable pre-aggregation work.

**Architecture:** The program is split into four independently testable milestones. Kubernetes Lease owns control-plane leadership, MetaStore owns workload recovery, object storage owns durable files, and `CubeCluster.status` reports observations only.

**Tech Stack:** Go 1.25, controller-runtime 0.18.4, Kubernetes API 0.30.1, Rust CubeStore, TypeScript CubeStoreDriver and Query Orchestrator, Kubernetes, object storage.

**Spec:** `docs/superpowers/specs/2026-09-01-cube-operator-kubernetes-native-cluster-design.md`

## Global Constraints

- Router and Refresher leadership must not require Redis or PostgreSQL.
- Use `coordination.k8s.io/v1 Lease` as the only leadership authority.
- Never use CR status, ConfigMap, Pod label, or EndpointSlice as an independent leadership grant.
- Permit a short no-leader interval; never permit overlapping ready Router leaders.
- Store high-frequency mutation and Job state in CubeStore MetaStore, not Kubernetes CRs.
- Store durable upload parts and CubeStore objects in shared object storage.
- Keep user Secrets, schema ConfigMaps, PVCs, and object-store buckets outside Operator ownership.
- Preserve the existing `CubestoreRouter` API until the migration plan proves rollback.
- Commit steps in child plans require explicit user authorization before execution.

---

## Plan map

| Milestone | Plan | Independent acceptance |
|---|---|---|
| M1 | `2026-09-01-cube-operator-full-cluster-foundation.md` | One CR deploys a queryable Cube cluster. |
| M2 | `2026-09-01-cube-operator-kubernetes-router-ha.md` | Router failover uses Kubernetes Lease with no Redis/PG. |
| M3 | `2026-09-01-cubestore-preaggregation-recovery.md` | In-flight pre-aggregation work reconciles without duplicate commits. |
| M4 | `2026-09-01-cube-operator-production-gates.md` | Security, upgrade, backup, observability, and failure gates pass. |

## Dependency graph

```text
M1 Task 1 API contract
  -> M1 resource builders and controller
  -> M1 full-cluster E2E

M1 API contract
  -> M2 Kubernetes LeaseStore and sidecar
  -> M2 Router promotion and no-Redis E2E

M1 MetaStore deployment + M2 fencing identity
  -> M3 mutation ledger
  -> M3 upload and Job fencing
  -> M3 Refresher takeover
  -> M3 failover matrix

M1 + M2 + M3
  -> M4 production hardening and sign-off
```

## Parallel execution waves

1. Wave 1: M1 API contract and M2 Kubernetes LeaseStore tests.
2. Wave 2: M1 workload builders, M2 sidecar, and M3 mutation model tests.
3. Wave 3: M1 aggregate controller, M2 Router integration, and M3 upload/Job implementation.
4. Wave 4: M1/M2 E2E, M3 Refresher and recovery E2E.
5. Wave 5: M4 security, operations, upgrade, backup, and 100-failover evidence.

## Program completion rule

The program is complete only when every milestone's acceptance is supported by its own executed evidence. Passing M1 or M2 must not be reported as proof of M3 or M4.
