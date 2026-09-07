# Router HA authority and build lifecycle protocol: proposed, not implemented

## Status and scope

This is a proposed extension to `bad90b9ee6`, not a description of deployed
capabilities. Approval is required for cross-role protocol changes, service
identity configuration, and a stop-write upgrade. No Redis, PostgreSQL, custom
Raft, or arbitrary-SQL exactly-once requirement is introduced.

The current remote `MetaStoreCall` does not carry a Router authority identity.
Local MutationGuard checks cannot retract a remote request already sent. The
separate JobAttempt RPC path does carry execution ownership and has passing
local real-RPC tests; healthy Workers must not be tied to Router epoch changes.

Current PRE_AGG identity/phase/manifest/table records do not have an atomic
retirement root or trusted context binding. CacheStore GET/SET/NX operations do
not make lifecycle changes atomic with MetaStore publication. Build-name
timestamps are not a trusted monotonic generation allocator.

## Proposed authority boundary

1. Kubernetes Lease remains the election authority. MetaStore verifies the
   configured Lease directly, without trusting caller-supplied epoch or Lease
   JSON, before installing or renewing a Router grant.
2. The installation caller must have an authenticated workload identity bound to
   the elected holder. Network reachability and a caller-provided scope string
   are not authentication. The concrete identity mechanism is not yet selected.
3. MetaStore persists the accepted Lease incarnation, epoch high-water mark,
   holder and grant identity in its single-writer authority domain. Missing or
   regressed authority enters controlled recovery, not automatic initialization.
4. Router control writes carry the grant. The serialized publication transaction
   checks it again; a superseded queued request cannot publish under an old grant.
5. Promotion installs the grant and receives an acknowledgement before exposing
   the new writable Router. Worker publication retains independent JobAttempt
   ownership checks and the persistent build association.

Kubernetes and RocksDB still have no common transaction. This design does not
promise instantaneous revocation at the exact Kubernetes Lease transition.
Locally bounded authorization requires conservative expiry, clock-error and
request-delay budgets; process restart invalidates in-memory deadlines. If these
bounds cannot be established, authorization must fail closed. Whether a detected
API read failure immediately closes writes or an existing grant lasts until its
conservative deadline is a pending availability decision.

## Proposed authoritative build ledger

Place the minimal authorization-critical ledger in MetaStore, alongside the
publication transaction. CacheStore may retain rebuildable indexes, not a second
independent authority.

| Record | Required meaning |
| --- | --- |
| Scope | Server-authorized cluster/context/schema binding, incarnation and configuration version |
| Build | Logical key, atomically allocated generation, immutable manifest hash, target identity and state |
| Retirement root | Non-regressing retired-through generation for a scope incarnation and logical build key |
| Reference and receipt | Active consumers, operation identity, committed result and retryable cleanup intent |

Core resolves an authorized context; Driver carries the original identity;
Router, upload, Job and Worker paths propagate it. Retrying a late request must
not silently allocate a new generation. Missing records are not proof that an
operation never ran.

Publication checks scope/incarnation, generation, immutable manifest, target
identity, operation ownership and legal state transition in the same authority
transaction. Strict mode rejects unbound legacy write paths rather than keeping
a bypass for older clients.

## Safe recovery and retirement

- Recovery is context-scoped and reconciles the original identity. No global
  client-side scan may replay another tenant's manifests.
- UNKNOWN is not retired by age. Reference acquisition, terminal confirmation
  and retirement must be coordinated at the authoritative transaction point.
- Advance only a safe continuous retirement boundary for the corresponding
  logical key; never skip an active older generation.
- Commit retirement and cleanup intent before deleting covered records. Keep
  the durable retirement root so late requests cannot revive removed identities.
- Physical object removal also needs consumer/reference safety. Until an
  approved retention or reader-lifetime contract exists, preserve objects and
  report backlog rather than guessing a TTL.
- Permanent UNKNOWN or long-lived references can block reclamation. No promise
  of constant bounded storage under all workloads is possible without a
  lifecycle/admission policy.

## Upgrade and acceptance

Drain incompatible writers during a maintenance window; install the compatible
MetaStore, Router/agent, Workers and API/Refresher set before enabling strict
protocol admission. A successful forward upgrade is not permission to roll back
after new authoritative state has been written.

Required tests include paused remote Router writes crossing grant replacement,
loss of control-plane access without a replacement, healthy Worker continuity,
reclaimed attempts, crashes at each build boundary, publication response loss,
concurrent reference acquisition and retirement, late requests after GC, scope
isolation, and upgrade/rollback attempts. Test data and identities must be
checked, not only exception messages or Pod readiness.

## Decisions still required

1. Approve this authority/ledger placement and the stop-write protocol upgrade.
2. Select deployable workload authentication and the permission boundary for
   grant installation, scope registration and retirement.
3. Set the allowed disconnected-write behavior and reliable timing bounds.
4. Set reader/late-request retention policy, supported tenant/data-source scope,
   RPO/RTO, and production-equivalent Kubernetes/storage environment.

Until these are resolved, A3, context recovery and full GC remain unfinished.

## 2026-09-07 acceptance and implementation checkpoint

The user approved proceeding with the cross-role protocol and a stop-write
upgrade. That approval does not imply production acceptance, enablement, or
permission to silently discard unknown builds. Concrete deployment timing,
retention, RPO/RTO and production-equivalent topology remain unspecified.

The selected transport is a dedicated HTTPS authority endpoint on port 9998,
with Pod-bound ServiceAccount tokens for audience
`cubestore-metastore-authority-v1`. MetaStore uses the Kubernetes TokenReview API,
checks the expected audience and service identity, and independently checks the
Pod name/UID against the authoritative Pod and Lease. References:
[Kubernetes service accounts](https://kubernetes.io/docs/concepts/security/service-accounts/)
and [bound identity claims](https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/).

The initial draft Lease assumption was corrected before implementation:

| Actual field | Meaning |
| --- | --- |
| spec.holderIdentity | Pod name, not Pod UID |
| spec.leaseTransitions | Router epoch |
| cubejs.io/lease-cluster-id | CubestoreRouter namespace/name, not parent CubeCluster UID |
| cubejs.io/lease-generation | Existing fencing generation |
| cubejs.io/lease-token | Existing fencing token; do not publish it in logs |
| cubejs.io/lease-holder-uid | May be empty; not a substitute for authenticated Pod UID |

No Lease format migration was performed. CubeCluster UID, CubestoreRouter
identity, Lease UID, authenticated Pod UID and full fence must not be conflated.

### Actual code state, not an enabled deployment

- Rust HTTPS install/RPC, TokenReview checks, transport/startup hooks and
  serialized write authorization were added. They are not certified working.
- A Worker context-ordering error was identified: admission occurs before the
  writer restores JobAttempt context, potentially rejecting valid Worker writes.
  It remains unfixed at this checkpoint.
- The locked Rust build fails before compilation because Cargo.lock does not
  yet reflect the added warp TLS feature. No new Rust binary or passing new
  authority test result was produced.
- lease-agent now has a strict install-ACK-before-active path and authoritative
  Lease/Pod rechecks, but its timeout test cleanup hangs. Its focused package
  acceptance did not pass.
- The per-cluster PKI helper has 21 passing independent test cases but is not
  called by the rendering path. It does not mean certificate mounting, RBAC,
  projections, activation or renewal has been delivered.
- No new CRD activation field or always-failing pretend activation mode was
  added. No strict rollout was performed.
- Trusted build scope, generation/retirement ledger, migration and full GC
  remain unimplemented. They must not be inferred from the authority patch.

The generated PKI helper uses an immutable, CubeCluster-owned Secret named
`<cluster>-metastore-tls` with `ca.crt`, `tls.crt`, `tls.key`; server certificates
have 90-day validity and no automatic renewal. Rotation remains an explicit
maintenance operation requiring integration and testing.

### Follow-up fix checkpoint

The Go authority timeout fixture now passes, as does the full offline Go suite.
The Rust writer restores JobAttempt before admission, and Cargo.lock now includes
the four required TLS dependencies without upgrading existing packages. However,
the locked library/binary compilation fails with E0004 because the TableId match
in rocks_store.rs does not cover RouterAuthority. No current authority or prior
RPC tests ran in this attempt. The strict protocol remains unvalidated and has
not been enabled; lifecycle ledger and GC implementation are still pending.
