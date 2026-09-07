# Controlled Kubernetes Lease recovery

Scope: missing Router Lease, not the Manager election Lease. This is an offline,
human-approved protocol, NOT automatic recovery or a production acceptance result.
Never delete `cubestore.io/lease-bootstrap-reserved`, erase CR/ConfigMap/Pod fencing
annotations, decrease an epoch, or restore an old live Lease to regain service.
Normal release now expires the existing Lease using resourceVersion CAS; it does
not delete the durable epoch. A fresh acquisition increments that epoch.

## Classify without mutation

1. Record the API server identity, namespace, Router CR name and UID, owning
   CubeCluster UID, Router Lease name and cluster ID. The Lease cluster ID is the
   child Router `namespace/name`, NOT the parent CubeCluster. Read directly from
   the API, not an informer cache. Confirm no CR deletion or namespace migration.
2. Back up the complete CR including status and annotations, Lease (if present),
   role ConfigMap including JSON and annotations, Pod UIDs and fencing annotations,
   agent leadership/promotion files, workload definitions, image digests, Manager
   election Lease and audit records. Protect backups: they contain fencing tokens.
3. A present, valid Lease after a timed-out Create means the request committed.
   Do not recreate it. Reconciliation can observe it, adopt an acknowledged healthy
   holder under Manager election, or wait for expiry before a new epoch.
4. A reservation with no Lease and no visible history is only *possibly* interrupted
   bootstrap. Absence of CR status or ConfigMap history does not prove no epoch was
   issued. A Lease can be created and acquired repeatedly before mirrors update.
   Historical CR, ConfigMap, Pod, agent, or audit evidence means state loss. Both
   ambiguous cases remain blocked until the protocol below is satisfied.

## Isolate and establish the epoch upper bound

1. Obtain explicit maintenance approval. Stop traffic and new work. Suspend all
   owning reconcilers and all Operator instances, including paused/partitioned old
   instances, before stopping Router/agent processes. Record previous replicas and
   configuration for resumption. Do not merely scale a child Deployment while its
   CubeCluster owner can recreate it.
2. Fence old processes from Kubernetes writes AND MetaStore/object-store publication.
   Require positive process/node termination or infrastructure-level isolation.
   A missing Pod, expired Manager Lease, failed readiness probe, or waiting one TTL
   is not proof of isolation. If a node cannot be fenced, STOP. Keep legitimate
   healthy Worker ownership separate; coordinate draining/quiescence with the
   business recovery owner rather than blindly cancelling valid attempts.
3. With every possible Lease writer stopped, settle outstanding API requests and
   read again. If a valid Lease exists, prefer retaining it and waiting for expiry.
   Do not overwrite it with a lower or guessed state.
4. Establish an inclusive, provable upper bound H on ALL previously issued epochs
   for this cluster identity, including unacknowledged acquisitions and deleted
   Leases. Use a complete authoritative backup plus complete subsequent Lease write
   audit history, reconciled against CR/ConfigMap/Pod/agent evidence. The maximum
   mirror value alone is NOT a bound. Missing audit intervals, uncertain identities,
   or unsettled writes mean `external_blocked`: do not restore this identity.
   Seek storage/control-plane recovery or a separately designed migration; this
   runbook does not authorize a fresh cluster ID to bypass persisted fences.

## Restore an expired tombstone, never a serving identity

1. Have a second operator review the isolated writer inventory, backups, H, exact
   target identity and recovery manifest. Require 0 <= H <= 2147483645; stop on
   exhaustion because Kubernetes leaseTransitions is signed int32. A proven virgin
   interrupted bootstrap may use H=0; missing evidence cannot justify H=0.
2. Prepare a new `coordination.k8s.io/v1` Lease for the exact missing Router Lease
   name/namespace. Keep the original CR and all guard markers intact. Include:

| Field | Required value |
| --- | --- |
| metadata.annotations["cubejs.io/lease-cluster-id"] | Exact child Router cluster ID |
| metadata.annotations["cubejs.io/lease-generation"] | Verified original generation (normally "1") |
| metadata.annotations["cubejs.io/lease-token"] | Fresh cryptographically random token, never an old token |
| metadata.annotations["cubejs.io/lease-holder-uid"] | Empty string; tombstone is not a Pod |
| spec.holderIdentity | Unique recovery-only name that is not any Router candidate |
| spec.leaseTransitions | H + 1, strictly greater than every prior issued epoch |
| spec.leaseDurationSeconds | 1 |
| spec.acquireTime and spec.renewTime | "1970-01-01T00:00:00.000000Z" |

3. Only after approval, use create-only semantics (for example `kubectl create -f`
   the reviewed manifest), never apply/replace/force. AlreadyExists means STOP and
   reread authority. A timeout means UNKNOWN: read the exact target and compare the
   full proposed identity. If absent, keep all writers isolated and resolve the
   outstanding request before any retry. Never remove guards or increment blindly.
4. Read back the created Lease directly and archive its UID/resourceVersion and
   full identity. Verify expiry, cluster/generation/token, H+1 and preserved guards.
   A modeled old token must fail renewal/fencing; no traffic is authorized yet.

## Resume and acceptance

1. Start only new, identified Router/agent processes and the intended elected
   Manager after infrastructure isolation remains established. The next acquisition
   must issue H+2 with a new token. The tombstone must never appear as serving.
2. Confirm matching authoritative Lease, CR and ConfigMap fences, fresh agent files,
   Router epoch/token-hash acknowledgement and Leader Service EndpointSlice. Keep
   traffic blocked on any disagreement. Confirm old token/epoch operations are
   rejected at both control-plane and business publication boundaries before
   releasing maintenance. Preserve old-node isolation until it is rebuilt or its
   processes/credentials are demonstrably retired.
3. Record old/new identities, cancellation and restart timestamps, query outage,
   mutation rejection and business recovery intervals separately. Test SIGTERM,
   SIGINT, SIGKILL, Manager handover and a paused old controller resuming on the
   frozen release. Local fake-client tests do not measure these live intervals or
   prove storage fencing, Manager multi-process behavior, or recovery under clock
   skew. A Lease read plus another resource's CAS is not a cross-resource atomic
   transaction. Manager election alone is not hard isolation.
4. If recovery fails, keep maintenance/isolation in place and retain the new epoch;
   never roll back to the backup Lease. Escalate with backups and audit evidence.

## Local evidence boundary

Scoped tests model interrupted reservation, competing initializers, committed and
uncommitted Create response loss, Lease deletion, historical CR/ConfigMap/Pod guards,
missing APIReader, independent stale cache/authority, write conflicts, cancelled
reconciliation, healthy adoption and expired recovery tombstones. They cannot prove
that an administrator supplied a complete H or actually isolated a node. There is
deliberately no online administrative recovery command with a self-attested safety
flag. Production execution and end-to-end old-writer rejection remain externally
gated acceptance work.
