# Read-only production prerequisites

This collector performs only Kubernetes GET requests. It never reads Secrets or
exports container environment values, changes workloads, deletes Pods, or changes
the current context. Specify the target context and namespace explicitly.

```bash
python3 operators/cube-operator/demo/k8s/production-preflight.py \
  --context orbstack --namespace cube-ha-remediation --cluster analytics \
  > /tmp/cube-production-preflight.json
```

Supply `--rpo-seconds` and `--rto-seconds` only after the deployment owner has
agreed to those targets. Omitting them is a blocker, not a default assumption.

Exit codes: 0 means the limited static prerequisites passed; 1 means observed
blockers; 2 means incomplete collection. Every result has `productionGo: false`.
Even `PREFLIGHT_PASS` is not authorization to deploy or a production certificate.

The report checks CubeCluster-labeled live Pod roles, Ready state, Router node
placement, digest-pinned container references, mounted PVC availability and
storage provisioners, presence of namespace NetworkPolicies, and Operator image
pinning. Terminating workloads block the check. Its multi-request observation is
not an atomic cluster snapshot; use it outside rollouts and retain the timestamp.

NetworkPolicy presence does not prove effective ingress restrictions or CNI
enforcement. Non-local storage does not prove attach/fencing/restore behavior.
Separate nodes do not prove independent physical failure domains. This collector
does not discover external object-store durability or certify StatefulSet PVCs
that are not currently mounted by observed Pods. No storage recovery is attempted.

Complete business recovery, protocol compatibility, RBAC, endpoint authentication,
drain behavior, actual RPO/RTO, and disaster recovery still require their dedicated
acceptance evidence. Preserve this report alongside that evidence rather than
promoting a static check into a full release verdict.

```bash
python3 operators/cube-operator/demo/k8s/production-preflight.test.py
```
