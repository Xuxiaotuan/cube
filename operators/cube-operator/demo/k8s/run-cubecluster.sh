#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${NAMESPACE:-cube-ha-remediation}"
[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] && [ "${#NAMESPACE}" -le 63 ] || {
  echo 'Invalid NAMESPACE' >&2; exit 1;
}
apply_namespaced() {
  # Also rewrites Service DNS, secret namespaces, and WATCH_NAMESPACE. Source
  # manifests and any existing namespace are left untouched.
  sed "s/cube-operator-demo/$NAMESPACE/g" "$1" | "$KUBECTL" apply -f -
}

echo "[1/6] namespace"
apply_namespaced demo/k8s/namespace.yaml
echo "[2/6] CRDs"
"$KUBECTL" apply -f config/crd/bases/cubestore.io_cubestorerouters.yaml
"$KUBECTL" apply -f config/crd/bases/cubestore.io_cubeclusters.yaml
echo "[3/6] object store dependency"
apply_namespaced demo/k8s/minio.yaml
echo "[4/6] operator RBAC"
apply_namespaced demo/k8s/operator-rbac.yaml
echo "[5/6] operator"
apply_namespaced config/manager/manager.yaml
echo "[6/6] CubeCluster"
apply_namespaced demo/k8s/cube-cluster-cr.yaml

echo
echo "CubeCluster submitted. Follow readiness with:"
echo "  kubectl -n $NAMESPACE get cube analytics -w"
echo "  kubectl -n $NAMESPACE get deploy,sts,svc,pdb"
