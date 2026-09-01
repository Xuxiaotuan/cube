#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
KUBECTL="${KUBECTL:-kubectl}"

echo "[1/6] namespace"
"$KUBECTL" apply -f demo/k8s/namespace.yaml
echo "[2/6] CRDs"
"$KUBECTL" apply -f config/crd/bases/cubestore.io_cubestorerouters.yaml
"$KUBECTL" apply -f config/crd/bases/cubestore.io_cubeclusters.yaml
echo "[3/6] object store dependency"
"$KUBECTL" apply -f demo/k8s/minio.yaml
echo "[4/6] operator RBAC"
"$KUBECTL" apply -f demo/k8s/operator-rbac.yaml
echo "[5/6] operator"
"$KUBECTL" apply -f config/manager/manager.yaml
echo "[6/6] CubeCluster"
"$KUBECTL" apply -f demo/k8s/cube-cluster-cr.yaml

echo
echo "CubeCluster submitted. Follow readiness with:"
echo "  kubectl -n cube-operator-demo get cube analytics -w"
echo "  kubectl -n cube-operator-demo get deploy,sts,svc,pdb"
