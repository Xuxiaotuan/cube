#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

IMAGE="${IMAGE:-cube-operator:dev}"
ROUTER_IMAGE="${ROUTER_IMAGE:-cube-studio-router:ha-local}"
API_IMAGE="${API_IMAGE:-cube-studio-api:ha-local}"
BUILD_API_IMAGE="${BUILD_API_IMAGE:-true}"
KUBECTL="${KUBECTL:-kubectl}"
ROUTER_NAMESPACE="cube-operator-demo"
ROUTER_DEPLOYMENT="cube-router-demo"
KUBECTL_VALIDATE="${KUBECTL_VALIDATE:-auto}"
KUBECTL_VALIDATE_FALLBACK="${KUBECTL_VALIDATE_FALLBACK:-true}"

log() {
  printf '[%s] %s\n' "$(date +'%F %T')" "$*"
}

apply_manifest() {
  local manifest_file="$1"
  if [ "$KUBECTL_VALIDATE" = "false" ]; then
    $KUBECTL apply --validate=false -f "$manifest_file"
    return
  fi
  if [ "$KUBECTL_VALIDATE" = "true" ]; then
    $KUBECTL apply --validate=true -f "$manifest_file"
    return
  fi

  set +e
  $KUBECTL apply --validate=true -f "$manifest_file"
  local rc=$?
  set -e

  if [ $rc -eq 0 ]; then
    return 0
  fi

  if [ "$KUBECTL_VALIDATE_FALLBACK" != "true" ]; then
    return $rc
  fi

  log "WARN: strict validation failed, fallback to --validate=false for ${manifest_file}"
  $KUBECTL apply --validate=false -f "$manifest_file"
}

cat <<'MSG'
[1/7] 安装 CRD / 资源
MSG
apply_manifest config/crd/bases/cubestore.io_cubestorerouters.yaml
apply_manifest demo/k8s/namespace.yaml
apply_manifest demo/k8s/operator-rbac.yaml

cat <<'MSG'
[1.5/7] 部署唯一 authoritative Cubestore MetaStore
MSG
TMP_METASTORE_MANIFEST="$(mktemp)"
sed "s|image: .*|image: ${ROUTER_IMAGE}|g" demo/k8s/metastore.yaml > "$TMP_METASTORE_MANIFEST"
apply_manifest "$TMP_METASTORE_MANIFEST"
rm -f "$TMP_METASTORE_MANIFEST"
$KUBECTL -n "$ROUTER_NAMESPACE" rollout status statefulset/cubestore-metastore --timeout=180s

cat <<'MSG'
[2/7] 构建 Operator 镜像
MSG
if command -v docker >/dev/null 2>&1; then
  docker build -t "$IMAGE" -f "$ROOT/Dockerfile" "$ROOT"
else
  echo "error: docker not found. please install docker or set IMAGE to prebuilt image and skip." >&2
  exit 1
fi

cat <<'MSG'
[2.5/7] 构建真实 Cube API 镜像（包含本地修改后的 CubeStoreDriver）
MSG
if [ "$BUILD_API_IMAGE" = "true" ]; then
  API_IMAGE="$API_IMAGE" "$ROOT/demo/cube-api/build-image.sh"
else
  echo "跳过 Cube API 镜像构建：BUILD_API_IMAGE=${BUILD_API_IMAGE}"
fi

cat <<'MSG'
[3/7] 部署 Operator
MSG
apply_manifest config/manager/manager.yaml

cat <<'MSG'
[3.1/7] 强制重启 Operator 使镜像与本次源码一致
MSG
if $KUBECTL -n "$ROUTER_NAMESPACE" get deploy cube-operator >/dev/null 2>&1; then
  $KUBECTL -n "$ROUTER_NAMESPACE" rollout restart deploy/cube-operator
  $KUBECTL -n "$ROUTER_NAMESPACE" rollout status deploy/cube-operator --timeout=180s
else
  echo "warn: cube-operator deployment 未就绪，等待下一步自动创建"
fi

cat <<'MSG'
[4/7] 检查并部署/升级真实 Router 实例（cube-studio-router）
MSG
TMP_ROUTER_MANIFEST="$(mktemp)"
sed "s|image: .*|image: ${ROUTER_IMAGE}|g" demo/k8s/mock-routers.yaml > "$TMP_ROUTER_MANIFEST"
apply_manifest "$TMP_ROUTER_MANIFEST"
rm -f "$TMP_ROUTER_MANIFEST"
$KUBECTL -n "$ROUTER_NAMESPACE" rollout status deploy/"$ROUTER_DEPLOYMENT" --timeout=180s

cat <<'MSG'
[5/7] 创建 CubestoreRouter CR（主备控制）
MSG
apply_manifest demo/k8s/cubestore-router-cr.yaml

cat <<'MSG'
[6/7] 创建 Leader Service
MSG
apply_manifest demo/k8s/leader-service.yaml

cat <<'MSG'
[6.5/7] 部署 Cube API（Cube API -> Driver -> Leader Service）
MSG
TMP_API_MANIFEST="$(mktemp)"
sed "s|image: .*|image: ${API_IMAGE}|g" demo/k8s/cube-api.yaml > "$TMP_API_MANIFEST"
apply_manifest "$TMP_API_MANIFEST"
rm -f "$TMP_API_MANIFEST"
$KUBECTL -n "$ROUTER_NAMESPACE" rollout status deploy/cube-api-demo --timeout=240s

cat <<'MSG'
[7/7] 观察状态
MSG
$KUBECTL -n cube-operator-demo get pods -l app=cube-router -o wide
$KUBECTL -n cube-operator-demo get cubestorerouter demo -o yaml
$KUBECTL -n cube-operator-demo get service cube-router-leader -o wide
$KUBECTL -n cube-operator-demo get service cube-api -o wide

cat <<'MSG'
演示：
1) 查看标签（leader/follower）：
   kubectl -n cube-operator-demo get pod -l app=cube-router --show-labels
2) 查看 operator 日志：
   kubectl -n cube-operator-demo logs deploy/cube-operator -f
3) 切主演示：
   kubectl -n cube-operator-demo get pod -l app=cube-router -l cubestore.io/router-role=leader -o custom-columns='NAME:.metadata.name'
   kubectl -n cube-operator-demo delete pod <leader-pod-name>
4) 观察漂移：
   kubectl -n cube-operator-demo get cubestorerouter demo -o yaml
MSG
