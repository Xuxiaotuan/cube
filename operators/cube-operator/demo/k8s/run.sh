#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

IMAGE="${IMAGE:-cube-operator:dev}"
ROUTER_IMAGE="${ROUTER_IMAGE:-cube-studio-router:ha-local}"
WORKER_IMAGE="${WORKER_IMAGE:-$ROUTER_IMAGE}"
LEASE_AGENT_IMAGE="${LEASE_AGENT_IMAGE:-cube-operator:dev}"
REDIS_URL="${REDIS_URL:-redis://redis.cube-operator-demo.svc:6379/0}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
META_STORE_ADDRESS="${META_STORE_ADDRESS:-cubestore-metastore.cube-operator-demo.svc:9999}"
API_IMAGE="${API_IMAGE:-cube-studio-api:ha-local}"
BUILD_API_IMAGE="${BUILD_API_IMAGE:-true}"
BUILD_OPERATOR_IMAGE="${BUILD_OPERATOR_IMAGE:-true}"
KUBECTL="${KUBECTL:-kubectl}"
ROUTER_NAMESPACE="cube-operator-demo"
ROUTER_DEPLOYMENT="cube-router-demo"
KUBECTL_VALIDATE="${KUBECTL_VALIDATE:-auto}"
KUBECTL_VALIDATE_FALLBACK="${KUBECTL_VALIDATE_FALLBACK:-true}"
DRY_RUN="false"

render_router_manifest() {
  local source="$1"
  local target="$2"
  sed \
    -e "s|\${ROUTER_IMAGE}|${ROUTER_IMAGE}|g" \
    -e "s|\${LEASE_AGENT_IMAGE}|${LEASE_AGENT_IMAGE}|g" \
    -e "s|\${REDIS_URL}|${REDIS_URL}|g" \
    -e "s|\${REDIS_PASSWORD}|${REDIS_PASSWORD}|g" \
    -e "s|\${META_STORE_ADDRESS}|${META_STORE_ADDRESS}|g" \
    "$source" > "$target"
}

case "${1:-}" in
  "") ;;
  --dry-run)
    DRY_RUN="true"
    shift
    ;;
  *)
    echo "usage: $0 [--dry-run]" >&2
    exit 2
    ;;
esac

log() {
  printf '[%s] %s\n' "$(date +'%F %T')" "$*"
}

run_dry_run() {
  local tmp_dir
  local metastore_manifest
  local worker_manifest
  local router_manifest
  local index=0
  local metastore_index=-1
  local worker_index=-1
  local router_index=-1
  tmp_dir="$(mktemp -d)"

  metastore_manifest="$tmp_dir/metastore.yaml"
  worker_manifest="$tmp_dir/workers.yaml"
  router_manifest="$tmp_dir/routers.yaml"
  sed "s|image: .*|image: ${ROUTER_IMAGE}|g" demo/k8s/metastore.yaml > "$metastore_manifest"
  sed "s|image: .*|image: ${WORKER_IMAGE}|g" demo/k8s/mock-workers.yaml > "$worker_manifest"
  render_router_manifest demo/k8s/mock-routers.yaml "$router_manifest"

  for manifest in \
    demo/k8s/namespace.yaml \
    demo/k8s/operator-rbac.yaml \
    "$metastore_manifest" \
    "$worker_manifest" \
    "$router_manifest"; do
    "$KUBECTL" create --dry-run=client -f "$manifest" >/dev/null
    index=$((index + 1))
    case "$manifest" in
      "$metastore_manifest") metastore_index=$index ;;
      "$worker_manifest") worker_index=$index ;;
      "$router_manifest") router_index=$index ;;
    esac
  done

  if (( metastore_index >= worker_index || worker_index >= router_index )); then
    echo "dry-run ordering failure: expected metastore -> workers -> routers" >&2
    rm -rf "$tmp_dir"
    return 1
  fi
  if ! rg -q 'name: CUBESTORE_META_ADDR' "$worker_manifest" || \
     ! rg -q 'name: CUBESTORE_META_ADDR' "$router_manifest"; then
    echo "dry-run configuration failure: workers and routers must consume CUBESTORE_META_ADDR" >&2
    rm -rf "$tmp_dir"
    return 1
  fi
  if ! rg -q 'name: CUBESTORE_WORKERS' "$worker_manifest" || \
     ! rg -q 'name: CUBESTORE_WORKERS' "$router_manifest"; then
    echo "dry-run configuration failure: workers and routers must consume CUBESTORE_WORKERS" >&2
    rm -rf "$tmp_dir"
    return 1
  fi
  if ! rg -q 'volumeClaimTemplates:' "$worker_manifest" || \
     ! rg -q 'serviceAccountName: cube-worker' "$worker_manifest" || \
     ! rg -q 'kind: RoleBinding' "$worker_manifest"; then
    echo "dry-run configuration failure: workers require PVC templates and scoped identity" >&2
    rm -rf "$tmp_dir"
    return 1
  fi
  if ! rg -q 'replicas: 1' "$metastore_manifest"; then
    echo "dry-run configuration failure: MetaStore must remain single-writer" >&2
    rm -rf "$tmp_dir"
    return 1
  fi

  printf 'dry-run: manifests valid; upgrade order verified: metastore -> workers -> routers\n'
  rm -rf "$tmp_dir"
}

if [[ "$DRY_RUN" == "true" ]]; then
  run_dry_run
  exit $?
fi

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

apply_redis_secret() {
  local secret_manifest
  secret_manifest="$(mktemp)"
  "$KUBECTL" -n "$ROUTER_NAMESPACE" create secret generic cube-router-demo-lease-store \
    --from-literal=dsn="$REDIS_URL" \
    --from-literal=url="$REDIS_URL" \
    --from-literal=password="$REDIS_PASSWORD" \
    --dry-run=client -o yaml > "$secret_manifest"
  apply_manifest "$secret_manifest"
  rm -f "$secret_manifest"
}

cat <<'MSG'
[1/7] 安装 CRD / 资源
MSG
apply_manifest config/crd/bases/cubestore.io_cubestorerouters.yaml
apply_manifest demo/k8s/namespace.yaml
apply_manifest demo/k8s/operator-rbac.yaml

cat <<'MSG'
[1.25/7] 部署 demo Redis（lease-agent 与 Operator 共用）
MSG
apply_manifest demo/k8s/redis.yaml
$KUBECTL -n "$ROUTER_NAMESPACE" rollout status deploy/redis --timeout=120s

cat <<'MSG'
[1.4/7] 部署共享 CubeStore 对象存储（MinIO）
MSG
apply_manifest demo/k8s/minio.yaml
$KUBECTL -n "$ROUTER_NAMESPACE" rollout status deploy/cube-router-object-store --timeout=180s
$KUBECTL -n "$ROUTER_NAMESPACE" wait --for=condition=complete job/cube-router-object-store-init --timeout=180s

cat <<'MSG'
[1.5/7] 部署唯一 authoritative Cubestore MetaStore
MSG
TMP_METASTORE_MANIFEST="$(mktemp)"
sed "s|image: .*|image: ${ROUTER_IMAGE}|g" demo/k8s/metastore.yaml > "$TMP_METASTORE_MANIFEST"
apply_manifest "$TMP_METASTORE_MANIFEST"
rm -f "$TMP_METASTORE_MANIFEST"
$KUBECTL -n "$ROUTER_NAMESPACE" rollout status statefulset/cubestore-metastore --timeout=180s

cat <<'MSG'
[1.75/7] 部署 Cubestore Workers（统一连接 authoritative MetaStore）
MSG
TMP_WORKER_MANIFEST="$(mktemp)"
sed "s|image: .*|image: ${WORKER_IMAGE}|g" demo/k8s/mock-workers.yaml > "$TMP_WORKER_MANIFEST"
apply_manifest "$TMP_WORKER_MANIFEST"
rm -f "$TMP_WORKER_MANIFEST"
$KUBECTL -n "$ROUTER_NAMESPACE" rollout status statefulset/cube-worker-demo --timeout=180s

cat <<'MSG'
[2/7] 构建 Operator 镜像
MSG
if [ "$BUILD_OPERATOR_IMAGE" = "true" ]; then
  if command -v docker >/dev/null 2>&1; then
    docker build -t "$IMAGE" -f "$ROOT/Dockerfile" "$ROOT"
  else
    echo "error: docker not found. please install docker or set BUILD_OPERATOR_IMAGE=false with a prebuilt image." >&2
    exit 1
  fi
else
  log "跳过 Operator 镜像构建：BUILD_OPERATOR_IMAGE=${BUILD_OPERATOR_IMAGE}，复用 ${IMAGE}"
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
render_router_manifest demo/k8s/mock-routers.yaml "$TMP_ROUTER_MANIFEST"
apply_manifest "$TMP_ROUTER_MANIFEST"
rm -f "$TMP_ROUTER_MANIFEST"
apply_redis_secret
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
