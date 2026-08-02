#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../../../" && pwd)"
IMAGE="${API_IMAGE:-cube-studio-api:ha-local}"
NATIVE_INDEX_NODE="${NATIVE_INDEX_NODE:-$ROOT/packages/cubejs-backend-native/target/release/libcubejs_native.so}"
CONTEXT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cube-api-context.XXXXXX")"

cleanup() {
  rm -rf "$CONTEXT_DIR"
}
trap cleanup EXIT

if [ ! -f "$NATIVE_INDEX_NODE" ]; then
  "$ROOT/operators/cube-operator/demo/cube-api/build-native.sh"
fi

mkdir -p "$CONTEXT_DIR/packages"
mkdir -p "$CONTEXT_DIR/rust"
for package_name in \
  cubejs-api-gateway \
  cubejs-base-driver \
  cubejs-backend-shared \
  cubejs-cubestore-driver \
  cubejs-query-orchestrator \
  cubejs-schema-compiler \
  cubejs-server \
  cubejs-server-core \
  cubejs-backend-cloud \
  cubejs-templates \
  cubejs-backend-native; do
  rsync -a --exclude node_modules --exclude coverage --exclude target \
    "$ROOT/packages/$package_name/" "$CONTEXT_DIR/packages/$package_name/"
done

# The workspace native module is usually built for macOS. Replace it with the
# Linux ELF built above so the container never receives a host-platform .node.
cp "$NATIVE_INDEX_NODE" "$CONTEXT_DIR/packages/cubejs-backend-native/native/index.node"

# Use the already-installed local dependency tree so the image contains the
# exact workspace build, including the modified CubeStoreDriver, without a
# second registry install inside Docker.
rsync -a --links --exclude .cache "$ROOT/node_modules/" "$CONTEXT_DIR/node_modules/"
rsync -a --exclude node_modules --exclude target "$ROOT/rust/cubestore/" "$CONTEXT_DIR/rust/cubestore/"

cp "$ROOT/operators/cube-operator/demo/cube-api/package.json" "$CONTEXT_DIR/package.json"
cp "$ROOT/operators/cube-operator/demo/cube-api/Dockerfile" "$CONTEXT_DIR/Dockerfile"
cp "$ROOT/operators/cube-operator/demo/cube-api/cube.js" "$CONTEXT_DIR/cube.js"
cp -R "$ROOT/operators/cube-operator/demo/cube-api/schema" "$CONTEXT_DIR/schema"

docker build -t "$IMAGE" "$CONTEXT_DIR"
