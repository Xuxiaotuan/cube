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
  echo "Missing Linux native module: $NATIVE_INDEX_NODE. Build it separately or set NATIVE_INDEX_NODE." >&2
  exit 1
fi

# The parent rebuilds packages first. Refuse missing/stale TypeScript outputs
# instead of silently shipping an old lib/ tree beside current dist/ code.
node "$ROOT/operators/cube-operator/demo/cube-api/check-build-inputs.js" "$ROOT" "$NATIVE_INDEX_NODE"

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
  rsync -a --exclude node_modules --exclude coverage --exclude target --exclude /lib \
    "$ROOT/packages/$package_name/" "$CONTEXT_DIR/packages/$package_name/"
done

# The workspace native module is usually built for macOS. Replace it with the
# Linux ELF built above so the container never receives a host-platform .node.
mkdir -p "$CONTEXT_DIR/packages/cubejs-backend-native/native"
cp "$NATIVE_INDEX_NODE" "$CONTEXT_DIR/packages/cubejs-backend-native/native/index.node"

# Use the already-installed local dependency tree so the image contains the
# exact workspace build, including the modified CubeStoreDriver, without a
# second registry install inside Docker.
rsync -a --links --exclude .cache "$ROOT/node_modules/" "$CONTEXT_DIR/node_modules/"
# Link every included workspace package explicitly. An installed registry copy
# or an absolute host symlink must not win over the source build in the image.
for package_dir in "$CONTEXT_DIR"/packages/*; do
  package_name="$(node -p 'require(process.argv[1]).name' "$package_dir/package.json")"
  rm -rf "$CONTEXT_DIR/node_modules/$package_name"
  ln -s "../../packages/$(basename "$package_dir")" "$CONTEXT_DIR/node_modules/$package_name"
done
rsync -a --exclude node_modules --exclude target "$ROOT/rust/cubestore/" "$CONTEXT_DIR/rust/cubestore/"

cp "$ROOT/operators/cube-operator/demo/cube-api/package.json" "$CONTEXT_DIR/package.json"
cp "$ROOT/operators/cube-operator/demo/cube-api/Dockerfile" "$CONTEXT_DIR/Dockerfile"
cp "$ROOT/operators/cube-operator/demo/cube-api/cube.js" "$CONTEXT_DIR/cube.js"
cp "$ROOT/operators/cube-operator/demo/cube-api/ha-fault-proxy.js" "$CONTEXT_DIR/ha-fault-proxy.js"
cp "$ROOT/operators/cube-operator/demo/cube-api/ha-scheduled-contexts.js" "$CONTEXT_DIR/ha-scheduled-contexts.js"
cp -R "$ROOT/operators/cube-operator/demo/cube-api/schema" "$CONTEXT_DIR/schema"

docker build -t "$IMAGE" "$CONTEXT_DIR"
