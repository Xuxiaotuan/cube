#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../../../" && pwd)"
IMAGE="${RUST_BUILDER_IMAGE:-rust:1.85-bookworm}"

docker run --rm \
  -v "$ROOT:/work" \
  -w /work \
  "$IMAGE" \
  cargo build --release --manifest-path packages/cubejs-backend-native/Cargo.toml

test -f "$ROOT/packages/cubejs-backend-native/target/release/libcubejs_native.so"
echo "Linux native module: $ROOT/packages/cubejs-backend-native/target/release/libcubejs_native.so"
