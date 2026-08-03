#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
DRIVER_DIR="$ROOT_DIR/packages/cubejs-cubestore-driver"
OP_DIR="$ROOT_DIR/operators/cube-operator"
RUST_DIR="$ROOT_DIR/rust/cubestore"

log() {
  printf '\n==> %s\n' "$*"
}

run_leadership_cas() {
  local missing=()
  [[ -n "${REDIS_URL:-}" ]] || missing+=(REDIS_URL)
  [[ -n "${POSTGRES_URL:-}" ]] || missing+=(POSTGRES_URL)
  if (( ${#missing[@]} > 0 )); then
    printf 'ERROR: --leadership-cas requires %s; real backend tests will not be skipped (exit 2).\n' \
      "${missing[*]}" >&2
    return 2
  fi

  log "Run real Redis/PostgreSQL leadership CAS integration tests"
  if ! (cd "$OP_DIR" && go test -count=1 ./internal/leadership); then
    printf 'ERROR: leadership CAS integration tests failed (exit 1).\n' >&2
    return 1
  fi
  log "Leadership CAS integration tests passed"
  return 0
}

if [[ "${1:-}" == "--leadership-cas" ]]; then
  if [[ "$#" -ne 1 ]]; then
    printf 'ERROR: --leadership-cas does not accept additional arguments (exit 2).\n' >&2
    exit 2
  fi
  run_leadership_cas
  exit $?
fi

run() {
  local cmd="$1"
  log "$cmd"
  if ! bash -lc "$cmd"; then
    echo "Command failed: $cmd" >&2
    exit 1
  fi
}

log "Step 1: install/workspace deps (stable mode)"
cd "$DRIVER_DIR"
# Skip lifecycle scripts to avoid environment-related postinstall (binary download / optional native build) failures.
yarn install --ignore-scripts

action() {
  run "$1"
}

log "Step 2: build dependency workspaces"
action "cd '$ROOT_DIR' && yarn workspace @cubejs-backend/shared build"
action "cd '$ROOT_DIR' && yarn workspace @cubejs-backend/base-driver build"
action "cd '$ROOT_DIR' && yarn workspace @cubejs-backend/cubestore build"
action "cd '$ROOT_DIR' && yarn workspace @cubejs-backend/native build"

action "cd '$DRIVER_DIR' && yarn tsc"
action "cd '$DRIVER_DIR' && yarn lint"
action "cd '$DRIVER_DIR' && yarn workspace @cubejs-backend/cubestore-driver build"

action "cd '$OP_DIR' && go test ./..."
action "cd '$RUST_DIR' && cargo check -p cubestore"

log "HA validation passed"
