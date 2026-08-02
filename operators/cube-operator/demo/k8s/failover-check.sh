#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cube-operator-demo}"
KUBECTL="${KUBECTL:-kubectl}"
CR_NAME="${CR_NAME:-demo}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
WAIT_FOR_STANDBY_READY="${WAIT_FOR_STANDBY_READY:-true}"
VERIFY_LEADER_STATUS_HTTP="${VERIFY_LEADER_STATUS_HTTP:-false}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-2}"
LEADER_SETTLE_STABLE_CHECKS="${LEADER_SETTLE_STABLE_CHECKS:-2}"
REQUIRE_LEADER_ELECTION_CONDITION="${REQUIRE_LEADER_ELECTION_CONDITION:-true}"
REQUIRE_LEADER_EPOCH="${REQUIRE_LEADER_EPOCH:-true}"

log() { printf '[%s] %s\n' "$(date +'%F %T')" "$*"; }

first_non_empty() {
  printf '%s' "$1" | awk 'NF{print; exit}'
}

count_non_empty_lines() {
  printf '%s\n' "$1" | awk 'NF{cnt+=1} END {print cnt+0}'
}

is_single_line() {
  local value="$1"
  local expected="$2"
  local count
  local first

  count="$(count_non_empty_lines "$value")"
  if [ "$count" != "1" ]; then
    return 1
  fi

  first="$(first_non_empty "$value")"
  [ "$first" = "$expected" ]
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: required command '$cmd' not found" >&2
    exit 1
  fi
}

require_cmd "$KUBECTL"
require_cmd "jq"

log "检查命名空间：$NAMESPACE"
$KUBECTL -n "$NAMESPACE" get namespace "$NAMESPACE" >/dev/null

list_leader_pods() {
  $KUBECTL -n "$NAMESPACE" get pod \
    -l app=cube-router,cubestore.io/router-role=leader \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
}

get_status_leader() {
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leader}'
}

get_status_leader_epoch() {
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leaderEpoch}'
}

get_leader_service_endpoints() {
  $KUBECTL -n "$NAMESPACE" get endpoints cube-router-leader -o jsonpath='{range .subsets[*].addresses[*]}{.targetRef.name}{"\n"}{end}'
}

get_leader_condition_status() {
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o json \
    | jq -r '
      (.status.conditions // [])
      | map(select(.type == "LeaderElection"))[0]
      | .status // empty
    '
}

is_pod_ready() {
  local pod="$1"
  local ready
  ready="$($KUBECTL -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  [ "$ready" = "True" ]
}

get_candidate_standby_pod() {
  local old="${1:-}"
  local candidates
  candidates="$(
    $KUBECTL -n "$NAMESPACE" get pod -l app=cube-router -o json \
    | jq -r --arg old "$old" '
      .items[]
      | select(.metadata.name != $old and .status.phase == "Running")
      | select(.metadata.labels["cubestore.io/router-role"] == "follower")
      | select((.status.containerStatuses // [] | map(.ready) | index(true)) != null)
      | .metadata.name
    ' \
    | head -n 1
  )"

  if [ -n "$candidates" ]; then
    echo "$candidates"
    return 0
  fi

  # 回退策略：若 follower 标签暂时未刷新，兜底选择任一其他 ready Pod（避免卡死）
  $KUBECTL -n "$NAMESPACE" get pod -l app=cube-router -o json \
    | jq -r --arg old "$old" '
      .items[]
      | select(.metadata.name != $old and .status.phase == "Running")
      | select((.status.containerStatuses // [] | map(.ready) | index(true)) != null)
      | .metadata.name
    ' \
    | head -n 1
}

check_router_status_http() {
  local pod="$1"
  if [ "$VERIFY_LEADER_STATUS_HTTP" != "true" ]; then
    return 0
  fi

  if ! $KUBECTL -n "$NAMESPACE" exec "$pod" -- sh -c 'command -v curl >/dev/null 2>&1'; then
    log "pod ${pod} 中未检测到 curl，跳过 HTTP 状态检查"
    return 0
  fi

  if ! $KUBECTL -n "$NAMESPACE" exec "$pod" -- \
    sh -c "curl -fsS --max-time 3 'http://127.0.0.1:3030/router/status?detail=1' >/dev/null"; then
    log "pod ${pod} 路由状态 HTTP 检查失败，继续执行"
    return 1
  fi

  return 0
}

wait_for_ready_standby() {
  local old="$1"
  local waited=0
  while [ "$waited" -lt "$WAIT_SECONDS" ]; do
    local ready_standby
    ready_standby="$(get_candidate_standby_pod)"
    if [ -n "$ready_standby" ] && [ "$ready_standby" != "$old" ]; then
      echo "$ready_standby"
      return 0
    fi
    sleep "$CHECK_INTERVAL_SECONDS"
    waited=$((waited + CHECK_INTERVAL_SECONDS))
  done
  return 1
}

wait_for_changed_leader() {
  local old="$1"
  local stable=0
  local waited=0
  while [ "$waited" -lt "$WAIT_SECONDS" ]; do
    local cur
    cur="$(get_status_leader)"
    if [ -n "$cur" ] && [ "$cur" != "$old" ] && is_pod_ready "$cur"; then
      if check_router_status_http "$cur" >/dev/null 2>&1; then
        local endpoints
        local leaders
        endpoints="$(get_leader_service_endpoints)"
        leaders="$(list_leader_pods)"

        if is_single_line "$endpoints" "$cur" && is_single_line "$leaders" "$cur"; then
          stable=$((stable + 1))
          if [ "$stable" -ge "$LEADER_SETTLE_STABLE_CHECKS" ]; then
            echo "$cur"
            return 0
          fi
        else
          stable=0
        fi
      else
        stable=0
      fi
    fi

    cur="$(first_non_empty "$(list_leader_pods)")"
    if [ -n "$cur" ] && [ "$cur" != "$old" ] && is_pod_ready "$cur"; then
      local endpoints
      endpoints="$(get_leader_service_endpoints)"
      if is_single_line "$endpoints" "$cur"; then
        stable=$((stable + 1))
        if [ "$stable" -ge "$LEADER_SETTLE_STABLE_CHECKS" ]; then
          echo "$cur"
          return 0
        fi
      else
        stable=0
      fi
    fi

    sleep "$CHECK_INTERVAL_SECONDS"
    waited=$((waited + CHECK_INTERVAL_SECONDS))
  done
  return 1
}

ensure_leader_condition_ok() {
  if [ "$REQUIRE_LEADER_ELECTION_CONDITION" != "true" ]; then
    return 0
  fi

  local condition
  condition="$(get_leader_condition_status)"
  if [ -z "$condition" ]; then
    echo "warn: LeaderElection condition not set yet"
    return 1
  fi

  [ "$condition" = "True" ]
}

ensure_leader_epoch_ok() {
  if [ "$REQUIRE_LEADER_EPOCH" != "true" ]; then
    return 0
  fi

  local epoch
  epoch="$(get_status_leader_epoch)"
  if [ -z "$epoch" ] || [ "$epoch" = "0" ] || [ "$epoch" = "null" ]; then
    echo "error: CR status.leaderEpoch is 0/null; leader epoch 未持久化"
    return 1
  fi
}

log_leader_condition_if_needed() {
  if [ "$REQUIRE_LEADER_ELECTION_CONDITION" != "true" ]; then
    return 0
  fi

  local condition
  condition="$(get_leader_condition_status)"
  if [ -z "$condition" ]; then
    echo "warn: LeaderElection condition not set yet (non-blocking)"
    return 0
  fi

  if [ "$condition" != "True" ]; then
    echo "warn: LeaderElection condition is ${condition} (non-blocking)"
  fi
}

log "读取当前 leader"
leader_before="$(get_status_leader)"
if [ -z "$leader_before" ]; then
  leader_before="$(first_non_empty "$(list_leader_pods)")"
fi
status_before="$(get_status_leader)"
epoch_before="$(get_status_leader_epoch)"
log "当前 leaderEpoch: ${epoch_before:-null}"
if [ -z "$leader_before" ]; then
  echo "error: 无 leader pod 可用，先等待 router 上线。" >&2
  exit 1
fi
if ! ensure_leader_epoch_ok; then
  exit 1
fi
if ! ensure_leader_condition_ok; then
  exit 1
fi
log "当前 leader Pod: ${leader_before}, CR status.leader=${status_before}"

log "查看 endpoints (leader service)"
$KUBECTL -n "$NAMESPACE" get endpoints cube-router-leader -o wide

if [ "$WAIT_FOR_STANDBY_READY" = "true" ]; then
  log "等待可用 follower，就绪后再执行故障切换"
  standby_pod="$(wait_for_ready_standby "$leader_before")" || {
    echo "error: 切换前未能等待到可用 follower（${WAIT_SECONDS}s）。" >&2
    $KUBECTL -n "$NAMESPACE" get pod -l app=cube-router --show-labels
    exit 1
  }
  log "检测到可用 follower: ${standby_pod}"
fi

echo "杀掉当前 leader Pod: ${leader_before}"
$KUBECTL -n "$NAMESPACE" delete pod "$leader_before"

log "等待新 leader 产生（最多 ${WAIT_SECONDS}s）"
new_leader="$(wait_for_changed_leader "$leader_before")" || {
  echo "error: leader 切换超时（${WAIT_SECONDS}s）" >&2
  $KUBECTL -n "$NAMESPACE" get pod -l app=cube-router --show-labels
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o yaml
  exit 1
}

status_after="$(get_status_leader)"
log "切换完成，新 leader Pod: ${new_leader}, CR status.leader=${status_after}"

log "再次检查 leader service 端点"
$KUBECTL -n "$NAMESPACE" get endpoints cube-router-leader -o wide
$KUBECTL -n "$NAMESPACE" get pod -l app=cube-router -o wide

current_leader="$(first_non_empty "$(get_status_leader)")"
leader_endpoints="$(get_leader_service_endpoints)"
leader_pods="$(list_leader_pods)"

echo "验证："
echo "- 旧 leader Pod 已被移除：$(if $KUBECTL -n "$NAMESPACE" get pod \"$leader_before\" >/dev/null 2>&1; then echo no; else echo yes; fi)"
echo "- CR status.leader: $current_leader"
if is_single_line "$leader_endpoints" "$current_leader"; then
  echo "- leader service endpoint 与 CR status 一致：yes"
else
  echo "- leader service endpoint 与 CR status 一致：no"
fi
if is_single_line "$leader_pods" "$current_leader"; then
  echo "- 仅有一个 leader 标签实例：yes"
else
  echo "- 仅有一个 leader 标签实例：no"
fi
if [ -n "$leader_endpoints" ] && is_single_line "$leader_endpoints" "$current_leader"; then
  echo "- Service endpoint 可直接落到新 leader：yes"
else
  echo "- Service endpoint 可直接落到新 leader：no"
fi

if is_single_line "$leader_endpoints" "$current_leader" && is_single_line "$leader_pods" "$current_leader"; then
  log "主备切换完成且入口对齐，空列表窗口已避免（或已收敛）"
  log_leader_condition_if_needed
  if ! ensure_leader_condition_ok; then
    exit 1
  fi
  if ! ensure_leader_epoch_ok; then
    exit 1
  fi
else
  exit 1
fi
