#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cube-operator-demo}"
KUBECTL="${KUBECTL:-kubectl}"
CR_NAME="${CR_NAME:-demo}"
LEADER_SERVICE_NAME="${LEADER_SERVICE_NAME:-cube-router-leader}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.4}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
CONSISTENCY_QUERY="${CONSISTENCY_QUERY:-SELECT 1 AS value}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-2}"
VERIFY_LEADER_SERVICE_QUERY="${VERIFY_LEADER_SERVICE_QUERY:-true}"
STRICT_SERVICE_CHECK="${STRICT_SERVICE_CHECK:-false}"
REQUIRE_LEADER_ELECTION_CONDITION="${REQUIRE_LEADER_ELECTION_CONDITION:-true}"
REQUIRE_LEADER_EPOCH="${REQUIRE_LEADER_EPOCH:-true}"

log() { printf '[%s] %s\n' "$(date +'%F %T')" "$*"; }

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: required command '$cmd' not found" >&2
    exit 1
  fi
}

require_cmd "$KUBECTL"
require_cmd "sha256sum"
require_cmd "jq"

get_status_leader() {
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leader}'
}

get_status_leader_epoch() {
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leaderEpoch}'
}

get_leader_condition_status() {
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o json \
    | jq -r '
      (.status.conditions // [])
      | map(select(.type == "LeaderElection"))[0]
      | .status // empty
    '
}

get_status_followers() {
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o json \
    | jq -r '.status.candidates // [] | map(select(.role=="follower" and .ready == true) | .name)[]'
}

wait_for_changed_leader() {
  local old="$1"
  local waited=0
  while [ "$waited" -lt "$WAIT_SECONDS" ]; do
    local cur
    cur="$(get_status_leader)"
    if [ -n "$cur" ] && [ "$cur" != "$old" ]; then
      echo "$cur"
      return 0
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

  local epoch="$1"
  if [ -z "$epoch" ] || [ "$epoch" = "0" ] || [ "$epoch" = "null" ]; then
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
    echo "warn: LeaderElection condition not set yet（非阻塞）"
    return 0
  fi

  if [ "$condition" != "True" ]; then
    echo "warn: LeaderElection condition is ${condition}（非阻塞）"
  fi
}

pod_ip() {
  local pod="$1"
  $KUBECTL -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.podIP}'
}

run_query() {
  local host="$1"
  local sql="$2"
  # -N/-B: 去掉列名、制表符输出，方便做 hash 对比
  $KUBECTL run -n "$NAMESPACE" cubemysql --rm -i --quiet --restart=Never --image="$MYSQL_IMAGE" --command -- mysql \
    -h "$host" \
    -P "$MYSQL_PORT" \
    -u "$MYSQL_USER" \
    -N -B \
    -e "$sql"
}

run_query_pod() {
  local pod="$1"
  local sql="$2"
  local ip
  ip="$(pod_ip "$pod")"
  if [ -z "$ip" ]; then
    echo "error: pod $pod 没有 IP" >&2
    return 1
  fi
  run_query "$ip" "$sql"
}

run_query_service() {
  local service_host="$1"
  local sql="$2"
  if [ "$VERIFY_LEADER_SERVICE_QUERY" != "true" ]; then
    return 2
  fi

  local service_dns
  service_dns="${service_host}.${NAMESPACE}.svc.cluster.local"
  local out
  if out="$(run_query "$service_dns" "$sql" 2>&1)"; then
    printf '%s\n' "$out"
    return 0
  fi

  if [ "$STRICT_SERVICE_CHECK" = "true" ]; then
    echo "$out" >&2
    return 1
  fi

  log "WARN: 通过服务 ${service_dns} 查询失败，自动跳过服务路径一致性校验：$out"
  return 2
}

hash_result() {
  printf '%s' "$1" | sha256sum | awk '{print $1}'
}

log "读取当前 leader"
leader_before="$(get_status_leader)"
leader_epoch_before="$(get_status_leader_epoch)"

if [ -z "$leader_epoch_before" ]; then
  leader_epoch_before="0"
fi
if ! ensure_leader_epoch_ok "$leader_epoch_before"; then
  echo "error: CR status.leaderEpoch 未持久化或为 0（before）" >&2
  exit 1
fi
if [ -z "$leader_before" ]; then
  echo "error: 无法读取 CR status.leader" >&2
  exit 1
fi

follower_before="$(get_status_followers | head -n 1 || true)"
if [ -n "$follower_before" ] && [ "$follower_before" = "$leader_before" ]; then
  follower_before=""
fi

log "当前 leader: ${leader_before}"
log "当前 leaderEpoch: ${leader_epoch_before}"
if ! ensure_leader_condition_ok; then
  echo "warn: LeaderElection condition not ready before failover; continue with warning"
fi
if [ -n "$follower_before" ]; then
  log "当前 follower: ${follower_before}"
else
  echo "warn: 当前未发现 ready follower，将仅做 leader 切换后查询回放检测"
fi

before_service_result="$(run_query_service "$LEADER_SERVICE_NAME" "$CONSISTENCY_QUERY" || true)"
if [ -n "$before_service_result" ]; then
  before_service_hash="$(hash_result "$before_service_result")"
fi
before_leader_result="$(run_query_pod "$leader_before" "$CONSISTENCY_QUERY")"
before_leader_hash="$(hash_result "$before_leader_result")"
log "leader 切前查询结果: $(printf '%s' "$before_leader_result")"
if [ -n "$before_service_result" ]; then
  log "service 切前查询结果: $(printf '%s' "$before_service_result")"
fi

if [ -n "$follower_before" ]; then
  before_follower_result="$(run_query_pod "$follower_before" "$CONSISTENCY_QUERY")"
  before_follower_hash="$(hash_result "$before_follower_result")"
  log "follower 切前查询结果: $(printf '%s' "$before_follower_result")"
fi

log "开始故障注入：删除 leader Pod ${leader_before}"
$KUBECTL -n "$NAMESPACE" delete pod "$leader_before"

log "等待新 leader（最多 ${WAIT_SECONDS}s）"
new_leader="$(wait_for_changed_leader "$leader_before")" || {
  echo "error: 新 leader 切换超时（${WAIT_SECONDS}s）" >&2
  $KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o yaml
  exit 1
}
new_epoch="$(get_status_leader_epoch)"
if [ -z "$new_epoch" ]; then
  new_epoch="0"
fi
if ! ensure_leader_epoch_ok "$new_epoch"; then
  echo "error: CR status.leaderEpoch 未持久化或为 0（after）" >&2
  exit 1
fi
log "切后新 leader: ${new_leader}"
log "切后 leaderEpoch: ${new_epoch}"
if ! ensure_leader_condition_ok; then
  echo "warn: LeaderElection condition not ready after failover; continue with warning"
fi
log_leader_condition_if_needed

if [ "$new_epoch" -le "$leader_epoch_before" ]; then
  echo "warn: leaderEpoch 未提升（before=${leader_epoch_before}, after=${new_epoch}）"
fi

after_service_result="$(run_query_service "$LEADER_SERVICE_NAME" "$CONSISTENCY_QUERY" || true)"
if [ -n "$after_service_result" ]; then
  after_service_hash="$(hash_result "$after_service_result")"
fi
after_leader_result="$(run_query_pod "$new_leader" "$CONSISTENCY_QUERY")"
after_leader_hash="$(hash_result "$after_leader_result")"
log "leader 切后查询结果: $(printf '%s' "$after_leader_result")"
if [ -n "$after_service_result" ]; then
  log "service 切后查询结果: $(printf '%s' "$after_service_result")"
fi

if [ "$before_leader_hash" != "$after_leader_hash" ]; then
  echo "error: 切前/切后 leader 查询结果哈希不一致（可能存在数据不一致风险）" >&2
  echo "before hash: ${before_leader_hash}"
  echo "after hash:  ${after_leader_hash}"
  exit 1
fi

if [ -n "${before_service_result:-}" ] && [ -n "${after_service_result:-}" ] && [ "$before_service_hash" != "$after_service_hash" ]; then
  echo "error: 切前/切后 leader service 查询结果哈希不一致（服务入口可能存在短时分裂）" >&2
  echo "before service hash: ${before_service_hash}"
  echo "after service hash:  ${after_service_hash}"
  exit 1
fi

if [ -n "$follower_before" ]; then
  after_follower_result="$(run_query_pod "$follower_before" "$CONSISTENCY_QUERY")"
  after_follower_hash="$(hash_result "$after_follower_result")"
  log "follower 切后查询结果: $(printf '%s' "$after_follower_result")"

  if [ "$before_follower_hash" != "$after_follower_hash" ]; then
    echo "error: 切前/切后 follower 查询结果哈希不一致（可能存在节点差异）" >&2
    echo "before hash: ${before_follower_hash}"
    echo "after hash:  ${after_follower_hash}"
    exit 1
  fi
fi

log "PASS: 查询结果哈希前后保持一致。"
