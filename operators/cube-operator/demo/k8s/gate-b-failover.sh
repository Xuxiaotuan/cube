#!/usr/bin/env bash
set -uo pipefail

NAMESPACE="${NAMESPACE:-cube-operator-demo}"
KUBECTL="${KUBECTL:-kubectl}"
CR_NAME="${CR_NAME:-demo}"
API_DEPLOYMENT="${API_DEPLOYMENT:-cube-api-demo}"
API_SECRET="${API_SECRET:-cube-router-ha-demo-secret}"
REPORT="${REPORT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/gate-b-report.md}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.4}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_POD="gate-b-mysql-$$"
API_TRACE="/tmp/gate-b-api-$$.trace"
FAILURES=0
MONITOR_PID=""

mkdir -p "$(dirname "$REPORT")"
printf '# Gate B failover report\n\n- Started: %s\n- Namespace: `%s`\n\n' "$(date -u +%FT%TZ)" "$NAMESPACE" > "$REPORT"

record() {
  printf '%s\n' "$*" | tee -a "$REPORT"
}

fail() {
  FAILURES=$((FAILURES + 1))
  record "- FAIL: $*"
}

pass() { record "- PASS: $*"; }

cleanup() {
  if [ -n "$MONITOR_PID" ]; then
    kill "$MONITOR_PID" 2>/dev/null || true
  fi
  "$KUBECTL" -n "$NAMESPACE" delete pod "$MYSQL_POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

need_cmd() { command -v "$1" >/dev/null 2>&1 || { fail "missing command $1"; return 1; }; }
need_cmd "$KUBECTL"
need_cmd jq
need_cmd curl

record '## Preflight'
if "$KUBECTL" -n "$NAMESPACE" wait --for=condition=available deploy/redis --timeout=120s >/dev/null 2>&1; then pass 'Redis Deployment available'; else fail 'Redis Deployment is not available'; fi
if "$KUBECTL" -n "$NAMESPACE" wait --for=condition=ready pod/cubestore-metastore-0 --timeout=120s >/dev/null 2>&1; then pass 'MetaStore ready'; else fail 'MetaStore not ready'; fi
if "$KUBECTL" -n "$NAMESPACE" wait --for=jsonpath='{.status.readyReplicas}'=2 statefulset/cube-worker-demo --timeout=120s >/dev/null 2>&1; then pass 'Workers ready: 2'; else fail 'Workers are not both ready'; fi
if "$KUBECTL" -n "$NAMESPACE" exec deploy/redis -- redis-cli ping 2>/dev/null | grep -q PONG; then pass 'Redis PING/PONG'; else fail 'Redis PING failed'; fi

secret_json="$($KUBECTL -n "$NAMESPACE" get secret cube-router-demo-lease-store -o json 2>/dev/null || true)"
if printf '%s' "$secret_json" | jq -e '.data.dsn and .data.url and .data.password' >/dev/null 2>&1; then
  pass 'Redis Secret contains dsn/url/password keys'
else
  fail 'Redis Secret missing dsn/url/password keys'
fi

leader_before="$($KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leader}' 2>/dev/null || true)"
epoch_before="$($KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leaderEpoch}' 2>/dev/null || true)"
if [ -z "$leader_before" ]; then
  leader_before="$($KUBECTL -n "$NAMESPACE" get pod -l app=cube-router,cubestore.io/router-role=leader -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
fi
if [ -z "$leader_before" ]; then
  leader_before="$($KUBECTL -n "$NAMESPACE" get endpointslice -l kubernetes.io/service-name=cube-router-leader -o json 2>/dev/null | jq -r '[.items[].endpoints[]? | select(.conditions.ready != false) | .targetRef.name] | unique | .[0] // empty' 2>/dev/null || true)"
fi
role_json="$($KUBECTL -n "$NAMESPACE" get configmap cube-router-demo-router-role-state -o jsonpath='{.data.route-role\.json}' 2>/dev/null || true)"
if [ -z "$epoch_before" ]; then
  epoch_before="$(printf '%s' "$role_json" | jq -r '.leaderEpoch // empty' 2>/dev/null || true)"
fi
old_ip=""
if [ -n "$leader_before" ]; then old_ip="$($KUBECTL -n "$NAMESPACE" get pod "$leader_before" -o jsonpath='{.status.podIP}' 2>/dev/null || true)"; fi
record "- Before leader: ${leader_before:-none} (epoch=${epoch_before:-none}, ip=${old_ip:-none})"
if [ -n "$leader_before" ] && [ -n "$old_ip" ] && [ "${epoch_before:-0}" -gt 0 ] 2>/dev/null; then pass 'CR leader and epoch present'; else fail 'CR leader/epoch preflight invalid'; fi

if printf '%s' "$role_json" | jq -e --arg leader "$leader_before" '.activeLeader == $leader and (.leaderEpoch|tonumber) == ('"${epoch_before:-0}"')' >/dev/null 2>&1; then pass 'promotion ConfigMap matches CR leader/epoch'; else fail 'promotion ConfigMap does not match CR leader/epoch'; fi

router_status() {
  local pod="$1" port="$2" pf
  "$KUBECTL" -n "$NAMESPACE" port-forward "pod/$pod" "$port:3030" >/dev/null 2>&1 & pf=$!
  sleep 1
  curl -sS --max-time 4 "http://127.0.0.1:$port/router/status" 2>/dev/null || true
  kill "$pf" 2>/dev/null || true
}

check_router_contract() {
  local pod="$1" label status_json promotion_json lease_json role
  label="$($KUBECTL -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.labels.cubestore\.io/router-role}' 2>/dev/null || true)"
  promotion_json="$($KUBECTL -n "$NAMESPACE" exec "$pod" -c cube-studio-router -- cat /var/run/cubestore-promotion/promotion.json 2>/dev/null || true)"
  lease_json="$($KUBECTL -n "$NAMESPACE" exec "$pod" -c cube-studio-router -- cat /var/run/cubestore-ha/leadership.json 2>/dev/null || true)"
  status_json="$(router_status "$pod" "$2")"
  if printf '%s' "$promotion_json" | jq -e '.activeLeader and (.leaderEpoch|tonumber)>0 and (.leaseEpoch|tonumber)>0 and .leaseClusterID and .leaseToken and .metaStoreReady == true' >/dev/null 2>&1; then :; else fail "$pod promotion.json contract invalid"; return; fi
  if printf '%s' "$lease_json" | jq -e '.holderId and (.epoch|tonumber)>0 and .tokenHash and .issuedAt and .expiresAt' >/dev/null 2>&1; then :; else fail "$pod leadership.json contract invalid"; return; fi
  if [ "$label" = leader ]; then role='true'; else role='false'; fi
  if printf '%s' "$status_json" | jq -e --arg node "$pod" --argjson leader "$role" '.nodeName == $node and .isLeader == $leader and (.leaderEpoch|tonumber)>0 and (.leaseEpoch|tonumber)>0 and (.leaseTokenHash|startswith("sha256:")) and .metaStoreReady == true' >/dev/null 2>&1; then
    pass "$pod promotion.json, leadership.json and /router/status ACK"
  else
    fail "$pod /router/status ACK or role contract invalid (status=$status_json)"
  fi
}

record '## Router contracts before deletion'
router_pods="$($KUBECTL -n "$NAMESPACE" get pod -l app=cube-router -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
index=0
while IFS= read -r pod; do
  [ -z "$pod" ] && continue
  index=$((index + 1)); check_router_contract "$pod" $((19080 + index))
done <<< "$router_pods"

leader_endpoint="$($KUBECTL -n "$NAMESPACE" get endpointslice -l kubernetes.io/service-name=cube-router-leader -o json 2>/dev/null | jq -r '[.items[].endpoints[]? | select(.conditions.ready != false) | .targetRef.name] | unique | .[0] // empty' 2>/dev/null || true)"
if [ -n "$leader_before" ] && [ "$leader_endpoint" = "$leader_before" ]; then pass 'leader Service EndpointSlice matches old leader'; else fail "leader EndpointSlice mismatch or empty: ${leader_endpoint:-none}"; fi
"$KUBECTL" -n "$NAMESPACE" get service cube-router-leader -o wide >> "$REPORT" 2>&1 || true
"$KUBECTL" -n "$NAMESPACE" get endpointslice -l kubernetes.io/service-name=cube-router-leader -o yaml >> "$REPORT" 2>&1 || true

if [ -z "$leader_before" ]; then
  fail 'no active leader Pod; destructive deletion was not executed'
  record '## Final resources'
  "$KUBECTL" -n "$NAMESPACE" get pods -l app=cube-router --show-labels -o wide >> "$REPORT" 2>&1 || true
  "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o yaml >> "$REPORT" 2>&1 || true
  "$KUBECTL" -n "$NAMESPACE" get endpointslice -l kubernetes.io/service-name=cube-router-leader -o yaml >> "$REPORT" 2>&1 || true
  "$KUBECTL" -n "$NAMESPACE" logs deploy/cube-operator --tail=80 >> "$REPORT" 2>&1 || true
  record "- Finished: $(date -u +%FT%TZ)"
  record ""
  record "## Result"
  record "FAIL: no safe leader target; failover was not attempted."
  exit "$FAILURES"
fi

record '## Destructive failover'
if "$KUBECTL" -n "$NAMESPACE" run "$MYSQL_POD" --restart=Never --image="$MYSQL_IMAGE" --env="MYSQL_PWD=$MYSQL_PASSWORD" --command -- sleep 180 >/dev/null 2>&1 && \
   "$KUBECTL" -n "$NAMESPACE" wait --for=condition=ready "pod/$MYSQL_POD" --timeout=120s >/dev/null 2>&1; then
  pass 'MySQL probe pod ready'
else
  fail 'MySQL probe pod failed to start'
fi

api_probe() {
  "$KUBECTL" -n "$NAMESPACE" exec "deploy/$API_DEPLOYMENT" -- env API_SECRET="$API_SECRET" node -e '
    const http=require("http"), jwt=require("jsonwebtoken");
    const body=JSON.stringify({query:{measures:["RouterHaProbe.rowCount"]}});
    const req=http.request({hostname:"127.0.0.1",port:4000,path:"/cubejs-api/v1/load",method:"POST",headers:{authorization:`Bearer ${jwt.sign({},process.env.API_SECRET)}`,"content-type":"application/json","content-length":Buffer.byteLength(body)}},res=>{process.stdout.write(String(res.statusCode));res.resume();res.on("end",()=>process.exit(0));});
    req.on("error",()=>{process.stdout.write("000");process.exit(0)}); req.setTimeout(3000,()=>{process.stdout.write("000");req.destroy()}); req.end(body);
  ' 2>/dev/null | tr -d '[:space:]'
}
monitor_api() {
  : > "$API_TRACE"
  local end=$((SECONDS + 35)) code
  while [ "$SECONDS" -lt "$end" ]; do
    code="$(api_probe)"; printf '%s %s\n' "$(date -u +%FT%T.%3NZ)" "${code:-000}" >> "$API_TRACE"
    sleep 0.5
  done
}

monitor_api & MONITOR_PID=$!
record "- Deleting old leader Pod: $leader_before"
delete_started="$(date -u +%FT%T.%3NZ)"
if "$KUBECTL" -n "$NAMESPACE" delete pod "$leader_before" --wait=false >/dev/null 2>&1; then pass "delete accepted at $delete_started"; else fail 'leader Pod delete failed'; fi

old_write_rejected=false
if [ -n "$old_ip" ]; then
  for _ in $(seq 1 30); do
    write_output="$($KUBECTL -n "$NAMESPACE" exec "$MYSQL_POD" -- mysql --protocol=TCP --connect-timeout=1 -h "$old_ip" -P "$MYSQL_PORT" -u "$MYSQL_USER" -N -B -e 'CREATE TABLE gate_b_write_probe (id Int32)' 2>&1 || true)"
    if printf '%s' "$write_output" | grep -qi 'STALE_LEADER'; then old_write_rejected=true; break; fi
    sleep 0.5
  done
fi
if [ "$old_write_rejected" = true ]; then pass 'old primary write rejected with STALE_LEADER'; else fail 'did not observe old primary write rejection'; fi

new_leader=""; waited=0
while [ "$waited" -lt "$WAIT_SECONDS" ]; do
  candidate="$($KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leader}' 2>/dev/null || true)"
  endpoint="$($KUBECTL -n "$NAMESPACE" get endpointslice -l kubernetes.io/service-name=cube-router-leader -o json 2>/dev/null | jq -r '[.items[].endpoints[]? | select(.conditions.ready != false) | .targetRef.name] | unique | .[0] // empty' 2>/dev/null || true)"
  ready="$($KUBECTL -n "$NAMESPACE" get pod "$candidate" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  if [ -n "$candidate" ] && [ "$candidate" != "$leader_before" ] && [ "$candidate" = "$endpoint" ] && [ "$ready" = True ]; then new_leader="$candidate"; break; fi
  sleep 2; waited=$((waited + 2))
done
if [ -n "$new_leader" ]; then pass "new leader: $new_leader"; else fail "no new leader within ${WAIT_SECONDS}s"; fi

epoch_after="$($KUBECTL -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leaderEpoch}' 2>/dev/null || true)"
if [ -n "$epoch_after" ] && [ "${epoch_after:-0}" -gt "${epoch_before:-0}" ] 2>/dev/null; then pass "leaderEpoch increased: $epoch_before -> $epoch_after"; else fail "leaderEpoch did not increase: $epoch_before -> $epoch_after"; fi
if [ -n "$new_leader" ]; then check_router_contract "$new_leader" 19100; fi

if wait "$MONITOR_PID" 2>/dev/null; then :; fi
if grep -Eq ' 503$' "$API_TRACE" 2>/dev/null; then pass 'observed transient API HTTP 503 during failover'; else fail 'no transient API HTTP 503 observed'; fi
if grep -Eq ' (200|201)$' "$API_TRACE" 2>/dev/null; then pass 'API recovered after failover'; else fail 'API did not show a successful post-failover response'; fi

record '## Final resources'
"$KUBECTL" -n "$NAMESPACE" get pods -l app=cube-router --show-labels -o wide >> "$REPORT" 2>&1 || true
"$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o yaml >> "$REPORT" 2>&1 || true
"$KUBECTL" -n "$NAMESPACE" get service cube-router-leader -o wide >> "$REPORT" 2>&1 || true
"$KUBECTL" -n "$NAMESPACE" get endpointslice -l kubernetes.io/service-name=cube-router-leader -o yaml >> "$REPORT" 2>&1 || true
record "- API trace: $API_TRACE"
record "- Finished: $(date -u +%FT%TZ)"
if [ "$FAILURES" -eq 0 ]; then
  record ""
  record "## Result"
  record "PASS: Gate B failover completed."
else
  record ""
  record "## Result"
  record "FAIL: $FAILURES check(s) failed."
fi
exit "$FAILURES"
