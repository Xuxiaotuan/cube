#!/usr/bin/env bash
set -euo pipefail

# Destructive only to the pinned router leader in the dedicated test namespace.
# This script does NOT deploy, patch a workload, use an external API secret, or
# remove a namespace/PVC. Each mode starts a new real Cube API rollup build.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${NAMESPACE:-cube-ha-remediation}"
API_DEPLOYMENT="${API_DEPLOYMENT:-analytics-api}"
CR_NAME="${CR_NAME:-analytics-router}"
LEADER_SERVICE_NAME="${LEADER_SERVICE_NAME:-analytics-router-leader}"
ROUTER_CONTAINER="${ROUTER_CONTAINER:-cube-studio-router}"
FAULT_MODES="${FAULT_MODES:-upload-failover receipt-loss receipt-failover drain-failover}"
HA_TRANSFER="${HA_TRANSFER:-rows}"
HA_ROWS="${HA_ROWS:-4096}"
HA_TIMEOUT_SECONDS="${HA_TIMEOUT_SECONDS:-600}"
FAILOVER_TIMEOUT_SECONDS="${FAILOVER_TIMEOUT_SECONDS:-45}"
HA_CONTROL_PORT="${HA_CONTROL_PORT:-13331}"
HA_CLEANUP="${HA_CLEANUP:-0}"
EVIDENCE_DIR="${EVIDENCE_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/cube-preagg-evidence.XXXXXX")}"

[ "$NAMESPACE" = cube-ha-remediation ] || {
  echo 'Refusing fault injection outside the authorized isolated namespace cube-ha-remediation' >&2; exit 1;
}
for command_name in "$KUBECTL" jq od tr date sleep; do
  command -v "$command_name" >/dev/null || { echo "Missing command: $command_name" >&2; exit 1; }
done
for value in "$HA_ROWS" "$HA_TIMEOUT_SECONDS" "$FAILOVER_TIMEOUT_SECONDS" "$HA_CONTROL_PORT"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo 'Numeric settings must be positive integers' >&2; exit 1; }
done
[ "$FAILOVER_TIMEOUT_SECONDS" -le 50 ] || {
  echo 'Failover barrier must finish within the driver 60s upload timeout (maximum 50s)' >&2; exit 1;
}
mkdir -p "$EVIDENCE_DIR"
k() { "$KUBECTL" --request-timeout=10s -n "$NAMESPACE" "$@"; }

control() {
  local control_body='{}'
  if [ "$#" -ge 2 ]; then control_body="$2"; fi
  k exec "$api_pod" -c "$api_container" -- env \
    HA_RUN="$run" HA_CONTROL_PORT="$HA_CONTROL_PORT" HA_CONTROL_PATH="$1" HA_CONTROL_BODY="$control_body" node -e '
    const http = require("http");
    const body = process.env.HA_CONTROL_BODY;
    const req = http.request({host:"127.0.0.1",port:Number(process.env.HA_CONTROL_PORT),path:process.env.HA_CONTROL_PATH,method:"POST",headers:{"x-ha-run":process.env.HA_RUN,"content-type":"application/json","content-length":Buffer.byteLength(body)}}, res => {
      let text=""; res.on("data", c=>text+=c); res.on("end",()=>{process.stdout.write(text);process.exit(res.statusCode===200?0:1);});
    });
    req.on("error", e=>{console.error(e.message);process.exit(1);});
    setTimeout(()=>{req.destroy(new Error("Control request deadline"));},8000).unref();
    req.end(body);
  '
}

pid=''
drain_pid=''
drain_target=''
drain_target_uid=''
drain_delete_requested=false
events=''
summary=''
extract_events() { jq -Rsc 'split("\n") | map(fromjson? | select(.kind == "cube-ha-preagg"))' "$events"; }
finish() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [ -n "$drain_pid" ]; then
    kill "$drain_pid" 2>/dev/null || true
    wait "$drain_pid" 2>/dev/null || true
  fi
  if [ "$exit_code" -ne 0 ] && [ -n "$drain_target" ]; then
    printf 'WARNING: drain was started on %s/%s uid=%s; draining does not reset. This Pod is NOT a healthy standby. deletionRequested=%s; inspect and replace only the pinned test Pod.\n' "$NAMESPACE" "$drain_target" "$drain_target_uid" "$drain_delete_requested" >&2
  fi
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    control /abort '{}' >/dev/null 2>&1 || true
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  if [ -n "$events" ] && [ -f "$events" ]; then
    extract_events | jq --argjson controllerExit "$exit_code" --arg drainTarget "$drain_target" --arg drainUid "$drain_target_uid" --argjson deletionRequested "$drain_delete_requested" \
      '{controllerExit:$controllerExit,status:(if $controllerExit == 0 and any(.[]; .event == "result" and .status == "PASS") then "PASS" else "FAIL" end),drainCleanup:(if $drainTarget == "" then null else {pod:$drainTarget,uid:$drainUid,drainStarted:true,healthyStandby:false,deletionRequested:$deletionRequested,action:"Inspect and replace only the pinned draining test Pod; drain never resets automatically"} end),events:.}' > "$summary"
    printf 'Evidence: %s\n' "$summary" >&2
  fi
  exit "$exit_code"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

deployment="$(k get deployment "$API_DEPLOYMENT" -o json)"
selector="$(printf '%s' "$deployment" | jq -r '.spec.selector.matchLabels | to_entries | map(.key + "=" + .value) | join(",")')"
api_container="${API_CONTAINER:-$(printf '%s' "$deployment" | jq -r '.spec.template.spec.containers[0].name')}"
api_pod="$(k get pods -l "$selector" -o json | jq -er '[.items[] | select(.metadata.deletionTimestamp == null) | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | if length == 1 then .[0].metadata.name else error("Require exactly one ready API pod for loopback fault routing") end')"
printf 'Pinned API pod: %s/%s; evidence directory: %s\n' "$NAMESPACE" "$api_pod" "$EVIDENCE_DIR"

for mode in $FAULT_MODES; do
  drain_target=''; drain_target_uid=''; drain_delete_requested=false
  case "$mode" in upload-failover|receipt-loss|receipt-failover|drain-failover) ;; *) echo "Invalid fault mode: $mode" >&2; exit 1 ;; esac
  run="r$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
  events="$EVIDENCE_DIR/$run-$mode.events.jsonl"
  summary="$EVIDENCE_DIR/$run-$mode.json"
  [ ! -e "$events" ] || { echo 'Evidence run collision' >&2; exit 1; }
  old="$(k get cubestorerouter "$CR_NAME" -o json)"
  old_leader="$(printf '%s' "$old" | jq -er '.status.leader | select(length > 0)')"
  old_epoch="$(printf '%s' "$old" | jq -er '.status.leaderEpoch | select(. > 0)')"
  printf '%s' "$old" | jq -e 'any(.status.conditions[]?; .type == "PromotionReady" and .status == "True")' >/dev/null
  old_pod="$(k get pod "$old_leader" -o json)"
  old_uid="$(printf '%s' "$old_pod" | jq -er '.metadata.uid')"
  old_ip="$(printf '%s' "$old_pod" | jq -er '.status.podIP')"
  printf '%s: start real rollup %s (leader=%s, epoch=%s)\n' "$mode" "$run" "$old_leader" "$old_epoch"
  "$KUBECTL" -n "$NAMESPACE" exec -i "$api_pod" -c "$api_container" -- env \
    HA_RUN="$run" HA_MODE="$mode" HA_TRANSFER="$HA_TRANSFER" HA_ROWS="$HA_ROWS" \
    HA_TIMEOUT_SECONDS="$HA_TIMEOUT_SECONDS" FAILOVER_TIMEOUT_SECONDS="$FAILOVER_TIMEOUT_SECONDS" HA_CONTROL_PORT="$HA_CONTROL_PORT" HA_CLEANUP="$HA_CLEANUP" \
    HA_OLD_LEADER="$old_leader" HA_OLD_LEADER_IP="$old_ip" HA_OLD_EPOCH="$old_epoch" \
    node - < "$HERE/preagg-fault-check.js" > "$events" 2> "$EVIDENCE_DIR/$run-$mode.stderr.log" &
  pid=$!
  end=$(( $(date +%s) + HA_TIMEOUT_SECONDS ))
  while ! extract_events | jq -e 'any(.[]; .event == "fault_ready")' >/dev/null; do
    kill -0 "$pid" 2>/dev/null || { wait "$pid" || true; echo 'Runner ended without observing the requested fault' >&2; exit 1; }
    [ "$(date +%s)" -lt "$end" ] || { echo 'Timed out waiting for an observed in-flight upload' >&2; exit 1; }
    sleep 0.25 # Bounded event polling, never used to guess when the build is active.
  done
  extract_events | jq -e --arg mode "$mode" 'any(.[]; .event == "fault_ready" and .queryCompleted == false and .manifest.phase == "uploading" and .upload.bytesForwarded > 0 and (if ($mode | startswith("receipt-")) then .upload.receipt.state == "uploaded" else .upload.blockedLastByte == true end))' >/dev/null

  new_leader="$old_leader"; new_epoch="$old_epoch"; endpoint="$old_leader"; drain_proof='null'
  if [ "$mode" != receipt-loss ]; then
    current="$(k get cubestorerouter "$CR_NAME" -o json)"
    [ "$(printf '%s' "$current" | jq -r '.status.leader')" = "$old_leader" ] && \
      [ "$(printf '%s' "$current" | jq -r '.status.leaderEpoch')" = "$old_epoch" ] || {
      echo 'Leader changed before controlled injection; refusing to attribute an unobserved fault' >&2; exit 1;
    }
    [ "$(k get pod "$old_leader" -o jsonpath='{.metadata.uid}')" = "$old_uid" ] || {
      echo 'Leader pod identity changed before injection' >&2; exit 1;
    }
    if [ "$mode" = drain-failover ]; then
      drain_target="$old_leader"; drain_target_uid="$old_uid"
      drain_log="$EVIDENCE_DIR/$run-drain-cli.log"
      "$KUBECTL" --request-timeout=10s -n "$NAMESPACE" exec "$old_leader" -c "$ROUTER_CONTAINER" -- /cube/cubestored --drain > "$drain_log" 2>&1 &
      drain_pid=$!
      # /stale independently waits for the pinned old router's draining state
      # and requires application-level refusal on the pre-existing websocket.
      control /stale '{}' >/dev/null
      control /drain-upload-abort '{}' >/dev/null
      while kill -0 "$drain_pid" 2>/dev/null; do
        [ "$(date +%s)" -lt "$end" ] || { echo 'Drain exceeded total run deadline' >&2; exit 1; }
        sleep 0.1
      done
      if ! wait "$drain_pid"; then
        drain_pid=''
        cat "$drain_log" >&2
        echo 'Drain CLI failed; HTTP 503 is NOT completed drain' >&2
        exit 1
      fi
      drain_pid=''
      # CLI exit0 requires an actual HTTP success with drained:true. Confirm
      # with an independent idempotent POST to the same old Pod, not Service.
      drain_proof="$(k exec "$api_pod" -c "$api_container" -- env HA_OLD_LEADER_IP="$old_ip" node -e '
        const assert=require("assert/strict");const {requestJson}=require("/cube/conf/ha-fault-proxy");
        const {CubeStoreDriver}=require("@cubejs-backend/cubestore-driver");const driver=new CubeStoreDriver();
        const headers=driver.config.user ? {authorization:`Basic ${Buffer.from(`${driver.config.user}:${driver.config.password || ""}`).toString("base64")}`} : {};
        requestJson(new URL(`http://${process.env.HA_OLD_LEADER_IP}:3030/router/drain`),{method:"POST",body:{},headers}).then(status=>{
          assert.equal(status.draining,true);assert.equal(status.drained,true);assert.equal(status.inFlight,0);
          console.log(JSON.stringify({cliExit:0,drainHttpSuccess:true,completionSource:"independent pinned-pod POST /router/drain after CLI exit0",oldLeaderIp:process.env.HA_OLD_LEADER_IP,observedAt:new Date().toISOString(),status}));
        }).catch(e=>{console.error(e.message);process.exitCode=1}).finally(()=>driver.release());
      ')"
    fi
    printf 'Injecting %s: delete pinned test leader %s/%s uid=%s\n' "$mode" "$NAMESPACE" "$old_leader" "$old_uid"
    k delete pod "$old_leader" --grace-period=0 --force --wait=false
    if [ "$mode" = drain-failover ]; then drain_delete_requested=true; fi
    failover_end=$(( $(date +%s) + FAILOVER_TIMEOUT_SECONDS ))
    while :; do
      current="$(k get cubestorerouter "$CR_NAME" -o json)"
      new_leader="$(printf '%s' "$current" | jq -r '.status.leader // empty')"
      new_epoch="$(printf '%s' "$current" | jq -r '.status.leaderEpoch // 0')"
      endpoint="$(k get endpointslice -l "kubernetes.io/service-name=$LEADER_SERVICE_NAME" -o json | jq -r '[.items[].endpoints[]? | select(.conditions.ready != false) | .targetRef.name // empty] | unique | if length == 1 then .[0] else "" end')"
      if [ -n "$new_leader" ] && [ "$new_leader" != "$old_leader" ] && [ "$new_epoch" -gt "$old_epoch" ] && [ "$endpoint" = "$new_leader" ] && \
        printf '%s' "$current" | jq -e 'any(.status.conditions[]?; .type == "PromotionReady" and .status == "True")' >/dev/null; then break; fi
      kill -0 "$pid" 2>/dev/null || { echo 'Runner failed during leader transition' >&2; exit 1; }
      [ "$(date +%s)" -lt "$failover_end" ] || { echo 'Failover did not become observable before the upload barrier deadline' >&2; exit 1; }
      sleep 0.25
    done
  fi
  proof="$(jq -n --arg oldLeader "$old_leader" --arg oldUid "$old_uid" --argjson oldEpoch "$old_epoch" \
    --arg newLeader "$new_leader" --argjson newEpoch "$new_epoch" --arg endpoint "$endpoint" --arg namespace "$NAMESPACE" --argjson drain "$drain_proof" \
    '{namespace:$namespace,oldLeader:$oldLeader,oldUid:$oldUid,oldEpoch:$oldEpoch,newLeader:$newLeader,newEpoch:$newEpoch,endpoint:$endpoint,drain:$drain}')"
  control /release "$proof" >/dev/null
  while kill -0 "$pid" 2>/dev/null; do
    [ "$(date +%s)" -lt "$end" ] || { echo 'Timed out waiting for recovered real rollup result' >&2; exit 1; }
    sleep 0.25
  done
  if ! wait "$pid"; then pid=''; echo 'Real pre-aggregation E2E failed; see evidence and stderr' >&2; exit 1; fi
  pid=''
  extract_events | jq -e 'any(.[]; .event == "result" and .status == "PASS")' >/dev/null
  extract_events | jq '{controllerExit:0,status:"PASS",events:.}' > "$summary"
  printf 'PASS %s (%s): %s\n' "$mode" "$run" "$summary"
done
