#!/usr/bin/env bash
set -euo pipefail

# Separate opt-in controller: never changes the original four default modes.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${NAMESPACE:-cube-ha-remediation}"
API_DEPLOYMENT="${API_DEPLOYMENT:-analytics-api}"
REFRESHER_DEPLOYMENT="${REFRESHER_DEPLOYMENT:-analytics-refresher}"
CR_NAME="${CR_NAME:-analytics-router}"
LEADER_SERVICE_NAME="${LEADER_SERVICE_NAME:-analytics-router-leader}"
HA_TIMEOUT_SECONDS="${HA_TIMEOUT_SECONDS:-600}"
FAILOVER_TIMEOUT_SECONDS="${FAILOVER_TIMEOUT_SECONDS:-45}"
HA_CONTROL_PORT="${HA_CONTROL_PORT:-13331}"
EVIDENCE_DIR="${EVIDENCE_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/cube-refresher-evidence.XXXXXX")}"
[ "$NAMESPACE" = cube-ha-remediation ] || { echo 'Refusing a non-isolated namespace' >&2; exit 1; }
for executable in "$KUBECTL" jq od tr date sleep; do command -v "$executable" >/dev/null; done
for value in "$HA_TIMEOUT_SECONDS" "$FAILOVER_TIMEOUT_SECONDS" "$HA_CONTROL_PORT"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo 'Invalid positive integer setting' >&2; exit 1; }
done
[ "$FAILOVER_TIMEOUT_SECONDS" -le 50 ] || { echo 'Fault barrier cannot exceed the upload timeout' >&2; exit 1; }
mkdir -p "$EVIDENCE_DIR"
k() { "$KUBECTL" --request-timeout=10s -n "$NAMESPACE" "$@"; }
select_pod() {
  local deployment selector
  deployment="$(k get deployment "$1" -o json)"
  selector="$(printf '%s' "$deployment" | jq -r '.spec.selector.matchLabels | to_entries | map(.key + "=" + .value) | join(",")')"
  k get pods -l "$selector" -o json | jq -ce '[.items[] | select(.metadata.deletionTimestamp == null) | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | if length == 1 then .[0] else error("Require exactly one ready pod per role") end'
}
api="$(select_pod "$API_DEPLOYMENT")"
builder="$(select_pod "$REFRESHER_DEPLOYMENT")"
api_pod="$(printf '%s' "$api" | jq -r '.metadata.name')"
api_uid="$(printf '%s' "$api" | jq -r '.metadata.uid')"
api_ip="$(printf '%s' "$api" | jq -er '.status.podIP')"
api_container="${API_CONTAINER:-$(printf '%s' "$api" | jq -r '.spec.containers[0].name')}"
builder_pod="$(printf '%s' "$builder" | jq -r '.metadata.name')"
builder_uid="$(printf '%s' "$builder" | jq -r '.metadata.uid')"
builder_container="${REFRESHER_CONTAINER:-$(printf '%s' "$builder" | jq -r '.spec.containers[0].name')}"
[ "$api_uid" != "$builder_uid" ] || { echo 'API and refresher must be distinct pods' >&2; exit 1; }
run="r$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
events="$EVIDENCE_DIR/$run-refresher-failover.events.jsonl"
summary="$EVIDENCE_DIR/$run-refresher-failover.json"
api_events="$EVIDENCE_DIR/$run-api-runtime.jsonl"
[ ! -e "$events" ] || { echo 'Run collision' >&2; exit 1; }
old="$(k get cubestorerouter "$CR_NAME" -o json)"
old_leader="$(printf '%s' "$old" | jq -er '.status.leader | select(length > 0)')"
old_epoch="$(printf '%s' "$old" | jq -er '.status.leaderEpoch | select(. > 0)')"
old_uid="$(k get pod "$old_leader" -o jsonpath='{.metadata.uid}')"
printf '%s' "$old" | jq -e 'any(.status.conditions[]?; .type == "PromotionReady" and .status == "True")' >/dev/null
extract() { jq -Rsc 'split("\n") | map(fromjson? | select(.kind == "cube-ha-preagg"))' "$events"; }
control() {
  local body='{}'
  if [ "$#" -ge 2 ]; then body="$2"; fi
  k exec "$builder_pod" -c "$builder_container" -- env HA_RUN="$run" HA_CONTROL_PORT="$HA_CONTROL_PORT" HA_PATH="$1" HA_BODY="$body" node -e '
    const {requestJson}=require("/cube/conf/ha-fault-proxy");
    requestJson(new URL(`http://127.0.0.1:${process.env.HA_CONTROL_PORT}${process.env.HA_PATH}`),{method:"POST",body:JSON.parse(process.env.HA_BODY),headers:{"x-ha-run":process.env.HA_RUN},timeout:8000})
    .then(r=>console.log(JSON.stringify(r))).catch(e=>{console.error(e.message);process.exit(1);});
  '
}
pid=''
finish() {
  local code=$?
  trap - EXIT INT TERM
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    control /abort '{}' >/dev/null 2>&1 || true
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  if [ -f "$events" ]; then
    extract | jq --argjson code "$code" --arg apiPod "$api_pod" --arg apiUid "$api_uid" --arg builderPod "$builder_pod" --arg builderUid "$builder_uid" --arg apiEvidence "$api_events" \
      '{controllerExit:$code,status:(if $code == 0 and any(.[]; .event == "result" and .status == "PASS") then "PASS" else "FAIL" end),workflow:"scheduled-refresher-warmup-then-api-consumption",apiPod:$apiPod,apiUid:$apiUid,builderPod:$builderPod,builderUid:$builderUid,apiRuntimeEvidence:$apiEvidence,events:.}' > "$summary"
  fi
  printf 'Refresher evidence: %s\n' "$summary" >&2
  exit "$code"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$KUBECTL" -n "$NAMESPACE" exec -i "$builder_pod" -c "$builder_container" -- env \
  HA_RUN="$run" HA_MODE=refresher-failover HA_TRANSFER="${HA_TRANSFER:-rows}" HA_ROWS="${HA_ROWS:-4096}" \
  HA_TIMEOUT_SECONDS="$HA_TIMEOUT_SECONDS" FAILOVER_TIMEOUT_SECONDS="$FAILOVER_TIMEOUT_SECONDS" HA_CONTROL_PORT="$HA_CONTROL_PORT" HA_CLEANUP="${HA_CLEANUP:-0}" \
  HA_API_URL="http://$api_ip:4000" HA_API_POD_NAME="$api_pod" HA_API_POD_UID="$api_uid" \
  HA_EXECUTOR_POD="$builder_pod" HA_EXECUTOR_UID="$builder_uid" HA_OLD_LEADER="$old_leader" HA_OLD_EPOCH="$old_epoch" \
  node - < "$HERE/preagg-fault-check.js" > "$events" 2> "$EVIDENCE_DIR/$run-refresher.stderr.log" &
pid=$!
end=$(( $(date +%s) + HA_TIMEOUT_SECONDS ))
while ! extract | jq -e 'any(.[]; .event == "fault_ready")' >/dev/null; do
  kill -0 "$pid" 2>/dev/null || { wait "$pid" || true; echo 'No real refresher upload barrier observed' >&2; exit 1; }
  [ "$(date +%s)" -lt "$end" ] || { echo 'Scheduler/barrier deadline exceeded' >&2; exit 1; }
  sleep 0.25
done
extract | jq -e --arg uid "$builder_uid" 'any(.[]; .event == "fault_ready" and .trigger == "scheduled-refresh-warmup" and .apiLoadStarted == false and .manifest.phase == "uploading" and .upload.blockedLastByte == true and .upload.bytesForwarded > 0 and .executorUid == $uid and .queueEvidence.podUid == $uid and .queueEvidence.role == "refresher")' >/dev/null
current="$(k get cubestorerouter "$CR_NAME" -o json)"
printf '%s' "$current" | jq -e --arg leader "$old_leader" --argjson epoch "$old_epoch" '.status.leader == $leader and .status.leaderEpoch == $epoch' >/dev/null
[ "$(k get pod "$old_leader" -o jsonpath='{.metadata.uid}')" = "$old_uid" ]
k delete pod "$old_leader" --grace-period=0 --force --wait=false
cutover_end=$(( $(date +%s) + FAILOVER_TIMEOUT_SECONDS ))
while :; do
  current="$(k get cubestorerouter "$CR_NAME" -o json)"
  new_leader="$(printf '%s' "$current" | jq -r '.status.leader // empty')"
  new_epoch="$(printf '%s' "$current" | jq -r '.status.leaderEpoch // 0')"
  endpoint="$(k get endpointslice -l "kubernetes.io/service-name=$LEADER_SERVICE_NAME" -o json | jq -r '[.items[].endpoints[]? | select(.conditions.ready != false) | .targetRef.name // empty] | unique | if length == 1 then .[0] else "" end')"
  if [ -n "$new_leader" ] && [ "$new_leader" != "$old_leader" ] && [ "$new_epoch" -gt "$old_epoch" ] && [ "$endpoint" = "$new_leader" ] && \
    printf '%s' "$current" | jq -e 'any(.status.conditions[]?; .type == "PromotionReady" and .status == "True")' >/dev/null; then break; fi
  kill -0 "$pid" 2>/dev/null || { echo 'Refresher runner failed during cutover' >&2; exit 1; }
  [ "$(date +%s)" -lt "$cutover_end" ] || { echo 'Observed failover deadline exceeded' >&2; exit 1; }
  sleep 0.25
done
proof="$(jq -n --arg oldLeader "$old_leader" --arg oldUid "$old_uid" --argjson oldEpoch "$old_epoch" --arg newLeader "$new_leader" --argjson newEpoch "$new_epoch" --arg endpoint "$endpoint" '{oldLeader:$oldLeader,oldUid:$oldUid,oldEpoch:$oldEpoch,newLeader:$newLeader,newEpoch:$newEpoch,endpoint:$endpoint}')"
control /release "$proof" >/dev/null
api_evidence_copied=false
while kill -0 "$pid" 2>/dev/null; do
  if [ "$api_evidence_copied" = false ] && extract | jq -e 'any(.[]; .event == "api_response" and .dataRows != null)' >/dev/null; then
    k exec "$api_pod" -c "$api_container" -- env HA_RUN="$run" node -e '
      const h=require("/cube/conf/ha-scheduled-contexts");
      for(const event of h.readRuntimeEvents(process.env.HA_RUN)) console.log(JSON.stringify(event));
    ' > "$api_events"
    k exec -i "$builder_pod" -c "$builder_container" -- env HA_RUN="$run" node -e '
      const fs=require("fs"); const h=require("/cube/conf/ha-scheduled-contexts");
      if(!h.validRun(process.env.HA_RUN)) throw Error("Invalid run");
      const file=`/tmp/cube-ha-api-${process.env.HA_RUN}.jsonl`;
      fs.writeFileSync(`${file}.tmp`,fs.readFileSync(0),{mode:0o600}); fs.renameSync(`${file}.tmp`,file);
    ' < "$api_events"
    api_evidence_copied=true
  fi
  [ "$(date +%s)" -lt "$end" ] || { echo 'Refresher/API completion deadline exceeded' >&2; exit 1; }
  sleep 0.25
done
if ! wait "$pid"; then pid=''; exit 1; fi
pid=''
extract | jq -e 'any(.[]; .event == "result" and .status == "PASS")' >/dev/null
# Independent API-process evidence: it must not have executed this build.
k exec "$api_pod" -c "$api_container" -- env HA_RUN="$run" node -e '
  const h=require("/cube/conf/ha-scheduled-contexts");
  for(const event of h.readRuntimeEvents(process.env.HA_RUN)) console.log(JSON.stringify(event));
' > "$api_events"
jq -se --arg run "$run" --arg uid "$api_uid" \
  'length > 0 and all(.[]; .podUid == $uid and .role == "api") and all(.[]; .message != "Uploading external pre-aggregation" and (.message != "Performing query" or .params.newVersionEntry == null))' "$api_events" >/dev/null
[ "$(k get pod "$api_pod" -o jsonpath='{.metadata.uid}')" = "$api_uid" ]
[ "$(k get pod "$builder_pod" -o jsonpath='{.metadata.uid}')" = "$builder_uid" ]
printf 'PASS real scheduled refresher -> upload fault -> ready rollup -> API consumption: %s\n' "$run"
