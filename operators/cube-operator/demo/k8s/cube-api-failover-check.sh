#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cube-operator-demo}"
CR_NAME="${CR_NAME:-demo}"
KUBECTL="${KUBECTL:-kubectl}"
API_DEPLOYMENT="${API_DEPLOYMENT:-cube-api-demo}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-2}"
QUERY_JSON='{"measures":["RouterHaProbe.total"]}'
API_SECRET="${API_SECRET:-cube-router-ha-demo-secret}"

log() {
  printf '[%s] %s\n' "$(date +'%F %T')" "$*"
}

for command_name in "$KUBECTL" jq; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "error: required command '$command_name' not found" >&2
    exit 1
  fi
done

get_leader() {
  "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leader}'
}

get_epoch() {
  "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leaderEpoch}'
}

get_service_endpoint_pod() {
  "$KUBECTL" -n "$NAMESPACE" get endpointslice \
    -l kubernetes.io/service-name=cube-router-leader -o json \
    | jq -r '[.items[].endpoints[]?.targetRef.name] | map(select(. != null)) | unique | .[0] // empty'
}

query_cube_api() {
  "$KUBECTL" -n "$NAMESPACE" exec "deploy/$API_DEPLOYMENT" -- env \
    API_SECRET="$API_SECRET" QUERY_JSON="$QUERY_JSON" node -e '
      const http = require("http");
      const jwt = require("jsonwebtoken");
      const query = encodeURIComponent(process.env.QUERY_JSON);
      const token = jwt.sign({}, process.env.API_SECRET);
      const req = http.get(`http://127.0.0.1:4000/cubejs-api/v1/load?query=${query}`, {
        headers: { authorization: `Bearer ${token}` }
      }, (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => { body += chunk; });
        res.on("end", () => {
          process.stdout.write(body);
          process.stderr.write(`\nHTTP_STATUS=${res.statusCode}\n`);
          process.exit(res.statusCode >= 200 && res.statusCode < 300 ? 0 : 1);
        });
      });
      req.on("error", (error) => { console.error(error.stack || error); process.exit(1); });
      req.setTimeout(30000, () => { req.destroy(new Error("Cube API request timeout")); });
    '
}

hash_result() {
  # requestId, lastRefreshTime and annotations can change between successful
  # requests; compare the returned business rows instead of the envelope.
  jq -cS '{data: .data}' |
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    sha256sum | awk '{print $1}'
  fi
}

wait_for_api() {
  "$KUBECTL" -n "$NAMESPACE" rollout status "deploy/$API_DEPLOYMENT" --timeout=240s >/dev/null
}

wait_for_failover() {
  local old_leader="$1"
  local waited=0
  while [ "$waited" -lt "$WAIT_SECONDS" ]; do
    local current_leader
    local endpoint_pod
    current_leader="$(get_leader)"
    endpoint_pod="$(get_service_endpoint_pod)"
    if [ -n "$current_leader" ] && [ "$current_leader" != "$old_leader" ] && [ "$endpoint_pod" = "$current_leader" ]; then
      echo "$current_leader"
      return 0
    fi
    sleep "$CHECK_INTERVAL_SECONDS"
    waited=$((waited + CHECK_INTERVAL_SECONDS))
  done
  return 1
}

wait_for_api
old_leader="$(get_leader)"
old_epoch="$(get_epoch)"
if [ -z "$old_leader" ]; then
  echo "error: no active Router leader" >&2
  exit 1
fi

log "Cube API 发送切换前真实请求，leader=${old_leader}, epoch=${old_epoch}"
before_response="$(query_cube_api)"
before_hash="$(printf '%s' "$before_response" | hash_result)"
log "切换前 Cube API 响应 hash=${before_hash}"
log "切换前 Cube API data=$(printf '%s' "$before_response" | jq -c '.data')"

log "删除当前 Router leader=${old_leader}"
"$KUBECTL" -n "$NAMESPACE" delete pod "$old_leader"

new_leader="$(wait_for_failover "$old_leader")" || {
  echo "error: Cube API E2E failover timed out" >&2
  "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o yaml
  exit 1
}
new_epoch="$(get_epoch)"
log "新 leader=${new_leader}, epoch=${new_epoch}, Service endpoint=$(get_service_endpoint_pod)"

after_response="$(query_cube_api)"
after_hash="$(printf '%s' "$after_response" | hash_result)"
log "切换后 Cube API 响应 hash=${after_hash}"
log "切换后 Cube API data=$(printf '%s' "$after_response" | jq -c '.data')"

if [ "$before_hash" != "$after_hash" ]; then
  echo "error: Cube API response hash changed across Router failover" >&2
  exit 1
fi

echo "--- Cube API logs ---"
"$KUBECTL" -n "$NAMESPACE" logs "deploy/$API_DEPLOYMENT" --tail=120
echo "--- cube-operator logs ---"
"$KUBECTL" -n "$NAMESPACE" logs deploy/cube-operator --tail=120
echo "PASS: Cube API -> CubeStoreDriver -> Leader Service -> Router failover response is consistent."
