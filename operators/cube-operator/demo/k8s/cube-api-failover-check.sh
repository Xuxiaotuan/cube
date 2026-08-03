#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cube-operator-demo}"
KUBECTL="${KUBECTL:-kubectl}"
API_DEPLOYMENT="${API_DEPLOYMENT:-cube-api-demo}"
API_SECRET="${API_SECRET:-cube-router-ha-demo-secret}"
ROUTER_HA_SCHEMA="${ROUTER_HA_SCHEMA:?set ROUTER_HA_SCHEMA to the fixture schema}"
ROUTER_HA_TABLE="${ROUTER_HA_TABLE:?set ROUTER_HA_TABLE to the fixture table}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-2}"

for command_name in "$KUBECTL" jq; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "error: required command '$command_name' not found" >&2; exit 1; }
done

get_leader() { "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "${CR_NAME:-demo}" -o jsonpath='{.status.leader}'; }
get_epoch() { "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "${CR_NAME:-demo}" -o jsonpath='{.status.leaderEpoch}'; }
get_service_endpoint_pod() {
  "$KUBECTL" -n "$NAMESPACE" get endpointslice -l kubernetes.io/service-name=cube-router-leader -o json |
    jq -r '[.items[].endpoints[]?.targetRef.name] | map(select(. != null)) | unique | .[0] // empty'
}
query_cube_api() {
  "$KUBECTL" -n "$NAMESPACE" exec "deploy/$API_DEPLOYMENT" -- env API_SECRET="$API_SECRET" QUERY_JSON='{"measures":["RouterHaProbe.rowCount","RouterHaProbe.totalAmount"]}' node -e '
    const http = require("http"); const jwt = require("jsonwebtoken");
    const req = http.get(`http://127.0.0.1:4000/cubejs-api/v1/load?query=${encodeURIComponent(process.env.QUERY_JSON)}`, {headers:{authorization:`Bearer ${jwt.sign({}, process.env.API_SECRET)}`}}, res => { let b=""; res.setEncoding("utf8"); res.on("data", c => b += c); res.on("end", () => { process.stdout.write(b); process.exit(res.statusCode >= 200 && res.statusCode < 300 ? 0 : 1); }); });
    req.on("error", e => { console.error(e.stack || e); process.exit(1); }); req.setTimeout(30000, () => req.destroy(new Error("Cube API request timeout")));
  '
}
hash_data() { jq -cS '.data' | { command -v shasum >/dev/null && shasum -a 256 || sha256sum; } | awk '{print $1}'; }
wait_for_failover() {
  local old="$1" waited=0
  while [ "$waited" -lt "$WAIT_SECONDS" ]; do
    local leader endpoint; leader="$(get_leader)"; endpoint="$(get_service_endpoint_pod)"
    if [ -n "$leader" ] && [ "$leader" != "$old" ] && [ "$leader" = "$endpoint" ]; then printf '%s' "$leader"; return 0; fi
    sleep "$CHECK_INTERVAL_SECONDS"; waited=$((waited + CHECK_INTERVAL_SECONDS))
  done
  return 1
}

"$KUBECTL" -n "$NAMESPACE" rollout status "deploy/$API_DEPLOYMENT" --timeout=240s >/dev/null
old_leader="$(get_leader)"; old_epoch="$(get_epoch)"
[ -n "$old_leader" ] || { echo "error: no active Router leader" >&2; exit 1; }
[ -n "$old_epoch" ] && [ "$old_epoch" != 0 ] || { echo "error: leaderEpoch is missing or zero" >&2; exit 1; }
before="$(query_cube_api)"; before_hash="$(printf '%s' "$before" | hash_data)"
"$KUBECTL" -n "$NAMESPACE" delete pod "$old_leader"
new_leader="$(wait_for_failover "$old_leader")" || { echo "error: Cube API E2E failover timed out" >&2; exit 1; }
new_epoch="$(get_epoch)"
[ "$new_epoch" -gt "$old_epoch" ] || { echo "error: leaderEpoch did not increase (before=$old_epoch after=$new_epoch)" >&2; exit 1; }
after="$(query_cube_api)"; after_hash="$(printf '%s' "$after" | hash_data)"
[ "$before_hash" = "$after_hash" ] || { echo "error: Cube API real-data response changed across failover" >&2; exit 1; }
printf 'PASS: Cube API real-data hash preserved (leader=%s epoch=%s)\n' "$new_leader" "$new_epoch"
