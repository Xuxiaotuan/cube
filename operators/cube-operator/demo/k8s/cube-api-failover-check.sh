#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cube-operator-demo}"
CR_NAME="${CR_NAME:-demo}"
KUBECTL="${KUBECTL:-kubectl}"
API_DEPLOYMENT="${API_DEPLOYMENT:-cube-api-demo}"
API_SECRET="${API_SECRET:-cube-router-ha-demo-secret}"
LEADER_SERVICE_NAME="${LEADER_SERVICE_NAME:-cube-router-leader}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.4}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_CONNECT_TIMEOUT_SECONDS="${MYSQL_CONNECT_TIMEOUT_SECONDS:-5}"
MYSQL_POD_RUNNING_TIMEOUT_SECONDS="${MYSQL_POD_RUNNING_TIMEOUT_SECONDS:-30}"
IMPORT_WAIT_SECONDS="${IMPORT_WAIT_SECONDS:-120}"
IMPORT_CHECK_INTERVAL_SECONDS="${IMPORT_CHECK_INTERVAL_SECONDS:-2}"
EXPECTED_ROW_COUNT="${EXPECTED_ROW_COUNT:-12}"
EXPECTED_TOTAL_AMOUNT="${EXPECTED_TOTAL_AMOUNT:-780}"
EXPECTED_ROW_ID="${EXPECTED_ROW_ID:-7}"
EXPECTED_ROW_AMOUNT="${EXPECTED_ROW_AMOUNT:-70}"
EXPECTED_ROW_REGION="${EXPECTED_ROW_REGION:-east}"
EXPECTED_ROW_PAYLOAD="${EXPECTED_ROW_PAYLOAD:-router-ha-east-07}"
FAILOVER_WAIT_SECONDS="${FAILOVER_WAIT_SECONDS:-120}"
FAILOVER_CHECK_INTERVAL_SECONDS="${FAILOVER_CHECK_INTERVAL_SECONDS:-2}"
API_ROLLOUT_TIMEOUT_SECONDS="${API_ROLLOUT_TIMEOUT_SECONDS:-240}"
API_READY_WAIT_SECONDS="${API_READY_WAIT_SECONDS:-120}"
API_READY_CHECK_INTERVAL_SECONDS="${API_READY_CHECK_INTERVAL_SECONDS:-2}"
API_REQUEST_TIMEOUT_SECONDS="${API_REQUEST_TIMEOUT_SECONDS:-45}"
FIXTURE="${FIXTURE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fixtures/router-ha-data.sql}"
SCHEMA="${ROUTER_HA_SCHEMA:-router_ha_probe}"
TABLE="${ROUTER_HA_TABLE:-router_ha_data}"
RUN_ID="${RUN_ID:-$(date +%s)-$$}"
RUN_ID="$(printf '%s' "$RUN_ID" | tr -cd '[:alnum:]_')"
MYSQL_CLIENT_SEQUENCE=0

for command_name in "$KUBECTL" jq sed tr awk sleep; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "error: required command '$command_name' not found" >&2; exit 1; }
done

if command -v shasum >/dev/null 2>&1; then
  HASH_TOOL="shasum"
elif command -v sha256sum >/dev/null 2>&1; then
  HASH_TOOL="sha256sum"
else
  echo "error: required command 'shasum' or 'sha256sum' not found" >&2
  exit 1
fi

require_positive_integer() {
  local name="$1" value="$2"
  case "$value" in
    ''|*[!0-9]*) echo "error: $name must be a positive integer, got '$value'" >&2; exit 1 ;;
  esac
  [ "$value" -gt 0 ] || { echo "error: $name must be greater than zero" >&2; exit 1; }
}

for timeout_name in \
  MYSQL_CONNECT_TIMEOUT_SECONDS MYSQL_POD_RUNNING_TIMEOUT_SECONDS IMPORT_WAIT_SECONDS IMPORT_CHECK_INTERVAL_SECONDS \
  FAILOVER_WAIT_SECONDS FAILOVER_CHECK_INTERVAL_SECONDS API_ROLLOUT_TIMEOUT_SECONDS \
  API_READY_WAIT_SECONDS API_READY_CHECK_INTERVAL_SECONDS API_REQUEST_TIMEOUT_SECONDS; do
  require_positive_integer "$timeout_name" "${!timeout_name}"
done

get_leader() { "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leader}'; }
get_epoch() { "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leaderEpoch}'; }
get_service_endpoint_pod() {
  "$KUBECTL" -n "$NAMESPACE" get endpointslice -l "kubernetes.io/service-name=$LEADER_SERVICE_NAME" -o json |
    jq -r '[.items[].endpoints[]?.targetRef.name] | map(select(. != null)) | unique | .[0] // empty'
}
mysql_query() {
  MYSQL_CLIENT_SEQUENCE=$((MYSQL_CLIENT_SEQUENCE + 1))
  local client_pod="cube-api-e2e-mysql-${RUN_ID}-${MYSQL_CLIENT_SEQUENCE}"
  "$KUBECTL" run -n "$NAMESPACE" "$client_pod" --rm -i --quiet --restart=Never \
    --pod-running-timeout="${MYSQL_POD_RUNNING_TIMEOUT_SECONDS}s" --image="$MYSQL_IMAGE" --command -- \
    mysql --protocol=TCP --connect-timeout="$MYSQL_CONNECT_TIMEOUT_SECONDS" \
      -h "$LEADER_SERVICE_NAME.$NAMESPACE.svc.cluster.local" -P "$MYSQL_PORT" -u "$MYSQL_USER" -N -B -e "$1"
}
query_cube_api() {
  "$KUBECTL" -n "$NAMESPACE" exec "deploy/$API_DEPLOYMENT" -- env API_SECRET="$API_SECRET" QUERY_JSON="$1" API_REQUEST_TIMEOUT_SECONDS="$API_REQUEST_TIMEOUT_SECONDS" node -e '
    const http = require("http"); const jwt = require("jsonwebtoken");
    const body = JSON.stringify({ query: JSON.parse(process.env.QUERY_JSON) });
    const req = http.request({hostname: "127.0.0.1", port: 4000, path: "/cubejs-api/v1/load", method: "POST", headers: {authorization: `Bearer ${jwt.sign({}, process.env.API_SECRET)}`, "content-type": "application/json", "content-length": Buffer.byteLength(body)}}, res => { let b = ""; res.setEncoding("utf8"); res.on("data", c => b += c); res.on("end", () => { process.stdout.write(b); process.exit(res.statusCode >= 200 && res.statusCode < 300 ? 0 : 1); }); });
    req.on("error", e => { console.error(e.stack || e); process.exit(1); });
    req.setTimeout(Number(process.env.API_REQUEST_TIMEOUT_SECONDS) * 1000, () => req.destroy(new Error("Cube API request timeout")));
    req.end(body);
  '
}
assert_fixture_loaded() {
  local rows amount
  rows="$(mysql_query "SELECT count(*) FROM $SCHEMA.$TABLE" | tr -d '[:space:]')"
  amount="$(mysql_query "SELECT sum(amount) FROM $SCHEMA.$TABLE" | tr -d '[:space:]')"
  [ "$rows" = "$EXPECTED_ROW_COUNT" ] || { echo "fatal: fixture row count mismatch (want=$EXPECTED_ROW_COUNT got=$rows)" >&2; return 1; }
  [ "$amount" = "$EXPECTED_TOTAL_AMOUNT" ] || { echo "fatal: fixture total amount mismatch (want=$EXPECTED_TOTAL_AMOUNT got=$amount)" >&2; return 1; }
}
import_fixture() {
  local sql existing_table
  sql="$(sed -e "s/__SCHEMA__/$SCHEMA/g" -e "s/__TABLE__/$TABLE/g" "$FIXTURE")"
  existing_table="$(mysql_query "SELECT id FROM system.tables WHERE table_schema = '$SCHEMA' AND table_name = '$TABLE'")"
  if [ -n "$existing_table" ]; then
    mysql_query "DROP TABLE $SCHEMA.$TABLE" >/dev/null
  fi
  mysql_query "$(printf '%s\n' "$sql" | sed -n '2p')" >/dev/null
  mysql_query "$(printf '%s\n' "$sql" | sed -n '3p')" >/dev/null
  mysql_query "$(printf '%s\n' "$sql" | sed -n '4,$p' | tr '\n' ' ')" >/dev/null
}
wait_for_fixture() {
  local waited=0 error_file="/tmp/cube-api-e2e-import.err"
  while [ "$waited" -le "$IMPORT_WAIT_SECONDS" ]; do
    if assert_fixture_loaded 2>"$error_file"; then
      return 0
    fi
    sleep "$IMPORT_CHECK_INTERVAL_SECONDS"
    waited=$((waited + IMPORT_CHECK_INTERVAL_SECONDS))
  done
  echo "fatal: fixture import did not reach expected rows/aggregate within ${IMPORT_WAIT_SECONDS}s" >&2
  cat "$error_file" >&2 || true
  return 1
}
assert_aggregate() {
  printf '%s' "$1" | jq -e --argjson rows "$EXPECTED_ROW_COUNT" --argjson amount "$EXPECTED_TOTAL_AMOUNT" \
    '(.data | length) == 1 and (.data[0]["RouterHaProbe.rowCount"] | tonumber) == $rows and (.data[0]["RouterHaProbe.totalAmount"] | tonumber) == $amount' \
    >/dev/null || { echo "fatal: Cube API aggregate mismatch" >&2; printf '%s\n' "$1" >&2; return 1; }
}
assert_concrete_row() {
  printf '%s' "$1" | jq -e --arg id "$EXPECTED_ROW_ID" --arg amount "$EXPECTED_ROW_AMOUNT" --arg region "$EXPECTED_ROW_REGION" --arg payload "$EXPECTED_ROW_PAYLOAD" \
    '(.data | length) == 1 and .data[0] == {"RouterHaProbe.id":$id,"RouterHaProbe.amount":$amount,"RouterHaProbe.region":$region,"RouterHaProbe.payload":$payload}' \
    >/dev/null || { echo "fatal: Cube API concrete-row mismatch" >&2; printf '%s\n' "$1" >&2; return 1; }
}
assert_groups() {
  printf '%s' "$1" | jq -e '(.data | length) == 4 and .data == [{"RouterHaProbe.region":"east","RouterHaProbe.rowCount":"3","RouterHaProbe.totalAmount":"240"},{"RouterHaProbe.region":"north","RouterHaProbe.rowCount":"3","RouterHaProbe.totalAmount":"60"},{"RouterHaProbe.region":"south","RouterHaProbe.rowCount":"3","RouterHaProbe.totalAmount":"150"},{"RouterHaProbe.region":"west","RouterHaProbe.rowCount":"3","RouterHaProbe.totalAmount":"330"}]' \
    >/dev/null || { echo "fatal: Cube API grouped aggregate mismatch" >&2; printf '%s\n' "$1" >&2; return 1; }
}
hash_snapshot() {
  jq -cS |
    if [ "$HASH_TOOL" = "shasum" ]; then shasum -a 256; else sha256sum; fi |
    awk '{print $1}'
}
wait_for_failover() {
  local old="$1" waited=0
  while [ "$waited" -le "$FAILOVER_WAIT_SECONDS" ]; do
    local leader endpoint; leader="$(get_leader)"; endpoint="$(get_service_endpoint_pod)"
    if [ -n "$leader" ] && [ "$leader" != "$old" ] && [ "$leader" = "$endpoint" ]; then printf '%s' "$leader"; return 0; fi
    sleep "$FAILOVER_CHECK_INTERVAL_SECONDS"; waited=$((waited + FAILOVER_CHECK_INTERVAL_SECONDS))
  done
  return 1
}
wait_for_cube_api_data() {
  local waited=0 output error_file="/tmp/cube-api-e2e-api.err"
  while [ "$waited" -le "$API_READY_WAIT_SECONDS" ]; do
    if output="$(query_cube_api "$aggregate_query" 2>"$error_file")" && assert_aggregate "$output" 2>>"$error_file"; then
      return 0
    fi
    sleep "$API_READY_CHECK_INTERVAL_SECONDS"
    waited=$((waited + API_READY_CHECK_INTERVAL_SECONDS))
  done
  echo "fatal: Cube API did not return the expected aggregate within ${API_READY_WAIT_SECONDS}s" >&2
  cat "$error_file" >&2 || true
  return 1
}

"$KUBECTL" -n "$NAMESPACE" rollout status "deploy/$API_DEPLOYMENT" --timeout="${API_ROLLOUT_TIMEOUT_SECONDS}s" >/dev/null
old_leader="$(get_leader)"; old_epoch="$(get_epoch)"
[ -n "$old_leader" ] || { echo "error: no active Router leader" >&2; exit 1; }
[ -n "$old_epoch" ] && [ "$old_epoch" != 0 ] || { echo "error: leaderEpoch is missing or zero" >&2; exit 1; }
aggregate_query='{"measures":["RouterHaProbe.rowCount","RouterHaProbe.totalAmount"]}'
row_query='{"dimensions":["RouterHaProbe.id","RouterHaProbe.amount","RouterHaProbe.region","RouterHaProbe.payload"],"filters":[{"member":"RouterHaProbe.id","operator":"equals","values":["7"]}],"limit":1}'
groups_query='{"measures":["RouterHaProbe.rowCount","RouterHaProbe.totalAmount"],"dimensions":["RouterHaProbe.region"],"order":{"RouterHaProbe.region":"asc"}}'
capture_api() {
  local aggregate row groups
  aggregate="$(query_cube_api "$aggregate_query")"; assert_aggregate "$aggregate"
  row="$(query_cube_api "$row_query")"; assert_concrete_row "$row"
  groups="$(query_cube_api "$groups_query")"; assert_groups "$groups"
  jq -n --argjson aggregate "$aggregate" --argjson row "$row" --argjson groups "$groups" '{aggregate:$aggregate.data,row:$row.data,groups:$groups.data}'
}

import_fixture
wait_for_fixture
wait_for_cube_api_data
before="$(capture_api)"; before_hash="$(printf '%s' "$before" | hash_snapshot)"
printf 'Cube API before: rows=%s amount=%s concrete_row_id=%s hash=%s\n' "$EXPECTED_ROW_COUNT" "$EXPECTED_TOTAL_AMOUNT" "$EXPECTED_ROW_ID" "$before_hash"
"$KUBECTL" -n "$NAMESPACE" delete pod "$old_leader"
new_leader="$(wait_for_failover "$old_leader")" || { echo "error: Cube API E2E failover timed out after ${FAILOVER_WAIT_SECONDS}s" >&2; exit 1; }
new_epoch="$(get_epoch)"
[ "$new_epoch" -gt "$old_epoch" ] || { echo "error: leaderEpoch did not increase (before=$old_epoch after=$new_epoch)" >&2; exit 1; }
wait_for_cube_api_data
after="$(capture_api)"; after_hash="$(printf '%s' "$after" | hash_snapshot)"
[ "$before_hash" = "$after_hash" ] || { echo "fatal: Cube API real-data result changed across failover" >&2; exit 1; }
printf 'Cube API after: rows=%s amount=%s concrete_row_id=%s hash=%s\n' "$EXPECTED_ROW_COUNT" "$EXPECTED_TOTAL_AMOUNT" "$EXPECTED_ROW_ID" "$after_hash"
printf 'PASS: Cube API real-data rows, concrete row, grouped aggregate and query result preserved (leader=%s epoch=%s)\n' "$new_leader" "$new_epoch"
