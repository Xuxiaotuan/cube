#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-cube-operator-demo}"
CR_NAME="${CR_NAME:-demo}"
KUBECTL="${KUBECTL:-kubectl}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.4}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
SERVICE="${LEADER_SERVICE_NAME:-cube-router-leader}"
WAIT="${WAIT_SECONDS:-120}"
INTERVAL="${CHECK_INTERVAL_SECONDS:-2}"
RUN_ID="${RUN_ID:-$(date +%s)-$$}"
RUN_ID="$(printf '%s' "$RUN_ID" | tr -cd '[:alnum:]_')"
SCHEMA="${ROUTER_HA_SCHEMA:-router_ha_$RUN_ID}"
TABLE="${ROUTER_HA_TABLE:-data_$RUN_ID}"
FIXTURE="${FIXTURE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fixtures/router-ha-data.sql}"

for c in "$KUBECTL" jq sha256sum; do
  command -v "$c" >/dev/null || { echo "error: missing $c" >&2; exit 1; }
done
q() { "$KUBECTL" run -n "$NAMESPACE" cubemysql --rm -i --quiet --restart=Never --image="$MYSQL_IMAGE" --command -- mysql -h "$1" -P "$MYSQL_PORT" -u "$MYSQL_USER" -N -B -e "$2"; }
sq() { q "$SERVICE.$NAMESPACE.svc.cluster.local" "$1"; }
leader() { "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leader}'; }
epoch() { "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o jsonpath='{.status.leaderEpoch}'; }
pod_q() { local ip="$( "$KUBECTL" -n "$NAMESPACE" get pod "$1" -o jsonpath='{.status.podIP}')"; [ -n "$ip" ] && q "$ip" "$2"; }

sql="$(sed -e "s/__SCHEMA__/$SCHEMA/g" -e "s/__TABLE__/$TABLE/g" "$FIXTURE")"
header="$(printf '%s\n' "$sql" | sed -n '2,3p')"
sq "$header"
batch="INSERT INTO $SCHEMA.$TABLE (id, amount, payload) VALUES"
while IFS= read -r row; do
  row="${row%;}"
  batch="$batch $row"
  case "$row" in
    *,) ;;
    *) sq "$batch;"; batch="INSERT INTO $SCHEMA.$TABLE (id, amount, payload) VALUES" ;;
  esac
done < <(sed -n '5,$p' "$FIXTURE" | awk 'NR % 250 == 0 { sub(/,$/, ";"); print; next } { print }')
filter="table_schema = '$SCHEMA' AND table_name = '$TABLE'"
tables() { sq "SELECT id, table_schema, table_name, has_data, is_ready FROM system.tables WHERE $filter ORDER BY id"; }
parts() { sq "SELECT id, index_id, active, main_table_row_count FROM system.partitions WHERE index_id = (SELECT id FROM system.tables WHERE $filter) ORDER BY id"; }
chunks() { sq "SELECT id, partition_id, row_count, active FROM system.chunks WHERE partition_id IN (SELECT id FROM system.partitions WHERE index_id = (SELECT id FROM system.tables WHERE $filter)) ORDER BY id"; }
business() { sq "SELECT id, amount, payload FROM $SCHEMA.$TABLE ORDER BY id"; }
capture() {
  T="$(tables)"; P="$(parts)"; C="$(chunks)"; B="$(business | sha256sum | awk '{print $1}')"; N="$(sq "SELECT count(*) FROM $SCHEMA.$TABLE" | tr -d '[:space:]')"
  [ "$N" = 10000 ] || { echo "error: expected 10000 rows, got $N" >&2; return 1; }
  [ -n "$T" ] && [ -n "$P" ] && [ -n "$C" ] || { echo "error: required metadata is empty" >&2; return 1; }
  I="$(printf '%s\n' "$T" | awk -F '\t' 'NR==1{print $1}')"
}

old="$(leader)"; old_epoch="$(epoch)"
[ -n "$old" ] || { echo "error: status.leader is empty" >&2; exit 1; }
[ -n "$old_epoch" ] && [ "$old_epoch" != 0 ] || { echo "error: status.leaderEpoch is missing or zero" >&2; exit 1; }
capture; OT="$T"; OP="$P"; OC="$C"; OB="$B"; OI="$I"
follower="$( "$KUBECTL" -n "$NAMESPACE" get cubestorerouter "$CR_NAME" -o json | jq -r '.status.candidates // [] | map(select(.role=="follower" and .ready==true) | .name)[]' | head -n1)"
if [ -n "$follower" ]; then
  if pod_q "$follower" "SELECT count(*) FROM $SCHEMA.$TABLE" >/tmp/router-ha-follower.out 2>/tmp/router-ha-follower.err; then echo "error: follower query unexpectedly succeeded" >&2; exit 1; fi
  grep -q STALE_LEADER /tmp/router-ha-follower.err || { cat /tmp/router-ha-follower.err >&2; echo "error: follower did not return STALE_LEADER" >&2; exit 1; }
fi
"$KUBECTL" -n "$NAMESPACE" delete pod "$old"
new=""; waited=0
while [ "$waited" -lt "$WAIT" ]; do x="$(leader)"; if [ -n "$x" ] && [ "$x" != "$old" ]; then new="$x"; break; fi; sleep "$INTERVAL"; waited=$((waited+INTERVAL)); done
[ -n "$new" ] || { echo "error: leader promotion timed out" >&2; exit 1; }
new_epoch="$(epoch)"
[ -n "$new_epoch" ] && [ "$new_epoch" -gt "$old_epoch" ] || { echo "error: leaderEpoch did not increase (before=$old_epoch after=$new_epoch)" >&2; exit 1; }
capture
[ "$I" = "$OI" ] || { echo "error: table ID changed" >&2; exit 1; }
[ "$T" = "$OT" ] || { echo "error: system.tables changed" >&2; exit 1; }
[ "$P" = "$OP" ] || { echo "error: system.partitions changed" >&2; exit 1; }
[ "$C" = "$OC" ] || { echo "error: system.chunks changed" >&2; exit 1; }
[ "$B" = "$OB" ] || { echo "error: business-row hash changed" >&2; exit 1; }
printf 'PASS: table_id=%s rows=%s business_hash=%s epoch=%s->%s\n' "$I" "$N" "$B" "$old_epoch" "$new_epoch"
