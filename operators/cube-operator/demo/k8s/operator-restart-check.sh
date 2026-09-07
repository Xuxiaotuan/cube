#!/usr/bin/env bash
set -euo pipefail

# Fault scope is deliberately fixed. Never use this as a production restart tool.
HERE="$(cd "$(dirname "$0")" && pwd)"
CONTEXT="${KUBE_CONTEXT:-orbstack}"
NAMESPACE="${NAMESPACE:-cube-ha-remediation}"
[ "$CONTEXT" = orbstack ] && [ "$NAMESPACE" = cube-ha-remediation ] || { echo 'Refusing a non-demo context/namespace' >&2; exit 1; }
: "${HA_RUN:?Supply the run ID of a verified existing rollup}"
: "${EXPECTED_ROWS:?Supply the independent source row count}"
: "${EXPECTED_AMOUNT:?Supply the independent source amount total}"
: "${EXPECTED_CHECKSUM:?Supply the independent source checksum}"
OBSERVE_SECONDS="${OBSERVE_SECONDS:-60}"
[[ "$OBSERVE_SECONDS" =~ ^[0-9]+$ ]] && [ "$OBSERVE_SECONDS" -ge 45 ] && [ "$OBSERVE_SECONDS" -le 600 ] || exit 1
EVIDENCE_DIR="${EVIDENCE_DIR:-$(mktemp -d /tmp/cube-operator-restart.XXXXXX)}"
mkdir -p "$EVIDENCE_DIR"
k() { kubectl --context "$CONTEXT" --request-timeout=10s -n "$NAMESPACE" "$@"; }
old="$(k get cubestorerouter analytics-router -o json)"
leader="$(printf '%s' "$old" | jq -er '.status.leader | select(length > 0)')"
epoch="$(printf '%s' "$old" | jq -er '.status.leaderEpoch | select(. > 0)')"
printf '%s' "$old" | jq '{metadata:{name:.metadata.name,uid:.metadata.uid},status:.status}' > "$EVIDENCE_DIR/router-before.json"
k get deployment cube-operator -o json | jq -e '.spec.template.spec.containers[] | select(.name == "manager") | any(.args[]?; . == "--leader-elect=true")' >/dev/null
selector="$(k get deployment analytics-api -o json | jq -r '.spec.selector.matchLabels | to_entries | map(.key+"="+.value) | join(",")')"
pod="$(k get pods -l "$selector" -o json | jq -er '[.items[] | select(.metadata.deletionTimestamp == null) | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | if length == 1 then .[0].metadata.name else error("Require one ready API") end')"
uid="$(k get pod "$pod" -o jsonpath='{.metadata.uid}')"
events="$EVIDENCE_DIR/queries.jsonl"
pid=''
finish() {
  code=$?; trap - EXIT
  if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  jq -n --argjson exitCode "$code" --arg leader "$leader" --argjson epoch "$epoch" --arg pod "$pod" \
    '{status:(if $exitCode == 0 then "PASS" else "FAIL" end),exitCode:$exitCode,leader:$leader,epoch:$epoch,apiPod:$pod,scope:"operator restart plus existing rollup consumption"}' > "$EVIDENCE_DIR/controller.json"
  echo "Evidence: $EVIDENCE_DIR" >&2
  exit "$code"
}
trap finish EXIT
k exec -i "$pod" -- env HA_RUN="$HA_RUN" OBSERVE_SECONDS="$OBSERVE_SECONDS" EXPECTED_ROWS="$EXPECTED_ROWS" \
  EXPECTED_AMOUNT="$EXPECTED_AMOUNT" EXPECTED_CHECKSUM="$EXPECTED_CHECKSUM" node - < "$HERE/control-plane-query-observer.js" \
  > "$events" 2> "$EVIDENCE_DIR/queries.stderr.log" &
pid=$!
deadline=$(( $(date +%s) + 20 ))
until jq -se 'any(.[]; .event == "query")' "$events" >/dev/null 2>&1; do
  kill -0 "$pid" 2>/dev/null || { echo 'No successful baseline query' >&2; exit 1; }
  [ "$(date +%s)" -lt "$deadline" ] || { echo 'Baseline query deadline exceeded' >&2; exit 1; }
  sleep 1
done
k rollout restart deployment/cube-operator > "$EVIDENCE_DIR/restart.log"
k rollout status deployment/cube-operator --timeout=120s >> "$EVIDENCE_DIR/restart.log"
wait "$pid"
pid=''
current="$(k get cubestorerouter analytics-router -o json)"
printf '%s' "$current" | jq '{metadata:{name:.metadata.name,uid:.metadata.uid},status:.status}' > "$EVIDENCE_DIR/router-after.json"
printf '%s' "$current" | jq -e --arg leader "$leader" --argjson epoch "$epoch" \
  '.status.leader == $leader and .status.leaderEpoch == $epoch and any(.status.conditions[]?; .type == "PromotionReady" and .status == "True")' >/dev/null
k get endpointslice -l kubernetes.io/service-name=analytics-router-leader -o json | \
  jq -e --arg leader "$leader" '[.items[].endpoints[]? | select(.conditions.ready != false) | .targetRef.name] | unique == [$leader]' > "$EVIDENCE_DIR/endpoint-matches.json"
[ "$(k get pod "$pod" -o jsonpath='{.metadata.uid}')" = "$uid" ]
jq -se 'any(.[]; .event == "result" and .status == "PASS")' "$events" >/dev/null
