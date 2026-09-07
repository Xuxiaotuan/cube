#!/usr/bin/env bash
set -euo pipefail

# No kubectl mutations, PID discovery, Pod operations or production name inputs.
# --live delegates only UUID test-owned Lease operations to guarded Go tests.
mode=${1:---synthetic}
if [[ "$#" -gt 1 || ( "$mode" != --synthetic && "$mode" != --live ) ]]; then
  printf 'Usage: bash %s [--synthetic|--live]\n' "$0" >&2
  exit 2
fi
if [[ "$mode" == --live && "${CUBE_HA_LIVE_LEASE_TEST:-}" != 1 ]]; then
  printf 'Live mode requires CUBE_HA_LIVE_LEASE_TEST=1 and current context orbstack.\n' >&2
  exit 2
fi
operator_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
audit=$(mktemp -d "${TMPDIR:-/tmp}/cube-ha-controlplane-audit.XXXXXX")
printf 'AUDIT_DIRECTORY=%s\n' "$audit"
cd "$operator_root"
{
  printf 'mode=%s\n' "$mode"
  date -u '+started_at=%Y-%m-%dT%H:%M:%SZ'
  printf 'scope=local-test-subprocesses;optional-UUID-Leases-only\n'
  printf 'not_proven=Manager-election-loss,node-isolation,business-publication-fencing,traffic-outage\n'
} > "$audit/scope.txt"
CUBE_HA_LIVE_LEASE_TEST=0 go test ./internal/leadership ./controllers \
  -run 'TestLeaseLifecycle|TestLeaseBootstrap|TestLeaseManagerAdoption|TestLeaseCancelled|TestKubernetesAuthority|TestKubernetesRelease|TestKubernetesManual|TestKubernetesCancelled' \
  -v -count=1 -timeout=120s 2>&1 | tee "$audit/synthetic.log"
if [[ "$mode" == --live ]]; then
  CUBE_HA_LIVE_LEASE_TEST=1 go test ./internal/leadership \
    -run '^TestKubernetesLive(LeaseCAS|ControlledRecovery)$' \
    -v -count=1 -timeout=120s 2>&1 | tee "$audit/live.log"
fi
printf 'ACCEPTANCE_PASS scope=%s audit=%s\n' "$mode" "$audit"
printf 'External gates remain: actual Manager handover/loss, infrastructure isolation and business recovery.\n'
