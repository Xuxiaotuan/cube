#!/usr/bin/env bash
set -euo pipefail

# Keep the historical entry point, but never allow a SELECT 1 check to pass.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/real-data-failover-check.sh" "$@"
