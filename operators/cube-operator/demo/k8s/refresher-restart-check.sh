#!/usr/bin/env bash
set -euo pipefail
# No deployment/build/config changes. Explicit opt-in is checked by the controller.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec node "$HERE/refresher-restart-check.js" controller
