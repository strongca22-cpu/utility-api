#!/usr/bin/env bash
#
# Script Name: auto_discovery_to_parse.sh
# Purpose: Monitors discovery sweep, then automatically launches cascade parse
#          when discovery completes. Chains: discovery → cascade parse → best
#          estimate rebuild → dashboard export.
# Author: AI-Generated
# Created: 2026-03-31
# Modified: 2026-03-31
#
# Usage:
#   tmux new-session -d -s auto_parse \
#       "cd ~/projects/utility-api && ./scripts/auto_discovery_to_parse.sh 2>&1 | tee logs/auto_parse.log"
#
# Notes:
#   - Polls for discovery_sweep tmux session every 2 minutes
#   - When discovery_sweep exits, launches cascade parse
#   - Cascade parse uses process_pwsid (rank 1-5 + deep crawl)
#   - After parse: rebuilds best_estimate + exports dashboard
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
readonly POLL_INTERVAL=120  # 2 minutes

cd "$PROJECT_ROOT"

# Load environment
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

echo "============================================================"
echo "Auto Discovery → Parse Pipeline"
echo "Started: $(date -Iseconds)"
echo "============================================================"
echo ""
echo "Waiting for discovery_sweep tmux session to complete..."

# Poll until discovery_sweep session is gone
while tmux has-session -t discovery_sweep 2>/dev/null; do
    echo "[$(date +%H:%M)] discovery_sweep still running. Next check in ${POLL_INTERVAL}s..."
    sleep "$POLL_INTERVAL"
done

echo ""
echo "============================================================"
echo "Discovery sweep complete — launching cascade parse"
echo "$(date -Iseconds)"
echo "============================================================"
echo ""

# Run cascade parse (all unparsed PWSIDs with Serper URLs)
python scripts/run_cascade_parse.py 2>&1
PARSE_EXIT=$?

if [ $PARSE_EXIT -eq 0 ]; then
    echo ""
    echo "============================================================"
    echo "CASCADE PARSE COMPLETE — $(date -Iseconds)"
    echo "============================================================"
    echo "Best estimate rebuilt. Dashboard data exported."
else
    echo ""
    echo "WARNING: cascade parse exited with code $PARSE_EXIT"
    echo "Check logs for errors."
fi
