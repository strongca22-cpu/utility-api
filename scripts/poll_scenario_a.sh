#!/usr/bin/env bash
#
# Script Name: poll_scenario_a.sh
# Purpose: Polls Scenario A batch status every 15 minutes. When complete,
#          processes results, rebuilds best_estimate, and exports dashboard data.
#          Designed to run in a tmux session and exit when done.
# Author: AI-Generated
# Created: 2026-03-31
# Modified: 2026-03-31
#
# Usage:
#   tmux new-session -d -s scenario_a_poll \
#       "cd ~/projects/utility-api && ./scripts/poll_scenario_a.sh 2>&1 | tee logs/scenario_a_poll.log"
#
# Dependencies:
#   - Python environment with utility-api installed
#   - ANTHROPIC_API_KEY in environment (loaded from .env)
#
# Notes:
#   - Polls every 15 minutes (configurable via POLL_INTERVAL)
#   - Exits after successful processing or after MAX_POLLS attempts
#   - Dashboard watcher (refresh_dashboard.sh) will pick up changes on its cycle
#   - Kill the tmux session to stop: tmux kill-session -t scenario_a_poll
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
readonly POLL_INTERVAL="${POLL_INTERVAL:-900}"  # 15 minutes default
readonly MAX_POLLS="${MAX_POLLS:-120}"          # 120 × 15min = 30 hours max

# Load environment
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

cd "$PROJECT_ROOT"

echo "============================================================"
echo "Scenario A Batch Poller"
echo "Started: $(date -Iseconds)"
echo "Poll interval: ${POLL_INTERVAL}s"
echo "Max polls: ${MAX_POLLS}"
echo "============================================================"

for i in $(seq 1 "$MAX_POLLS"); do
    echo ""
    echo "[Poll $i/$MAX_POLLS — $(date -Iseconds)]"

    # Check status — this updates local DB from Anthropic API
    STATUS_OUTPUT=$(python scripts/run_scenario_a.py --check-status 2>&1)
    echo "$STATUS_OUTPUT"

    # Check if any batch is now completed (either just transitioned or already was)
    if echo "$STATUS_OUTPUT" | grep -q "'local_status': 'completed'" || \
       echo "$STATUS_OUTPUT" | grep -q "'api_status': 'ended'"; then
        echo ""
        echo "============================================================"
        echo "BATCH COMPLETED — Processing results..."
        echo "============================================================"

        # Use process_scenario_a_batch.py which queries status='completed' directly,
        # avoiding the race condition where --check-status consumes the transition
        # before --process-batch can see it.
        python scripts/process_scenario_a_batch.py 2>&1
        PROCESS_EXIT=$?

        if [ $PROCESS_EXIT -eq 0 ]; then
            echo ""
            echo "============================================================"
            echo "PROCESSING COMPLETE — $(date -Iseconds)"
            echo "============================================================"
            echo "Best estimate has been rebuilt for affected states."
            echo "Dashboard watcher will pick up changes on its next cycle."
            echo ""
            echo "Next steps:"
            echo "  1. Check logs/scenario_a_poll.log for results"
            echo "  2. Run gap-sourced targeted batch if needed:"
            echo "     python scripts/run_targeted_research.py --batch top25_gap_sourced"
            exit 0
        else
            echo ""
            echo "WARNING: process-batch exited with code $PROCESS_EXIT"
            echo "Check output above for errors. NOT retrying automatically."
            exit 1
        fi
    fi

    # Check if still in progress
    if echo "$STATUS_OUTPUT" | grep -q "'local_status': 'in_progress'"; then
        echo "  Still processing. Next check in ${POLL_INTERVAL}s..."
        sleep "$POLL_INTERVAL"
    else
        # Neither completed nor in_progress — something unexpected
        echo "  Unexpected status. Checking again in ${POLL_INTERVAL}s..."
        sleep "$POLL_INTERVAL"
    fi
done

echo ""
echo "MAX POLLS REACHED ($MAX_POLLS). Batch still not complete."
echo "Re-run this script or check manually:"
echo "  python scripts/run_scenario_a.py --check-status"
exit 1
