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
echo "Waiting for discovery_sweep and bulk_scrape to complete..."

# Poll until both sessions are gone
while tmux has-session -t discovery_sweep 2>/dev/null || tmux has-session -t bulk_scrape 2>/dev/null; do
    DS="done"
    BS="done"
    tmux has-session -t discovery_sweep 2>/dev/null && DS="running"
    tmux has-session -t bulk_scrape 2>/dev/null && BS="running"
    echo "[$(date +%H:%M)] discovery=$DS, scrape=$BS. Next check in ${POLL_INTERVAL}s..."
    sleep "$POLL_INTERVAL"
done

echo ""
echo "============================================================"
echo "Discovery sweep complete — scraping + submitting batch"
echo "$(date -Iseconds)"
echo "============================================================"
echo ""

# Scrape new URLs and submit to Anthropic Batch API for parsing.
# Batch pricing is 50% cheaper than direct API. Results auto-processed
# by poll_scenario_a.sh when the batch completes (~24hr SLA).
python scripts/submit_discovery_batch.py --strategy shotgun 2>&1
SUBMIT_EXIT=$?

if [ $SUBMIT_EXIT -eq 0 ]; then
    echo ""
    echo "============================================================"
    echo "BATCH SUBMITTED — $(date -Iseconds)"
    echo "============================================================"
    echo "Batch will process at Anthropic (~24hr SLA)."
    echo "Start poller to auto-process results:"
    echo "  tmux new-session -d -s discovery_poll ./scripts/poll_scenario_a.sh"
    echo ""
    echo "Starting poller automatically..."
    cd "$PROJECT_ROOT"
    ./scripts/poll_scenario_a.sh 2>&1 | tee -a logs/discovery_poll.log
else
    echo ""
    echo "WARNING: batch submission exited with code $SUBMIT_EXIT"
    echo "Check logs for errors."
fi
