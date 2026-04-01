#!/usr/bin/env bash
#
# Script Name: prioritized_scrape_and_batch.sh
# Purpose: Prioritized pipeline: scrape rank 1 → batch rank 1 → scrape rank 2-5
#          concurrently with batch rank 2-5 (excluding rank 1 successes)
# Author: AI-Generated
# Created: 2026-03-31
# Modified: 2026-03-31
#
# Flow:
#   1. Wait for discovery_sweep to finish
#   2. Scrape rank 1 URLs only
#   3. Submit rank 1 batch immediately
#   4. Start scraping rank 2-5 in background
#   5. Wait for rank 1 batch to complete + process results
#   6. Submit rank 2-5 batch (excluding rank 1 successes — handled by
#      build_parse_tasks which checks NOT EXISTS in rate_schedules)
#   7. Poll + process rank 2-5 batch
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

echo "============================================================"
echo "Prioritized Scrape + Batch Pipeline"
echo "Started: $(date -Iseconds)"
echo "============================================================"

# --- Step 1: Wait for discovery ---
echo ""
echo "[Step 1] Waiting for discovery_sweep..."
while tmux has-session -t discovery_sweep 2>/dev/null; do
    echo "  [$(date +%H:%M)] discovery still running..."
    sleep 120
done
echo "  Discovery complete."

# --- Step 2: Scrape rank 1 only ---
echo ""
echo "============================================================"
echo "[Step 2] Scraping rank 1 URLs only"
echo "$(date -Iseconds)"
echo "============================================================"
python scripts/bulk_scrape_pending.py \
    --since "2026-03-31 17:15:00" \
    --rank 1 \
    --idle-timeout 300 \
    2>&1

echo "  Rank 1 scrape complete."

# --- Step 3: Submit rank 1 batch ---
echo ""
echo "============================================================"
echo "[Step 3] Submitting rank 1 batch"
echo "$(date -Iseconds)"
echo "============================================================"
python scripts/submit_discovery_batch.py \
    --strategy rank1_only \
    --label "discovery_r1" \
    2>&1

# --- Step 4: Start rank 2-5 scrape in background ---
echo ""
echo "============================================================"
echo "[Step 4] Starting rank 2-5 scrape in background"
echo "$(date -Iseconds)"
echo "============================================================"
tmux new-session -d -s bulk_scrape_r25 \
    "cd $PROJECT_ROOT && python scripts/bulk_scrape_pending.py --since '2026-03-31 17:15:00' --rank-min 2 --idle-timeout 300 2>&1 | tee logs/bulk_scrape_r25.log"
echo "  Rank 2-5 scrape running in tmux:bulk_scrape_r25"

# --- Step 5: Poll for rank 1 batch completion + process ---
echo ""
echo "============================================================"
echo "[Step 5] Polling for rank 1 batch completion..."
echo "$(date -Iseconds)"
echo "============================================================"
./scripts/poll_scenario_a.sh 2>&1
echo "  Rank 1 batch processed."

# --- Step 6: Wait for rank 2-5 scrape, then submit batch ---
echo ""
echo "============================================================"
echo "[Step 6] Waiting for rank 2-5 scrape to finish..."
echo "$(date -Iseconds)"
echo "============================================================"
while tmux has-session -t bulk_scrape_r25 2>/dev/null; do
    echo "  [$(date +%H:%M)] rank 2-5 scrape still running..."
    sleep 120
done
echo "  Rank 2-5 scrape complete."

echo ""
echo "============================================================"
echo "[Step 7] Submitting rank 2-5 batch (excluding rank 1 successes)"
echo "$(date -Iseconds)"
echo "============================================================"
python scripts/submit_discovery_batch.py \
    --strategy shotgun \
    --min-rank 2 \
    --label "discovery_r2_5" \
    2>&1

# --- Step 8: Poll for rank 2-5 batch ---
echo ""
echo "============================================================"
echo "[Step 8] Polling for rank 2-5 batch completion..."
echo "$(date -Iseconds)"
echo "============================================================"
./scripts/poll_scenario_a.sh 2>&1

echo ""
echo "============================================================"
echo "PIPELINE COMPLETE — $(date -Iseconds)"
echo "============================================================"
