#!/usr/bin/env bash
#
# Script Name: chain_remaining_ranks.sh
# Purpose: Scrape and submit all remaining ranks in cascade order.
#          Each rank is scraped with 20 workers, then batch-submitted.
#
#          Order: Earlier R4 → Earlier R5 → Tier C R2 → Tier C R3 →
#                 Tier C R4 → Tier C R5
#
#          NOTE: Ideally each rank batch would be processed before the
#          next rank is submitted (cascade exclusion). In practice,
#          build_parse_tasks uses NOT EXISTS rate_schedules, so PWSIDs
#          that already have rates from a higher rank are automatically
#          excluded from the batch. The cost waste from overlap is small.
#
# Author: AI-Generated
# Created: 2026-04-02
#
# Usage:
#   tmux new-session -d -s chain_ranks \
#       "cd ~/projects/utility-api && bash scripts/chain_remaining_ranks.sh 2>&1 | tee logs/chain_remaining_ranks.log"
#

set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; source .env; set +a; fi
log() { echo "[$(date '+%H:%M:%S')] $*"; }

WORKERS=20
IDLE_TIMEOUT=1800
EARLIER_SINCE="2026-03-31 17:15:00"
TIERC_SINCE="2026-04-02 07:52:00"

scrape_and_submit() {
    local label="$1"
    local rank="$2"
    local since="$3"
    local batch_label="$4"

    log "--- $label: Scraping rank $rank (since $since) ---"
    python scripts/bulk_scrape_parallel.py \
        --workers $WORKERS \
        --rank "$rank" \
        --since "$since" \
        --idle-timeout $IDLE_TIMEOUT \
        2>&1

    log "--- $label: Submitting batch (rank $rank) ---"
    python scripts/submit_discovery_batch.py \
        --strategy rank1_only \
        --min-rank "$rank" --max-rank "$rank" \
        --label "$batch_label" \
        2>&1

    log "--- $label: Done ---"
    echo ""
}

log "============================================================"
log "Remaining Ranks Cascade"
log "============================================================"

# Earlier pipeline ranks 4-5
scrape_and_submit "Earlier R4" 4 "$EARLIER_SINCE" "discovery_r4"
scrape_and_submit "Earlier R5" 5 "$EARLIER_SINCE" "discovery_r5"

# Tier C ranks 2-5
scrape_and_submit "Tier C R2" 2 "$TIERC_SINCE" "bulk_replace_c_r2"
scrape_and_submit "Tier C R3" 3 "$TIERC_SINCE" "bulk_replace_c_r3"
scrape_and_submit "Tier C R4" 4 "$TIERC_SINCE" "bulk_replace_c_r4"
scrape_and_submit "Tier C R5" 5 "$TIERC_SINCE" "bulk_replace_c_r5"

log "============================================================"
log "ALL REMAINING RANKS COMPLETE — $(date -Iseconds)"
log "============================================================"
