#!/usr/bin/env bash
#
# Script Name: chain_remaining_fast.sh
# Purpose: Fast pass through remaining ranks — scrape, submit, move on.
#          Slow tail URLs deferred to final sweep.
#          Uses shorter idle-timeout (300s) to avoid tail-grinding.
#
# Remaining: Earlier R5, Tier C R2-R5
# R4 already submitted separately.
#
# Author: AI-Generated
# Created: 2026-04-02

set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; source .env; set +a; fi
log() { echo "[$(date '+%H:%M:%S')] $*"; }

WORKERS=20
IDLE_TIMEOUT=300  # 5 min — move on quickly, tail sweep later

scrape_and_submit() {
    local label="$1"
    local rank="$2"
    local since="$3"
    local batch_label="$4"

    log "=== $label: Scraping rank $rank ==="
    python scripts/bulk_scrape_parallel.py \
        --workers $WORKERS \
        --rank "$rank" \
        --since "$since" \
        --idle-timeout $IDLE_TIMEOUT \
        2>&1

    log "=== $label: Submitting batch ==="
    python scripts/submit_discovery_batch.py \
        --strategy rank1_only \
        --min-rank "$rank" --max-rank "$rank" \
        --label "$batch_label" \
        2>&1

    log "=== $label: Done ==="
    echo ""
}

log "============================================================"
log "Fast Remaining Ranks (5 min idle timeout, tail deferred)"
log "============================================================"

scrape_and_submit "Earlier R5" 5 "2026-03-31 17:15:00" "discovery_r5"
scrape_and_submit "Tier C R2"  2 "2026-04-02 07:52:00"  "bulk_replace_c_r2"
scrape_and_submit "Tier C R3"  3 "2026-04-02 07:52:00"  "bulk_replace_c_r3"
scrape_and_submit "Tier C R4"  4 "2026-04-02 07:52:00"  "bulk_replace_c_r4"
scrape_and_submit "Tier C R5"  5 "2026-04-02 07:52:00"  "bulk_replace_c_r5"

log "============================================================"
log "FAST PASS COMPLETE — $(date -Iseconds)"
log "All batches submitted. Slow tail URLs deferred."
log "============================================================"
