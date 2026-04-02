#!/usr/bin/env bash
#
# Script Name: scrape_all_remaining.sh
# Purpose: Scrape-only pass through all remaining ranks. NO batch submissions.
#          Batches submitted manually in cascade order as Anthropic returns results.
#
# Ranks: Earlier R5, Tier C R2-R5
# Uses 5min idle timeout per tier — moves on quickly, tail deferred.
#
# Author: AI-Generated
# Created: 2026-04-02

set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; source .env; set +a; fi
log() { echo "[$(date '+%H:%M:%S')] $*"; }

WORKERS=20
IDLE_TIMEOUT=300

scrape_rank() {
    local label="$1"
    local rank="$2"
    local since="$3"

    log "=== $label: Scraping rank $rank ==="
    python scripts/bulk_scrape_parallel.py \
        --workers $WORKERS \
        --rank "$rank" \
        --since "$since" \
        --idle-timeout $IDLE_TIMEOUT \
        2>&1
    log "=== $label: Scrape done ==="
    echo ""
}

log "============================================================"
log "Scrape All Remaining Ranks (NO batch submissions)"
log "============================================================"

scrape_rank "Earlier R5" 5 "2026-03-31 17:15:00"
scrape_rank "Tier C R2"  2 "2026-04-02 07:52:00"
scrape_rank "Tier C R3"  3 "2026-04-02 07:52:00"
scrape_rank "Tier C R4"  4 "2026-04-02 07:52:00"
scrape_rank "Tier C R5"  5 "2026-04-02 07:52:00"

log "============================================================"
log "ALL SCRAPING COMPLETE — $(date -Iseconds)"
log ""
log "Text is in the DB. Submit batches manually in cascade order:"
log "  1. Wait for R4 batch to return + process"
log "  2. Submit R5:     python scripts/submit_discovery_batch.py --strategy rank1_only --min-rank 5 --max-rank 5 --label discovery_r5"
log "  3. Wait for R5 + process"
log "  4. Submit TC-R2:  python scripts/submit_discovery_batch.py --strategy rank1_only --min-rank 2 --max-rank 2 --label bulk_replace_c_r2"
log "  5. Continue cascade: TC-R3, TC-R4, TC-R5"
log "============================================================"
