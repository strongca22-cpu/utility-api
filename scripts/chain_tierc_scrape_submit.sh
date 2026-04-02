#!/usr/bin/env bash
#
# Script Name: chain_tierc_scrape_submit.sh
# Purpose: Scrape Tier C discovered URLs (20 workers) → submit batch
# Author: AI-Generated
# Created: 2026-04-02
#
# Notes:
#   - Tier C discovery already complete (3,097 PWSIDs)
#   - Retry fix active: 0-char URLs marked dead after 2 attempts
#   - Ranks 4/5 NOT included — deferred to next session
#

set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; source .env; set +a; fi
log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "Scraping Tier C discovered URLs (20 workers)..."
python scripts/bulk_scrape_parallel.py \
    --workers 20 \
    --rank 1 \
    --since "2026-04-02 07:52:00" \
    --idle-timeout 1800 \
    2>&1
log "Tier C scrape complete."

log "Submitting Tier C parse batch (rank 1 only)..."
python scripts/submit_discovery_batch.py \
    --strategy rank1_only \
    --label "bulk_replace_c" \
    2>&1
log "Tier C batch submitted."

log "============================================================"
log "DONE — $(date -Iseconds)"
log "Batches at Anthropic:"
log "  Rank 2: msgbatch_01V6pooL84nzTphfu6Q6ZeVd (2,776)"
log "  Rank 3: msgbatch_01Kt8Dip45w8weAHjGQAHmtr (5,037)"
log "  Tier C: (see above)"
log ""
log "INCOMPLETE: Ranks 4/5 scrape+batch deferred to next session."
log "============================================================"
