#!/usr/bin/env bash
#
# Script Name: chain_r3_and_tierc.sh
# Purpose: Scrape rank 3 + Tier C discovered URLs with 20 workers,
#          then auto-submit both batches. Runs parallel with ongoing
#          Tier C discovery (picks up new URLs as they appear).
# Author: AI-Generated
# Created: 2026-04-02
#
# Usage:
#   tmux new-session -d -s chain_r3c \
#       "cd ~/projects/utility-api && bash scripts/chain_r3_and_tierc.sh 2>&1 | tee logs/chain_r3c.log"
#
# Notes:
#   - 20 workers (system has 24 cores, 8GB free RAM)
#   - Rank 3 serper URLs: ~6,183 pending
#   - Tier C discovered URLs: ~4,849+ pending (growing as discovery runs)
#   - Workers scrape BOTH rank 3 and Tier C URLs simultaneously
#   - After scrape: submit rank 3 batch, then Tier C batch
#   - Ranks 4/5 are NOT included — deferred to next session
#

set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a; source .env; set +a
fi

log() { echo "[$(date '+%H:%M:%S')] $*"; }

WORKERS=20
IDLE_TIMEOUT=1800
TIERC_SINCE="2026-04-02 07:52:00"

# ─── Step 1: Scrape rank 3 (serper only) ──────────────────────────

log "Step 1: Scraping rank 3 serper URLs with $WORKERS workers..."
python scripts/bulk_scrape_parallel.py \
    --workers $WORKERS \
    --rank 3 \
    --since "2026-03-31 17:15:00" \
    --idle-timeout $IDLE_TIMEOUT \
    2>&1
log "  Rank 3 scrape complete."

# ─── Step 2: Submit rank 3 batch ─────────────────────────────────

log "Step 2: Submitting rank 3 parse batch..."
python scripts/submit_discovery_batch.py \
    --strategy rank1_only \
    --min-rank 3 --max-rank 3 \
    --label "discovery_r3" \
    2>&1
log "  Rank 3 batch submitted."

# ─── Step 3: Wait for Tier C discovery to finish ─────────────────

log "Step 3: Checking if Tier C discovery is still running..."
if tmux has-session -t bulk_discover 2>/dev/null; then
    log "  Discovery still running — waiting..."
    while tmux has-session -t bulk_discover 2>/dev/null; do
        log "  bulk_discover still running..."
        sleep 60
    done
fi
log "  Tier C discovery complete."

# ─── Step 4: Scrape Tier C discovered URLs ───────────────────────

log "Step 4: Scraping Tier C discovered URLs with $WORKERS workers..."
# These are rank 1 serper URLs created since discovery started
python scripts/bulk_scrape_parallel.py \
    --workers $WORKERS \
    --rank 1 \
    --since "$TIERC_SINCE" \
    --idle-timeout $IDLE_TIMEOUT \
    2>&1
log "  Tier C scrape complete."

# ─── Step 5: Submit Tier C batch ─────────────────────────────────

log "Step 5: Submitting Tier C parse batch..."
# Shotgun strategy: send all viable URLs per PWSID
python scripts/submit_discovery_batch.py \
    --strategy shotgun \
    --label "bulk_replace_c" \
    2>&1
log "  Tier C batch submitted."

log ""
log "============================================================"
log "CHAIN COMPLETE — $(date -Iseconds)"
log ""
log "Batches submitted:"
log "  - Rank 2:  msgbatch_01V6pooL84nzTphfu6Q6ZeVd (2,776 tasks)"
log "  - Rank 3:  (see above)"
log "  - Tier C:  (see above)"
log ""
log "NOTE: Ranks 4 and 5 scrape+batch still pending."
log "      Run separately in next session."
log ""
log "Process batches when Anthropic returns:"
log "  python scripts/run_prompt_reparse.py --check-status"
log "============================================================"
