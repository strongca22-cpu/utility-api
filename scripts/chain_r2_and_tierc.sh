#!/usr/bin/env bash
#
# Script Name: chain_r2_and_tierc.sh
# Purpose: Auto-chain: wait for rank 2 scrape → submit batch →
#          start Tier C scrape (overlaps with discovery) → submit batch
# Author: AI-Generated
# Created: 2026-04-02
#
# Usage:
#   tmux new-session -d -s chain_pipeline \
#       "cd ~/projects/utility-api && bash scripts/chain_r2_and_tierc.sh 2>&1 | tee logs/chain_pipeline.log"
#

set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a; source .env; set +a
fi

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ─── Step 1: Wait for rank 2 scrape ────────────────────────────────

log "Step 1: Waiting for rank 2 scrape (tmux:scrape_r2)..."
while tmux has-session -t scrape_r2 2>/dev/null; do
    log "  scrape_r2 still running..."
    sleep 60
done
log "  Rank 2 scrape complete."

# ─── Step 2: Submit rank 2 batch ──────────────────────────────────

log "Step 2: Submitting rank 2 parse batch..."
python scripts/submit_discovery_batch.py \
    --strategy rank1_only \
    --min-rank 2 --max-rank 2 \
    --label "discovery_r2" \
    2>&1
log "  Rank 2 batch submitted."

# ─── Step 3: Start Tier C scrape while discovery is still running ─
# Discovery creates new scrape_registry rows continuously.
# The scraper picks up newly created rows with scraped_text IS NULL.
# Using --since the discovery start time + --any-source to catch
# all url_sources (serper + domain_guesser from discovery).
# The idle-timeout of 1800s means the scraper keeps polling for new
# URLs as discovery creates them, and exits 30 min after the last one.

log "Step 3: Starting Tier C scrape (overlapping with discovery)..."
log "  Discovery may still be running — scraper will pick up URLs as they appear."

# Wait a bit for discovery to have created some URLs
sleep 120

python scripts/bulk_scrape_parallel.py \
    --workers 10 \
    --since "2026-04-02 07:52:00" \
    --any-source \
    --idle-timeout 1800 \
    2>&1
log "  Tier C scrape complete."

# ─── Step 4: Verify discovery finished ────────────────────────────

if tmux has-session -t bulk_discover 2>/dev/null; then
    log "Step 4: Discovery still running — waiting..."
    while tmux has-session -t bulk_discover 2>/dev/null; do
        sleep 60
    done
    log "  Discovery finished. Running one more scrape pass for stragglers..."
    python scripts/bulk_scrape_parallel.py \
        --workers 10 \
        --since "2026-04-02 07:52:00" \
        --any-source \
        --idle-timeout 600 \
        2>&1
fi

# ─── Step 5: Submit Tier C parse batch ────────────────────────────

log "Step 5: Submitting Tier C parse batch..."
python scripts/submit_discovery_batch.py \
    --strategy shotgun \
    --label "bulk_replace_c" \
    2>&1
log "  Tier C batch submitted."

log ""
log "============================================================"
log "CHAIN COMPLETE — $(date -Iseconds)"
log "Next: process batches when Anthropic returns results"
log "  python scripts/run_prompt_reparse.py --check-status"
log "============================================================"
