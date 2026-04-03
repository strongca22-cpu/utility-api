#!/usr/bin/env bash
#
# Script Name: chain_tc_cascade_final.sh
# Purpose: Auto-cascade TC-R3 → TC-R4 → TC-R5 → tail sweep.
#          Polls Anthropic, processes each batch before submitting next.
# Author: AI-Generated
# Created: 2026-04-03
#

set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; source .env; set +a; fi
log() { echo "[$(date '+%H:%M:%S')] $*"; }

poll_and_process() {
    local label="$1"
    log "Polling for $label batch completion..."
    while true; do
        status=$(python3 -c "
from utility_api.agents.batch import BatchAgent
statuses = BatchAgent().check_status()
completed = [s for s in statuses if s.get('local_status') == 'completed']
print(len(completed))
" 2>/dev/null)
        if [ "$status" -gt 0 ] 2>/dev/null; then
            log "$label batch completed — processing..."
            python3 -c "
from utility_api.agents.batch import BatchAgent
agent = BatchAgent()
statuses = agent.check_status()
for s in statuses:
    if s.get('local_status') == 'completed':
        result = agent.process_batch(s['batch_id'])
        print(f'  {s[\"batch_id\"][:25]}: {result.get(\"succeeded\",0)} succeeded, {result.get(\"failed\",0)} failed')
" 2>&1
            log "$label processed."
            break
        fi
        log "  Still waiting..."
        sleep 120
    done
}

log "============================================================"
log "TC Cascade Final: TC-R3 → TC-R4 → TC-R5 → Tail Sweep"
log "============================================================"

# Step 1: Wait for TC-R3 (already submitted)
poll_and_process "TC-R3"

# Step 2: Submit TC-R4, wait, process
log "Submitting TC-R4 batch..."
python scripts/submit_discovery_batch.py \
    --strategy rank1_only --min-rank 4 --max-rank 4 \
    --label "bulk_replace_c_r4" 2>&1
poll_and_process "TC-R4"

# Step 3: Submit TC-R5, wait, process
log "Submitting TC-R5 batch..."
python scripts/submit_discovery_batch.py \
    --strategy rank1_only --min-rank 5 --max-rank 5 \
    --label "bulk_replace_c_r5" 2>&1
poll_and_process "TC-R5"

# Step 4: Tail sweep scrape
log "Starting tail sweep scrape (all pending, any source)..."
python scripts/bulk_scrape_parallel.py \
    --workers 20 --any-source --idle-timeout 600 2>&1

# Step 5: Submit tail sweep batch
log "Submitting tail sweep batch..."
python scripts/submit_discovery_batch.py \
    --strategy shotgun --label "tail_sweep" 2>&1

log "============================================================"
log "CASCADE COMPLETE — $(date -Iseconds)"
log "Final batch (tail_sweep) at Anthropic."
log "Process when returned:"
log "  python scripts/run_prompt_reparse.py --check-status"
log "============================================================"
