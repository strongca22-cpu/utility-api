#!/usr/bin/env bash
#
# Script Name: chain_ny_locality.sh
# Purpose: Automated chain for NY locality discovery pipeline
#          Waits for TC-R2, runs bug-fix rescrape, submits parse batches,
#          processes completed batches — all hands-free.
# Author: AI-Generated
# Created: 2026-04-02
# Modified: 2026-04-02
#
# Usage:
#   tmux new-session -s ny_chain
#   bash scripts/chain_ny_locality.sh 2>&1 | tee logs/chain_ny_locality.log
#
# Dependencies:
#   - utility_api (local package)
#   - TC-R2 scrape (PID 51353) running or already finished
#
# Chain steps:
#   1. Wait for TC-R2 to finish (poll PID 51353)
#   2. Process ny_locality_r1 batch (already complete at Anthropic)
#   3. Run bug-fix rescrape (183 URLs: 114 HTML + 69 PDF)
#   4. Submit locality_discovery rank 2-5 parse batch
#   5. Submit bug-fix rescrape parse batch
#   6. Poll and process all pending batches
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Ensure src is on PYTHONPATH
export PYTHONPATH="${PROJECT_DIR}/src:${PYTHONPATH:-}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# ======================================================================
# Step 1: Wait for TC-R2
# ======================================================================
TC_R2_PID=51353

if kill -0 "$TC_R2_PID" 2>/dev/null; then
    log "STEP 1: Waiting for TC-R2 (PID $TC_R2_PID) to finish..."
    while kill -0 "$TC_R2_PID" 2>/dev/null; do
        sleep 30
    done
    log "STEP 1: TC-R2 finished."
else
    log "STEP 1: TC-R2 already finished (PID $TC_R2_PID not running). Continuing."
fi

# ======================================================================
# Step 2: Process ny_locality_r1 batch (already complete)
# ======================================================================
log "STEP 2: Processing ny_locality_r1 batch..."
python3 -c "
import sys
sys.path.insert(0, 'src')
from utility_api.agents.batch import BatchAgent
agent = BatchAgent()
result = agent.process_batch('msgbatch_019FMYcpEwA4ri9zrfkcCey6')
succeeded = result.get('succeeded', 0)
failed = result.get('failed', 0)
print(f'  ny_locality_r1: {succeeded} succeeded, {failed} failed')
" 2>&1
log "STEP 2: Done."

# ======================================================================
# Step 3: Bug-fix rescrape (HTML first, then PDF)
# ======================================================================
log "STEP 3: Running bug-fix rescrape — HTML (114 URLs)..."
python3 scripts/rescrape_bugfix_ny.py --html-only 2>&1
log "STEP 3a: HTML rescrape done."

log "STEP 3: Running bug-fix rescrape — PDF (69 URLs)..."
python3 scripts/rescrape_bugfix_ny.py --pdf-only 2>&1
log "STEP 3b: PDF rescrape done."

# ======================================================================
# Step 4: Submit locality_discovery rank 2-5 parse batch
# ======================================================================
log "STEP 4: Submitting locality_discovery rank 2-5 parse batch..."
python3 -c "
import sys
sys.path.insert(0, 'src')
from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.rate_parser import DOMAIN_BLACKLIST
from utility_api.agents.batch import BatchAgent
from sqlalchemy import text
from urllib.parse import urlparse

schema = settings.utility_schema

with engine.connect() as conn:
    rows = conn.execute(text(f'''
        SELECT sr.id as registry_id, sr.pwsid, sr.scraped_text,
               sr.content_type, sr.url as source_url,
               sr.discovery_rank,
               c.population_served
        FROM {schema}.scrape_registry sr
        JOIN {schema}.cws_boundaries c ON sr.pwsid = c.pwsid
        WHERE sr.url_source = '\''locality_discovery'\''
          AND sr.discovery_rank >= 2
          AND sr.scraped_text IS NOT NULL
          AND LENGTH(sr.scraped_text) > 200
          AND NOT EXISTS (
            SELECT 1 FROM {schema}.rate_schedules rs
            WHERE rs.pwsid = sr.pwsid AND rs.source_key = '\''scraped_llm'\''
          )
        ORDER BY c.population_served DESC, sr.discovery_rank ASC
    ''')).fetchall()

tasks = []
for r in rows:
    hostname = (urlparse(r.source_url or '').hostname or '').lower()
    if hostname in DOMAIN_BLACKLIST:
        continue
    tasks.append({
        'pwsid': r.pwsid,
        'raw_text': r.scraped_text[:45000],
        'content_type': r.content_type or 'html',
        'source_url': r.source_url or '',
        'registry_id': r.registry_id,
    })

print(f'  Locality r2-5: {len(tasks)} parse tasks')
if tasks:
    agent = BatchAgent()
    result = agent.submit(parse_tasks=tasks, state_filter='locality_ny_r2_5')
    bid = result.get('batch_id')
    print(f'  Batch submitted: {bid}')
    with open('/tmp/ny_locality_r2_5_batch_id.txt', 'w') as f:
        f.write(bid or '')
else:
    print('  No tasks to submit.')
" 2>&1
log "STEP 4: Done."

# ======================================================================
# Step 5: Submit bug-fix rescrape parse batch
# ======================================================================
log "STEP 5: Submitting bug-fix rescrape parse batch..."
python3 -c "
import sys
sys.path.insert(0, 'src')
from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.rate_parser import DOMAIN_BLACKLIST
from utility_api.agents.batch import BatchAgent
from sqlalchemy import text
from urllib.parse import urlparse

schema = settings.utility_schema

with engine.connect() as conn:
    rows = conn.execute(text(f'''
        SELECT sr.id as registry_id, sr.pwsid, sr.scraped_text,
               sr.content_type, sr.url as source_url,
               c.population_served
        FROM {schema}.scrape_registry sr
        JOIN {schema}.cws_boundaries c ON sr.pwsid = c.pwsid
        WHERE sr.notes LIKE '\''%rescrape:sprint27_bugfix%'\''
          AND sr.scraped_text IS NOT NULL
          AND LENGTH(sr.scraped_text) > 200
          AND NOT EXISTS (
            SELECT 1 FROM {schema}.rate_schedules rs
            WHERE rs.pwsid = sr.pwsid AND rs.source_key = '\''scraped_llm'\''
          )
        ORDER BY c.population_served DESC
    ''')).fetchall()

tasks = []
for r in rows:
    hostname = (urlparse(r.source_url or '').hostname or '').lower()
    if hostname in DOMAIN_BLACKLIST:
        continue
    tasks.append({
        'pwsid': r.pwsid,
        'raw_text': r.scraped_text[:45000],
        'content_type': r.content_type or 'html',
        'source_url': r.source_url or '',
        'registry_id': r.registry_id,
    })

print(f'  Bugfix rescrape: {len(tasks)} parse tasks')
if tasks:
    agent = BatchAgent()
    result = agent.submit(parse_tasks=tasks, state_filter='bugfix_rescrape_ny')
    bid = result.get('batch_id')
    print(f'  Batch submitted: {bid}')
    with open('/tmp/ny_bugfix_batch_id.txt', 'w') as f:
        f.write(bid or '')
else:
    print('  No tasks — rescrape may have had no successes.')
" 2>&1
log "STEP 5: Done."

# ======================================================================
# Step 6: Poll and process pending batches
# ======================================================================
log "STEP 6: Polling for batch completion..."

poll_and_process() {
    local batch_id="$1"
    local label="$2"

    if [ -z "$batch_id" ]; then
        log "  $label: No batch ID, skipping."
        return
    fi

    log "  $label: Polling $batch_id..."
    while true; do
        status=$(python3 -c "
import sys
sys.path.insert(0, 'src')
import utility_api.config
import anthropic
client = anthropic.Anthropic()
b = client.messages.batches.retrieve('$batch_id')
print(b.processing_status)
" 2>/dev/null)

        if [ "$status" = "ended" ]; then
            log "  $label: Batch complete. Processing..."
            python3 -c "
import sys
sys.path.insert(0, 'src')
from utility_api.agents.batch import BatchAgent
agent = BatchAgent()
result = agent.process_batch('$batch_id')
succeeded = result.get('succeeded', 0)
failed = result.get('failed', 0)
print(f'  $label: {succeeded} succeeded, {failed} failed')
" 2>&1
            break
        else
            log "  $label: Status=$status, waiting 60s..."
            sleep 60
        fi
    done
}

# Read batch IDs from temp files
LOCALITY_R2_5_BID=$(cat /tmp/ny_locality_r2_5_batch_id.txt 2>/dev/null || echo "")
BUGFIX_BID=$(cat /tmp/ny_bugfix_batch_id.txt 2>/dev/null || echo "")

poll_and_process "$LOCALITY_R2_5_BID" "locality_r2_5"
poll_and_process "$BUGFIX_BID" "bugfix_rescrape"

# ======================================================================
# Step 7: Final coverage report
# ======================================================================
log "STEP 7: Final NY coverage report..."
python3 -c "
import sys
sys.path.insert(0, 'src')
from utility_api.config import settings
from utility_api.db import engine
from sqlalchemy import text
schema = settings.utility_schema

with engine.connect() as conn:
    # NY gap before
    total = conn.execute(text(f'''
        SELECT count(*) FROM {schema}.cws_boundaries
        WHERE state_code = '\''NY'\'' AND population_served >= 3000
    ''')).scalar()

    # NY with rate now
    with_rate = conn.execute(text(f'''
        SELECT count(DISTINCT cb.pwsid)
        FROM {schema}.cws_boundaries cb
        JOIN {schema}.rate_best_estimate rbe ON rbe.pwsid = cb.pwsid
        WHERE cb.state_code = '\''NY'\'' AND cb.population_served >= 3000
    ''')).scalar()

    gap = total - with_rate
    print(f'  NY CWS pop>=3k:       {total}')
    print(f'  With rate:            {with_rate}')
    print(f'  Gap remaining:        {gap}')
    print(f'  Coverage:             {with_rate/total*100:.1f}%')

    # New rates from locality discovery
    new_locality = conn.execute(text(f'''
        SELECT count(DISTINCT rs.pwsid)
        FROM {schema}.rate_schedules rs
        JOIN {schema}.scrape_registry sr ON sr.url = rs.source_url AND sr.pwsid = rs.pwsid
        WHERE sr.url_source = '\''locality_discovery'\''
          AND rs.source_key = '\''scraped_llm'\''
    ''')).scalar()
    print(f'  New from locality:    {new_locality}')

    # New rates from bugfix rescrape
    new_bugfix = conn.execute(text(f'''
        SELECT count(DISTINCT rs.pwsid)
        FROM {schema}.rate_schedules rs
        JOIN {schema}.scrape_registry sr ON sr.url = rs.source_url AND sr.pwsid = rs.pwsid
        WHERE sr.notes LIKE '\''%rescrape:sprint27_bugfix%'\''
          AND rs.source_key = '\''scraped_llm'\''
    ''')).scalar()
    print(f'  New from bugfix:      {new_bugfix}')
" 2>&1
log "STEP 7: Done."

log "=========================================="
log "NY LOCALITY CHAIN COMPLETE"
log "=========================================="

# Cleanup temp files
rm -f /tmp/ny_locality_r2_5_batch_id.txt /tmp/ny_bugfix_batch_id.txt
