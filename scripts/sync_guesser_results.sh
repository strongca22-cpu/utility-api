#!/usr/bin/env bash
#
# Script Name: sync_guesser_results.sh
# Purpose: Watch VPS for new domain guesser state CSVs, download, import, and process.
# Author: AI-Generated
# Created: 2026-03-25
# Modified: 2026-03-25
#
# Usage:
#   tmux new-session -d -s guesser_sync "bash scripts/sync_guesser_results.sh"
#
# Dependencies:
#   - ssh/scp access to VPS (via ~/.ssh/config alias 'vultr')
#   - Python with utility_api installed
#   - scripts/import_guesser_state.py
#   - scripts/process_guesser_batch.py
#
# Notes:
#   - Runs in a loop, checking VPS every 10 minutes
#   - Tracks processed states in .processed_states.txt to avoid re-import
#   - Checks file size stability before downloading (avoids partial files)
#   - Logs to /var/log/uapi/guesser_sync.log
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
readonly VPS_ALIAS="vultr"  # SSH config alias
readonly VPS_RESULTS_DIR="/home/botuser/bots/domain_guesser/results"
readonly LOCAL_DIR="$PROJECT_DIR/data/guesser_incoming"
readonly PROCESSED_LOG="$LOCAL_DIR/.processed_states.txt"
readonly LOG_FILE="/var/log/uapi/guesser_sync.log"
readonly SLEEP_SECONDS=600  # 10 minutes
readonly MAX_PROCESS_PER_STATE=500  # process all URLs per state

# Ensure directories exist
mkdir -p "$LOCAL_DIR"
mkdir -p "$(dirname "$LOG_FILE")"
touch "$PROCESSED_LOG"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOG_FILE"
}

log "=== Guesser sync watcher started ==="
log "VPS: $VPS_ALIAS:$VPS_RESULTS_DIR"
log "Local: $LOCAL_DIR"
log "Already processed: $(cat "$PROCESSED_LOG" | tr '\n' ' ')"

while true; do
    log "Checking VPS for new state CSVs..."

    # List completed CSVs on VPS
    REMOTE_FILES=$(ssh "$VPS_ALIAS" "ls $VPS_RESULTS_DIR/guessed_domains_*.csv 2>/dev/null" 2>/dev/null | sort) || {
        log "SSH failed — will retry in $SLEEP_SECONDS seconds"
        sleep "$SLEEP_SECONDS"
        continue
    }

    if [ -z "$REMOTE_FILES" ]; then
        log "No CSV files found on VPS"
        sleep "$SLEEP_SECONDS"
        continue
    fi

    NEW_COUNT=0
    for REMOTE_PATH in $REMOTE_FILES; do
        FILENAME=$(basename "$REMOTE_PATH")
        # Extract state code: guessed_domains_VA.csv -> VA
        STATE=$(echo "$FILENAME" | sed 's/guessed_domains_//; s/\.csv//')

        # Skip already-processed states
        if grep -q "^${STATE}$" "$PROCESSED_LOG" 2>/dev/null; then
            continue
        fi

        # Check if the file is still being written (size changing)
        SIZE1=$(ssh "$VPS_ALIAS" "stat -c %s '$REMOTE_PATH' 2>/dev/null") || continue
        sleep 5
        SIZE2=$(ssh "$VPS_ALIAS" "stat -c %s '$REMOTE_PATH' 2>/dev/null") || continue

        if [ "$SIZE1" != "$SIZE2" ]; then
            log "$STATE: still being written ($SIZE1 -> $SIZE2 bytes), skipping"
            continue
        fi

        # Download
        log "$STATE: downloading $FILENAME ($SIZE2 bytes)..."
        if ! scp "$VPS_ALIAS:$REMOTE_PATH" "$LOCAL_DIR/$FILENAME"; then
            log "$STATE: SCP failed"
            continue
        fi

        ROWS=$(wc -l < "$LOCAL_DIR/$FILENAME")
        log "$STATE: downloaded ($((ROWS - 1)) data rows)"

        # Import to scrape_registry
        log "$STATE: importing to registry..."
        cd "$PROJECT_DIR"
        IMPORT_OUTPUT=$(python3 scripts/import_guesser_state.py "$LOCAL_DIR/$FILENAME" 2>&1) || {
            log "$STATE: import FAILED"
            log "$IMPORT_OUTPUT"
            continue
        }
        log "$STATE: $IMPORT_OUTPUT"

        # Mark as processed
        echo "$STATE" >> "$PROCESSED_LOG"
        NEW_COUNT=$((NEW_COUNT + 1))
    done

    if [ "$NEW_COUNT" -gt 0 ]; then
        log "Imported $NEW_COUNT new state(s). Processing through pipeline..."

        cd "$PROJECT_DIR"
        python3 scripts/process_guesser_batch.py --max "$MAX_PROCESS_PER_STATE" 2>&1 | tee -a "$LOG_FILE" || {
            log "Processing encountered errors (see above)"
        }

        log "Refreshing coverage..."
        ua-ops refresh-coverage 2>&1 | tail -3 | tee -a "$LOG_FILE"

        # Coverage summary
        ua-ops coverage-report 2>&1 | grep "TOTAL" | tee -a "$LOG_FILE"

        log "Cycle complete."
    fi

    log "Sleeping $SLEEP_SECONDS seconds..."
    sleep "$SLEEP_SECONDS"
done
