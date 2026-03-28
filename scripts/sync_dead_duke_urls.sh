#!/usr/bin/env bash
#
# Script Name: sync_dead_duke_urls.sh
# Purpose: Periodically export dead duke-reference URLs from DB and sync to VPS
#          for domain guesser re-processing. Dead URLs = known PWSIDs with stale
#          researcher-verified domains. The stale domain is a high-value hint.
# Author: AI-Generated
# Created: 2026-03-26
# Modified: 2026-03-26
#
# Usage:
#   tmux new-session -d -s duke_dead_sync "bash scripts/sync_dead_duke_urls.sh"
#
# Dependencies:
#   - psql (PostgreSQL client)
#   - ssh/scp access to VPS (via ~/.ssh/config alias 'vultr')
#   - PGPASSWORD set in environment or .env
#
# Notes:
#   - Runs every 30 minutes
#   - Exports dead duke URLs in domain guesser input format
#   - Includes stale_url column as domain hint for the guesser
#   - Syncs to VPS at /home/botuser/bots/domain_guesser/duke_dead_for_guessing.csv
#   - Also maintains a guesser-compatible version (no extra columns) for --input
#   - Only re-syncs if the export changed (row count delta)
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
readonly VPS_ALIAS="vultr"
readonly VPS_DEST="/home/botuser/bots/domain_guesser"
readonly LOCAL_FULL="$PROJECT_DIR/data/duke_dead_urls_for_guesser.csv"
readonly LOCAL_GUESSER_INPUT="$PROJECT_DIR/data/duke_dead_guesser_input.csv"
readonly LOG_FILE="/var/log/uapi/duke_dead_sync.log"
readonly SLEEP_SECONDS=1800  # 30 minutes

# DB connection — reads from .env if PGPASSWORD not set
if [ -z "${PGPASSWORD:-}" ]; then
    if [ -f "$PROJECT_DIR/.env" ]; then
        PGPASSWORD=$(grep -oP 'DB_PASSWORD=\K.*' "$PROJECT_DIR/.env" 2>/dev/null || echo "changeme")
        export PGPASSWORD
    else
        export PGPASSWORD="changeme"
    fi
fi

readonly DB_HOST="${DB_HOST:-localhost}"
readonly DB_USER="${DB_USER:-strong_strategic}"
readonly DB_NAME="${DB_NAME:-strong_strategic}"

mkdir -p "$(dirname "$LOG_FILE")"
mkdir -p "$(dirname "$LOCAL_FULL")"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOG_FILE"
}

export_dead_urls() {
    # Full export with stale URL and rate status
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "\copy (
        SELECT
            sr.pwsid,
            pc.state_code AS state,
            COALESCE(pc.population_served, 0) AS population_served,
            pc.pws_name,
            '' AS county_served,
            COALESCE(ss.city, '') AS city,
            sr.url AS stale_url,
            CASE WHEN pc.has_rate_data THEN 'yes' ELSE 'no' END AS has_existing_rates
        FROM utility.scrape_registry sr
        JOIN utility.pwsid_coverage pc ON pc.pwsid = sr.pwsid
        LEFT JOIN utility.sdwis_systems ss ON ss.pwsid = sr.pwsid
        WHERE sr.url_source = 'duke_reference' AND sr.status = 'dead'
        ORDER BY pc.population_served DESC NULLS LAST
    ) TO STDOUT WITH CSV HEADER" > "$LOCAL_FULL"

    # Guesser-compatible version (standard 6 columns only)
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "\copy (
        SELECT
            sr.pwsid,
            pc.state_code AS state,
            COALESCE(pc.population_served, 0) AS population_served,
            pc.pws_name,
            '' AS county_served,
            COALESCE(ss.city, '') AS city
        FROM utility.scrape_registry sr
        JOIN utility.pwsid_coverage pc ON pc.pwsid = sr.pwsid
        LEFT JOIN utility.sdwis_systems ss ON ss.pwsid = sr.pwsid
        WHERE sr.url_source = 'duke_reference' AND sr.status = 'dead'
        ORDER BY pc.population_served DESC NULLS LAST
    ) TO STDOUT WITH CSV HEADER" > "$LOCAL_GUESSER_INPUT"
}

count_rows() {
    local file="$1"
    if [ -f "$file" ]; then
        echo $(( $(wc -l < "$file") - 1 ))
    else
        echo 0
    fi
}

log "=== Dead duke URL sync started ==="
log "Full export: $LOCAL_FULL"
log "Guesser input: $LOCAL_GUESSER_INPUT"
log "VPS dest: $VPS_ALIAS:$VPS_DEST/"

LAST_COUNT=0
if [ -f "$LOCAL_FULL" ]; then
    LAST_COUNT=$(count_rows "$LOCAL_FULL")
    log "Existing export: $LAST_COUNT rows"
fi

while true; do
    log "Exporting dead duke URLs from DB..."

    if ! export_dead_urls; then
        log "DB export failed — will retry in $SLEEP_SECONDS seconds"
        sleep "$SLEEP_SECONDS"
        continue
    fi

    NEW_COUNT=$(count_rows "$LOCAL_FULL")
    NO_RATES=$(grep -c ',no$' "$LOCAL_FULL" 2>/dev/null || echo 0)

    if [ "$NEW_COUNT" -eq "$LAST_COUNT" ]; then
        log "No change: $NEW_COUNT dead URLs ($NO_RATES without rates). Skipping sync."
        sleep "$SLEEP_SECONDS"
        continue
    fi

    DELTA=$((NEW_COUNT - LAST_COUNT))
    log "Export: $NEW_COUNT dead URLs ($NO_RATES without rates), delta: $DELTA"

    # Sync both files to VPS
    log "Syncing to VPS..."
    if scp "$LOCAL_FULL" "$VPS_ALIAS:$VPS_DEST/duke_dead_urls_full.csv" && \
       scp "$LOCAL_GUESSER_INPUT" "$VPS_ALIAS:$VPS_DEST/duke_dead_for_guessing.csv"; then
        log "Synced to VPS: duke_dead_urls_full.csv + duke_dead_for_guessing.csv"
    else
        log "SCP failed — will retry next cycle"
    fi

    LAST_COUNT=$NEW_COUNT
    log "Sleeping $SLEEP_SECONDS seconds..."
    sleep "$SLEEP_SECONDS"
done
