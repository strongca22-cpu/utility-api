#!/usr/bin/env bash
#
# Script Name: refresh_dashboard.sh
# Purpose: Periodically export latest rate data, rebuild the UAPI dashboard,
#          and restart uvicorn. Logs each refresh with coverage stats.
# Author: AI-Generated
# Created: 2026-03-30
# Modified: 2026-03-30
#
# Usage:
#   # Run directly (loops every INTERVAL seconds):
#   ./scripts/refresh_dashboard.sh [--interval 600]
#
#   # Run in tmux (recommended):
#   tmux new-session -d -s dashboard_refresh \
#       "cd ~/projects/utility-api && ./scripts/refresh_dashboard.sh 2>&1 | tee -a logs/dashboard_refresh.log"
#
# Dependencies:
#   - Python environment with utility-api installed
#   - Node.js / npm (for Vite build)
#   - uvicorn serving the strong-strategic dashboard on port 9090
#
# Notes:
#   - Checks rate_best_estimate count before/after to skip no-op refreshes
#   - Logs cumulative stats to logs/dashboard_refresh.log
#   - Kill the tmux session to stop: tmux kill-session -t dashboard_refresh
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
readonly SS_ROOT="$HOME/projects/strong-strategic"
readonly LOG_FILE="$PROJECT_ROOT/logs/dashboard_refresh.log"
readonly INTERVAL="${1:-600}"  # Default 10 minutes

# Database connection
readonly PGUSER="strong_strategic"
readonly PGPASS="changeme"
readonly PGDB="strong_strategic"
readonly PGHOST="localhost"

db_query() {
    PGPASSWORD="$PGPASS" psql -U "$PGUSER" -h "$PGHOST" -d "$PGDB" -t -A -c "$1" 2>/dev/null
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

get_rbe_count() {
    db_query "SELECT COUNT(DISTINCT pwsid) FROM utility.rate_best_estimate;"
}

get_coverage_stats() {
    db_query "
        SELECT json_build_object(
            'rbe_pwsids', (SELECT COUNT(DISTINCT pwsid) FROM utility.rate_best_estimate),
            'rs_pwsids', (SELECT COUNT(DISTINCT pwsid) FROM utility.rate_schedules),
            'rs_not_in_rbe', (
                SELECT COUNT(DISTINCT rs.pwsid)
                FROM utility.rate_schedules rs
                LEFT JOIN utility.rate_best_estimate rbe ON rbe.pwsid = rs.pwsid
                WHERE rbe.pwsid IS NULL
            ),
            'scraped_today', (
                SELECT COUNT(*)
                FROM utility.rate_schedules
                WHERE source_key = 'scraped_llm'
                AND scrape_timestamp::date = CURRENT_DATE
            )
        );
    "
}

refresh_dashboard() {
    local start_time
    start_time=$(date +%s)

    # Step 1: Export
    log "Exporting data..."
    cd "$PROJECT_ROOT"
    local export_output
    export_output=$(python scripts/export_dashboard_data.py 2>&1)

    # Extract key numbers from export output
    local with_rate pop_cov
    with_rate=$(echo "$export_output" | grep "With rate data:" | sed 's/.*: *//' | sed 's/ .*//')
    pop_cov=$(echo "$export_output" | grep "Population coverage:" | sed 's/.*: *//' | sed 's/%.*//')

    # Step 2: Build
    log "Building dashboard..."
    cd "$PROJECT_ROOT/dashboard"
    npm run build --silent 2>&1

    # Step 3: Restart uvicorn
    log "Restarting uvicorn..."
    kill $(pgrep -f "uvicorn.*9090") 2>/dev/null || true
    sleep 2
    cd "$SS_ROOT"
    .venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 9090 --app-dir dashboard \
        >> "$SS_ROOT/logs/dashboard.log" 2>&1 &
    disown
    sleep 3

    # Step 4: Verify
    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" http://100.103.211.71:9090/utility-rate-explorer/ 2>/dev/null || echo "000")

    local elapsed=$(( $(date +%s) - start_time ))

    if [ "$http_code" = "200" ]; then
        log "REFRESH OK | PWSIDs: ${with_rate} | Pop: ${pop_cov}% | ${elapsed}s"
    else
        log "REFRESH WARN | Export done but dashboard returned HTTP $http_code | ${elapsed}s"
    fi
}

# ─── Main loop ───────────────────────────────────────────────────────────────

log "=========================================="
log "Dashboard auto-refresh starting"
log "Interval: ${INTERVAL}s ($(( INTERVAL / 60 ))m)"
log "=========================================="

# Initial stats
log "Initial DB state: $(get_coverage_stats)"

last_rbe_count=$(get_rbe_count)
cycle=0

while true; do
    cycle=$(( cycle + 1 ))

    # Check if there are new PWSIDs
    current_rbe_count=$(get_rbe_count)
    delta=$(( current_rbe_count - last_rbe_count ))

    if [ "$delta" -gt 0 ] || [ "$cycle" -eq 1 ]; then
        log "Cycle $cycle: +${delta} PWSIDs in rate_best_estimate (${last_rbe_count} → ${current_rbe_count}). Refreshing..."
        refresh_dashboard
        last_rbe_count="$current_rbe_count"
    else
        # Still log a heartbeat with current gap info
        local_gap=$(db_query "SELECT COUNT(DISTINCT rs.pwsid) FROM utility.rate_schedules rs LEFT JOIN utility.rate_best_estimate rbe ON rbe.pwsid = rs.pwsid WHERE rbe.pwsid IS NULL;" 2>/dev/null || echo "?")
        log "Cycle $cycle: No new PWSIDs in rate_best_estimate (still ${current_rbe_count}). RBE gap: ${local_gap}. Skipping."
    fi

    sleep "$INTERVAL"
done
