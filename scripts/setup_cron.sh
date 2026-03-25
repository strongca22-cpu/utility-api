#!/usr/bin/env bash
#
# Script Name: setup_cron.sh
# Purpose: Install UAPI pipeline cron jobs for automated daily execution
# Author: AI-Generated
# Created: 2026-03-25
# Modified: 2026-03-25
#
# Usage:
#   ./scripts/setup_cron.sh          # Install cron jobs
#   ./scripts/setup_cron.sh --remove # Remove cron jobs
#
# Dependencies:
#   - cron service running (sudo service cron start)
#   - /var/log/uapi/ directory (created by this script, needs sudo)
#
# Notes:
#   - Requires sudo for log directory creation
#   - Cron entries run as the current user (not root)
#   - ANTHROPIC_API_KEY is sourced from .env before each run
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
readonly VENV_BIN="/home/colin/projects/strong-strategic/.venv/bin"
readonly ENV_FILE="${PROJECT_DIR}/.env"
readonly LOG_DIR="/var/log/uapi"

# Marker comment for identifying UAPI cron entries
readonly CRON_MARKER="# UAPI-PIPELINE"

# --- Functions ---

create_log_dir() {
    if [ ! -d "$LOG_DIR" ]; then
        echo "Creating log directory ${LOG_DIR} (requires sudo)..."
        echo "Please run: sudo mkdir -p ${LOG_DIR} && sudo chown $(whoami):$(whoami) ${LOG_DIR}"
        echo "Then re-run this script."
        exit 1
    fi
    echo "Log directory exists: ${LOG_DIR}"
}

install_cron() {
    create_log_dir

    # Check if cron entries already exist
    if crontab -l 2>/dev/null | grep -q "${CRON_MARKER}"; then
        echo "UAPI cron entries already installed. Use --remove first to reinstall."
        crontab -l | grep "${CRON_MARKER}" -A 1
        return
    fi

    # Build the cron entries
    local cron_block
    cron_block=$(cat <<CRON
${CRON_MARKER} — Daily orchestrator: discover + scrape + parse top 15 PWSIDs
0 2 * * * cd ${PROJECT_DIR} && set -a && source ${ENV_FILE} && set +a && ${VENV_BIN}/ua-run-orchestrator --execute 15 >> ${LOG_DIR}/orchestrator.log 2>&1

${CRON_MARKER} — Daily coverage refresh: update materialized views
0 5 * * * cd ${PROJECT_DIR} && set -a && source ${ENV_FILE} && set +a && ${VENV_BIN}/ua-ops refresh-coverage >> ${LOG_DIR}/coverage.log 2>&1

${CRON_MARKER} — Daily batch processing: check and process completed batches
0 10 * * * cd ${PROJECT_DIR} && set -a && source ${ENV_FILE} && set +a && ${VENV_BIN}/ua-ops process-batches >> ${LOG_DIR}/batch.log 2>&1

${CRON_MARKER} — Weekly bulk source check: detect new data vintages (Sunday 6 AM)
0 6 * * 0 cd ${PROJECT_DIR} && set -a && source ${ENV_FILE} && set +a && ${VENV_BIN}/ua-ops check-sources >> ${LOG_DIR}/sources.log 2>&1
CRON
)

    # Append to existing crontab
    (crontab -l 2>/dev/null; echo ""; echo "$cron_block") | crontab -

    echo "UAPI cron entries installed:"
    echo "  02:00 daily  — Orchestrator (discover + scrape + parse top 50)"
    echo "  05:00 daily  — Coverage refresh"
    echo "  10:00 daily  — Process completed batches"
    echo "  06:00 Sunday — Bulk source freshness check"
    echo ""
    echo "Logs: ${LOG_DIR}/"
    echo ""
    echo "Verify with: crontab -l | grep UAPI"
}

remove_cron() {
    if ! crontab -l 2>/dev/null | grep -q "${CRON_MARKER}"; then
        echo "No UAPI cron entries found."
        return
    fi

    # Remove lines containing the marker and the line after each marker
    crontab -l | grep -v "${CRON_MARKER}" | sed '/^$/N;/^\n$/d' | crontab -
    echo "UAPI cron entries removed."
}

# --- Main ---

case "${1:-}" in
    --remove)
        remove_cron
        ;;
    *)
        install_cron
        ;;
esac
