#!/usr/bin/env bash
#
# Script Name: backup_db.sh
# Purpose: Tier 1 of the utility-api backup system. Takes a compressed,
#          custom-format pg_dump of the `utility` schema and writes it to
#          ~/backups/utility-api/daily/. Implements rolling retention:
#          14 daily / 8 weekly / infinite monthly.
# Author: AI-Generated
# Created: 2026-04-06
# Modified: 2026-04-06
#
# Usage:
#   ./scripts/backup_db.sh
#
# Dependencies:
#   - pg_dump (PostgreSQL client tools)
#   - python3 (to read DB connection from utility_api.config.settings)
#   - The utility-api repo installed in editable mode (pip install -e .)
#
# Notes:
#   - Reads DB connection settings via utility_api.config.settings, which
#     loads from .env. No credentials live in this script.
#   - Backups go to ~/backups/utility-api/, deliberately OUTSIDE the project
#     directory to survive `rm -rf data/` and accidental git operations.
#   - Idempotent: each run produces a new timestamped file. Re-running on
#     the same day is safe (no overwrite).
#   - Exits non-zero on failure so cron mailer surfaces problems.
#
# Retention buckets:
#   - daily/   keeps last 14 daily dumps
#   - weekly/  keeps last 8 weekly dumps (the Sunday dump is promoted)
#   - monthly/ keeps all monthly dumps forever (the 1st-of-month dump is promoted)
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly BACKUP_ROOT="${HOME}/backups/utility-api"
readonly DAILY_DIR="${BACKUP_ROOT}/daily"
readonly WEEKLY_DIR="${BACKUP_ROOT}/weekly"
readonly MONTHLY_DIR="${BACKUP_ROOT}/monthly"
readonly LOG_DIR="${BACKUP_ROOT}/logs"

readonly TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
readonly DATE_TODAY="$(date -u +%Y-%m-%d)"
readonly DOW="$(date -u +%u)"   # 1-7, Mon=1 Sun=7
readonly DOM="$(date -u +%d)"   # day of month

readonly DUMP_FILE="${DAILY_DIR}/utility_${TIMESTAMP}.dump"
readonly LOG_FILE="${LOG_DIR}/backup_${DATE_TODAY}.log"

mkdir -p "${DAILY_DIR}" "${WEEKLY_DIR}" "${MONTHLY_DIR}" "${LOG_DIR}"

log() {
    local msg="$1"
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ${msg}" | tee -a "${LOG_FILE}"
}

fail() {
    log "ERROR: $1"
    exit 1
}

log "=== utility-api backup_db.sh start ==="
log "REPO_ROOT=${REPO_ROOT}"
log "BACKUP_ROOT=${BACKUP_ROOT}"
log "DUMP_FILE=${DUMP_FILE}"

# --- Read DB settings from utility_api.config (no secrets in this script) ---
log "Reading DB connection from utility_api.config.settings..."

# We export PG* env vars by sourcing a tempfile produced by python.
PG_ENV_TMP="$(mktemp -t utility_api_pgenv.XXXXXX)"
trap 'rm -f "${PG_ENV_TMP}"' EXIT

cd "${REPO_ROOT}"
python3 - >"${PG_ENV_TMP}" <<'PYEOF' || { echo "python failed to read settings" >&2; exit 1; }
from urllib.parse import urlparse
from utility_api.config import settings

u = urlparse(settings.database_url.replace("+psycopg", ""))
print(f"export PGHOST={u.hostname or 'localhost'}")
print(f"export PGPORT={u.port or 5432}")
print(f"export PGUSER={u.username}")
print(f"export PGPASSWORD={u.password or ''}")
print(f"export PGDATABASE={u.path[1:]}")
print(f"export UTILITY_SCHEMA={settings.utility_schema}")
PYEOF

chmod 600 "${PG_ENV_TMP}"
# shellcheck disable=SC1090
source "${PG_ENV_TMP}"

log "Connection: ${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE} schema=${UTILITY_SCHEMA}"

# --- Run pg_dump ---
log "Starting pg_dump..."
START_EPOCH=$(date +%s)

if ! pg_dump \
        --schema="${UTILITY_SCHEMA}" \
        --format=custom \
        --compress=9 \
        --no-owner \
        --no-acl \
        -f "${DUMP_FILE}" 2>>"${LOG_FILE}"; then
    fail "pg_dump failed; see ${LOG_FILE}"
fi

END_EPOCH=$(date +%s)
DURATION=$((END_EPOCH - START_EPOCH))
DUMP_SIZE=$(du -h "${DUMP_FILE}" | cut -f1)
log "pg_dump complete: ${DUMP_SIZE} in ${DURATION}s"

# --- Promote to weekly / monthly buckets ---
# Sunday (DOW=7) → copy to weekly
if [[ "${DOW}" == "7" ]]; then
    cp "${DUMP_FILE}" "${WEEKLY_DIR}/utility_${TIMESTAMP}.dump"
    log "Promoted to weekly: ${WEEKLY_DIR}/utility_${TIMESTAMP}.dump"
fi

# 1st of month → copy to monthly
if [[ "${DOM}" == "01" ]]; then
    cp "${DUMP_FILE}" "${MONTHLY_DIR}/utility_${TIMESTAMP}.dump"
    log "Promoted to monthly: ${MONTHLY_DIR}/utility_${TIMESTAMP}.dump"
fi

# --- Rolling retention ---
# Keep last 14 daily dumps
DAILY_COUNT=$(find "${DAILY_DIR}" -maxdepth 1 -name 'utility_*.dump' -type f | wc -l)
if (( DAILY_COUNT > 14 )); then
    TO_DELETE=$((DAILY_COUNT - 14))
    log "Pruning ${TO_DELETE} old daily dumps (keeping last 14 of ${DAILY_COUNT})"
    find "${DAILY_DIR}" -maxdepth 1 -name 'utility_*.dump' -type f -printf '%T@ %p\n' \
        | sort -n \
        | head -n "${TO_DELETE}" \
        | awk '{print $2}' \
        | while read -r f; do
            log "  delete daily: $(basename "${f}")"
            rm -f "${f}"
        done
fi

# Keep last 8 weekly dumps
WEEKLY_COUNT=$(find "${WEEKLY_DIR}" -maxdepth 1 -name 'utility_*.dump' -type f | wc -l)
if (( WEEKLY_COUNT > 8 )); then
    TO_DELETE=$((WEEKLY_COUNT - 8))
    log "Pruning ${TO_DELETE} old weekly dumps (keeping last 8 of ${WEEKLY_COUNT})"
    find "${WEEKLY_DIR}" -maxdepth 1 -name 'utility_*.dump' -type f -printf '%T@ %p\n' \
        | sort -n \
        | head -n "${TO_DELETE}" \
        | awk '{print $2}' \
        | while read -r f; do
            log "  delete weekly: $(basename "${f}")"
            rm -f "${f}"
        done
fi

# Monthly: infinite retention — never prune

log "=== utility-api backup_db.sh complete (${DUMP_SIZE} in ${DURATION}s) ==="
exit 0
