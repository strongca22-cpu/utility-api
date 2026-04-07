# Session Summary — utility-api Backup System v0

**Date:** 2026-04-06
**Chat:** `docs/chat_prompts/utility_api_backups_v0.md`
**Status:** Tier 1 + Tier 2 shipped, tested, and verified. Tier 3 documented, not installed.

---

## What was built

A three-tier backup system for the `utility` schema, motivated by the source_url integrity audit incident on the same day (see `2026-04-06_source_url_audit_and_recovery.md`).

### Tier 1 — daily `pg_dump`
- **Script:** `scripts/backup_db.sh` (executable, headed per CLAUDE.md)
- **Output:** `~/backups/utility-api/daily/utility_<UTC-timestamp>.dump` (custom format, compress=9, no-owner, no-acl)
- **Retention:** rolling 14 daily / 8 weekly (Sunday promoted) / ∞ monthly (1st-of-month promoted)
- **Logging:** `~/backups/utility-api/logs/backup_<date>.log`
- **Credentials:** read at runtime from `utility_api.config.settings` via a python heredoc → temp env file → sourced; no secrets in the script
- **Idempotent + non-zero on failure** for cron mailer

### Tier 2 — pre-write snapshots
- **Module:** `src/utility_api/ops/snapshot.py` exposing `snapshot_critical_tables(reason: str) -> dict[str, Path]`
- **Tables:** `rate_schedules`, `rate_best_estimate`, `pwsid_coverage`, `scrape_registry` (the last with `scraped_text` and `last_parse_raw_response` omitted — large blobs that rarely change in a rebuild)
- **Output:** `~/backups/utility-api/snapshots/<table>_<reason>_<UTC-timestamp>.csv.gz`
- **Mechanism:** psycopg `COPY (...) TO STDOUT WITH (FORMAT csv, HEADER)` streamed through gzip — fast, low-memory
- **Auto-prune:** snapshots older than 30 days deleted at end of each call
- **Wired into:** `src/utility_api/ops/best_estimate.py::run_best_estimate()` immediately before the destructive `TRUNCATE` / `DELETE WHERE state_code=`. New `snapshot: bool = True` parameter; failures log + continue (do NOT block the rebuild).
- **CLI:** `ua-ops snapshot --reason "manual_pre_migration"` (added to `src/utility_api/cli/ops.py`)

### Tier 3 — documented only
- Comparison table of 4 options (Tailscale rsync, rclone B2, rclone S3/GCS, USB) in `docs/backup_system.md`
- **Recommendation:** Tailscale rsync first (cheapest, fastest activation, uses existing tailnet) → add B2 later for geographic offsite
- Config sketches for both options
- **NOT installed** — awaiting user choice

---

## Verification (Task 5)

| Check | Result |
|---|---|
| `scripts/backup_db.sh` manual run | ✓ 1.2 GB dump in 177 s, log written |
| Test restore to fresh PostGIS 16 container (`postgis/postgis:16-3.4` in docker on port 55432) | ✓ ~45 s; container torn down after |
| Row count match prod vs restored on `rate_schedules`, `rate_best_estimate`, `pwsid_coverage`, `scrape_registry` | ✓ 100% match (27,743 / 18,575 / 44,643 / 117,893) |
| Manual `ua-ops snapshot --reason "backup_system_setup_test"` | ✓ 4 .csv.gz files in `~/backups/utility-api/snapshots/` |
| `ua-ops build-best-estimate --state RI` (small, real, prod-touching test) | ✓ snapshot fired before DELETE; 35/35 RI PWSIDs rebuilt; snapshot files written with reason=`best_estimate_rebuild` |

---

## Files touched

| File | Action |
|---|---|
| `scripts/backup_db.sh` | NEW — Tier 1 |
| `src/utility_api/ops/snapshot.py` | NEW — Tier 2 module |
| `src/utility_api/ops/best_estimate.py` | EDIT — added `snapshot: bool = True` param + pre-DELETE call |
| `src/utility_api/cli/ops.py` | EDIT — added `snapshot` command |
| `docs/backup_system.md` | NEW — full design + restore + Tier 3 docs |
| `docs/next_steps.md` | EDIT — moved Backup System bullet from open to done |
| `docs/session_summaries/2026-04-06_backup_system_v0.md` | NEW — this file |

`~/backups/utility-api/` is OUTSIDE the repo. No changes to `.gitignore` needed (the existing `data/` ignore already covers the previous in-repo backups, and the new path lives in `$HOME` and is never under git control).

---

## What is still open (the user must do these manually)

1. **Install the cron line for Tier 1.** Documented exactly in `docs/backup_system.md` under "Cron entry to install". Run `crontab -e` and paste:
   ```cron
   0 3 * * * /home/colin/projects/utility-api/scripts/backup_db.sh >> /home/colin/backups/utility-api/logs/cron.log 2>&1
   ```
2. **Pick a Tier 3 option.** Recommendation: Tailscale rsync first, B2 later. Both config sketches are in `docs/backup_system.md`. Neither is installed.
3. **Decide the fate of the legacy ad-hoc backups in `data/backups/`.** The new system supersedes them. They are gitignored already. Safe to leave in place; safe to delete after confirming Tier 1 is running cleanly for a few days.

---

## Notes for the next session

- The dump is 1.2 GB compressed and takes ~3 min. If the cron line gets installed, by day 14 the daily/ dir will hold ~17 GB. weekly/ (8 dumps) ~10 GB. monthly/ grows ~1.2 GB/month. All trivially small.
- The `--no-owner --no-acl` flags are MANDATORY because PostGIS extension objects can't be reassigned during restore. Don't drop them.
- The snapshot module deliberately catches its own exceptions in the `run_best_estimate` wiring point — a snapshot failure must NOT block a legitimate rebuild. This is the trade-off: better to lose one backup than to lose the ability to rebuild.
- If the snapshot CLI ever fails with `psycopg` import errors, the issue is that this module imports `psycopg` indirectly via SQLAlchemy's `.connection.cursor().copy()` API — only psycopg 3 supports `.copy()` as a context manager. This codebase already uses psycopg3 (see `db.py`), so should be fine.
