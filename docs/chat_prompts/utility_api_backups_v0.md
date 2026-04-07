# utility-api Backup System v0

## What this chat is for

Set up an aggressive, layered backup system for the `utility-api` PostgreSQL database (specifically the `utility` schema). The data is **trivially small** (~4.6 GB total, ~37 MB for the rate intelligence tables alone) compared to modern storage, so the design philosophy is **back up early, back up often, back up before anything destructive**. Storage cost is not a constraint; recoverability and audit trail are.

This chat is **build-and-implement scoped**, not investigation-scoped. The design is mostly settled (see below); the work is to implement it cleanly, document it, and verify it.

Working repo: `~/projects/utility-api/`. Schema: `utility` (configurable via `settings.utility_schema`). Database is local PostgreSQL accessed via `from utility_api.db import engine`.

## Why we care — the trigger

During the source_url integrity audit (chat: `chat_prompt_source_url_audit_v0.md`, completed 2026-04-06), a code regression caused 91% of `rate_best_estimate.source_url` rows to be NULL after a series of bulk rebuilds. The data was recovered (re-running BestEstimateAgent from a fresh Python process restored 86% URL coverage), but the incident exposed two gaps:

1. **No pre-rebuild snapshots existed.** If the rebuild had failed silently or made things worse, there was no way to recover the previous state. (We took an ad-hoc CSV snapshot manually before the recovery — this should be automatic.)
2. **No regular schema backup exists.** There's no recent `pg_dump` of the `utility` schema. A disk failure, accidental DROP, or `rm -rf data/` would lose 4+ GB of accumulated rate, scrape, and discovery data including SDWIS, EFC, Duke, eAR, and ~16,958 LLM-parsed scraped_llm rows representing real Anthropic API spend (~$50–100 of cumulative parse cost).

The user's directive: *"given the trivial size of this data compared to storage, it would be worth aggressively backing up regularly."*

## Design — three tiers, agreed in advance

### Tier 1 — Daily full schema dump
- **Tool:** `pg_dump --schema=utility --format=custom --compress=9`
- **Output:** `~/backups/utility-api/daily/utility_<YYYY-MM-DDTHHMMSSZ>.dump`
- **Expected size:** ~500 MB–1 GB compressed (text/JSONB compresses very well; the 2.5 GB `ingest_log` is the bulk and is highly compressible)
- **Schedule:** cron at 03:00 local time
- **Retention:** 14 daily, 8 weekly, infinite monthly (rolling)
- **Restore command:** `pg_restore --schema=utility --clean --if-exists -d <dbname> <dumpfile>` — document this in the README

### Tier 2 — Pre-write snapshot of critical tables
- **Trigger:** call this BEFORE any destructive op (rebuild, truncate, mass UPDATE)
- **Tables:** `rate_schedules`, `rate_best_estimate`, `pwsid_coverage`, `scrape_registry` (excluding `scraped_text` column — too big and rarely changes)
- **Output:** `~/backups/utility-api/snapshots/<table>_<reason>_<YYYY-MM-DDTHHMMSSZ>.csv.gz`
- **Implementation:** small Python module `src/utility_api/ops/snapshot.py` exposing `snapshot_critical_tables(reason: str)` that writes timestamped gzipped CSVs and returns the file paths
- **Wired into:** `BestEstimateAgent.run()` (call before TRUNCATE) — pass `reason="best_estimate_rebuild"`. Also expose as a CLI command for manual use before risky operations.
- **Retention:** 30 days (these are taken often; daily Tier 1 is the long-term record)

### Tier 3 — Offsite copy
- **What:** `rsync` or `rclone` push of `~/backups/utility-api/` to a separate physical location
- **Options to discuss with user:** (a) Tailscale-attached secondary host on the user's network, (b) cloud storage (rclone to Backblaze B2 / S3 / Drive), (c) external USB drive (manual)
- **Schedule:** daily after Tier 1 completes
- **Encryption:** if cloud — yes, use rclone crypt or age. If LAN-only Tailscale — optional but recommended.

**Critical design choice:** backups go to `~/backups/utility-api/` (HOME directory), **NOT** `~/projects/utility-api/data/backups/` (project directory). Backups inside the project directory are vulnerable to `rm -rf data/` cleanup, accidental git operations, and venv reinstalls. Place them OUTSIDE the project tree.

## Tasks

### Task 1 — Confirm environment + size estimates

Run pg_dump in dry-run / size-estimate mode (or just take one full dump as a one-shot to size it) and report:
- Compressed size of `--format=custom --compress=9` for the `utility` schema
- Approximate dump duration
- Any permissions / locale / extensions issues that would block automated dumps (e.g., postgis extension may need special handling)

If the size is alarming (>10 GB), pause and report — we may need to exclude `ingest_log` or partition it.

### Task 2 — Implement Tier 1 (daily pg_dump)

Create `scripts/backup_db.sh`:
- Header (per CLAUDE.md good code standards)
- Reads DB connection from `.env` or `settings`
- Writes to `~/backups/utility-api/daily/utility_$(date -u +%Y%m%dT%H%M%SZ).dump`
- Logs to `~/backups/utility-api/logs/backup_<date>.log`
- Implements rolling retention: 14 daily / 8 weekly / infinite monthly. Use file mtimes + naming convention to identify which bucket each dump belongs to.
- Idempotent: safe to run multiple times per day (just creates a new timestamped file)
- Exits non-zero on failure so cron mailer surfaces it
- Tested by running it once manually and verifying the dump file exists and is restorable to a scratch DB

Add a crontab entry suggestion to a NEW file `docs/backup_system.md`. **Do not install the cron job automatically** — present it to the user for approval and let them install via `crontab -e`. Document the exact line.

### Task 3 — Implement Tier 2 (pre-write snapshots)

Create `src/utility_api/ops/snapshot.py`:
- `snapshot_critical_tables(reason: str) -> dict[str, Path]` — writes gzipped CSVs for `rate_schedules`, `rate_best_estimate`, `pwsid_coverage`, `scrape_registry` (omit `scraped_text` and `last_parse_raw_response` columns from `scrape_registry`), returns `{table_name: filepath}` mapping
- Output dir: `~/backups/utility-api/snapshots/`
- Filename format: `<table>_<reason>_<UTC-timestamp>.csv.gz`
- Use `pandas.to_csv(compression='gzip')` or `psycopg2 COPY ... TO STDOUT WITH (FORMAT csv, HEADER)` piped through `gzip`. The COPY approach is faster but requires open-file handling; use whichever is cleaner in the existing codebase style.
- Add a 30-day cleanup at the end of the function (delete snapshots older than 30 days from the same dir)
- Add a docstring with the standard header

Wire it into `src/utility_api/ops/best_estimate.py`:
- At the top of `run_best_estimate()`, immediately before the `TRUNCATE` (or `DELETE WHERE state_code=`), call `snapshot_critical_tables(reason="best_estimate_rebuild")` and log the file paths
- Make this conditional on a `snapshot: bool = True` parameter so tests can disable it
- The snapshot failure should NOT block the rebuild (log and continue), but should be visible

Add a CLI entry for manual snapshots: in `src/utility_api/cli/ops.py` (or wherever the existing CLI lives), add a `snapshot` subcommand that takes `--reason` and runs the function. Allows manual invocation: `python -m utility_api.cli ops snapshot --reason "manual_pre_migration"`.

### Task 4 — Document Tier 3 options + present recommendation

Tier 3 (offsite) requires user input on storage choice. **Do not implement automatically.** In `docs/backup_system.md`, present a short comparison:

| Option | Cost | Setup effort | Resilience |
|---|---|---|---|
| Tailscale secondary host | $0 if user has spare hardware | Moderate (rsync over Tailscale) | LAN-level only |
| rclone → Backblaze B2 | ~$0.005/GB/month (~$5/year for 1 TB) | Easy (rclone config) | Geographic offsite |
| rclone → S3/GCS | ~$0.023/GB/month | Easy | Geographic offsite |
| External USB (manual) | One-time hardware | Trivial | Manual frequency |

Recommend ONE option based on what the user has already mentioned in conversation history (Tailscale is in MEMORY.md, so they have a Tailscale network — start there). Provide the config snippet but don't run it.

### Task 5 — Verify end-to-end

1. Run `scripts/backup_db.sh` once manually. Confirm a `.dump` file appears in `~/backups/utility-api/daily/`. Note the file size and creation duration.
2. Run a test restore to a scratch database (`createdb utility_api_restore_test`, `pg_restore -d utility_api_restore_test <dumpfile>`, verify table counts match, then `dropdb utility_api_restore_test`). **Do not touch the production DB.**
3. Manually invoke `snapshot_critical_tables(reason="backup_system_setup_test")` and confirm 4 `.csv.gz` files appear in `~/backups/utility-api/snapshots/`.
4. Run `BestEstimateAgent().run(state='RI')` (a small state — RI has ~50 PWSIDs) in a test, verify a snapshot was taken before the rebuild.
5. Report file sizes, durations, and any issues.

### Task 6 — Document everything in `docs/backup_system.md`

A single markdown file describing:
- The three-tier design and why
- Where backups live (`~/backups/utility-api/`) and the directory structure
- How to take a manual backup (Tier 1 + Tier 2 commands)
- How to restore from a Tier 1 dump (full and partial)
- How to read a Tier 2 CSV snapshot (pandas one-liner)
- Cron entry to install + how
- Tier 3 setup (whichever option is selected)
- How to verify backups are still being taken (the next check command, e.g., `ls -lt ~/backups/utility-api/daily/ | head`)

### Task 7 — Commit + update progress logs

Per CLAUDE.md:
- `git add` the new files
- Commit with message: "Add layered backup system for utility schema (daily pg_dump + pre-write snapshots)"
- Update `docs/next_steps.md` (note Tier 3 offsite as the open follow-up)
- Create or update a session summary

## Constraints

- **No modifications to existing data.** Backups are read-only operations; the only writes are to `~/backups/utility-api/`.
- **No automatic cron installation.** Present the line and have the user install it.
- **No automatic Tier 3 push.** Present options and have the user choose.
- **Backups go OUTSIDE the project directory** (`~/backups/utility-api/`, not `~/projects/utility-api/data/backups/`). This is a deliberate safety choice.
- **Stop and ask** if any task surfaces an issue (e.g., dump is enormous, restore fails, postgis extension blocks pg_dump). Don't paper over issues.
- **Don't commit anything to `~/backups/`** — that path should NOT be under version control. Add it to `.gitignore` if there's any chance of accidental staging.

## Done condition

- Tier 1 daily pg_dump script exists, has been tested with one successful manual run, and a successful test restore to a scratch DB has been verified.
- Tier 2 snapshot module exists, is wired into `BestEstimateAgent.run()`, has a CLI invocation, and has been tested.
- Tier 3 has been **documented** with a clear recommendation and setup snippet, but **not installed** (awaiting user choice).
- `docs/backup_system.md` exists and is comprehensive.
- All code has standard headers per CLAUDE.md.
- Git commit made.
- `docs/next_steps.md` updated.
- Session summary written.

## Scratch context / known facts

- Total `utility` schema size: **4.6 GB**
- Largest tables: `ingest_log` (2.5 GB / 21M rows — this is the bulk; could be a pruning candidate separately), `cws_boundaries` (582 MB, geometry), `pwsid_coverage` (494 MB), `scrape_registry` (340 MB — includes scraped_text), `aqueduct_polygons` (245 MB, geometry), `permits` (171 MB)
- Critical rate intelligence tables (the heart of M1): `rate_schedules` (32 MB / 27,731 rows), `rate_best_estimate` (5 MB / 18,575 rows), `pwsid_coverage` (494 MB / 43,782 rows)
- Database extensions in use: PostGIS (so `pg_dump` needs `--no-owner --no-acl` or similar to avoid permission issues on extension objects — verify in Task 1)
- A manual ad-hoc CSV backup of `rate_best_estimate` was taken on 2026-04-06 at `~/projects/utility-api/data/backups/rate_best_estimate_pre_rebuild_20260407T050330Z.csv` — this is fine to leave in place but the new system supersedes it
- User has a Tailscale network (per `feedback_dashboard_url.md` in MEMORY.md — dashboard is at `100.103.211.71:9090`)
- Sudo password is required for some operations on this desktop — per CLAUDE.md, recommend in chat and have user paste

## What NOT to do

- Don't install cron automatically.
- Don't push to a cloud provider automatically.
- Don't put backups inside the project directory.
- Don't try to back up `ingest_log` separately or prune it — that's a different effort.
- Don't add any backup logic to ingest scripts that aren't already destructive — only `BestEstimateAgent` needs Tier 2 wired in initially.
- Don't bikeshed retention policy beyond "14 daily / 8 weekly / infinite monthly" unless the user objects.
- Don't extend this chat into other backup/data-protection topics (point-in-time recovery via WAL archiving, replication, etc.). Those are M2+ concerns. Keep this chat scoped.
