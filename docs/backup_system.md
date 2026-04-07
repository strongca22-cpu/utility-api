# utility-api Backup System

**Created:** 2026-04-06
**Status:** Tier 1 + Tier 2 implemented and verified. Tier 3 (offsite) documented but not installed — awaiting user choice.

---

## Why this exists

The `utility` schema in PostgreSQL holds ~4.6 GB of accumulated rate, scrape, and discovery data, including ~16,958 LLM-parsed `scraped_llm` rows representing real Anthropic API spend (~$50–100). The trigger for this system was a code regression on 2026-04-06 that NULLed 91% of `rate_best_estimate.source_url` values during a series of bulk rebuilds. The data was recovered, but the incident exposed two gaps:

1. No pre-rebuild snapshots existed to roll back to.
2. No regular schema-level backup existed at all.

The user's directive: *"given the trivial size of this data compared to storage, it would be worth aggressively backing up regularly."*

The design philosophy: **back up early, back up often, back up before anything destructive**. Storage cost is not a constraint; recoverability and audit trail are.

---

## Architecture: three tiers

| Tier | What | When | Where | Retention |
|---|---|---|---|---|
| **1 — Daily full schema dump** | `pg_dump --schema=utility --format=custom --compress=9` | Daily via cron at 03:00 local | `~/backups/utility-api/daily/` | 14 daily / 8 weekly / ∞ monthly |
| **2 — Pre-write snapshots** | Gzipped CSVs of 4 critical tables | Automatically before any destructive op (`run_best_estimate`); manually via `ua-ops snapshot` before risky work | `~/backups/utility-api/snapshots/` | 30 days |
| **3 — Offsite copy** | rsync/rclone of `~/backups/utility-api/` to a separate physical location | Daily after Tier 1 | (TBD — see "Tier 3" below) | (TBD) |

**Critical design choice:** backups go to `~/backups/utility-api/` (HOME directory), **NOT** `~/projects/utility-api/data/backups/`. Backups inside the project directory are vulnerable to `rm -rf data/`, accidental git operations, and venv reinstalls. They live outside the project tree on purpose.

### Directory structure

```
~/backups/utility-api/
├── daily/        Tier 1 — last 14 days of pg_dump custom-format files
├── weekly/       Tier 1 — last 8 weeks (Sunday dumps promoted here)
├── monthly/      Tier 1 — every month-1 dump, kept forever
├── snapshots/    Tier 2 — gzipped CSVs of 4 critical tables, last 30 days
└── logs/         per-day backup_<date>.log files
```

---

## Tier 1 — daily pg_dump

### Script: [scripts/backup_db.sh](../scripts/backup_db.sh)

The script:
- Reads DB connection from `utility_api.config.settings` (no credentials live in the script)
- Writes to `~/backups/utility-api/daily/utility_<UTC-timestamp>.dump`
- Uses `--format=custom --compress=9 --no-owner --no-acl` (no-owner/no-acl is needed because PostGIS extension objects can't be reassigned during restore)
- Promotes Sunday dumps to `weekly/`
- Promotes the 1st-of-month dump to `monthly/` (kept forever)
- Prunes daily down to last 14 / weekly down to last 8
- Logs to `~/backups/utility-api/logs/backup_<YYYY-MM-DD>.log`
- Exits non-zero on failure so cron mailer surfaces problems
- Idempotent — re-running the same day produces a new timestamped file, no overwrite

### Manual run

```bash
cd ~/projects/utility-api
./scripts/backup_db.sh
```

### Verified performance (2026-04-06 baseline)

| Metric | Value |
|---|---|
| Dump duration | ~3 minutes |
| Compressed size | 1.2 GB |
| Test restore (custom format → fresh PostGIS 16 container) | ~45 seconds |
| Test restore row count match | 100% (rate_schedules, rate_best_estimate, pwsid_coverage, scrape_registry all matched prod exactly) |

### Cron entry to install

**Not installed automatically.** Install manually with `crontab -e`:

```cron
# utility-api Tier 1 backup — daily 03:00 local
0 3 * * * /home/colin/projects/utility-api/scripts/backup_db.sh >> /home/colin/backups/utility-api/logs/cron.log 2>&1
```

If cron mailing is configured, exit-non-zero on failure will trigger the local MTA. To verify the line is installed:

```bash
crontab -l | grep backup_db.sh
```

### Restore — full

```bash
# 1. Spin up a target DB with PostGIS available
createdb utility_api_restore_test
psql -d utility_api_restore_test -c 'CREATE EXTENSION postgis;'

# 2. Restore the dump
pg_restore -d utility_api_restore_test --no-owner --no-acl \
    ~/backups/utility-api/daily/utility_<timestamp>.dump

# 3. Verify
psql -d utility_api_restore_test -c "SELECT COUNT(*) FROM utility.rate_best_estimate;"
```

### Restore — partial (single table)

```bash
# List objects in the dump
pg_restore --list ~/backups/utility-api/daily/utility_<timestamp>.dump | grep rate_best_estimate

# Restore one table only (use --data-only if the table already exists)
pg_restore -d <target_db> --no-owner --no-acl \
    --table=rate_best_estimate --schema=utility \
    ~/backups/utility-api/daily/utility_<timestamp>.dump
```

### Restore — into PRODUCTION (recovery scenario)

**This is destructive — only do this if you've decided the current production state is wrong.** Always take a fresh snapshot first:

```bash
ua-ops snapshot --reason "pre_disaster_recovery"
pg_restore -d strong_strategic --schema=utility --clean --if-exists --no-owner --no-acl \
    ~/backups/utility-api/daily/utility_<timestamp>.dump
```

---

## Tier 2 — pre-write snapshots

### Module: [src/utility_api/ops/snapshot.py](../src/utility_api/ops/snapshot.py)

Public API:

```python
from utility_api.ops.snapshot import snapshot_critical_tables

paths = snapshot_critical_tables(reason="manual_pre_migration")
# returns {table_name: Path} mapping
```

Tables snapshotted:
- `rate_schedules` (full)
- `rate_best_estimate` (full)
- `pwsid_coverage` (full)
- `scrape_registry` (excludes `scraped_text` and `last_parse_raw_response` — both are large blobs that rarely change in a rebuild)

Output filename: `<table>_<reason>_<UTC-timestamp>.csv.gz`

The function uses psycopg's `COPY (...) TO STDOUT WITH (FORMAT csv, HEADER)` streamed through gzip — fast and low-memory. After writing, snapshots older than 30 days in the same directory are deleted.

**Failure behavior:** snapshot failure does NOT block the caller. Failures are logged with `loguru.warning` and the function returns whichever tables succeeded.

### Wired into

[src/utility_api/ops/best_estimate.py](../src/utility_api/ops/best_estimate.py) — `run_best_estimate(snapshot=True)` calls `snapshot_critical_tables(reason="best_estimate_rebuild")` immediately before the `TRUNCATE` / `DELETE WHERE state_code=`. The `snapshot=False` flag exists for tests.

### CLI

```bash
ua-ops snapshot --reason "manual_pre_migration"
# or
python -m utility_api.cli.ops snapshot --reason "manual_pre_migration"
```

### Reading a snapshot

```python
import pandas as pd
df = pd.read_csv(
    "~/backups/utility-api/snapshots/rate_best_estimate_<reason>_<timestamp>.csv.gz",
    compression="gzip",
)
```

### Verified sizes (2026-04-06 baseline)

| Table | Rows | Compressed CSV |
|---|---|---|
| rate_schedules | 27,743 | ~4 MB |
| rate_best_estimate | 18,575 | ~600 KB |
| pwsid_coverage | 44,643 | ~1.3 MB |
| scrape_registry (minus scraped_text + last_parse_raw_response) | 117,893 | ~10 MB |

Total per snapshot call: ~16 MB. Duration: ~2 seconds.

### When to call manually

Before any operation that mutates the critical tables in bulk and is not already wired into the snapshot path. Examples:

- About to run a DB migration that touches `rate_*` tables
- About to run a one-off bulk `UPDATE` or `DELETE` from psql
- About to re-import a source from scratch
- Anytime you're about to do something you might regret

The cost is ~2 seconds and ~16 MB of disk. There is no good reason not to.

---

## Tier 3 — offsite copy (NOT INSTALLED — awaiting user choice)

The user has a Tailscale network already (per memory: dashboard at `100.103.211.71:9090`), so option 1 has the lowest activation energy.

| Option | Cost | Setup effort | Resilience |
|---|---|---|---|
| **(1) Tailscale secondary host** (rsync over Tailscale to another machine on the user's tailnet) | $0 if user has spare hardware | Moderate (rsync command in cron) | LAN-level only — survives disk failure on the desktop, does NOT survive house fire / theft |
| **(2) rclone → Backblaze B2** | ~$0.005/GB/month (~$5/year for 1 TB; ~$0.20/year for 40 GB of dumps) | Easy (one rclone config) | Geographic offsite — survives anything |
| **(3) rclone → S3 / GCS / Drive** | ~$0.023/GB/month for S3-standard | Easy | Geographic offsite |
| **(4) External USB (manual)** | One-time hardware | Trivial | Manual frequency — only as fresh as the last plug-in |

### Recommendation: start with Tailscale (option 1), add B2 (option 2) later

**Rationale:** option 1 is free, uses infrastructure the user already runs, and protects against the most likely failure mode (desktop disk failure). Option 2 is the right complement for catastrophic loss (fire / theft / ransomware) — it's cheap enough that "a few dollars per year" isn't worth deliberating over, but it does add a second moving piece (rclone config, encryption key custody). Doing them in sequence keeps the immediate exposure window small without overcommitting.

### Tailscale rsync — config sketch (DO NOT RUN AUTOMATICALLY)

On the secondary host (e.g., `colin-nas` in the tailnet):
```bash
# As the receiving user, ensure the backup target dir exists
mkdir -p ~/backups/utility-api-mirror
```

On the desktop, add to cron AFTER the Tier 1 line:
```cron
# Tier 3 — push backups to Tailscale secondary host, daily 03:30
30 3 * * * rsync -az --delete /home/colin/backups/utility-api/ colin@<TAILSCALE_HOSTNAME>:~/backups/utility-api-mirror/ >> /home/colin/backups/utility-api/logs/rsync.log 2>&1
```

Replace `<TAILSCALE_HOSTNAME>` with the actual Tailscale machine name. SSH key auth must be set up first (`ssh-copy-id colin@<TAILSCALE_HOSTNAME>`).

### B2 rclone — config sketch (DO NOT RUN AUTOMATICALLY)

```bash
# One-time setup
rclone config            # create remote "b2-utility" pointing at a B2 bucket
rclone config            # create remote "b2-crypt" wrapping b2-utility with encryption
```

Then add to cron:
```cron
# Tier 3 — encrypted offsite to Backblaze B2, daily 03:45
45 3 * * * rclone sync /home/colin/backups/utility-api/ b2-crypt:utility-backups/ >> /home/colin/backups/utility-api/logs/rclone.log 2>&1
```

**Encryption key custody:** the rclone crypt password is the only thing standing between an attacker who compromises the B2 account and full read access to the backups. Store it in a password manager AND a sealed offline copy. Without it, the backups are unrecoverable.

---

## Health checks

### Is Tier 1 still running?

```bash
ls -lt ~/backups/utility-api/daily/ | head
# The most recent file should be from today (or yesterday if checked before 03:00).
```

### Most recent dump size + age

```bash
ls -lah ~/backups/utility-api/daily/ | tail -1
stat -c '%y %s %n' ~/backups/utility-api/daily/utility_*.dump | sort | tail -3
```

### Is the daily cron line installed?

```bash
crontab -l | grep backup_db.sh
```

### Spot-check a recent snapshot

```bash
ls -lt ~/backups/utility-api/snapshots/ | head
zcat ~/backups/utility-api/snapshots/rate_best_estimate_*.csv.gz | head -3
```

### Verify dump integrity (without restoring)

```bash
pg_restore --list ~/backups/utility-api/daily/utility_<timestamp>.dump | head
# Should list ~hundreds of objects (TABLE, CONSTRAINT, INDEX, etc.) without errors.
```

---

## What is NOT in scope (yet)

- **Point-in-time recovery via WAL archiving** — M2+ concern
- **Streaming replication** — M2+ concern
- **Backups of `ingest_log`** are included (it's part of the schema dump) but no separate effort to prune it. The 2.5 GB bulk is highly compressible and is fine inside the dump.
- **Backups of databases other than `strong_strategic.utility`** — out of scope; the strong-strategic codebase has its own backup needs
- **Automated cron / Tier 3 installation** — by design, the user installs these manually after reviewing this document
