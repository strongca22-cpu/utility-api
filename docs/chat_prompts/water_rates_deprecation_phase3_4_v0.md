# water_rates Deprecation Phase 3-4 — v0

## Context

Phases 1-2 are complete (Sprint 26 session, 2026-03-31):
- **Phase 1:** Migration 022 added bill_6ccf/9ccf/12ccf/24ccf to rate_schedules
- **Phase 2:** All 7,149 water_rates records synced to rate_schedules. Zero gap. Best estimate rebuilt globally with correct priority hierarchy.

**The hierarchy bug that prompted this:** `best_estimate.py` was silently dropping EFC data for ~5,400 PWSIDs across 18 states. The backfill logic (line ~402-421 in `best_estimate.py`) only included water_rates records for PWSIDs NOT already in rate_schedules. Since Duke was in rate_schedules, EFC data stuck in water_rates was excluded. Duke (priority 8) was winning over EFC (priority 2). Now that all data lives in rate_schedules, the immediate bug is fixed — but the dual-table architecture continues to create risk.

**Goal of Phases 3-4:** Eliminate all writes to water_rates and all reads from water_rates, making rate_schedules the sole rate data table. water_rates becomes read-only legacy (not deleted, per CLAUDE.md rules).

## Audit Results (from Sprint 26 exploration)

### Phase 3 — Redirect WRITE Paths (~6 files)

These ingest modules currently write to water_rates. Each needs to be updated to write directly to rate_schedules instead:

| Module | Source Key | Current Behavior |
|---|---|---|
| `src/utility_api/ingest/ear_ingest.py` | swrcb_ear_YYYY | DELETE+INSERT to water_rates, synced via BulkIngestAgent |
| `src/utility_api/ingest/owrs_ingest.py` | owrs | DELETE+INSERT to water_rates, synced via BulkIngestAgent |
| `src/utility_api/ingest/efc_fl_ingest.py` | efc_fl_2020 | DELETE+INSERT to water_rates, synced via BulkIngestAgent |
| `src/utility_api/ingest/nm_nmed_ingest.py` | nm_nmed_2025 | DELETE+INSERT to water_rates, synced via BulkIngestAgent |
| `src/utility_api/ingest/rates.py` | scraped_llm | DELETE+INSERT to water_rates (legacy path, NOT synced) |
| `src/utility_api/ingest/rate_discovery.py` | (staging) | INSERT staging rows with parse_confidence='pending' |

**Key notes:**
- The bulk ingest modules (eAR, OWRS, EFC, NMED) already have a sync path via `BulkIngestAgent._sync_rate_schedules()`. The migration is: write directly to rate_schedules instead of water_rates+sync.
- `rates.py` is the legacy LLM scraping ingest — the newer `ParseAgent` already writes to rate_schedules. This legacy path may be dead code. Verify before modifying.
- `rate_discovery.py` uses water_rates as a staging table (parse_confidence='pending'). This should move to scrape_registry or be eliminated if the discovery pipeline already uses scrape_registry.
- The existing `water_rate_to_schedule()` helper in `src/utility_api/ops/rate_schedule_helpers.py` handles the conversion from legacy schema (fixed 4-tier columns) to canonical schema (JSONB tiers). Ingest modules can use this or write directly to rate_schedules format.
- `write_rate_schedule()` in the same helpers file handles ON CONFLICT upsert.

### Phase 4 — Redirect READ Paths (~10 files)

| File | What It Reads | Fix |
|---|---|---|
| `src/utility_api/ops/best_estimate.py` (~line 402-421) | Backfills from water_rates for PWSIDs not in rate_schedules | Remove backfill — all data is in rate_schedules now |
| `src/utility_api/api/routers/rates.py` (line ~193-210) | Fallback from rate_schedules to water_rates | Remove fallback — rate_schedules is sole source |
| `src/utility_api/api/routers/resolve.py` (line ~90-94) | EXISTS check on water_rates for has_rate_data boolean | Change to rate_schedules |
| `scripts/run_scenario_a.py` (line ~113-114) | NOT EXISTS on water_rates for gap targeting | Change to rate_schedules |
| `scripts/reconcile_rates.py` | Reads water_rates for cross-source comparison | Change to rate_schedules |
| `scripts/standalone_discover.py` | Reads water_rates for already-parsed PWSIDs | Change to rate_schedules |
| `scripts/build_best_estimate.py` | Legacy best_estimate script (reads water_rates) | May be dead code — the ops/ version replaced it |
| `scripts/seed_source_catalog.py` | COUNT from water_rates for PWSID counts | Change to rate_schedules |
| `scripts/fix_ear_tier_inflation.py` | UPDATE water_rates for data correction | Change to rate_schedules or mark as historical |
| `scripts/batch_discover_ca_urls.py` | Reads water_rates for parsed PWSIDs | Change to rate_schedules |
| Coverage views (migrations 009, 011) | EXISTS + aggregates on water_rates | New migration to update views to use rate_schedules |

### Phase 4b — Update BulkIngestAgent

`src/utility_api/agents/bulk_ingest.py`:
- `_sync_rate_schedules()` — no longer needed if Phase 3 writes directly to rate_schedules
- `_get_source_pwsid_count()` — change from water_rates to rate_schedules

## Tasks

### Task 1: Phase 3 — Redirect Writes
For each ingest module:
1. Verify it's still actively used (not dead code)
2. Update to write directly to rate_schedules using `write_rate_schedule()` or direct SQL
3. Remove the water_rates INSERT/DELETE/UPDATE
4. Remove the BulkIngestAgent sync step if applicable
5. Test by re-running the ingest for one source and verifying data in rate_schedules

### Task 2: Phase 4 — Redirect Reads
For each read location:
1. Update SQL queries from water_rates to rate_schedules
2. Update column names (source → source_key, rate_effective_date → vintage_date, etc.)
3. Handle the schema differences (fixed tiers → JSONB, etc.)
4. Test each endpoint/script

### Task 3: Coverage View Migration
Create migration 023 to update the pwsid_coverage materialized view to check rate_schedules instead of water_rates.

### Task 4: Verification
1. Run `sync_water_rates_to_rate_schedules.py --verify-only` to confirm zero gap
2. Rebuild best_estimate globally
3. Test API endpoints (/rates/{pwsid}, /resolve)
4. Run reconcile_rates.py against rate_schedules
5. Confirm Scenario A gap targeting still works correctly

## What NOT to Do

- Do not DELETE water_rates table or data — per CLAUDE.md rules, preserve as legacy
- Do not modify the water_rates model — leave it for historical reference
- Do not remove the sync script — it's useful for verification
- Do not batch all changes into one commit — commit per-phase for safety

## Deferred: Duke-Sourced Targeted Research Batch

After this migration is complete, run the Duke-sourced targeted batch:

```bash
python scripts/run_targeted_research.py --batch top25_duke_sourced --process-only
```

**Status as of Sprint 26:**
- 11 Duke-sourced PWSIDs (~5.3M combined pop) need scraped_llm data to replace Duke
- All 11 already have URLs in scrape_registry from prior discovery runs
- `--process-only` will skip discovery and just run cascade parse on existing URLs
- CA3010092 (Irvine Ranch) has wrong URLs (Newport Beach, OCSAN) — needs `--force` re-discovery after process-only attempt
- TX utilities (Houston, Fort Worth, Plano, Lubbock, Irving, Laredo, Garland) have both Duke and TML at priority 8 — scraped_llm (priority 3) would replace both
- After processing: best_estimate will automatically rebuild for affected states

This is intentionally sequenced AFTER the migration so that:
1. All rate data lives in rate_schedules when new scraped_llm data is written
2. best_estimate reads only from rate_schedules (no dual-table bugs)
3. The gap-sourced batch (14 PWSIDs) runs after Scenario A returns to avoid duplication

## Key Files

- `src/utility_api/ops/rate_schedule_helpers.py` — water_rate_to_schedule(), write_rate_schedule()
- `src/utility_api/ops/best_estimate.py` — run_best_estimate(), select_best_estimate()
- `src/utility_api/agents/bulk_ingest.py` — _sync_rate_schedules() (to be removed)
- `config/source_priority.yaml` — priority hierarchy
- `scripts/sync_water_rates_to_rate_schedules.py` — verification tool
