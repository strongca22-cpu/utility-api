# Session Summary: Sprint 26 — Metro Serper Migration + water_rates Deprecation

**Date:** 2026-03-31
**Chat prompt:** Metro Top-20 Targeted Scan — v0

## What Was Done

### 1. Targeted Research Pipeline (NEW)
- Created `scripts/run_targeted_research.py` — lightweight orchestrator: PWSID list → DiscoveryAgent → process_pwsid → rebuild best_estimate
- Created `config/targeted_research.yaml` — 25 high-pop uncovered PWSIDs in two batches (Duke-sourced and gap-sourced)
- Supports: `--batch`, `--pwsids`, `--dry-run`, `--discovery-only`, `--process-only`, `--force`

### 2. Metro Scan Serper Migration
- Updated `scripts/run_metro_scan.py` to use Serper-based DiscoveryAgent instead of Claude web_search
- Added `discover_metro_urls()` function that loops DiscoveryAgent per utility
- Eliminated the metro_url_importer step (DiscoveryAgent writes directly to scrape_registry)
- Legacy batch functions preserved for collecting in-flight batches
- Cost reduction: ~$0.15/batch → ~$0.004/utility (~97% savings)

### 3. water_rates Deprecation (Phase 1+2)
- **Migration 022:** Added bill_6ccf/9ccf/12ccf/24ccf to rate_schedules (eAR benchmarks)
- **Sync script:** Migrated all 7,149 water_rates records into rate_schedules. Zero FK violations.
- **Backfilled** 364 eAR bill records with 6/9/12/24 CCF columns

### 4. Best Estimate Hierarchy Bug Fixed
- **Root cause:** ~5,400 EFC PWSIDs across 18 states existed ONLY in water_rates, never synced to rate_schedules. The `best_estimate.py` backfill logic only included water_rates for PWSIDs NOT in rate_schedules — so when Duke was in rate_schedules, EFC was excluded.
- **Impact:** Duke (priority 8) was winning over EFC (priority 2) for thousands of PWSIDs
- **Fix:** Syncing all data to rate_schedules. Duke dropped from dominant to 13.1%, EFC sources now ~52%.
- **Rebuilt** best_estimate globally with correct hierarchy

## Key Findings

1. **Scenario A batch** (4,540 PWSIDs) still in_progress at Anthropic. 0 succeeded as of session end.
2. **Duke data is in rate_schedules, not water_rates** — Scenario A gap query checks both tables, so Duke PWSIDs are correctly excluded from Scenario A.
3. **All 6 recent pipeline patches confirmed applied:** score threshold 45, 45k text cap, PDF section extraction, rate_structure_type normalization, bill computation fix, CA service area fix.
4. **water_rates has no unique value left** — all data now exists in rate_schedules. Phases 3-4 (redirect writes/reads) can proceed in a separate chat.

## What's Left

- [ ] Run Duke-sourced targeted batch (11 PWSIDs, ~5.3M pop) — ready to execute
- [ ] Process Scenario A batch when it returns
- [ ] water_rates deprecation Phases 3-4 (separate chat)
- [ ] Re-export dashboard after Scenario A processing

## Files Created/Modified

| File | Action |
|------|--------|
| `config/targeted_research.yaml` | Created |
| `scripts/run_targeted_research.py` | Created |
| `scripts/run_metro_scan.py` | Modified (Serper migration) |
| `scripts/sync_water_rates_to_rate_schedules.py` | Created |
| `migrations/versions/022_add_ear_bill_columns_to_rate_schedules.py` | Created |
| `docs/next_steps.md` | Updated |
