# Session Summary — Sprint 26b: water_rates Deprecation Phases 3-4

**Date:** 2026-03-31
**Scope:** Complete elimination of active writes/reads to water_rates table

## What Was Done

### Phase 3: Redirect All WRITES (commit a29057f)

1. **rate_schedule_helpers.py** — Added `bill_6ccf`, `bill_9ccf`, `bill_12ccf`, `bill_24ccf` to both `water_rate_to_schedule()` return dict and `write_rate_schedule()` INSERT/upsert. Previously these eAR benchmark columns were silently dropped during conversion.

2. **8 bulk ingest modules** — All redirected from water_rates to rate_schedules:
   - ear_ingest.py, owrs_ingest.py, efc_fl_ingest.py, efc_nc_ingest.py, efc_generic.py, nm_nmed_ingest.py, wv_psc_ingest.py, in_iurc_ingest.py
   - Pattern: DELETE+INSERT now targets rate_schedules via `water_rate_to_schedule()` + `write_rate_schedule()` helpers

3. **rates.py** (legacy LLM path) — Redirected `_store_rate_record()` through helpers. Deprecation warning added.

4. **rate_discovery.py** — Staging moved from water_rates (parse_confidence='pending') to scrape_registry (status='pending'). Clean fit — no schema changes needed.

5. **BulkIngestAgent** — Removed `_sync_rate_schedules()` call (no longer needed). Updated `_get_source_pwsid_count()` to query rate_schedules.

### Phase 4: Redirect All READS (commit 7fbeb31)

1. **best_estimate.py** — Most complex change. Removed 3 water_rates query blocks:
   - eAR bill backfill (now in rate_schedules via migration 022)
   - water_rates-only PWSID backfill (empty set after Phase 2 sync)
   - Full fallback (rate_schedules always has data now)
   - Single clean query to rate_schedules with actual bill_6/9/12/24ccf columns

2. **coverage.py** — Simplified UNION of both tables to rate_schedules only

3. **API routers** — rates.py: removed fallback, rewrote list endpoint. resolve.py: EXISTS check.

4. **cli/ops.py** — 4 queries redirected. Sync command → informational no-op.

5. **6 scripts** — run_scenario_a.py, reconcile_rates.py, standalone_discover.py, seed_source_catalog.py, fix_ear_tier_inflation.py, batch_discover_ca_urls.py

## Audit Findings vs. Original Plan

The original plan (docs/chat_prompts/water_rates_deprecation_phase3_4_v0.md) listed 6 write paths and ~10 read paths. The audit found:
- **4 additional write paths**: efc_nc_ingest.py, efc_generic.py, wv_psc_ingest.py, in_iurc_ingest.py
- **2 additional read paths**: ops/coverage.py, cli/ops.py
- **Critical gap**: rate_schedule_helpers.py was missing bill_6/9/12/24ccf in both conversion and upsert

## Remaining water_rates References

All remaining references are either:
- **Comments/docstrings** (historical documentation)
- **Model definition** (water_rate.py — preserved per rules)
- **Dead code** (build_best_estimate.py, _sync_rate_schedules body)
- **Historical one-time scripts** (migrate_to_rate_schedules.py, migrate_urls_to_registry.py, reparse_combined_rates.py, analyze_ear_rate_changes.py)
- **Verification tool** (sync_water_rates_to_rate_schedules.py — kept for parity checks)

## Verification Steps (For User)

1. `python scripts/sync_water_rates_to_rate_schedules.py --verify-only` — confirm zero gap
2. Rebuild best_estimate globally
3. Test API endpoints (/rates/{pwsid}, /resolve)
4. Run reconcile_rates.py against rate_schedules
5. Confirm Scenario A gap targeting still works

## Next Up

- [ ] Scenario A batch processing (when Anthropic returns)
- [ ] Duke-sourced targeted research batch (11 PWSIDs, ~5.3M pop) — now safe since all writes go to rate_schedules
