# Sprint 11 — Canonical Rate Schema

**Date**: 2026-03-24
**Context**: Sprint 10 built the infrastructure layers (source_catalog, scrape_registry, pwsid_coverage, SDWIS 50-state, generalized best-estimate). Sprint 11 establishes the canonical data schema.

---

## What Was Built

### rate_schedules Table (Migration 010)
- JSONB `volumetric_tiers`: `[{tier, min_gal, max_gal, rate_per_1000_gal}]` — no tier count limit
- JSONB `fixed_charges`: `[{name, amount, meter_size}]` — supports multiple fixed charges
- JSONB `surcharges`: `[{name, rate_per_1000_gal, condition}]` — drought/seasonal
- `conservation_signal`: highest/lowest tier ratio (measures conservation pricing strength)
- `bill_20ccf`: new bill snapshot at 20 CCF (14,960 gal)
- `needs_review` + `review_reason`: quality flags for margin check failures
- GIN index on `volumetric_tiers` for containment queries
- Unique constraint: `(pwsid, source_key, vintage_date, customer_class)`

### Unit Convention
- water_rates: CCF + $/CCF (legacy US convention)
- rate_schedules: gallons + $/1000 gallons (canonical, universal)
- 1 CCF = 748 gallons; $/CCF × 1.337 = $/1000gal

### Data Migration
- All 1,472 water_rates records → rate_schedules via `scripts/migrate_to_rate_schedules.py`
- 1,291 with volumetric tiers (181 had no tier data — eAR inflation-NULLed records)
- 1,384 with fixed charges
- 716 with conservation signal (requires ≥2 tiers with different rates)
- 1,397 with bill_20ccf computed from tiers

### API Updates
- `/rates/{pwsid}` reads from rate_schedules first (JSONB tiers), falls back to water_rates
- Response includes: `fixed_charges`, `tiers` (JSONB), `surcharges`, `conservation_signal`, `bill_20ccf`, `needs_review`
- Best-estimate reads from rate_schedules when populated

### Sync Command
- `ua-ops sync-rate-schedules`: converts any new water_rates records not yet in rate_schedules
- Replaces inline dual-write in each ingest module — less invasive, same result
- Sprint 12 agents will write directly to rate_schedules

---

## Files Created
- `src/utility_api/models/rate_schedule.py` — ORM model
- `src/utility_api/ops/rate_schedule_helpers.py` — conversion utilities, bill computation, dual-write helper
- `migrations/versions/010_add_rate_schedules_table.py` — Alembic migration
- `scripts/migrate_to_rate_schedules.py` — one-time data transform

## Files Modified
- `src/utility_api/models/__init__.py` — added RateSchedule
- `src/utility_api/ops/best_estimate.py` — reads from rate_schedules when available
- `src/utility_api/api/routers/rates.py` — serves JSONB tiers from rate_schedules
- `src/utility_api/cli/ops.py` — added sync-rate-schedules command
- `docs/next_steps.md` — Sprint 11 completion + Sprint 12 prompt

---

## Architecture Note

water_rates stays as a legacy/audit table. rate_schedules is the source of truth. During the transition:
- Existing ingest modules still write to water_rates
- `ua-ops sync-rate-schedules` propagates to rate_schedules
- Sprint 12 agents will write directly to rate_schedules
- water_rates can be dropped once all consumers are migrated
