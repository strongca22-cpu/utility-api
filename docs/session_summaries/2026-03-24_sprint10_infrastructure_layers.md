# Sprint 10 — Infrastructure Layers

**Date**: 2026-03-24
**Context**: Sprint 9 paused data acquisition after NC EFC ingest (403 records). Strategic review (`docs/uapi_progress_and_infrastructure_review.md`) identified that the system has no memory, no coordination, and no strategic awareness. This sprint builds the machine.

---

## What Was Built

### Layer B — Data Operations Manager
1. **source_catalog table** — Registry of all 6 known data sources with operational state (type, states, vintage, cadence, ingest status). Seeded with swrcb_ear_2020/2021/2022, owrs, efc_nc_2025, scraped_llm.
2. **pwsid_coverage materialized view** — Joins CWS (44,643) + water_rates + SDWIS + rate_best_estimate. Indexed on pwsid, state_code, has_rate_data. Will migrate to a regular table in Sprint 12 when mutable columns needed.
3. **Generalized best-estimate** — Config-driven source priority (config/source_priority.yaml). Works for all states: CA multi-source with eAR anchor, NC/VA single-source. 846 PWSIDs covered (up from 443 CA-only).
4. **ua-ops CLI** — `status`, `coverage-report`, `refresh-coverage`, `build-best-estimate`. Every future acquisition session starts with `ua-ops status`.

### Layer A — Scrape Registry (table + seed)
5. **scrape_registry table** — Per-URL tracking: fetch state, parse state, retry scheduling, change detection. 128 entries seeded from YAML files and water_rates backfill.
6. **YAML migration script** — Loaded rate_urls_va.yaml (27), rate_urls_ca.yaml (56), rate_urls_va_candidates.yaml (22), water_rates backfill (101).
7. Pipeline wiring deferred to Sprint 12 — write-only logging first, Sprint 13 wires orchestrator to read.

### Quick Wins
8. **SDWIS 50-state expansion** — 44,633 records (up from 3,711). `/resolve` now returns complete SDWIS data nationwide. Config change: `sdwis_states: ALL` in sources.yaml.
9. **Best-estimate generalization** — NC (403 PWSIDs) and VA (28 PWSIDs) now flow into rate_best_estimate alongside CA (415).

---

## Files Created/Modified

### New Files
- `src/utility_api/models/source_catalog.py` — ORM model
- `src/utility_api/models/scrape_registry.py` — ORM model
- `src/utility_api/models/rate_best_estimate.py` — ORM model (replaces raw SQL)
- `src/utility_api/ops/__init__.py` — Ops package
- `src/utility_api/ops/best_estimate.py` — Generalized best-estimate logic
- `src/utility_api/cli/ops.py` — ua-ops CLI
- `migrations/versions/009_add_infrastructure_layers.py` — Alembic migration
- `config/source_priority.yaml` — Source priority configuration
- `scripts/seed_source_catalog.py` — Source catalog seed script
- `scripts/migrate_urls_to_registry.py` — YAML → registry migration

### Modified Files
- `src/utility_api/models/__init__.py` — Added 3 new model imports
- `src/utility_api/ingest/sdwis.py` — Handle `target_states=None` for ALL states
- `src/utility_api/cli/ingest.py` — Updated sdwis command docstring
- `config/sources.yaml` — Added `sdwis_states: ALL`
- `pyproject.toml` — Added `ua-ops` entry point
- `docs/next_steps.md` — Sprint 10 completion + Sprint 11–13 roadmap

---

## Key Numbers

| Metric | Before | After |
|--------|--------|-------|
| SDWIS systems | 3,711 (VA+CA) | 44,633 (all 50 states) |
| Best-estimate PWSIDs | 443 (CA only) | 846 (CA+NC+VA) |
| Scrape registry entries | 0 | 128 |
| Source catalog entries | 0 | 6 |
| Rate coverage (national) | 1.9% | 1.9% (unchanged — no new rate data) |
| SDWIS coverage | 8.3% | 100% |

---

## Architecture Decisions

1. **Mat view over regular table** for pwsid_coverage: correct for Sprint 10 (no mutable columns needed). Sprint 12 migration point acknowledged.
2. **Write-only scrape registry**: table built now, pipeline wiring deferred. Sprint 12 builds agents that write, Sprint 13 builds orchestrator that reads.
3. **Source priority as YAML config**: allows non-engineers to understand and modify the priority hierarchy. Python code reads config at runtime.
4. **Old build_best_estimate.py preserved**: scripts/build_best_estimate.py stays as legacy reference. New version at src/utility_api/ops/best_estimate.py.

---

## Next Sprint

Sprint 11: `rate_schedules` table with JSONB tier storage, migration transform from water_rates, API updates to serve best-estimate for all states.
