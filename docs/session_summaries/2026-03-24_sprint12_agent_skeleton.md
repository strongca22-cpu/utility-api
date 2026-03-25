# Sprint 12 — Agent Skeleton + Scrape Registry Wiring

**Date**: 2026-03-24
**Context**: Sprints 10-11 built infrastructure layers and canonical schema. Sprint 12 adds the agent framework and wires the scraping pipeline to the registry.

---

## What Was Built

### Agent Framework
- **BaseAgent ABC** — minimal: `run()` + `log_run()`. No LLM, no async, no framework. Agents are plain Python classes.
- **BulkIngestAgent** — wraps existing ingest modules (eAR, OWRS, EFC, SDWIS). Updates source_catalog, syncs rate_schedules, logs to ingest_log.
- **BestEstimateAgent** — wraps ops/best_estimate.py. Refreshes pwsid_coverage after building.
- **ingest_log table** — agent audit trail (agent_name, source_key, status, rows_affected, timing).

### Scrape Registry Wiring (Write-Only)
- `rates.py` writes to scrape_registry at three pipeline stages:
  1. After discovery: URL + source + query logged
  2. After fetch: HTTP status + content hash + length logged
  3. After parse: confidence + cost + model logged
- Registry writes wrapped in try/except — pipeline never breaks on registry errors
- `registry_writer.py` provides `log_discovery()`, `log_fetch()`, `log_parse()` helpers

### Coverage Infrastructure
- pwsid_coverage migrated from materialized view to regular table (migration 011)
- Added `scrape_status` column: not_attempted | url_discovered | attempted_failed | succeeded | stale
- Added `priority_tier` column (nullable, populated later)
- `refresh_coverage_derived()`: recomputes rate/SDWIS columns via UPDATE, preserves mutable columns
- `update_scrape_status()`: syncs from scrape_registry state

### CLI
- `ua-ops refresh-coverage` — updated for regular table, includes scrape_status sync
- `ua-ops scrape-status [--state]` — URL status, parse outcomes, HTTP codes, failures

---

## Files Created
- `src/utility_api/agents/__init__.py`, `base.py`, `bulk_ingest.py`, `best_estimate.py`
- `src/utility_api/ops/coverage.py` — coverage refresh + scrape_status sync
- `src/utility_api/ops/registry_writer.py` — write-only registry instrumentation
- `migrations/versions/011_pwsid_coverage_table_and_ingest_log.py`

## Files Modified
- `src/utility_api/cli/ops.py` — refresh-coverage, scrape-status commands
- `src/utility_api/ingest/rates.py` — registry writes at discovery/fetch/parse stages

---

## Key Decisions

1. **Write-only registry**: Sprint 12 writes, Sprint 13 reads. Keeps changes additive.
2. **Registry writes in rates.py** (Option A from analysis): single integration point, not spread across 4 modules.
3. **pwsid_coverage as table** (not mat view): enables mutable scrape_status. Refresh logic explicitly preserves mutable columns.
4. **No ingest_log replacement of pipeline_runs**: both coexist. pipeline_runs is legacy, ingest_log is for agents.

## Next: Sprint 13
Orchestrator + autonomous agents. First sprint with LLM calls (Discovery + Parse agents only).
