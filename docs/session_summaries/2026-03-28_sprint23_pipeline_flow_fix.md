# Session Summary — Sprint 23: Pipeline Flow Fix

**Date:** 2026-03-28
**Sprint:** 23 — Pipeline Flow Fix & Scraped Content Persistence
**Commit:** Sprint 23: Pipeline flow fix — persist scraped text, unified chain, auto-triage

## Problem

ScrapeAgent kept raw text in memory only. If the handoff to ParseAgent broke (crash, script bug, pipeline path that skips parse), the text was lost forever. Result: 1,987 URLs sitting `active` with content metadata but no parse result and no recoverable text.

Root cause example: `run_mn_discovery.py` called `ParseAgent().run(registry_id=r.id, pwsid=r.pwsid)` without passing `raw_text` — the parse had nothing to work with.

## What Was Built

### Migration 018
- `scraped_text TEXT` column on `scrape_registry`
- `url_quality VARCHAR(20)` column with backfill from existing parse results

### ScrapeAgent Changes (scrape.py)
- Persists `scraped_text` on every successful fetch
- Persists text for deep-crawled URLs (both initial and `_register_deep_url`)
- New `fetch_single_url()` method for lightweight re-fetch in backfill/sweep scenarios

### ParseAgent Changes (parse.py)
- `raw_text` parameter now optional (was required)
- DB fallback: `_load_scraped_text(registry_id)` and `_load_best_text_for_pwsid(pwsid)`
- Auto-classifies `url_quality` after every parse attempt in `_update_registry()`

### Unified Chain (pipeline/chain.py)
- New `scrape_and_parse()` function — single implementation of scrape→parse→best_estimate
- All four callers updated to use it:
  - `scripts/process_guesser_batch.py`
  - `scripts/parse_deep_crawl_backlog.py`
  - `src/utility_api/cli/ops.py` (process-backlog command)
  - `scripts/run_mn_discovery.py`

### Triage CLI (ops.py)
- `ua-ops triage-backlog` — classifies backlog, shows rate-relevant vs junk, --execute to blacklist
- Reusable pattern for any future bulk import source

### Parse Sweep Daemon (parse_sweep.py)
- Polls every 30 min, finds unparsed entries with text in DB
- Parses them, batches BestEstimate per state
- Designed for tmux: `--interval`, `--max-per-sweep`, `--once` flags
- Cost guard: 900s minimum interval

### Pipeline Health Additions (ops.py)
- URL quality distribution in `ua-ops pipeline-health`
- "sweep-ready" count (entries with text in DB awaiting parse)
- `process-backlog --dry-run` shows text availability and url_quality per entry

## Files Changed
- `migrations/versions/018_add_scraped_text_and_url_quality.py` (new)
- `src/utility_api/agents/scrape.py` (modified)
- `src/utility_api/agents/parse.py` (modified)
- `src/utility_api/pipeline/__init__.py` (new)
- `src/utility_api/pipeline/chain.py` (new)
- `src/utility_api/cli/ops.py` (modified — triage-backlog + pipeline-health + process-backlog)
- `scripts/process_guesser_batch.py` (modified)
- `scripts/parse_deep_crawl_backlog.py` (modified)
- `scripts/run_mn_discovery.py` (modified)
- `scripts/parse_sweep.py` (new)

## NOT Yet Done (Requires User Action)
1. Run migration: `alembic -c migrations/alembic.ini upgrade head`
2. Run triage: `ua-ops triage-backlog` then `--execute`
3. Process backlog (SearXNG first): `ua-ops process-backlog --max 120 --source searxng`
4. Start sweep daemon in tmux
5. Verify with `ua-ops pipeline-health`

## Key Design Decisions
- **DB storage over filesystem** for scraped_text (~35MB total, trivially small)
- **One migration** for both columns (same sprint, same table)
- **Unified chain** replaces all inline scrape→parse implementations
- **process-backlog** now smart: uses persisted text when available (no re-fetch), falls back to unified chain when text not in DB
- **url_quality** set automatically — no manual classification needed
