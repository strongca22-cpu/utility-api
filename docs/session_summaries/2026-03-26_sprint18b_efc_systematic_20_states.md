# Session Summary — Sprint 18b: Generic EFC Module + 20-State Systematic Ingest

**Date:** 2026-03-25/26
**Commits:** `68ef3df`, `7fb58e2` (master)

## What Was Done

### Generic EFC Module
- Built `src/utility_api/ingest/efc_generic.py` — single module for all 20 EFC states
- Parameterized by `config/efc_dashboards.yaml` (dashboard IDs, vintages, source keys)
- Auto-discovers utility IDs from dashboard HTML `<option>` elements at ingest time
- CLI: `ua-ingest efc --state WI`, `ua-ingest efc --all --skip-ingested`, `ua-ingest efc --list`

### Dashboard Discovery
- All 20 states discovered via automated HTML scraping
- Dashboard IDs range from 4 (SC) to 271 (AZ)
- Total: 7,096 dashboard utilities across all states
- Most states use 500-gal bill curve increments (31 points)
- AR uses 1000-gal (16 points), SC uses custom (4 points)

### Variable Increment Fix
- Initial code assumed 500-gal increments; AR got base charges but no bills/tiers
- Fixed tier extraction and bill interpolation to auto-detect increments
- AR: 599 records now fully populated (was 0/599 with bills, now 599/599)

### Ingest Results (17 New States)
All fetched via JSON API at 0.5s/req. Total runtime ~45 minutes.

| State | Dashboard Utils | Records Written | Match Rate |
|-------|----------------|----------------|-----------|
| AR | 746 | 599 | 80% |
| IA | 696 | 570 | 82% |
| WI | 579 | 569 | 98% |
| GA | 584 | 488 | 84% |
| OH | 383 | 367 | 96% |
| MS | 317 | 359 | >100%* |
| AZ | 452 | 329 | 73% |
| AL | 541 | 323 | 60% |
| MA | 408 | 272 | 67% |
| IL | 253 | 242 | 96% |
| NH | 206 | 167 | 81% |
| CT | 153 | 151 | 99% |
| ME | 238 | 144 | 61% |
| MO | 57 | 73 | >100%* |
| HI | 41 | 69 | >100%* |
| DE | 49 | 24 | 49% |
| SC | 257 | 6 | 2% |

*>100% when utilities have multiple PWSIDs

### Database State After Session

| Metric | Value |
|--------|-------|
| Total water_rates records | 6,746 |
| Unique PWSIDs | 6,120 |
| EFC + PSC subtotal | 5,677 |
| States with rate data | 22 |
| LLM API cost | $0 |

## Key Issues Found
1. **AZ outlier**: max bill @10CCF = $9,315 — likely EFC source data error
2. **NH outlier**: max bill @10CCF = $3,414 — needs investigation
3. **SC low yield**: 223/257 utilities lack PWSIDs in API (`sdwis` field empty)
4. **AL low match**: 60% — many small rural systems not in CWS boundaries

## Files Created/Modified
- `src/utility_api/ingest/efc_generic.py` — generic EFC ingest module
- `config/efc_dashboards.yaml` — per-state dashboard config
- `config/bulk_source_catalog.yaml` — full bulk source research catalog
- `src/utility_api/cli/ingest.py` — added `efc` command
- `data/raw/efc_{state}/api_cache.json` — cached API responses (17 states)
