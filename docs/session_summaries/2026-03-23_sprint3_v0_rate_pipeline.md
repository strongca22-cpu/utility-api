# Session 4: Sprint 3 v0 — LLM Rate Parsing Pipeline

**Date:** 2026-03-23
**Focus:** Build the water rate extraction pipeline (discovery → scrape → Claude API parse → store)

## What Was Built

### New Table: `utility.water_rates` (migration 007)
- Stores structured rate tier data per CWS utility
- Schema: fixed charge + 4 volumetric tiers + computed bill_5ccf/bill_10ccf + full provenance (source_url, raw_text_hash, parse_confidence, parse_model)
- Replaced never-populated `avg_monthly_bill_5ccf/10ccf` columns on `mdwd_financials` (dropped)
- Unique constraint on (pwsid, rate_effective_date)

### Pipeline Modules (5 new files)
1. **`rate_discovery.py`** — DuckDuckGo HTML search per utility. Scores results by rate-keyword relevance, .gov domain preference. Returns best URL.
2. **`rate_scraper.py`** — HTTP fetch + BeautifulSoup text extraction. Strips nav/footer, detects JS-heavy pages, computes SHA-256 hash.
3. **`rate_parser.py`** — Claude API structured extraction. System prompt instructs extraction of residential tier structure, unit conversion ($/1000gal → $/CCF), billing frequency normalization. JSON output with confidence rating.
4. **`rate_calculator.py`** — Pure Python bill calculator. Takes tier list + consumption → dollar amount. Handles flat, uniform, and multi-tier increasing block structures.
5. **`rates.py`** — End-to-end orchestrator. Chains all four steps, stores results incrementally, logs to pipeline_runs. Resumable (skips utilities with existing high/medium parses).

### CLI Command
```bash
ua-ingest rates --state VA --limit 10       # Process 10 VA utilities
ua-ingest rates --pwsid VA6510010           # Single utility
ua-ingest rates --state VA --dry-run        # Discover + scrape only, no API calls
```

### Schema Changes
- `/resolve` endpoint: `avg_monthly_bill_5ccf/10ccf` → `has_rate_data` boolean
- `mdwd_financials` model: bill columns removed
- MDWD ingest: bill column mapping removed

## Key Findings

### The JS Problem
Most VA/CA municipal websites use CivicPlus or Granicus CMS platforms that render content client-side. Static HTTP requests get only `<noscript>` shells — typically 20 chars of "Skip to Main Content."

**Sites that work (static HTML):**
- Fairfax Water (`fairfaxwater.org/rates`) — 5,575 chars of rich rate data
- Some utility authority sites (non-CivicPlus)

**Sites that don't work (JS-rendered):**
- Norfolk, Charlottesville, Fredericksburg, Bakersfield, Ceres, Sacramento — all CivicPlus
- Blacksburg, Staunton, Leesburg — 403 bot block
- Most `.gov` municipal sites

**Implication:** Playwright is required for the majority of utilities. This is the #1 priority for Sprint 3 v1.

### PDF Rate Schedules
Many utilities publish rate schedules as PDF documents linked from their website. The discovery step correctly finds these (Alexandria → VA Water Tariff PDF, Blacksburg → residential rates PDF). PDF parsing is a separate capability needed.

### Discovery Works Well
DuckDuckGo HTML search reliably finds rate pages for utilities with clear names. Scoring by rate keywords + domain type effectively ranks results.

## Target Utilities
- **VA:** 31 MDWD utilities (all have financial data, need rate data)
- **CA:** 194 MDWD utilities
- **Fairfax Water** is not in MDWD (it's a regional authority, not a municipality) but is the best test case

## Blocking Issue
- **ANTHROPIC_API_KEY** not yet configured — Claude parsing untested end-to-end
- Add to `~/projects/utility-api/.env`: `ANTHROPIC_API_KEY=sk-ant-...`

## Architecture Notes
- Rate data is deliberately separate from MDWD fiscal data — different provenance, different update cadence, different collection method
- Pipeline is designed for incremental runs — safe to re-run, skips already-parsed utilities
- Each step rate-limits to avoid being blocked (configurable via CLI flags)
- Parse confidence (high/medium/low/failed) enables progressive quality filtering
