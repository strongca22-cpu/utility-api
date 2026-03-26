# Session Summary — Sprint 18c: Duke Reference Ingest + URL Extraction

**Date:** 2026-03-26
**Commits:** `08c92fc`, `e8eb00a` (master)

## What Was Done

### Duke Data Integration (Two Tracks)

**Track A — Reference Data (Internal Only)**
- Cloned Duke GitHub repo (10 states, 3,297 PWSIDs, 43,595 rate rows)
- Built `duke_reference_ingest.py` with full tier extraction from Excel
- Migration 015: `duke_reference_rates` table with `license_restriction` column
- 3,178 records ingested across 10 states
- Handles NC's non-standard PWSID format (dashed → EPA)
- CC BY-NC-ND 4.0 — stored separately, never exposed via API

**Track B — URL Extraction (Commercially Clean)**
- Extracted 6,384 utility URLs from `ratesMetadata.website`
- Imported 3,718 gap-fill URLs to `scrape_registry`
- These are direct rate page URLs — much higher quality than domain guesser

### Gap Analysis Results

| State | Duke Records | Duke Only | Overlap | Gap% |
|-------|-------------|-----------|---------|------|
| TX | 723 | 723 | 0 | 100% |
| KS | 411 | 411 | 0 | 100% |
| PA | 324 | 324 | 0 | 100% |
| CA | 667 | 296 | 371 | 44% |
| WA | 244 | 244 | 0 | 100% |
| NJ | 213 | 213 | 0 | 100% |
| NC | 479 | 90 | 389 | 19% |
| NM | 50 | 50 | 0 | 100% |
| CT | 58 | 12 | 46 | 21% |
| OR | 9 | 9 | 0 | 100% |
| **Total** | **3,178** | **2,372** | **806** | **75%** |

### Coverage After This Session

| Metric | Value |
|--------|-------|
| Commercial water_rates | 6,120 PWSIDs |
| Duke reference (internal) | +2,372 PWSIDs |
| Total with any rate data | 8,492 PWSIDs |
| Total CWS | 44,643 |
| Coverage (commercial) | 13.7% |
| Coverage (with Duke ref) | 19.0% |
| Duke URLs in scrape_registry | 3,718 pending |

## Files Created
- `src/utility_api/ingest/duke_reference_ingest.py` — reference ingest module
- `migrations/versions/015_add_duke_reference_rates_table.py` — migration
- `config/duke_ingest_spec.yaml` — full schema documentation
- `docs/research_artifacts/duke_dashboard_research.md` — research report
- `data/duke_urls.csv` — extracted utility URLs
- `data/duke_raw/` — cloned repo (gitignored)

## Next Steps
1. Validate 5 TX Duke URLs through scrape pipeline
2. Batch-process gap-fill state URLs if validation succeeds
3. Expected yield: 500-1,200 new commercial PWSIDs
