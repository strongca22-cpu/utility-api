# Session Summary — Sprint 18: FL EFC API + WV PSC Ingest

**Date:** 2026-03-25
**Commit:** `7f3fd73` (master)

## What Was Done

### FL EFC API Ingest (281 records)
- Built API client for UNC EFC Topsail JSON API (dashboard_id=15)
- Fetched all 227 FL utilities at 1 req/sec (~4 min), cached to `data/raw/efc_fl/api_cache.json`
- API returns bill curves at 500-gal increments (0–15,000 gal) — identical format to NC CSV
- Reused NC's tier extraction logic (marginal rate detection from bill curve)
- 227 API responses → 281 DB records (some utilities have multiple PWSIDs)
- 10 skipped (no PWSID), 1 skipped (PWSID not in CWS boundaries)
- Source: `efc_fl_2020`, vintage: Raftelis 2020 survey

### WV PSC HTML Scrape Ingest (241 records)
- Scraped WV Public Service Commission rankings at 3,400 and 4,000 gallons
- Regex-based HTML parsing (nested tables prevented standard HTMLParser)
- PSC site requires browser User-Agent header (blocks default httpx UA)
- 2-point bill curve → derived volumetric rate, fixed charge, bill estimates
- Fuzzy name matching against SDWIS: normalized names + substring + word overlap
- 241/325 matched (74%), 42 unmatched, 42 duplicate PWSIDs
- Handles edge cases: flat-rate systems (zero volumetric rate), declining block (negative derived rate)
- Source: `wv_psc_2026`, vintage: March 2026

### Key Architectural Finding
All 24 EFC state dashboards use the same Rails/Topsail platform with identical JSON API:
- Endpoint: `/dashboards/{id}/chart_data.json?rate_structure_id={id}&...`
- "Download Data" button returns 500 on all dashboards (server-side broken)
- Utility IDs are in HTML `<option>` elements
- API is unauthenticated, returns full bill curves + PWSID + metadata
- **This means one generic module can cover all 24 states** (Track B, next session)

## DB State After This Session

| Source | Records | Avg @10CCF | Range |
|--------|---------|-----------|-------|
| efc_fl_2020 | 281 | $36.95 | $13.91–$86.02 |
| efc_nc_2025 | 403 | $60.52 | $18.26–$176.61 |
| wv_psc_2026 | 241 | $100.84 | $3.00–$251.07 |
| Total water_rates | 1,994 | — | — |

## Files Created
- `src/utility_api/ingest/efc_fl_ingest.py` — FL EFC API ingest module
- `src/utility_api/ingest/wv_psc_ingest.py` — WV PSC HTML scrape module
- `data/raw/efc_fl/api_cache.json` — cached API responses (2MB)
- `data/raw/efc_fl/fl_efc_utility_mapping.json` — 227 utility ID→name mappings
- `data/raw/wv_psc/rankings_4000.html` — cached PSC HTML
- `data/raw/wv_psc/rankings_3400.html` — cached PSC HTML
- `data/raw/wv_psc/name_match_log.json` — full match log for review

## Next Steps (Track B)
1. Generalize FL API pattern → generic EFC module for all 24 states
2. Extract utility IDs from each state's dashboard HTML
3. Estimated yield: 4,000–5,000 PWSIDs from one engineering session
4. WV PSC name matching improvements (42 unmatched utilities)
