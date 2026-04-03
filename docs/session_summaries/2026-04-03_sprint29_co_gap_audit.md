# Session Summary — Sprint 29: CO Gap Audit + Pipeline Fixes

**Date:** 2026-04-03
**Sprint:** 29
**Focus:** Colorado gap audit, discovery state disambiguation, JS rendering fix

## Context

CO is the #1 state gap: 30 PWSIDs, 1.48M pop, 78.1% coverage. Major Front Range
cities (Aurora 533k, Fort Collins 180k, Broomfield 106k, Highlands Ranch 103k)
in the gap despite having scraped text in the registry.

## Key Findings

### Root Cause Analysis

The CO gap is **not** a discovery gap — all 30 PWSIDs had scrape_registry entries
(308 total). The issues are:

1. **Wrong-entity URLs (~50 entries):** Standard discovery returned wrong-state
   results (lafayette.in.gov for CO Lafayette, psc.ky.gov for Morgan County CO)
   because `score_url_relevance()` had no state validation and queries used "CO"
   (2-letter code Google ignores).

2. **JS-rendered empty tables (~30 entries):** CivicPlus municipal sites loaded
   table headers/labels but rate cell values via AJAX. 5-second Playwright wait
   was insufficient. Broomfield, Dacono, Lafayette CO all had structure but no
   dollar amounts.

3. **PDF 403 blockers (16 entries):** `.colorado.gov/sites/` CMS blocks bot
   User-Agents. Sprint 27 browser UA retry should handle these.

4. **Only 1 true reparse candidate** (Highlands Ranch) — rest were correctly
   rejected by the parser because the text genuinely lacked rate values.

## Actions Taken

### Pipeline Fixes (all states, not CO-specific)

1. **Playwright wait: 5s → 12s** (`rate_scraper.py`)
   - No added cost, just wall-clock time
   - Covers AJAX round-trip on slow municipal servers

2. **Discovery state disambiguation** (`discovery.py`)
   - `_detect_state_from_hostname()`: Parses `.XX.gov`, `.colorado.gov`, `.XX.us`
   - `-40` score penalty for wrong-state `.gov` domains
   - Queries use full state name ("Colorado" not "CO")
   - `_get_city_from_name()`: Handles "X CITY OF" pattern, FT→Fort expansion

3. **Locality extraction CO fixes** (`locality_discovery.py`)
   - "X CITY OF" / "X TOWN OF" suffix (CO SDWIS convention)
   - WWWA, WWSA, "MD NO" suffixes for special districts
   - "NO N" district number stripping
   - CSU/YMCA/housing campus institutional filter
   - FT/MT/ST/CNTY abbreviation expansion
   - CO ambiguous city names in disambiguation list
   - Full state name in locality queries

### Direct Recoveries

- **Highlands Ranch WSD (103k pop):** Reparsed existing text → budget-based,
  3 tiers, $108.18 @10CCF. Coverage: 78.1% → ~80% immediately.

- **Genesee WSD entity fix:** geneseewater.colorado.gov rates assigned to wrong
  PWSID (Lamar). Reassigned to CO0130035.

### In-Flight (tmux sessions)

- `co_rescrape`: 48 URLs (32 JS + 16 PDF 403) with improved scraper
- `co_locality`: 27 PWSIDs × 4 queries = 108 Serper queries

## Commits

1. `3d78377` — Fix discovery state disambiguation + extend Playwright JS wait
2. `dca39da` — Fix locality extraction for CO + add CO gap rescrape script

## Next Steps (for follow-up session)

1. **Check rescrape results:** Review tmux `co_rescrape` output, check which URLs
   now have dollar amounts in scraped text
2. **Check locality discovery results:** Review new URLs found, scrape rank 1s
3. **Submit parse batch:** Combine rescrape + locality rank 1 URLs for batch parse
4. **CivicPlus investigation:** If rate cells still empty after 12s wait, may need
   table-specific `page.wait_for_selector('table td:not(:empty)')` approach
5. **Denver metro curation refresh:** `denver_cos_metro_rate_curation.json` URLs
   are stale (several 404'd). Could refresh and ingest.
6. **Scale workers:** VPS can add 10-15 scrape workers (4GB+ RAM). Desktop at 20.
