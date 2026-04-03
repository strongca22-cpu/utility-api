# Session Summary — Locality Discovery Pipeline (Sprint 28)
**Date:** 2026-04-02
**Commit:** df39b66

## What Was Built

**Problem:** Standard PWSID-name-based Serper discovery fails for NY (91→76 gap PWSIDs, 1M pop). Formal SDWIS names like "SCHENECTADY CITY WATER WORKS" return ny.gov state portals instead of the actual municipal rate pages. 82% of the remaining population gap is wrong URLs, not parse failures.

**Solution:** Locality discovery pipeline — a fallback cascade that extracts the municipality name from the formal PWSID system name and reformulates search queries.

### New Components

1. **`LocalityDiscoveryAgent`** (`src/utility_api/agents/locality_discovery.py`)
   - `extract_municipality()`: strips suffixes (WATER WORKS, WD, CSA, etc.), prefixes (CITY OF, TOWN OF), parentheticals, district numbers. Detects private companies (Veolia, Aqua) and institutions (universities, prisons). County-aware disambiguation for short names (Lee → "Lee" Oneida County NY).
   - `build_locality_queries()`: 4 Serper queries per municipality. Adds county context for ambiguous names.
   - `score_locality_url()`: wraps standard `score_url_relevance()` + municipality-in-domain bonus + cross-contamination penalty.
   - Writes to `scrape_registry` with `url_source='locality_discovery'`.

2. **`scripts/run_locality_discovery.py`**: Batch runner with `--dry-run`, `--state`, `--pwsids`, `--min-pop` flags.

3. **`scripts/rescrape_bugfix_ny.py`**: Targeted re-scrape of URLs affected by Sprint 27 scraper bug fixes.

4. **`scripts/chain_ny_locality.sh`**: Automated chain — polls TC-R2 → processes locality r1 batch → runs bug-fix rescrape → submits r2-5 + bugfix batches → polls and processes → prints coverage report.

## NY Pilot Results

| Step | Result |
|------|--------|
| Municipality extraction | 70/76 valid (92%). 1 private (Veolia), 5 institutions, 0 unextractable |
| Discovery | 70/70 PWSIDs got new URLs (100%). 266 URLs written |
| Scrape (all 5 ranks) | 248/266 succeeded (93%) |
| Parse rank 1 batch | 35 tasks submitted, 35/35 complete at Anthropic |
| Bug-fix rescrape queued | 183 URLs reset (114 HTML + 69 PDF) |

### Bug-Fix Rescrape Audit
183 prior URLs across 69 NY PWSIDs were affected by Sprint 27 fixes:
- **72** form stripping victims (.gov/ASP.NET sites)
- **55** dollar amount stripping victims (parse failed, no $ in text)
- **45** PDF 403 bot-block victims
- **34** JS timeout victims (thin HTML < 500 chars)

Notable: SCWA rate page (scwa.com, 34k pop) had only 38 chars. WJWW PDFs (60k pop) never fetched.

## Active at Session End
- `ny_chain` tmux session running, waiting for TC-R2 (PID 51353) to finish
- Locality r1 batch complete, chain will process it then continue through steps 3-7
- TC-R2 still scraping (~2,912 URLs, 20 workers)

## Design Decisions
- **Separate agent (not subclass):** `LocalityDiscoveryAgent` inherits from `BaseAgent`, not `DiscoveryAgent`. Avoids pulling in domain guessing and keeps the standard pipeline untouched.
- **url_source = 'locality_discovery':** Distinct from 'serper' for tracking success rates separately.
- **County disambiguation:** Short names (<=5 chars) and common names (Troy, Clinton, Avon) get county context in queries to avoid cross-state contamination.
- **Slash-separated compounds:** Takes first part only (NEW CASTLE/STANWOOD → New Castle).
- **LIBERTY false positive fixed:** Private company pattern requires "LIBERTY UTILITIES" not bare "LIBERTY" (which is a NY village name).

## Key Metrics to Watch
- Parse success rate on locality r1 batch (35 tasks)
- How many of the 76 NY gap PWSIDs get new scraped_llm rates
- Bug-fix rescrape: how much new content the fixed scraper recovers from SCWA, WJWW, CivicPlus sites
