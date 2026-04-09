# Session Summary — Sprint 27 Mega Session (Apr 1-5, 2026)

**Duration:** ~4 days active work
**Scope:** Prompt consolidation, coverage expansion, bug fixes, state-targeted closures

## Coverage Delta

| Metric | Session Start | Session End | Delta |
|--------|--------------|-------------|-------|
| PWSIDs | 13,258 | **18,575** | **+5,317** |
| Lower 48 pop | ~83% | **95.9%** | **+12.9pp** |
| L48 gap (>=3k pop) | ~5,500 PWSIDs | **418 PWSIDs** | |
| Dashboard display | 82.8% | 95.4% (filtered) | |

## What Was Done

### 1. Prompt Consolidation (commit 7e19ef4)
- Unified `build_parse_user_message()` across all 3 code paths
- Added water/sewer separation, ordinance format, PDF table awareness rules to system prompt
- Strengthened retry addendum
- Added `last_parse_raw_response` column for post-batch diagnostics (migration 024)

### 2. Source Hierarchy Change (commit 7a6d163)
- `scraped_llm` promoted to priority 1 (was 3)
- Bulk sources (EFC, Duke, eAR) demoted to fallback/QA
- CA anchor logic: flag-only (no demotion on divergence)

### 3. Four Systemic Scraper Bug Fixes
| Bug | Fix | Commit | Impact |
|-----|-----|--------|--------|
| Playwright `networkidle` timeout | Changed to `load` + 5s wait | 2d1eb42 | Chat widget sites returned empty |
| `<form>` tag stripping | Removed from STRIP_TAGS | 2d1eb42 | ASP.NET sites content destroyed |
| PDF 403 no retry | Browser UA fallback | b287165 | Bot-blocked PDFs failed silently |
| Short-line filter | `>3` + preserve `$` lines | 0decc33 | Dollar amounts stripped from tables |

### 4. Parallel Scraper Infrastructure
- `scripts/bulk_scrape_parallel.py` — modulo-partitioned workers (20 workers on 24-core system)
- `--any-source` flag (excludes domain_guess/domain_guesser)
- Dead URL marking after 2 failed attempts (prevents infinite retry loops)

### 5. Domain Blacklist
6 domains with ~100% parse failure: `ny.gov`, `nyc.gov`, `houstonwaterbills`, `psc.wi.gov`, `louisianawater.com`, `dam.assets.ohio.gov`. In `rate_parser.py:DOMAIN_BLACKLIST`.

### 6. Batch Processing (~25 batches processed)
| Category | Tasks | Succeeded | Cost |
|----------|-------|-----------|------|
| Prompt reparse | 2,807 | 131 | $14 |
| Orphan parse | 2,496 | 1,113 | $19 |
| Discovery R1-R5 | ~13,500 | ~5,600 | ~$85 |
| Tier C (bulk replace) R1-R5 | ~11,000 | ~3,600 | ~$68 |
| NY targeted | ~150 | ~22 | $1 |
| AZ targeted | ~25 | ~6 | $0.50 |
| Gap cascade + retries | ~2,500 | ~550 | ~$15 |
| **Total** | **~32,500** | **~11,000** | **~$250** |

### 7. State-Targeted Closures

**NY (4 manual + batch)**
- SCWA (1.1M pop) — form stripping + Playwright timeout fix
- Rochester (214k) — PDF 403 browser headers fix
- MVWA (126k) — curated bond document URL
- South Huntington (82k) — short-line filter fix
- NY locality discovery: 115 URLs, 48 batch parsed
- NY unparsed Sonnet batch: 64 tasks, 15 succeeded

**AZ (2 manual + locality)**
- Chandler (247k) — re-scrape with all 4 bug fixes
- AZ Water Co Pinal Valley (134k) — curated tariff PDF
- AZ locality discovery: 115 URLs, 25 batch parsed, 6 succeeded
- EPCOR (97k) blocked — ACC tariff PDF has no dollar amounts

### 8. Re-scrape Recovery Infrastructure
- `scripts/rescrape_diagnose.py` — identifies 10,500+ candidate URLs affected by bugs
- `scripts/rescrape_recover.py` — parallel re-scrape with fixed code
- `scripts/rescrape_ny_test.py` — NY pilot (50% recovery rate proven)
- Full re-scrape not yet executed globally

### 9. Tier C Bulk Replacement
- 3,101 PWSIDs with bulk-only rates discovered via parallel Serper (4 workers)
- Ranks 1-5 scraped and batched in cascade order
- TC-R1 alone: 3,245 succeeded (47.1%)

### 10. Analytics & Reporting
- `docs/batch_analytics_report_sprint27.md` — 36-hour batch analysis
- `docs/high_pop_gap_analysis_sprint27.md` — 255 gap PWSIDs >= 10k failure mode categorization
- Lower 48 primary metric added to dashboard (export + sidebar + coverage bar)

## Active Background Processes

**As of session end (Apr 5):**
- Tail sweep: **COMPLETED** (0 workers). Ran ~24 hours, marked ~15,000 dead URLs.
- `parse_sweep` tmux: Active, cycling every 30 min (background parse)
- `dashboard_refresh` tmux: Active, auto-cycling every 10 min
- No batches pending at Anthropic
- No discovery or scrape processes running

## Key Files Created This Session

| File | Purpose |
|------|---------|
| `scripts/bulk_scrape_parallel.py` | Parallel scraper (20 workers, modulo partitioning) |
| `scripts/run_prompt_reparse.py` | Reparse failed text with new prompts |
| `scripts/run_orphan_parse.py` | Parse orphaned never-parsed text |
| `scripts/run_gap_cascade.py` | Sonnet-only parse of untried rank URLs |
| `scripts/run_bulk_replace_discovery.py` | Serial Serper for bulk-only PWSIDs |
| `scripts/run_bulk_replace_discovery_parallel.py` | Parallel version (4 workers) |
| `scripts/rescrape_diagnose.py` | Identify bug-affected rows |
| `scripts/rescrape_recover.py` | Parallel re-scrape with fixed code |
| `scripts/rescrape_ny_test.py` | NY re-scrape pilot |
| `scripts/chain_tc_cascade_final.sh` | Auto-poll + cascade TC-R3→R4→R5→tail |
| `scripts/chain_remaining_fast.sh` | Fast scrape chain (5 min timeout) |
| `scripts/scrape_all_remaining.sh` | Scrape-only chain for remaining ranks |
| `migrations/versions/024_add_parse_raw_response.py` | Raw LLM response column |
| `docs/batch_analytics_report_sprint27.md` | Batch analysis report |
| `docs/high_pop_gap_analysis_sprint27.md` | Gap failure mode analysis |

## Remaining Gap (>=3k pop, L48)

**418 PWSIDs, 5.4M pop. Top states:**

| State | PWSIDs | Pop | Notes |
|-------|--------|-----|-------|
| NY | 46 | 658k | Locality discovery running. Portal contamination pattern. |
| AZ | 17 | 246k | EPCOR (97k) needs ACC decision docs. Remaining are smaller. |
| MI | 39 | 559k | **NEXT TARGET** — chat prompt ready, not started. |
| CO | 17 | 415k | Cross-contamination between CO water districts. Locality discovery needed. |
| OH | 25 | 310k | Aqua Ohio (96k) is PSC-regulated private. |
| CA | 19 | 302k | San Bernardino Valley (110k) has wrong URLs. |
| UT | 11 | 230k | Herriman (60k) is largest. |
| TX | 29 | 224k | Scattered small systems. |

## Known Issues / Blockers

1. **EPCOR AZ (97k pop)**: ACC-filed tariff PDF has rate formulas by decision number, not dollar amounts. Needs ACC decision document or manual rate lookup.
2. **Nav crawl quality**: Can replace good content (1,521ch rates) with worse content (10,730ch fee schedule) because it accepts any content > 500 chars without quality comparison. Documented, not fixed.
3. **JS framework rendering**: Some React/Angular sites render rates via AJAX after page load. Playwright `load` + 5s wait catches most, but some sites need longer waits or specific element waits.
4. **Scanned image PDFs**: PyMuPDF returns 0 chars. Needs OCR. Affects some AZ Water Co tariffs.
5. **Discovery > PWSID matchup issue**: Flagged by user — correct rates may be filed to wrong utility. **Separate bug investigation in progress in another chat.** This session's work should be reviewed after that bug is resolved.

## Continuation Instructions

**MI gap closure is next.** Chat prompt ready at user's request (provided above in conversation). The plan has 8 tasks with 4+ checkpoints (A, A.5, B, B.5, C, D). First action: read-only audit query (Task 1), stop at Checkpoint A.

**Before starting MI:** Wait for the PWSID matchup bug investigation to complete. The discovery→PWSID filing issue could mean some rates from this session were assigned to wrong utilities. The MI closure should use the corrected discovery pipeline.

**Global re-scrape (10,500 URLs):** The 4 bug fixes are live but most existing URLs were scraped with the old buggy code. The NY pilot showed 50% recovery rate. A full re-scrape would recover significant content. Tools are built (`rescrape_diagnose.py`, `rescrape_recover.py`) but not executed at scale. Queue this after MI and the PWSID matchup fix.
