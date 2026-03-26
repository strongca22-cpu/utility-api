# Session Summary — Sprint 17b: Multi-Level Deep Crawl + Domain Guesser

**Date:** 2026-03-25
**Coverage:** 851 → 869 PWSIDs (+18 total session)
**API Cost:** ~$1.20 (session total)

## What Happened

### Multi-Level Deep Crawl Implementation
Replaced single-level deep crawl with configurable-depth loop (default 3 levels):
- **Level 1:** Broad navigation scoring — finds water/utility department pages from government homepages
- **Level 2+:** Rate-focused scoring — finds actual rate/fee schedule pages
- Each level follows up to 3 same-domain links, scored by keyword heuristics
- Max 15 HTTP fetches per utility prevents runaway crawling
- Configurable via `config/agent_config.yaml` (`deep_crawl_max_depth: 3`) or `--max-depth` CLI flag

### Validation Results
- **Juneau AK:** SUCCESS — navigated `juneau.org/` → `/engineering-public-works/utilities-division` (L1, score 50) → `/utilities-division/rates-flat` (L2, "current utility rates", score 50). Parsed with high confidence, flat rate $42.76 fixed.
- **Henrico VA:** Navigated correctly to `/utility` (L1, score 90) then to `/utility/water-sewer-connection-fees/` (L2) — but those are connection fees, not consumption rates. Right department, wrong subpage.
- **Loudoun, Fredericksburg VA:** JS-heavy CivicPlus sites — Playwright renders text but strips `<a>` tags, so no links for deep crawl
- **PWCSA VA:** 19 chars returned — broken/empty page
- **Anchorage AK:** Water utility is a separate authority (AWWU) at a different domain entirely
- **Regression:** 3/3 previously-working VA utilities still parse correctly

### Domain Guesser Import
- Downloaded VA (2,351 rows, 345 PWSIDs) and AK (483 rows, 89 PWSIDs) from VPS
- Selected best URL per PWSID (prefer .gov, filter bad redirects)
- Imported 434 URLs to scrape_registry as `pending`
- Parse success rate: **~6% (1/16)** — too low for blind batching

### Why Domain-Guessed URLs Fail
1. **City gov ≠ water utility.** Many cities outsource water to separate authorities (AWWU, PWCSA) at different domains
2. **JS-heavy platforms.** CivicPlus/Granicus sites render content but strip link structure
3. **Rate pages are 2-4 clicks deep.** Even with 3-level crawl, some sites require district selection or have rates on authority subdomains

## Files Changed
- `src/utility_api/agents/scrape.py` — complete rewrite: multi-level crawl, level-aware scoring, raw_html passthrough
- `config/agent_config.yaml` — added `scrape.deep_crawl_max_depth: 3`
- `src/utility_api/cli/orchestrator.py` — added `--max-depth` flag, threaded to ScrapeAgent
- `docs/next_steps.md` — updated with results

## Database Changes
- 434 domain-guessed URLs imported (VA + AK)
- 4 new rate records (1 Juneau + 3 VA batch)
- Deep crawl registry entries for navigated pages

### URL Selection Fix (Sprint 17b addendum)
- Original `pick_best_url()` had a blanket `.gov` preference that overrode the guesser's confidence tiers
- Loudoun Water: guesser found `loudounwater.org` (confidence 80) but selection picked `loudoun.gov` (confidence 40)
- **15 PWSIDs** had water-specific domains discarded for .gov; **172 PWSIDs** had higher-confidence non-.gov options ignored
- Fix: respect confidence tiers, prioritize water-keyword domains, use .gov only as tiebreaker
- Re-imported 431 URLs with corrected logic (28 water-specific, 14 .gov, 300 other for VA)
- **Loudoun Water now parses:** `loudounwater.org` → L1 `/rates-billing` → L2 `/rates-fees-charges-penalties` → $109.02/month, high confidence
- **Frederick Water also parses:** `frederickwater.com` → L1 `/water-wastewater-rates` → high confidence
- Coverage: 866 → 869 (+3 from corrected URLs + batch)

## Key Insight
The multi-level crawl works correctly — Juneau, Loudoun Water, and Frederick Water all navigated 1-2 levels to find rate pages. The initial ~6% success rate was caused by a URL selection bug, not a crawl depth problem. With corrected selection, water-specific domains parse well. The remaining ~300 non-water-specific domain-guessed URLs (generic city/county sites) are a lower-yield population but may still produce results as they process through the pipeline.
