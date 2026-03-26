# Session Summary — Sprint 17b: Multi-Level Deep Crawl + Domain Guesser

**Date:** 2026-03-25
**Coverage:** 862 → 866 PWSIDs (+4)
**API Cost:** ~$0.85 (session total, $0.25 for 7-day total)

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

## Key Insight
The multi-level crawl is mechanically correct — it navigates government site hierarchies successfully. The bottleneck is now **URL quality**, not crawl depth. Domain-guessed URLs point to general city/county sites that may not even serve water. The highest-leverage next step is better URL targeting (water authority domains specifically) rather than deeper crawling.
