# Sprint 16 Session Summary — 2026-03-25

## What Was Built

### Prerequisite: Sprint 15 IOU URL Validation (Critical Fix)

Ran `ua-run-orchestrator --execute 10 --state VA` to validate IOU URLs.
**Result: 100% failure.** All toolkit IOU URLs were stale:
- American Water migrated from `{state}amwater.com` to `amwater.com/{state}aw/`
- Aqua/Essential uses single URL (`aquawater.com/customers/water-rates`), not per-state
- SJW, Middlesex, Artesian URLs all 404

**Fix:** Manually verified correct URLs for all IOUs via HTTP requests. Updated
iou_mapper.py with corrected URLs. Marked 224 old entries dead, re-ran mapper
with 228 corrected entries.

**Orchestrator fix:** The orchestrator was generating `discover_and_scrape` tasks
for IOU PWSIDs but would have sent them through SearXNG discovery first (wasting
search queries). Fixed to:
1. Include `scrape_status='url_discovered'` in task queue
2. Check scrape_registry for pending URLs before running discovery
3. Skip SearXNG when pending URLs exist

### Deliverable 1: ScrapeAgent Deep Crawl

Added `_is_thin_content()`, `_follow_best_links()`, `_score_link()`, and
`_register_deep_url()` methods to ScrapeAgent. When initial scrape returns a
landing page (<2000 chars or missing rate keywords), the agent:
1. Extracts same-domain links from the HTML
2. Scores links for rate-relevance (keyword heuristic, no LLM)
3. Follows top 3 candidates via `scrape_rate_page()`
4. Inserts new scrape_registry row for deeper URL (preserves original)

### Deliverable 3: Domain Guesser

`DomainGuesser` generates county/name-based domain candidates and DNS-checks
them. No search engine, no LLM, no rate limiting. County-only patterns (no city
in SDWIS). Integrated into DiscoveryAgent as step before SearXNG.

Test results: found live domains for 3/3 VA test utilities:
- Fairfax County: 12 live domains including fairfaxcountyva.gov
- Henrico County: 3 live domains
- Spotsylvania County: 4 live domains

### Deliverable 5: Parse Retry

ParseAgent now retries with a rate-search addendum when first attempt fails
with `no_tier_1_rate` on substantive content (>2000 chars). Same system prompt
(cache hit), modified user message. Negligible additional cost.

### Deliverable 2: IOU Subsidiary Name Database

`config/iou_subsidiaries.yaml` maps local subsidiary names to parent company
URLs. Covers AWK, WTRG, SJW, MSEX, ARTNA, Aquarion, CSWR, Nexus.
`_match_subsidiary()` does normalized name comparison.

Web research was limited by SEC (blocks automated), Wikipedia (403), and
company sites (limited detail). Initial YAML added 3 new matches:
- Avon Water Company (CT) → SJW Group
- Pinelands Water Company (NJ) → Middlesex
- Beckley Water Company (WV) → American Water

## Key Findings

1. **IOU URL templates are fragile.** Corporate sites restructure without
   redirects. Every URL template should be HTTP-verified before batch processing.

2. **The orchestrator had a gap.** PWSIDs with `scrape_status='url_discovered'`
   were excluded from the task queue. Fixed in this session.

3. **Domain guessing works well for county governments** but produces many
   candidates per utility. Need to limit/deduplicate in production sweeps.

4. **Subsidiary name database yield is low initially** (3 new matches) but
   the infrastructure is correct. Expanding the YAML with SEC 10-K research
   would significantly increase yield.

## Commits

- `8541887` Fix orchestrator to handle PWSIDs with existing pending URLs
- `1cb1772` Fix IOU mapper URLs — validation found all American Water + Aqua URLs were 404
- `f20d892` Sprint 16: Deep crawl + parse retry for landing page recovery
- `9a7a982` Sprint 16: Domain guessing agent — bypass search engines for municipal utilities
- `e65d34a` Sprint 16: IOU subsidiary name database + enhanced matcher

## What's Next

1. **National coverage push** (Deliverable 4): Run corrected IOU URLs through
   scrape→parse, domain guess sweep across Priority 1 states, SearXNG gap fill
2. **Expand subsidiary YAML** with SEC 10-K data (manual research)
3. **Add city column** to SDWIS for better domain guessing
4. **Test deep crawl** against the 3 VA utilities that failed in Sprint 14
