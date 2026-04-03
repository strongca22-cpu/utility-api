# Session Summary — Sprint 29: NV Targeted Investigation
**Date:** 2026-04-03
**Sprint:** 29
**Chat prompt:** `docs/chat_prompts/sprint_29_nv_targeted_investigation_v0.md`

## Objective
Surgical investigation of 5 NV gap PWSIDs representing 729,935 population — the highest pop-per-PWSID gap in the country. NV had 86.5% coverage (32/37 systems >= 3k pop).

## Key Findings

### Root Cause: Akamai Bot Manager, Not Discovery Failure
All 5 PWSIDs had been through multiple discovery and parse rounds (R1-R5, cascade, prompt reparse, bulk replace). The pipeline *did* find the correct URLs for the two big utilities (NLV + Henderson). The failures were all scraping problems:

- **NV0000175 (NLV, 377k) + NV0000076 (Henderson, 337k):** Akamai Bot Manager returns 403 for all automated requests — httpx, Playwright headless with Chrome UA, and WebFetch all blocked. Both sites use Granicus CMS + Akamai CDN.
- **NV0002216 (Douglas County, 6.5k):** CivicPlus JS-rendered site returns 148 chars (Google Translate widget only)
- **NV0005062 (Southern Desert Correctional, 6.2k):** NDOC prison — no public water rates exist
- **NV0000920 (Mountain Falls/GBWC, 4.2k):** NV PUC-regulated private utility, tariff listing at myutility.us is JS-rendered

### Las Vegas Metro Water Structure
- SNWA = regional wholesaler (Lake Mead intake + treatment)
- LVVWD = retail for unincorporated Clark County
- Henderson, NLV, Boulder City = buy wholesale from SNWA, set own retail rates via city council ordinance
- NV PUC does NOT regulate municipal utilities

### Solution: Manual Browser Save
User saved NLV + Henderson rate pages as HTML from a real browser, bypassing Akamai. Clean rate text extracted and registered in scrape_registry as `url_source='curated'`, `url_quality='confirmed_rate_page'`.

## Actions Taken

1. **Identified 5 gap PWSIDs** via `rate_best_estimate` table query
2. **Audited scrape_registry** — 15 entries for NLV, 11 for Henderson, all with failed parse results
3. **Classified each PWSID** — right URL bot-blocked (2), JS-rendered (1), institutional (1), PUC-regulated JS-blocked (1)
4. **Ran locality discovery dry-run** — confirmed it wouldn't help (correct URLs already found)
5. **Attempted automated PDF/page fetch** — httpx, Playwright, WebFetch, SearXNG all returned 403
6. **Searched for alternative sources** — news articles paywalled/truncated, municode JS-rendered, alternate Henderson domain timed out
7. **Received manual browser saves** from user for NLV + Henderson
8. **Extracted structured rate text** from HTML, registered as curated entries in scrape_registry
9. **Submitted parse batch** `msgbatch_0125BrwR1FrYNQGrp8Tj52k3` (2 tasks)

## Rate Data Captured

### NV0000175 — North Las Vegas (3/4" SFR)
- **Water:** 4-tier, daily service charge $0.44
  - Tier 1: first 6,000 gal → $2.48/1,000 gal
  - Tier 2: next 9,000 gal → $3.21/1,000 gal
  - Tier 3: next 9,000 gal → $4.18/1,000 gal
  - Tier 4: over 24,000 gal → $5.40/1,000 gal
- **Sewer:** 2-tier flat rate
  - Tier 1: up to 5,000 gal → $18.96/mo flat (monthly service charge $5.24)
  - Tier 2: over 5,000 gal → +$18.00 additional flat charge

### NV0000076 — Henderson (SFR, multiple meter sizes)
- **Water:** 4-tier, daily service charges by meter size
  - 3/4": $0.647/day ($19.41/mo)
  - 1": $1.329/day ($39.86/mo)
  - 1.5": $2.545/day ($76.35/mo)
  - 2": $3.958/day ($118.73/mo)
  - Tier 1: first 200 gal/day → $1.84/1,000 gal
  - Tier 2: next 333 gal/day → $3.06/1,000 gal
  - Tier 3: next 467 gal/day → $4.32/1,000 gal
  - Tier 4: over 1,000 gal/day → $11.32/1,000 gal
- **Wastewater:** $31.61/mo flat (SFR, 30-day period)
- **SNWA surcharges (2026):**
  - Commodity: $0.67/1,000 gal
  - Infrastructure: $10.65/mo (typical SFR)
  - Drought Protection: $6.32/mo (typical SFR)
  - Reliability: 0.25% of water charges (residential)

## DB Changes
- `utility.scrape_registry` id=17185 (NV0000175): updated with curated text, url_source='curated', url_quality='confirmed_rate_page'
- `utility.scrape_registry` id=17190 (NV0000076): updated with curated text, same treatment
- `utility.batch_jobs`: new batch `msgbatch_0125BrwR1FrYNQGrp8Tj52k3` (2 tasks, state_filter='nv_curated_investigation')

## Outstanding
- Process batch results when complete (~24h)
- Douglas County (6.5k pop): browser save needed for douglascountynv.gov rate page
- GBWC/Mountain Falls (4.2k pop): browser save needed for myutility.us tariff page
- Southern Desert Correctional (6.2k pop): flag as institutional/excluded
- NLV + Henderson full rate PDFs: also Akamai-blocked, need browser save as secondary sources

## Systemic Finding
**Akamai Bot Manager is a systemic blocker** for Granicus-hosted municipal sites. The project's scraping infrastructure (httpx + Playwright headless) cannot bypass Akamai's JavaScript challenge. This likely affects other Granicus customers nationally. Manual browser-save + curated registration is the workaround. Consider maintaining a list of known Akamai-blocked domains for future reference.
