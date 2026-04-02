# High-Population Gap Analysis — Sprint 27

**255 PWSIDs >= 10k population without rate data — 10.5M people**

This analysis categorizes every remaining gap PWSID by its specific failure mode and identifies systemic patterns that can be addressed with pipeline improvements.

---

## Executive Summary

The remaining high-pop gap is **not a discovery problem** — 252 of 255 PWSIDs have URLs in scrape_registry, 247 have substantive scraped text. The pipeline found pages for these utilities. The problem is that **the pages it found are wrong**.

Two failure modes dominate: **wrong URLs** (47% of gap population) and **wrong PDFs classified as extraction failures** (35%). Together they account for **82% of the gap population (8.6M people)**. Both have the same root cause: discovery is returning rate-adjacent pages (news articles, neighboring utility pages, stormwater/sewer docs, national survey PDFs) instead of the actual utility's rate schedule.

---

## Failure Mode Breakdown

| Failure Mode | PWSIDs | % | Population | % Pop | Systemic Fix |
|---|---:|---:|---:|---:|---|
| wrong_url_all_ranks | 77 | 30.2% | 4,963,298 | 47.2% | Better discovery scoring + domain blacklist |
| pdf_extraction_failed | 89 | 34.9% | 3,671,570 | 34.9% | Same as above — these are wrong PDFs, not extraction bugs |
| never_parsed | 53 | 20.8% | 813,564 | 7.7% | Just parse them — text exists, never sent to LLM |
| water_sewer_combined | 17 | 6.7% | 435,458 | 4.1% | Prompt tweak: accept water-only from combined pages |
| rates_behind_link | 9 | 3.5% | 320,372 | 3.0% | Nav crawl: follow links to actual rate PDFs |
| scrape_failed | 8 | 3.1% | 249,328 | 2.4% | Manual URL entry or Spanish-language search (PR) |
| auth_paywall | 1 | 0.4% | 57,339 | 0.5% | Manual |
| partial_extraction | 1 | 0.4% | 11,646 | 0.1% | Relax validation threshold |

### Key insight: "pdf_extraction_failed" is misnamed

89 PWSIDs are categorized as PDF extraction failures, but examining the actual URLs reveals these are overwhelmingly **wrong PDFs**, not extraction failures on correct PDFs:
- `www.foodandwaterwatch.org` rate survey — a national advocacy group's comparative report, not a utility rate schedule (appears for Henderson NV 337k, Anchorage AK 221k, and others)
- `dam.assets.ohio.gov` — Ohio EPA/PUCO regulatory filings, not rate schedules (9 PWSIDs)
- `lpsc.louisiana.gov` — Louisiana PSC filings (8 PWSIDs)
- `www.nyc.gov` — NYC rate documents served for non-NYC utilities (6 PWSIDs)
- `fclwd.com` — Fort Collins Loveland WD pages for other CO districts (5 PWSIDs)
- `docs.fcc.gov` — FCC broadband reports (completely unrelated)

**Combined: wrong_url + wrong_pdf = 166 PWSIDs, 8.6M pop, 82% of the gap.**

---

## The Top 20 by Population

| # | PWSID | Name | State | Pop | Failure Mode | What Went Wrong |
|---|-------|------|-------|----:|---|---|
| 1 | NY5110526 | Suffolk County Water Auth | NY | 1,100,000 | wrong_url | Best URL is a newspaper article (southshorepress.com) |
| 2 | CO0103005 | Aurora City Of | CO | 533,407 | wrong_url | Got stormwater authority page (semswa.org), not water utility |
| 3 | NV0000175 | North Las Vegas Utilities | NV | 376,515 | wrong_url | Got Las Vegas entertainment feed (northlasvegas.com/feed/) |
| 4 | NV0000076 | Henderson City Of | NV | 336,534 | wrong_pdf | Best PDF is Food & Water Watch national survey, not utility rates |
| 5 | AZ0407090 | Chandler City Of | AZ | 247,328 | wrong_pdf | Got AZ Water Bank operations PDF, not Chandler water rates |
| 6 | MI0000220 | Ann Arbor | MI | 241,868 | wrong_url | Got Washtenaw County recorder's office fee schedule |
| 7 | AK2210906 | Anchorage (AWWU) | AK | 221,351 | wrong_pdf | Food & Water Watch survey again |
| 8 | NY2704518 | Rochester City | NY | 214,000 | rates_behind_link | Right page, but rates are in linked PDFs not on page |
| 9 | WV3302016 | WV American Water (Kanawha) | WV | 207,319 | wrong_url | Got sewer tariff PDF, not water tariff |
| 10 | CO0135291 | Ft Collins City Of | CO | 179,901 | wrong_url | Got FCLWD misc fees page, not water rate schedule |
| 11 | CO0107152 | Boulder City Of | CO | 166,080 | wrong_url | Got HUD utility allowance schedule (housing, not utility rates) |
| 12 | AZ0411009 | Arizona Water Co - Pinal | AZ | 134,432 | rates_behind_link | azwater.com index page lists tariffs but no actual rates |
| 13 | AZ0407695 | EPCOR - Agua Fria | AZ | 127,718 | wrong_pdf | Got EPCOR tariff table of contents, not actual rate schedule |
| 14 | NY3202411 | MVWA Mohawk Valley | NY | 126,250 | wrong_pdf | Got Oneida County sewer rate schedule |
| 15 | MI0000220 | Ann Arbor | MI | 118,017 | wrong_url | County fee schedule, not city water rates |
| 16 | MT0000153 | Billings City Of | MT | 114,000 | never_parsed | Has text but never sent to LLM |
| 17 | PR0003824 | Ponce Urbano | PR | 111,926 | wrong_pdf | Got FCC broadband report |
| 18 | CA3610019 | San Bernardino Valley WD | CA | 109,608 | wrong_url | Got "how to read your bill" explainer page |
| 19 | CO0107155 | Broomfield City & County | CO | 106,153 | wrong_url | Got neighboring district's page |
| 20 | MD0120016 | Harford County DPW | MD | 104,567 | never_parsed | Has text, never parsed |

---

## Discovery Quality: The Root Cause

### Failure patterns in URL discovery

1. **Cross-contamination**: A URL for Utility A is returned as a result for Utility B because they share a county, state, or service area. Example: Fort Collins Loveland WD pages served for 5 different CO water districts. Serper returns these because the query mentions the county/city and the result mentions water rates.

2. **News articles instead of rate pages**: Discovery returns news articles _about_ rate changes (southshorepress.com, local newspapers) rather than the utility's own rate schedule. The article mentions the utility name and "water rates" — perfect keyword match, wrong content type.

3. **National/state survey PDFs**: `foodandwaterwatch.org` rate survey PDF appears for multiple utilities across states. It contains comparative bill data but not parseable rate structures. Similarly, `dam.assets.ohio.gov` regulatory filings contain utility names but are compliance documents, not rate schedules.

4. **Portal/index pages**: `azwater.com/rates/` is an index page listing 20+ service area tariffs. The LLM correctly identifies it as a navigation page. The actual tariff PDFs are one click deeper.

5. **Wrong utility type**: Stormwater fees (semswa.org for Aurora CO), sewer tariffs (WV American Water), housing utility allowances (Boulder County HUD) — correct geographic area, wrong utility type.

### Domains that should be blacklisted or deprioritized

| Domain | Gap PWSIDs affected | Issue |
|--------|---:|---|
| www.nyc.gov | 7 | NYC docs served for non-NYC utilities |
| fclwd.com | 5 | Fort Collins WD pages cross-contaminating |
| www.elcowater.org | 5 | Regional utility page served for neighbors |
| www.azwater.com | 4 | Index page, not actual rates |
| southshorepress.com | 3 | News articles about rates |
| www.semswa.org | 3 | Stormwater authority, not water supply |
| www.foodandwaterwatch.org | 3+ | National survey PDF, not utility rates |
| dam.assets.ohio.gov | 9 | Ohio regulatory filings |
| lpsc.louisiana.gov | 8 | Louisiana PSC filings |

---

## State Concentration

| State | Total Gap | wrong_url | wrong_pdf | never_parsed | water_sewer | rates_behind_link | scrape_failed |
|-------|----------:|----------:|----------:|-------------:|------------:|------------------:|--------------:|
| NY | 29 | 9 | 10 | 8 | 0 | 2 | 0 |
| CO | 20 | 12 | 6 | 1 | 0 | 1 | 0 |
| MI | 18 | 6 | 5 | 3 | 3 | 1 | 0 |
| PR | 17 | 2 | 8 | 2 | 1 | 0 | 4 |
| AZ | 14 | 4 | 4 | 3 | 0 | 3 | 0 |
| MN | 14 | 6 | 4 | 4 | 0 | 0 | 0 |
| OH | 13 | 1 | 9 | 0 | 3 | 0 | 0 |
| TX | 12 | 2 | 3 | 3 | 0 | 2 | 2 |

**CO** is dominated by wrong_url (12/20) — cross-contamination between CO water districts. **OH** is dominated by wrong_pdf (9/13) — `dam.assets.ohio.gov` regulatory filings. **PR** has 4 scrape_failed — no English-language URLs found for Puerto Rico systems.

---

## Recoverable vs Non-Recoverable

### Immediately recoverable (no engineering changes needed)

| Category | PWSIDs | Pop | Action |
|----------|-------:|----:|--------|
| never_parsed | 53 | 813k | Submit existing text to parser |
| partial_extraction | 1 | 12k | Relax validation threshold |
| **Subtotal** | **54** | **825k** | Batch submit, ~$1 cost |

### Recoverable with targeted fixes

| Category | PWSIDs | Pop | Fix Required |
|----------|-------:|----:|---|
| rates_behind_link | 9 | 320k | Nav crawl: extract PDF links from landing pages, follow them |
| water_sewer_combined | 17 | 435k | Prompt: "extract water portion even if page shows combined bill" |
| **Subtotal** | **26** | **755k** | Prompt + pipeline code changes |

### Requires fresh discovery

| Category | PWSIDs | Pop | Fix Required |
|----------|-------:|----:|---|
| wrong_url + wrong_pdf | 166 | 8,635k | Better search queries, domain blacklist, per-PWSID re-discovery |
| scrape_failed | 8 | 249k | Spanish-language search (PR), manual URL entry |
| auth_paywall | 1 | 57k | Manual |
| **Subtotal** | **175** | **8,941k** | Discovery improvements |

---

## Recommended Engineering Priorities

### Priority 1: Parse the "never_parsed" queue (53 PWSIDs, 813k pop)

These have text in the DB that was never sent to the LLM. Submit as a batch immediately. Expected ~45% success rate based on orphan batch performance. Cost: ~$1.

### Priority 2: Targeted re-discovery for top 20 systems (3.8M pop)

The top 20 by population are well-known utilities with public rate schedules. Suffolk County, Aurora CO, North Las Vegas, Henderson NV, Chandler AZ, Ann Arbor — all have official websites with rate pages. A targeted discovery pass using:
- Utility name + "water rates" + official domain (.gov, .org)
- Exclude known-bad domains (foodandwaterwatch.org, southshorepress.com, semswa.org)

This could be done with 20 manual URL lookups in an hour, or a more targeted Serper query strategy.

### Priority 3: Domain blacklist for discovery (reduces future waste)

Blacklist the 9 domains identified above. These generate hundreds of wasted scrape+parse cycles across multiple PWSIDs. Implementation: add a blacklist to the discovery scoring function that penalizes or excludes known-bad domains.

### Priority 4: Nav crawl for "rates behind link" pages (9 PWSIDs, 320k pop)

Rochester City NY (214k) is the biggest win here. The pipeline found the right page but the actual rates are in linked PDFs. The scraper already has `_extract_rate_links()` — it just needs to trigger on parse failure, not just on thin content.

### Priority 5: Water/sewer separation prompt (17 PWSIDs, 435k pop)

The current prompt says "If you truly cannot separate water from sewer charges, set parse_confidence to low." For these 17 systems, the page has water rates visible alongside sewer — the LLM should extract the water portion rather than refusing. Prompt adjustment: "Extract the water-only portion. If the page shows a combined water+sewer total, look for the line items that specify water service charges separately."

### Priority 6: Anti-cross-contamination in discovery scoring

The CO cross-contamination pattern (12 PWSIDs getting the wrong CO utility's page) suggests discovery scoring should penalize URLs that appear for multiple PWSIDs. If a URL is returned as rank 1 for 5 different PWSIDs, it's likely a regional/state page, not utility-specific. Add a dedup score penalty.

---

## Coverage Impact Projections

If all recoverable categories succeed at historical rates:

| Action | PWSIDs targeted | Expected success | New rates | Pop gained |
|--------|----------------:|-----------------:|----------:|-----------:|
| Parse never_parsed | 54 | ~45% | ~24 | ~370k |
| Top 20 re-discovery | 20 | ~50% | ~10 | ~1.9M |
| Nav crawl | 9 | ~60% | ~5 | ~190k |
| Water/sewer prompt | 17 | ~30% | ~5 | ~130k |
| Domain blacklist + re-discovery | 166 | ~30% | ~50 | ~2.6M |
| **Total** | **266** | | **~94** | **~5.2M** |

This would close roughly half the remaining 10.5M population gap, pushing Lower 48 coverage from 93.6% toward **95.2%**.
