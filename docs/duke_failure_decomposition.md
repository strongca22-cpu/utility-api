# Duke Batch Failure Decomposition

**Date:** 2026-03-31
**Scope:** 1,259 Duke PWSIDs (>= 3k pop, Duke as only non-LLM source). 735 succeeded (58%), 524 failed.

---

## 1. Failure by Pipeline Stage

| Stage | Count | % | Description |
|---|---|---|---|
| **6. SUCCESS** | 735 | 58.4% | LLM rate extracted, bill computed, written |
| **5. Bill computation failed** | 35 | 2.8% | Tiers extracted but bill calculation returned $0 or implausible |
| **4. Parse failed (LLM)** | 459 | 36.5% | Content reached LLM but no valid rate structure extracted |
| **3. Content thin** | 3 | 0.2% | Page fetched but < 200 chars usable text |
| **2. Content retrieval failed** | 12 | 1.0% | URL found but fetch returned empty/error |
| **1. No URL discovered** | 15 | 1.2% | Serper found no relevant URLs |

**Key finding: 87.5% of failures are Stage 4 (parse failed).** The pipeline successfully discovers and retrieves content for almost everything — the LLM just can't extract rates from what it receives. Discovery and retrieval are not the bottleneck.

---

## 2. Stage 4 Parse Failures — Content Analysis

### Content Source Type (459 parse failures)

| Source | Count | % |
|---|---|---|
| **PDF** | 368 | 80% |
| HTML | 91 | 20% |

**80% of parse failures involve PDF content.** This is the single most important infrastructure finding.

### Content Length Distribution

| Length | Count | % | Notes |
|---|---|---|---|
| >15k chars | 242 | 53% | Large PDFs — rate section likely buried |
| 5k-15k | 101 | 22% | Medium — may have extractable content |
| 2k-5k | 68 | 15% | Manageable size, should be parseable |
| 500-2k | 26 | 6% | Short pages — often navigation/landing |
| <500 | 22 | 5% | Essentially empty scrapes |

**53% of parse failures have >15k chars of content.** The 15k truncation limit means the LLM sees the first 15k characters — for a 45k-char tariff PDF, the rate section may be on page 40.

### Sample Categorization (30 highest-population failures)

| Category | Count | % | Description |
|---|---|---|---|
| **Rate visible, parse failed** | 13 | 43% | Rate data present in content, LLM couldn't extract |
| **PDF buried** | 10 | 33% | Large PDF, rates in a section the LLM never saw |
| **Access denied** | 3 | 10% | 403/paywall blocked the scraper |
| **No rate content** | 2 | 7% | Correct utility but wrong page (fees, not rates) |
| **JS shell** | 2 | 7% | JavaScript app, content not rendered |
| **False URL match** | 0 | 0% | Not observed in top 30 |

**43% have parseable rate content the LLM failed on.** This is a prompt/model issue, not infrastructure. The remaining 33% are PDF section extraction failures — an infrastructure problem.

---

## 3. Stage by State

### Stage 4: Parse Failed (459 total)

| State | Count | PDF | HTML | Key Pattern |
|---|---|---|---|---|
| CA | 111 | 97 (87%) | 14 | Large PDF tariffs, complex tiered/seasonal |
| TX | 95 | 68 (72%) | 27 | MUDs, small towns, mixed |
| NJ | 88 | 88 (100%) | 0 | **100% PDF** — NJ American Water tariff covers most |
| WA | 65 | 32 (49%) | 33 | Mixed HTML/PDF, some JS-heavy |
| PA | 40 | 40 (100%) | 0 | **100% PDF** — Pennsylvania PUC tariff format |
| NC | 33 | 20 (61%) | 13 | Smaller utilities, sparse pages |
| KS | 19 | 15 (79%) | 4 | Rural utilities |

**NJ and PA are 100% PDF failures.** Both states use regulated tariff documents (NJ American Water's tariff is a single ~130-page PDF covering dozens of service areas). This is a structural problem that won't be fixed by prompt improvements.

### Other Failure Stages

| Stage | States |
|---|---|
| No URL discovered (15) | TX: 10, KS: 4, NC: 1 — mostly TX Fort Bend County MUDs |
| Content retrieval failed (12) | KS: 3, PA: 2, NC: 2, TX: 2, WA: 1, NM: 1, OR: 1 |
| Bill computation failed (35) | TX: 18, KS: 4, NJ: 4, WA: 3, NC: 2, CA: 2, OR: 1, PA: 1 |

---

## 4. The Tariff PDF Problem

| State | PDF Parse Failures | % of State Failures | Notes |
|---|---|---|---|
| **NJ** | 88 | 100% | NJ American Water single tariff covers 88+ PWSIDs |
| **PA** | 40 | 100% | PA PUC tariffs, multi-service-area documents |
| **CA** | 97 | 87% | Large utility tariffs, budget-based structures |
| **TX** | 68 | 72% | Mixed — some MUD tariffs, some city PDFs |
| **WA** | 32 | 49% | Regional utility tariffs |

**Total: 368 PDF-based parse failures.** Of the 459 total parse failures, **80% are PDF-related.**

The NJ American Water tariff alone accounts for ~88 PWSIDs (the same 130-page PDF is the top Serper result for all of them). This is the clearest case for PDF section extraction: one infrastructure fix would recover ~88 rates.

### Effort Estimate for PDF Section Extraction

If implemented:
- NJ American Water tariff: ~88 PWSIDs recoverable from a single document
- PA PUC tariffs: ~40 PWSIDs, but each utility has a separate tariff section
- CA tariffs: ~50-70 PWSIDs recoverable (some are genuinely complex, not just buried)
- **Total estimated recovery: 150-200 PWSIDs** from a targeted PDF section extraction feature

---

## 5. CA Content Examination (15 samples)

All 15 sampled CA failures had rate-related content — the LLM received pages about water rates but couldn't extract them.

| Signal | Found in |
|---|---|
| Rate keywords | 15/15 |
| Dollar amounts | 8/15 |
| Tier structure | 2/15 |
| Seasonal signals | 2/15 |

**Key patterns:**
- Many CA URLs point to the wrong service area within the same utility (e.g., Irvine Ranch → Newport Beach rates page)
- LA County Waterworks Districts: PDF tariff with rate schedules in Part 2, but Part 3 (construction fees) was fetched instead
- CPUC-regulated utilities (Liberty, Cal Water): tariff PDFs with rates buried after legal boilerplate

**Two-pass approach assessment:** For the 8/15 with visible dollar amounts, a two-pass (extract tiers → compute bill deterministically) would likely recover 4-6. The other 7 have content problems (wrong service area, wrong PDF section, access denied) that a better prompt won't fix.

---

## 6. Top 20 Failed PWSIDs by Population

| PWSID | St | Pop | Stage | Utility | Issue |
|---|---|---|---|---|---|
| TX2200012 | TX | 955,900 | Parse failed | Fort Worth | 15k-char PDF, rates buried |
| NJ0238001 | NJ | 792,713 | Parse failed | Veolia/Hackensack | NJ American Water tariff |
| CA3010092 | CA | 444,800 | Parse failed | Irvine Ranch | Wrong service area URL |
| KS2017308 | KS | 395,699 | Parse failed | Wichita | HTML rate page, parse should work |
| NJ0906001 | NJ | 262,000 | Parse failed | Jersey City | NJ American Water tariff |
| TX2400001 | TX | 260,046 | Parse failed | Laredo | Webb County PDF |
| TX0570010 | TX | 248,822 | Parse failed | Garland | City PDF, short content |
| TX0430005 | TX | 231,910 | Bill failed | Frisco | Tiers extracted, bill $0 |
| NJ0712001 | NJ | 217,230 | Parse failed | NJ American Short Hills | NJ American Water tariff |
| NJ1111001 | NJ | 217,000 | Parse failed | Trenton | NJ American Water tariff |
| CA1910070 | CA | 204,673 | Parse failed | LA CWWD 40 | Wrong PDF section |
| TX1880001 | TX | 201,291 | Bill failed | Amarillo | Southwest Water tariff |
| CA3410029 | CA | 184,896 | Parse failed | SCWA Laguna | County fee schedule, not rate |
| TX0310001 | TX | 176,362 | Parse failed | Brownsville | Aqua Water tariff PDF |
| TX1650001 | TX | 157,000 | Parse failed | Midland | Short fee schedule page |
| NC0229025 | NC | 153,632 | Bill failed | Davidson Water | PDF tariff |
| CA5410016 | CA | 148,496 | Parse failed | Cal Water Visalia | Access denied (403) |
| TX0610004 | TX | 140,880 | Parse failed | Lewisville | eLaws PDF |
| PA1230004 | PA | 140,437 | Parse failed | Chester Water | Short PDF |
| NJ2004001 | NJ | 134,000 | Parse failed | NJ American Liberty | Wrong state URL (KY PSC) |

**Combined population of top 20: 4.8M.** Manual curation of these 20 PWSIDs would add significant population coverage. Estimated effort: 2-4 hours for a human to look up rate schedules and enter them.

---

## 7. False URL Match Check

Of 448 parse/bill failures checked: **1 potential false match detected** (TX0130001 → content mentions "Washington" but is a Beeville TX fiscal document, not a real false match).

**False URL matching is not a significant failure mode** in this batch. The Sprint 25 Greater Ramsey issue was an anomaly. The discovery scoring effectively filters false matches for >3k pop utilities.

---

## 8. No-Discovery PWSIDs (15)

All 15 are either Texas Fort Bend County MUDs (10) or Kansas rural water districts (4) + 1 NC association. These are small special-purpose districts that genuinely have no web presence.

**CCR availability:** Consumer Confidence Reports are federally mandated for all CWS. These 15 PWSIDs should have CCRs at the EPA's SDWIS database or state drinking water program. CCRs sometimes include rate tables. A CCR-specific scraping module could recover some, but the effort/reward ratio is low for 15 PWSIDs.

---

## 9. Recommendations Prioritized by Impact

### High Impact (recovers 150-200 rates)

1. **PDF section extraction for NJ/PA tariffs.** NJ American Water's single tariff covers 88+ PWSIDs. A targeted extractor that finds the "Schedule of Rates" or "Tariff Rider" section and extracts just those pages would recover the most rates for the least effort. Same pattern applies to PA PUC tariffs.

### Medium Impact (recovers 50-80 rates)

2. **Prompt improvement for "rate visible, parse failed" cases.** 43% of sampled failures had rate content the LLM should have parsed. A two-pass approach (extract structure → compute bill) and/or providing the LLM with the full 45k chars instead of 15k truncation would help.

3. **Manual curation of top 20 by population.** 4.8M combined population. 2-4 hours of effort for a human to look up and enter 20 rate schedules.

### Low Impact (recovers 10-30 rates)

4. **Improve CA service area matching.** Several CA failures are correct utility but wrong service area URL. Discovery needs to match the specific PWSID's service area, not just the utility name.

5. **JS rendering improvements for TX.** 7% of samples were JS shell pages. The Playwright escalation exists but didn't resolve them.

6. **CCR scraping module for the 15 no-discovery PWSIDs.** Low priority — tiny population impact.
