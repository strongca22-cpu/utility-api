# Scenario A Batch — Failure Analysis

**Date:** 2026-03-31
**Batch ID:** msgbatch_01FhetQeo9TfoTkBroYFHT1T
**Tasks:** 4,540 | **Succeeded:** 2,070 (45.6%) | **Failed:** 2,470 (54.4%) | **Cost:** $8.52

## Executive Summary

The 54.4% failure rate breaks down into three distinct failure modes with very different root causes:

| Category | Count | % of Total | Root Cause | Pipeline Stage |
|---|---|---|---|---|
| **LLM: no rates in text** | 2,144 | 47.2% | Wrong URL, irrelevant content, or complex rates | URL discovery + scrape quality |
| **Bill consistency → low** | 230 | 5.1% | LLM returned identical bills at 5/10/20 CCF with non-flat type | LLM extraction logic |
| **JSON parse error** | 96 | 2.1% | LLM returned malformed JSON | LLM output formatting |
| **API/DB errors** | 0 | 0.0% | — | — |

**The dominant failure mode (87% of failures) is not a parsing problem — it's a content quality problem.** The LLM correctly identified that the scraped text didn't contain extractable water rates. The pipeline fed it the wrong content.

---

## Failure Mode 1: LLM Said "No Rates Here" (2,144 tasks, 47.2%)

These are tasks where the LLM returned valid JSON but with `parse_confidence: "low"` or `"failed"`, meaning it read the text and determined it didn't contain residential water rate information.

### What was in the text?

| Content Signal | Count | % of Failures |
|---|---|---|
| Contains "monthly" + "charge" | 1,050 | 41% |
| Contains "water rate" | 934 | 37% |
| Sewer-only (no "water rate") | 646 | 25% |
| Meeting/agenda/minutes | 404 | 16% |
| Contains "per 1,000" (volumetric) | 621 | 24% |
| Error/404 pages | 228 | 9% |

**Key insights:**
- **25% sewer-only** — Serper returned sewer rate pages instead of water rate pages. The scoring heuristic doesn't distinguish water from sewer.
- **16% meeting documents** — Council minutes, budget hearings, rate study discussions that mention rates but don't contain the actual rate table.
- **9% dead/error pages** — URLs that returned 404 or error content at scrape time.
- **41% have "monthly charge" text** — many of these are rate-adjacent pages (utility homepages, bill explanations, application forms) that discuss charges without providing the actual rate structure.

### Text length distribution of failures

| Text Length | HTML | PDF | Total | Interpretation |
|---|---|---|---|---|
| <500 chars (thin) | 353 | 2 | 355 | Scrape failed or page is redirect/stub |
| 500–2k chars | 354 | 125 | 479 | Landing pages, contact info, generic utility pages |
| 2k–10k chars | 473 | 426 | 899 | Could contain rates but doesn't, or rates in wrong format |
| 10k–45k chars | 131 | 602 | 733 | Long PDFs — rate studies, budgets, multi-utility compilations |
| >45k (capped) | 9 | 65 | 74 | Massive PDFs truncated at 45k — rates may be buried |

**355 tasks (14% of failures) had <500 chars** — these are scrape failures masquerading as parse failures. The pipeline should catch thin content before submitting to the LLM.

**PDF dominates** in the 10k+ range. Long PDFs (rate studies, comprehensive annual reports, multi-utility compilations) are hard for the LLM because the rate table is buried in context.

### Pipeline implications

1. **Pre-submission content filter:** Reject scraped text <500 chars before LLM submission. Saves cost on guaranteed failures.
2. **Sewer/water disambiguation in scoring:** Add negative weight for URLs with "sewer" in path/title when "water" is absent. Currently the scoring heuristic treats all utility rate URLs equally.
3. **Meeting document filter:** Add negative keywords for "meeting", "agenda", "minutes", "budget hearing" at the scrape/scoring stage (some of these are already in the scoring heuristic but they're still getting through at score >45).
4. **PDF section extraction:** The `extract_service_area_section()` function exists but may not be applied in the batch pipeline path. Long PDFs (>10k) should have section extraction applied before LLM submission.

---

## Failure Mode 2: Bill Consistency Downgrade (230 tasks, 5.1%)

The LLM returned a parse with `rate_structure_type: "increasing_block"` (or similar tiered type) but the computed bills at 5/10/20 CCF were identical. This means the LLM claimed tiers but the tier structure didn't actually produce different bills at different consumption levels.

### What's happening

The bill consistency check (`check_bill_consistency()`) catches cases where:
- LLM says "increasing_block" but returns a single tier (no actual blocks)
- LLM says tiers exist but returns the same rate for all tiers
- Tier limits are misconfigured so all consumption falls in one tier
- The rate is actually flat/uniform but the LLM misclassified the structure type

### Pipeline implications

1. **This is working as intended** — the consistency check is a useful guardrail that caught 230 false positives. Without it, these would be "succeeded" with incorrect rate structures.
2. **Recovery opportunity:** Many of these may actually have valid flat/uniform rates. If the LLM returned a valid `bill_10ccf` but the wrong structure type, the data could be salvaged by:
   - Re-classifying as `flat` or `uniform`
   - Accepting the bill amount at face value
   - This would recover ~230 PWSIDs.
3. **Prompt refinement:** The parse prompt could emphasize that if all consumption volumes produce the same bill, the structure type should be `flat` or `uniform`, not `increasing_block`.

---

## Failure Mode 3: JSON Parse Errors (96 tasks, 2.1%)

The LLM returned text that couldn't be parsed as JSON.

| JSON Error Type | Count | Cause |
|---|---|---|
| Missing comma delimiter | 42 | LLM forgot comma between JSON fields |
| Extra data (multiple blocks) | 32 | LLM returned JSON + extra text after closing brace |
| Unterminated string | 15 | Response truncated mid-field (max_tokens hit) |
| Missing property name | 5 | Structural JSON error |
| Other | 2 | — |

### Pipeline implications

1. **JSON repair:** A simple JSON repair step could recover most of these:
   - "Extra data": take only the first JSON object
   - "Missing comma": regex-based comma insertion
   - "Unterminated string": close the string and complete the JSON
   - Libraries like `json_repair` handle these cases.
2. **These are recoverable.** 96 tasks × ~$0.002/task = $0.19 cost wasted. Adding JSON repair would recover ~70-80% of these for free.

---

## State-Level Success Rates

States with the lowest parse success rates point to structural discovery/content issues:

### Bottom 10 (worst performers, ≥10 attempts)

| State | Attempted | Succeeded | Rate | Likely Issue |
|---|---|---|---|---|
| NC | 65 | 5 | 7.7% | EFC already covers NC well; gap PWSIDs are tiny/rural |
| KS | 64 | 5 | 7.8% | Small rural water districts, limited web presence |
| NJ | 169 | 14 | 8.3% | Multi-utility districts (NJ American Water), complex tariffs |
| ND | 28 | 3 | 10.7% | Small systems, limited web presence |
| DE | 18 | 2 | 11.1% | Small state, few utilities |
| SD | 36 | 4 | 11.1% | Small rural systems |
| AK | 17 | 2 | 11.8% | Remote systems, limited web presence |
| WA | 138 | 17 | 12.3% | Complex multi-district landscape |
| CA | 261 | 36 | 13.8% | Multi-area utilities, PDF tariffs, JS-heavy sites |
| AZ | 57 | 9 | 15.8% | Private utilities (AZ Water Company), PDF tariffs |

### Top 5 (best performers)

| State | Attempted | Succeeded | Rate |
|---|---|---|---|
| IA | 12 | 10 | 83.3% |
| IL | 237 | 184 | 77.6% |
| HI | 14 | 10 | 71.4% |
| MO | 202 | 128 | 63.4% |
| KY | 139 | 78 | 56.1% |

**IL stands out** — 77.6% on 237 attempts suggests IL utility websites are well-structured with clear rate pages. This is a model for what "good" discovery + parse looks like.

---

## Pipeline Improvement Priorities

Ranked by impact (estimated PWSIDs recoverable):

### 1. Pre-submission content filter (~355 PWSIDs)
**Stage:** Between scrape and batch submission
**Fix:** Reject scraped text <500 chars. These are guaranteed failures that waste LLM budget.
**Cost savings:** 355 × $0.002 = $0.71/batch + faster processing.

### 2. Bill consistency recovery (~230 PWSIDs)
**Stage:** Post-parse validation
**Fix:** When bill consistency fires and all bills are identical, reclassify as `flat`/`uniform` and accept the bill amount. Currently these are thrown away entirely.
**Net new rates:** ~200+ PWSIDs with flat rates.

### 3. JSON repair (~70 PWSIDs)
**Stage:** Post-LLM response processing
**Fix:** Apply json_repair or regex-based fixups before JSON.loads(). "Extra data" (trim after first `}`) and "missing comma" are the easiest wins.
**Recovery rate:** ~70-80% of 96 JSON failures.

### 4. Sewer/water disambiguation (~640 PWSIDs affected)
**Stage:** URL scoring (discovery)
**Fix:** Add negative scoring weight when URL/title contains "sewer" without "water". Currently the scoring heuristic doesn't distinguish utility type.
**Impact:** Better URL selection → fewer wasted parse attempts.

### 5. Meeting/agenda document filter (~400 PWSIDs affected)
**Stage:** URL scoring (discovery) or pre-submission filter
**Fix:** Boost negative keyword penalties for meeting documents. Some of these keywords ("meeting", "agenda", "minutes") are already in the scoring heuristic but URLs are still passing the 45 threshold.

### 6. PDF section extraction for batch path
**Stage:** Between scrape and batch submission
**Fix:** Ensure `extract_service_area_section()` is applied to PDFs >5k chars before batch submission. This is already used in the direct pipeline (`process_pwsid`) but may not be in the batch path.
**Impact:** Better parse rates on long multi-utility PDFs.

### 7. Thin-content deep crawl
**Stage:** Between scrape and batch submission
**Fix:** For URLs that returned <500 chars, trigger a reactive deep crawl (already exists in `process_pwsid`) to find child pages with actual rate content.

---

## Data Quality Notes

### Outlier bills
- 22 PWSIDs with bill <$10/mo at 10 CCF — likely commercial/wholesale rates or free-tier parsing
- 20 PWSIDs with bill >$200/mo at 10 CCF — may be correct (very expensive utilities) or sewer included
- 1 PWSID at $754/mo — needs review
- Max in best_estimate: $9,315 — clearly wrong, needs filtering

### Unmapped rate_structure_types
142 unique unmapped types returned by the LLM. Most are verbose descriptions of standard types:
- "ascending_block", "tiered_with_base_charge", "inclining block" → `increasing_block`
- "single_rate_flat", "Flat Volumetric", "uniform (flat rate per unit volume)" → `uniform`/`flat`
- "sewer", "stormwater_fee_not_water_rate" → should be rejected entirely
- "failed", "unknown", "unable_to_determine" → confidence should be `failed`

**Action:** Expand the normalization map in `rate_structure_normalize.py` with these 142 variants.

---

## Summary of Recoverable PWSIDs

| Improvement | Est. Recovery | Effort |
|---|---|---|
| Content filter (<500 chars) | Save cost, no new rates | Low |
| Bill consistency → flat | ~200 PWSIDs | Low |
| JSON repair | ~70 PWSIDs | Low |
| Rate_structure_type normalization | Quality improvement | Low |
| Sewer disambiguation | Better URL selection | Medium |
| Meeting doc filter | Better URL selection | Medium |
| PDF section extraction | Better parse on long PDFs | Medium |
| Deep crawl for thin content | ~200-350 PWSIDs | Medium |

**Quick wins (Low effort): ~270 new PWSIDs + quality improvements**
**Medium effort: Better discovery → fewer wasted parse attempts in future batches**
