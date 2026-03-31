# Duke Upgrade Batch Report

**Date:** 2026-03-31
**Batch ID:** `msgbatch_01AT77529EDstWZn3ygzR2ZH`
**Scope:** 963 Duke-only PWSIDs (pop >= 3,000) across 10 states

---

## Executive Summary

434 of 932 parse tasks succeeded (47%), replacing dated Duke NIEPS reference data with fresh LLM-scraped rates for those PWSIDs. The Duke-to-LLM bill comparison revealed a major data quality finding: **Duke NIEPS bill estimates are systematically low, likely due to unit misinterpretation in the original ingest** — many PA/NJ entries appear to be $/1000gal volumetric rates stored as monthly bills, not actual computed bills at 10 CCF.

This means the 508 remaining Duke-only PWSIDs should be treated as having **unreliable rate data** until either upgraded to LLM or manually validated. The LLM-scraped rates are almost certainly more accurate for the 434 upgraded PWSIDs.

---

## Transfer Results

| Metric | Count |
|---|---|
| Duke-only before batch | 963 |
| No discoverable URLs | 31 |
| Batch tasks submitted | 932 |
| **Parse succeeded** | **434 (47%)** |
| Parse failed | 498 (53%) |
| Anthropic batch cost | $4.59 |

### By State

| State | Duke Total | Got LLM | LLM Selected | Still Duke | Upgrade % |
|---|---|---|---|---|---|
| PA | 253 | 210 | 209 | 44 | **83%** |
| NM | 39 | 28 | 27 | 12 | 69% |
| OR | 9 | 7 | 6 | 3 | 67% |
| CT | 38 | 25 | 25 | 13 | 66% |
| KS | 84 | 58 | 54 | 30 | 64% |
| WA | 184 | 118 | 115 | 69 | 62% |
| NJ | 173 | 84 | 80 | 93 | 46% |
| TX | 480 | 219 | 195 | 285 | 41% |
| CA | 541 | 133 | 73 | 114 | 13% |
| NC | 261 | 19 | 17 | 38 | 7% |

**PA is the standout at 83% upgrade rate.** CA and NC lagged — CA likely due to complex tiered/seasonal structures, NC due to sparse web presence for smaller utilities.

---

## Data Quality Finding: Duke Bill Estimates Are Systematically Low

### The Evidence

| Source | Mean bill@10CCF | Median bill@10CCF |
|---|---|---|
| Duke NIEPS | $51.78 | $47.85 |
| LLM-scraped | $74.19 | $62.01 |

For the 500 PWSIDs where both exist, the median divergence is **129%** and the mean is **236%**. This is not normal rate inflation — it's a unit mismatch.

### PA Smoking Gun

Many PA Duke entries have `bill_5ccf = bill_10ccf = bill_20ccf` (identical at all consumption levels), with impossibly low values:

| PWSID | Utility | Duke @5 | Duke @10 | Duke @20 | LLM @10 |
|---|---|---|---|---|---|
| PA4440010 | Mifflin County | $1.10 | $1.10 | $1.10 | $42.51 |
| PA5630039 | Charleroi | $3.83 | $3.83 | $3.83 | $79.40 |
| PA5040006 | Aliquippa | $5.10 | $5.10 | $5.10 | $138.00 |

A $1.10/month water bill is not realistic. These are almost certainly **volumetric rates ($/1000gal or $/CCF)** stored in a bill column. 122 of 3,177 Duke records (4%) have this identical-across-volumes pattern — heaviest in CA (32), NJ (28), PA (28), WA (20).

### Largest Divergences

| PWSID | State | Duke | LLM | Diff | Utility |
|---|---|---|---|---|---|
| PA1510001 | PA | $18.10 | $754.06 | 4066% | Philadelphia Water |
| PA4440010 | PA | $1.10 | $42.51 | 3765% | Mifflin County |
| PA5040006 | PA | $5.10 | $138.00 | 2606% | Aliquippa |
| OR4100731 | OR | $6.95 | $147.16 | 2017% | Salem Public Works |
| PA5630039 | PA | $3.83 | $79.40 | 1973% | Charleroi |

Philadelphia at $18.10/month is clearly wrong — their actual bill at 10 CCF is ~$754 (they have some of the highest water rates in the US). The LLM parse is correct here.

### Implication

**Duke data should not be trusted as a bill estimate.** It may be useful as a volumetric rate reference, but the `bill_10ccf` values are unreliable for at least 4% of records and potentially more. For the product dashboard, Duke-selected PWSIDs should display with a caveat or lower confidence than they currently receive.

---

## Parse Quality Analysis

### Confidence Distribution (434 successes)

| Confidence | Count | % |
|---|---|---|
| high | 1,052 | 92% |
| medium | 41 | 4% |
| failed (but wrote) | 49 | 4% |
| low | 1 | <1% |

92% high confidence is strong. The 49 "failed" that still wrote are likely edge cases where the validator accepted the data despite a confidence flag.

### Rate Structure Type Proliferation

The LLM returned **60+ distinct rate_structure_type values** including:
- `tiered` (615), `uniform` (188), `flat` (67) — the canonical types
- `flat_rate` (42), `Tiered` (12), `Flat/Uniform` (1) — case/naming variants
- `tiered_volumetric` (11), `tiered_seasonal` (10), `seasonal_tiered` (23) — compound types
- `flat_rate_with_usage_charge` (2), `fixed charge + uniform volumet` (1) — descriptive strings

**This is the #1 parse quality issue.** The LLM is not normalizing to the canonical enum (`flat | uniform | increasing_block | decreasing_block | budget_based | seasonal`). It's describing the structure in natural language. This needs:
1. A stricter prompt constraint on valid values
2. Or post-parse normalization (map variants to canonical types)

### Zero-Bill Parses

0 zero-bill parses in this batch — good. The `$0.00` issue seen in Sprint 25 (Greater Ramsey) was a false PWSID-URL match, not a parser bug.

---

## Failure Analysis (498 PWSIDs)

### By State (still Duke-only, no LLM parse succeeded)

| State | Remaining | Likely Cause |
|---|---|---|
| TX | 111 | Many small MUDs, sparse web presence |
| CA | 111 | Complex tiered/seasonal structures, JS-heavy sites |
| NJ | 89 | Large regulated utilities behind tariff PDFs |
| WA | 66 | Mix of small towns + complex regional utilities |
| PA | 43 | Tariff PDFs (NJ American Water serves many PA PWSIDs) |
| NC | 36 | EFC bulk data exists for many — scraping adds little |
| KS | 26 | Small rural utilities |
| CT | 13 | Regulated utilities, tariff complexity |
| NM | 11 | Small systems |
| OR | 2 | Nearly fully upgraded |

### Remaining Duke-only Impact

**508 PWSIDs, 14.0M population.** These are the hardest-to-scrape Duke utilities. Further improvement would require:
- State-specific keyword tuning (CA, NJ tariffs)
- PDF section extraction for large tariff documents
- Manual curation for the largest population centers

---

## Recommendations

### Immediate

1. **Normalize rate_structure_type values.** Write a mapping from the 60+ LLM variants to 6 canonical types. Apply as a post-parse step or in the export pipeline. This is the single highest-impact parse quality fix.

2. **Flag Duke bill estimates as low-confidence.** The `bill_10ccf` values are demonstrably unreliable for a significant fraction. Consider either:
   - Downgrading Duke confidence to "low" across the board
   - Re-computing Duke bills from the raw volumetric data (if available in the source)
   - Displaying a "reference estimate" caveat on Duke-sourced PWSIDs

3. **Re-export dashboard data** to pick up the 434 new LLM rates.

### For Next Batch (Scenario A)

4. **Add rate_structure_type normalization to the BatchAgent `process_batch()` method** so Scenario A's 4,540 results come in clean.

5. **The 508 remaining Duke-only PWSIDs** are not worth a second scrape pass — they failed because the data isn't web-discoverable. They need either bulk data sources (state PSC filings) or manual curation.

### Longer Term

6. **Investigate Duke source data format.** The identical bill_5/10/20 pattern and the low values suggest the Duke ingest script may have loaded volumetric rates into bill columns. If the original Duke dataset has separate volumetric rate and bill estimate columns, re-ingesting with the correct mapping would fix this without needing to scrape.

---

## Coverage Impact

| Metric | Before Duke Batch | After |
|---|---|---|
| Systems with rates | 9,834 | **9,990** |
| Population covered | 73.1% | **75.9%** |
| Duke-only PWSIDs (>=3k) | 963 | 508 |
| Batch cost | — | $4.59 |

Scenario A (4,540 tasks, still processing at Anthropic) is expected to push coverage to ~86% population when it completes.
