# Duke NIEPS QA Cross-Reference Analysis

**Date:** 2026-04-02
**Scope:** Compare `duke_nieps_10state` (academic reference) against `scraped_llm` (primary) in `utility.rate_schedules`
**Purpose:** Use Duke as a QA benchmark to find errors in scraped data, and document Duke issues that affect the comparison.

---

## Executive Summary

- **1,585 PWSIDs overlap** between Duke and scraped_llm; 1,525 have valid bill_10ccf from both sources.
- **Median scraped bill is 36.5% higher than Duke.** This is expected — Duke data is 2019–2021 vintage while scraped data is mostly 2024+. A 4-year gap at ~5–8% annual rate increases explains ~20–35% of this.
- **Only 15.7% of pairs agree within 10%.** 49.8% disagree by >50%.
- **The disagreement is overwhelmingly directional:** 87% of >50% disagreements have scraped higher than Duke, consistent with real rate increases over the vintage gap.
- **108 PWSIDs have 5x+ bill differences** — these are probable scraped errors (unit conversions, wrong page parsed).
- **60 PWSIDs** have scraped bill_10ccf = NULL where Duke has a reasonable value — extraction failures.
- **Duke has zero NULL/negative bills**, but 76 records are classified as "increasing_block" yet produce identical bills at 5/10/20 CCF (tier boundaries set above 20 CCF).
- **64.3% of Duke records** have vintage before 2021. The dataset is aging and increasingly unreliable as a bill-level benchmark.

---

## 1. Duke Ingest Audit

### Pipeline: Source → rate_schedules

| Step | Detail |
|------|--------|
| **Source files** | `data/duke_raw/data/rates_data/rates_{state}.xlsx` — 10 states |
| **Sheets** | `ratesMetadata` (one row per PWSID+service) + `rateTable` (one row per rate component) |
| **Filters** | `rate_code='water'` only; PWSID must exist in `cws_boundaries` |
| **Fixed charge** | `rate_type='service_charge'`, `meter_size=0.625` (5/8" residential), normalized to monthly |
| **Volumetric tiers** | `commodity_charge_*` + `volumetric='yes'`; cubic feet converted to gallons |
| **Bill calc** | `fixed_monthly + tiered_volumetric` at 5/10/20 × 748.052 gal/CCF |
| **Write** | DELETE existing state records, then INSERT (idempotent) |

### Bills are monthly USD — directly comparable to scraped_llm bill_10ccf.

### Documentation Issues Found (not code bugs)

1. **`source_priority.yaml` line 58** says "Aggregate bills only, no rate structure detail" — **incorrect**. Duke data has full tier structure and the ingest extracts it correctly into `volumetric_tiers` JSONB.
2. **`duke_ingest_spec.yaml`** uses `source_key: "duke_affordability_2021"` but the actual ingest uses `"duke_nieps_10state"`. Docs mismatch only.
3. Docstring says "UPSERT" but code does DELETE+INSERT. Functionally equivalent but misleading.

### Data Vintage

- **2019–2021 effective dates** (most are 2020). Median vintage gap vs scraped: **4 years**.
- Data vintage is per-utility (not uniform) — ranges from 1964 to 2021-11-12.

---

## 2. Head-to-Head Comparison (n=1,525)

### Overall Bill Difference at 10 CCF

| Metric | Value |
|--------|-------|
| Median % diff (scraped − duke) | **+36.5%** |
| Mean % diff | +116.1% |
| P25 | +0.0% |
| P75 | +124.2% |
| Std Dev | 308.7% |

### Agreement Buckets

| Bucket | Count | % |
|--------|-------|---|
| <10% | 240 | 15.7% |
| 10–25% | 210 | 13.8% |
| 25–50% | 316 | 20.7% |
| 50–100% | 322 | 21.1% |
| >100% | 437 | 28.7% |

### Direction of >50% Disagreements (n=759)

- **Scraped HIGHER: 662 (87%)**
- Scraped LOWER: 97 (13%)

This asymmetry is consistent with real rate increases over the 4-year vintage gap, not random scraping errors.

### State-by-State Agreement

| State | N | Median %Diff | <10% | 10–25% | 25–50% | >50% | Avg Duke | Avg Scraped |
|-------|---|-------------|------|--------|--------|------|----------|-------------|
| CA | 182 | +14.9% | 38 | 42 | 55 | 47 | $57.10 | $71.00 |
| CT | 56 | +42.0% | 14 | 12 | 7 | 23 | $52.49 | $98.72 |
| KS | 108 | +31.4% | 17 | 14 | 25 | 52 | $53.21 | $73.95 |
| NC | 38 | +50.3% | 6 | 4 | 5 | 23 | $49.18 | $78.88 |
| NJ | 216 | +87.0% | 30 | 8 | 43 | 135 | $37.71 | $85.71 |
| NM | 39 | +15.3% | 7 | 8 | 8 | 16 | $45.52 | $62.99 |
| OR | 14 | +336.2% | 2 | 0 | 1 | 11 | $23.30 | $67.13 |
| PA | 325 | +24.3% | 45 | 36 | 51 | 193 | $59.20 | $87.25 |
| TX | 372 | +50.9% | 51 | 45 | 74 | 202 | $46.69 | $77.35 |
| WA | 175 | +23.7% | 30 | 41 | 47 | 57 | $47.79 | $65.17 |

**Worst states:** OR (+336%), NJ (+87%), TX (+51%), NC (+50%). CA and NM have the best agreement.

**NJ and OR are suspicious.** The median +87% for NJ and +336% for OR likely reflect a mix of real rate increases and scraping errors in those states.

### By Rate Structure Type

| Structure | N | Median %Diff | <10% | >50% |
|-----------|---|-------------|------|------|
| tiered | 669 | +33.4% | 116 | 309 |
| uniform | 399 | +67.2% | 42 | 250 |
| flat | 78 | +15.6% | 18 | 34 |
| increasing_block | 140 | +27.7% | 22 | 66 |
| seasonal | 54 | +18.2% | 7 | 17 |

**Uniform structures have the worst agreement** (median +67.2%, 63% with >50% diff). This is partly because many scraped "uniform" records may actually be tiered utilities where the LLM extracted only one tier.

### Vintage Gap Effect

| Gap (years) | N | Median Abs %Diff |
|-------------|---|-----------------|
| 0 | 103 | 23.7% |
| +1 | 48 | 22.0% |
| +3 | 176 | 65.5% |
| +5 | 228 | 58.0% |
| +7 | 77 | 80.0% |
| +10 | 20 | 73.7% |

Same-year vintage pairs still disagree by 24% median — meaning **vintage gap explains some but not all of the divergence**. The residual ~24% gap at year=0 suggests methodology differences (Duke's academic approach vs LLM parsing) and possibly different rate classes being captured.

---

## 3. Scraped Data Errors Flagged

### Category 1: Bill 5x+ Different (probable unit/page errors)

**Count: 108 PWSIDs** (all 108 have scraped higher than Duke)

These are almost certainly scraped errors — a 5x difference cannot be explained by rate increases alone.

Top examples:

| PWSID | Scraped | Duke | %Diff | Likely Issue |
|-------|---------|------|-------|-------------|
| NJ1213002 | $909.10 | $15.28 | +5850% | Middlesex Water tariff — likely parsed sewer+water combined or commercial rate |
| PA1510001 | $693.18 | $18.10 | +3730% | Philadelphia Water — may have parsed combined water+sewer+stormwater |
| PA5040006 | $138.00 | $5.10 | +2606% | Source URL missing — suspicious |
| OR4100731 | $147.16 | $6.95 | +2017% | URL is an Illinois tariff — **wrong utility entirely** |
| CT0830011 | $685.49 | $36.67 | +1769% | Middlesex Water CT tariff — same Middlesex issue |

**Common patterns in the 5x+ errors:**
- Multiple PA records point to American Water tariffs — LLM may be parsing commercial/industrial rates
- Middlesex Water records appear repeatedly — possible combined water+sewer parsing
- OR4100731 scraped an Illinois tariff for an Oregon utility — **wrong page**

### Category 2: Scraped NULL/Zero Where Duke Has Value

**Count: 60 PWSIDs** — extraction failures

Duke bills for these range from $9.89 to $124.44. These are PWSIDs where the LLM scraper found a URL but failed to extract a bill amount.

### Category 3: Extreme Outliers (Scraped >$500, Duke <$100)

**Count: 6 PWSIDs**

All involve Middlesex Water or Philadelphia Water tariffs. These are a subset of Category 1.

### Category 4: Possible Rate Structure Misclassification

**477 scraped records classified as flat/uniform** overlap with Duke. Of these, **319 have Duke bills varying >2.5x between 5 and 20 CCF**, suggesting the utility actually has tiered rates.

However, this is nuanced:
- The scraped "uniform" records DO show varying bills at 5/10/20 CCF (a single volumetric rate produces different bills at different volumes — that's correct)
- Duke may have captured tiers that were later simplified by the utility
- Or the LLM may have missed tier breakpoints in the rate schedule

**Recommendation:** Spot-check 20–30 of these to determine whether the LLM is genuinely missing tiers or if rate structures changed. Prioritize NJ and TX where the pattern is most frequent.

### Summary of Scraped Error Flags

| Category | Count | Severity |
|----------|-------|----------|
| 5x+ difference (unit/page error) | 108 | **High** — re-parse |
| NULL extraction failure | 60 | **Medium** — re-crawl/re-parse |
| Extreme outlier (>$500 vs <$100) | 6 | **High** — subset of Cat 1 |
| Possible tier under-extraction | 319 | **Low** — needs manual review |

---

## 4. Duke Data Issues

### Issue 1: NULL/Zero/Negative Bills

**Count: 0.** Duke data has no missing or invalid bill_10ccf values. Clean.

### Issue 2: Non-Flat Structures with Identical Bills

**76 Duke records** are classified as "increasing_block" but produce identical bill amounts at 5/10/20 CCF.

This happens when tier breakpoints are set above 20 CCF (e.g., first tier covers 0–20,000 gallons, so all three benchmarks fall in tier 1). Not technically wrong, but the structure classification is misleading.

### Issue 3: Systematic State Bias

| State | N | Median %Diff | Direction | Systematic? |
|-------|---|-------------|-----------|-------------|
| KS | 108 | +31.4% | scraped higher | YES |
| NJ | 216 | +87.0% | scraped higher | YES |
| OR | 14 | +336.2% | scraped higher | YES |
| TX | 372 | +50.9% | scraped higher | YES |
| WA | 175 | +23.7% | scraped higher | YES |

All "systematic" biases go the same direction (scraped higher). Given the 4-year vintage gap, this is more likely **real rate inflation** than systematic Duke understatement. NJ and OR warrant closer examination due to extreme magnitude.

### Issue 4: Stale Vintage

**64.3% of Duke records (2,040)** have vintage before 2021. Distribution:
- Pre-2015: 138 records (4.3%)
- 2015–2018: 545 records (17.2%)
- 2019: 488 records (15.4%)
- 2020: 795 records (25.1%)
- 2021: 1,131 records (35.7%)

**138 records with vintage before 2015 are effectively useless as benchmarks** — rates from 10+ years ago have no bearing on current accuracy.

---

## 5. Recommendations

### Scraped Records to Re-Parse (Priority Order)

1. **108 PWSIDs with 5x+ difference** — High priority. Focus on:
   - American Water (PA) tariffs: check if commercial vs residential rate was extracted
   - Middlesex Water (NJ, CT): check if combined water+sewer was parsed
   - OR4100731: wrong utility URL entirely — needs re-crawl
2. **60 PWSIDs with NULL scraped bill** — Medium priority. Re-crawl + re-parse.
3. **6 extreme outliers (>$500 vs <$100)** — Already covered by #1.
4. **Spot-check 20–30 of the 319 "uniform" scraped / "tiered" Duke** — Low priority. Determine if LLM is systematically missing tiers.

### Duke Records to Exclude or Downgrade

1. **138 records with vintage < 2015** — Consider excluding from any cross-reference comparison. Too stale to validate current rates.
2. **76 records with identical bills across volumes despite "increasing_block" classification** — Not wrong per se, but should not be used to flag scraped structure misclassification.
3. **14 OR records** — Too few for meaningful state-level comparison. Exclude OR from state-level QA conclusions.

### Source Priority Adjustments

- **No changes recommended.** Duke remains correctly at priority 8. The vintage gap and methodological differences mean Duke should stay as a last-resort fallback, not a QA gate.
- **Fix the documentation:** Update `source_priority.yaml` line 58 to note Duke has full rate structure, not just aggregate bills.

### Structural Observations

- The 24% median disagreement even at same-year vintage suggests **fundamental methodology differences** between Duke (manually digitized by researchers) and scraped_llm (LLM-parsed from current rate pages). Neither is necessarily "right" — they measure slightly different things.
- Duke's value is highest for the **~1,125 PWSIDs where it's the sole source** (no scraped data). For overlap PWSIDs, it serves as a sanity check but not a ground truth.

---

## Appendix: Analysis Methodology

- Analysis script: `scripts/duke_qa_analysis.py`
- Raw output: `data/interim/duke_qa_analysis_output.txt`
- Database: `utility.rate_schedules` filtered to `source_key IN ('duke_nieps_10state', 'scraped_llm')`
- % difference = `(scraped_bill_10ccf - duke_bill_10ccf) / duke_bill_10ccf × 100`
- "Systematic" bias defined as: median abs diff > 20% AND >70% of pairs in the same direction
